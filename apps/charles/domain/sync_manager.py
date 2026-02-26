"""
Module 4: Processor Sync Manager
Orchestrates syncing data from payment processors into the company database.

This module:
1. Fetches payouts from processors (Stripe, Authorize.net)
2. Queries QBO for unlinked Payments
3. Matches processor charges to QBO Payments (by amount)
4. Inserts matched data into the company database
5. Data is then ready for Module 3 to create QBO deposits

The key insight is that this module produces the SAME data structure
as create_test_data.py - so Module 3 works identically whether data
comes from manual entry or from real processor APIs.

================================================================================
PAYOUT STATE MODEL (Authoritative Definition)
================================================================================

payout_batches.status is the SINGLE SOURCE OF TRUTH for payout workflow state.

ALLOWED STATES:
    'pending'   - Payout record created, processing in progress
    'complete'  - Sync finished, ready for QBO deposit creation  
    'posted'    - QBO Deposit created successfully
    'failed'    - Sync or posting failed (RETRYABLE)

STATE TRANSITIONS:
    pending  →  complete   (sync succeeded)
    pending  →  failed     (sync failed)
    complete →  posted     (deposit created)
    complete →  failed     (deposit creation failed)
    failed   →  pending    (retry initiated)

IDEMPOTENCY RULE:
    A payout is "already synced" if and only if:
        SELECT 1 FROM payout_batches
        WHERE processor_payout_id = ?
        AND status IN ('pending', 'complete', 'posted')

    A 'failed' payout is NOT considered synced — it can be retried.

RAW EVENT IDEMPOTENCY (Option B Semantics):
    - raw_events has UNIQUE(processor, processor_event_id)
    - Duplicate webhook delivery FAILS with IntegrityError (not a graceful skip)
    - Callers must handle duplicate delivery errors appropriately
    - Canonical idempotency (graceful skip) applies only at canonical_facts layer
    - The system does NOT provide end-to-end silent duplicate handling

STRIPE STATUS vs CHARLES STATUS:
    - Stripe statuses ('paid', 'in_transit', 'pending', etc.) are EXTERNAL FACTS
    - They go in payout.metadata, NOT in payout_batches.status
    - Charles workflow states are INTERNAL and independent of processor states
================================================================================
"""

import uuid
import json
import hashlib
import logging
import requests
from dataclasses import dataclass
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import List, Optional, Dict, Tuple, Any
import sqlite3

from .reconciliation_engine import ProcessorBase, ProcessorPayout, ProcessorPayment, ProcessorRefund
from .canonical_events import (
    Canonicalizer,
    NORMALIZATION_VERSION,
)
from .canonical_facts import generate_deterministic_canonical_id
from shared.structured_logging import EventType
from shared.idempotency import (
    IdempotencyManager,
    IdempotencyStatus,
    IdempotencyResult,
    generate_idempotency_key,
    generate_idempotency_key_v2,
    _generate_legacy_key,
    get_idempotency_manager
)


logger = logging.getLogger(__name__)


@dataclass
class MatchScoreBreakdown:
    """
    Score breakdown for multi-factor payment matching.

    Scoring Algorithm (0-100 scale, capped):
    - Amount Match: 40 points (required gate - exact match only)
    - Date Proximity: 0-25 points (asymmetric for bookkeeping lag)
    - Customer Email: 20 points (exact match) - IDENTITY SIGNAL
    - Uniqueness Bonus: +10 points (only when identity_present, capped)

    IDENTITY SIGNALS (any one enables identity_present):
    - Email match (20 points)
    - Prior match continuity (confirmed pair from previous match)

    SAFETY RULE: Auto-match (HIGH tier) requires identity_present == True.
    This prevents coincidental signals (date + uniqueness) from overriding identity.
    Without identity evidence, even a high score results in manual review.

    Confidence Tiers (score-based, NOT percentages):
    - HIGH (score ≥80 AND identity_present): Auto-match
    - MEDIUM (score 50-79, OR ≥80 without identity): Manual review
    - LOW (score <50): Manual review with warning

    WHY 80 IS THE HIGH THRESHOLD:
    - 80 = amount(40) + date(25) + email(20) - minimum for identity + temporal match
    - Lowered from 85 to allow matches without uniqueness bonus when identity is present
    - Identity gate prevents false confidence regardless of numeric score

    PRIOR MATCH CONTINUITY:
    - prior_pair_match enables identity_present without email
    - It does NOT add to the score - only contributes to identity gating
    - Pair-level: (processor_customer, QBO_customer) pairs, not global
    - Only learned from Charles-confirmed decisions (auto/manual matches)
    """
    amount_points: int = 0
    date_points: int = 0
    email_points: int = 0
    uniqueness_bonus: int = 0
    prior_pair_match: bool = False  # Prior confirmed (proc_customer, qbo_customer) pair
    is_deterministic: bool = False  # True when matched via Transaction ID → REF NO

    # Confidence tier thresholds (score-based, not percentages)
    HIGH_THRESHOLD = 80   # Requires identity_present for auto-match
    MEDIUM_THRESHOLD = 50

    @property
    def identity_present(self) -> bool:
        """
        Identity is present when email matches OR prior pair exists.

        Identity verification now includes two signals:
        1. Email match (processor email == QBO customer email)
        2. Prior match continuity (confirmed pair from previous Charles match)

        This is pair-level - a processor customer can legitimately pay multiple
        QBO customers. Each (processor_customer, QBO_customer) pair is tracked
        independently.

        Coincidental signals (date, uniqueness) cannot substitute for identity.
        Auto-match is gated on this flag regardless of total score.
        """
        return self.email_points > 0 or self.prior_pair_match

    @property
    def total(self) -> int:
        """Return total score (0-100, capped). This is a numeric score, not a percentage."""
        raw = self.amount_points + self.date_points + self.email_points + self.uniqueness_bonus
        return min(raw, 100)

    @property
    def confidence_level(self) -> str:
        """
        Return confidence tier based on total score AND identity presence.

        Tiers are derived from numeric score thresholds (not percentages):
        - HIGH: score >= 80 AND identity_present (auto-match eligible)
        - MEDIUM: score 50-79, or >= 80 without identity (manual review)
        - LOW: score < 50 (manual review with warning)

        CRITICAL: HIGH tier requires BOTH score >= 80 AND identity_present.
        This ensures coincidence (date + uniqueness) cannot trigger auto-match
        without identity verification. This is a safety invariant.
        """
        if self.total >= self.HIGH_THRESHOLD and self.identity_present:
            return 'high'
        elif self.total >= self.MEDIUM_THRESHOLD:
            return 'medium'
        else:
            return 'low'

    def to_dict(self) -> Dict:
        """
        Return score breakdown as dictionary.

        Includes numeric score and tier label (not percentages).
        This format is used for UI display and audit logging.
        """
        return {
            'amount_points': self.amount_points,
            'date_points': self.date_points,
            'email_points': self.email_points,
            'uniqueness_bonus': self.uniqueness_bonus,
            'prior_pair_match': self.prior_pair_match,  # Identity from prior confirmed pair
            'is_deterministic': self.is_deterministic,  # True when matched via Transaction ID → REF NO
            'total': self.total,  # Numeric score (0-100), not a percentage
            'identity_present': self.identity_present,
            'confidence_level': self.confidence_level  # Tier label: high/medium/low
        }


@dataclass
class MatchCandidate:
    """A QBO Payment candidate with its match score."""
    qbo_payment: Dict
    score: MatchScoreBreakdown

    @property
    def qbo_payment_id(self) -> str:
        """Get QBO payment ID - supports both production (Id) and test (qbo_payment_id) formats."""
        return self.qbo_payment.get('Id') or self.qbo_payment.get('qbo_payment_id') or self.qbo_payment.get('id')


# =============================================================================
# CONFIRMED MATCH PAIR PERSISTENCE
# =============================================================================
# These functions manage the confirmed_match_pairs table, which records
# confirmed (processor_customer, QBO_customer) pairs learned from:
# - HIGH-confidence auto-matches
# - Explicit user-confirmed manual matches
#
# DESIGN PRINCIPLES (per spec):
# 1. Pair-level, not global - a processor customer may pay multiple QBO customers
# 2. Only Charles-owned decisions - no inference from QBO history
# 3. Conservative and auditable - explicit write conditions only
# =============================================================================


def check_prior_pair_exists(
    db_conn: sqlite3.Connection,
    workspace_id: str,
    processor_type: str,
    processor_customer_id: str,
    qbo_customer_id: str
) -> bool:
    """
    Check if a confirmed (processor_customer, QBO_customer) pair exists.

    This is the READ side of prior match continuity. Called during scoring
    to determine if identity_present should be set based on prior confirmation.

    Args:
        db_conn: Database connection
        workspace_id: Workspace for data isolation
        processor_type: 'stripe', 'authorize_net', etc.
        processor_customer_id: Processor-native customer identifier
        qbo_customer_id: QBO customer reference ID

    Returns:
        True if a confirmed pair exists, False otherwise
    """
    if not processor_customer_id or not qbo_customer_id:
        return False

    try:
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT 1 FROM confirmed_match_pairs
            WHERE workspace_id = ?
              AND processor_type = ?
              AND processor_customer_id = ?
              AND qbo_customer_id = ?
        """, (workspace_id, processor_type, processor_customer_id, qbo_customer_id))

        return cursor.fetchone() is not None

    except sqlite3.OperationalError:
        # Table may not exist yet (pre-migration)
        return False


def get_confirmed_pairs_for_processor_customer(
    db_conn: sqlite3.Connection,
    workspace_id: str,
    processor_type: str,
    processor_customer_id: str
) -> List[str]:
    """
    Get all QBO customer IDs that have confirmed pairs with a processor customer.

    This is used to check which QBO candidates should have prior_pair_match=True.
    A processor customer may legitimately have pairs with multiple QBO customers.

    Args:
        db_conn: Database connection
        workspace_id: Workspace for data isolation
        processor_type: 'stripe', 'authorize_net', etc.
        processor_customer_id: Processor-native customer identifier

    Returns:
        List of QBO customer IDs with confirmed pairs (may be empty)
    """
    if not processor_customer_id:
        return []

    try:
        cursor = db_conn.cursor()
        cursor.execute("""
            SELECT qbo_customer_id FROM confirmed_match_pairs
            WHERE workspace_id = ?
              AND processor_type = ?
              AND processor_customer_id = ?
        """, (workspace_id, processor_type, processor_customer_id))

        return [row[0] for row in cursor.fetchall()]

    except sqlite3.OperationalError:
        # Table may not exist yet (pre-migration)
        return []


def record_confirmed_pair(
    db_conn: sqlite3.Connection,
    workspace_id: str,
    processor_type: str,
    processor_customer_id: str,
    qbo_customer_id: str,
    match_source: str,  # 'auto' or 'manual'
    canonical_event_id: Optional[str] = None
) -> bool:
    """
    Record a confirmed (processor_customer, QBO_customer) pair.

    This is the WRITE side of prior match continuity. Called ONLY when:
    - A HIGH-confidence auto-match completes successfully
    - A user explicitly confirms a manual match

    NEVER called for:
    - Suggestions
    - MEDIUM/LOW confidence outcomes (unless user confirms)
    - Inferred or heuristic matches

    Args:
        db_conn: Database connection
        workspace_id: Workspace for data isolation
        processor_type: 'stripe', 'authorize_net', etc.
        processor_customer_id: Processor-native customer identifier
        qbo_customer_id: QBO customer reference ID
        match_source: 'auto' (HIGH confidence) or 'manual' (user confirmed)
        canonical_event_id: Optional canonical event ID for audit trail

    Returns:
        True if pair was recorded or updated, False if skipped (missing IDs)
    """
    # CRITICAL: Do NOT synthesize missing identifiers
    if not processor_customer_id or not qbo_customer_id:
        logger.debug(
            f"Skipping pair recording: missing processor_customer_id={processor_customer_id} "
            f"or qbo_customer_id={qbo_customer_id}"
        )
        return False

    try:
        cursor = db_conn.cursor()
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        pair_id = str(uuid.uuid4())

        # UPSERT: Insert new pair or update existing
        cursor.execute("""
            INSERT INTO confirmed_match_pairs (
                id, workspace_id, processor_type, processor_customer_id,
                qbo_customer_id, first_confirmed_at, last_confirmed_at,
                match_source, confirmation_count,
                first_canonical_event_id, last_canonical_event_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(workspace_id, processor_type, processor_customer_id, qbo_customer_id)
            DO UPDATE SET
                last_confirmed_at = excluded.last_confirmed_at,
                confirmation_count = confirmation_count + 1,
                last_canonical_event_id = excluded.last_canonical_event_id
        """, (
            pair_id,
            workspace_id,
            processor_type,
            processor_customer_id,
            qbo_customer_id,
            now,
            now,
            match_source,
            canonical_event_id,
            canonical_event_id
        ))

        logger.info(
            f"Recorded confirmed pair: {processor_type}/{processor_customer_id} -> {qbo_customer_id} "
            f"(source={match_source}, workspace={workspace_id})"
        )
        return True

    except sqlite3.OperationalError as e:
        # Table may not exist yet (pre-migration)
        logger.debug(f"Could not record confirmed pair (table may not exist): {e}")
        return False


class QBOPaymentMatcher:
    """
    Multi-factor payment matcher for QBO Payments.

    Implements sophisticated scoring algorithm (0-100 scale, capped):
    - Amount Match: 40 points (required gate)
    - Date Proximity: 25 points (asymmetric for bookkeeping lag)
    - Customer Email: 20 points (IDENTITY SIGNAL - required for auto-match)
    - Uniqueness Bonus: +10 points (only applied when identity_present)

    SAFETY INVARIANT: Auto-match requires identity_present == True.
    Coincidental signals (date + uniqueness) assist ranking but cannot
    independently trigger auto-match. This prevents false confidence
    where wrong-customer matches score high due to temporal coincidence.

    Confidence Tiers (score-based, NOT percentages):
    - HIGH (score ≥80 AND identity_present): Auto-match eligible
    - MEDIUM (score 50-79, or ≥80 without identity): Manual review
    - LOW (score <50): Manual review with warning

    WHY SCORE IS PREFERRED OVER PERCENTAGE:
    - Scores are deterministic and auditable (sum of component points)
    - Percentages imply statistical confidence which is misleading
    - Score + tier clearly communicates what was measured and why

    QBO Payments are created when invoices are paid via Stella or QBO.
    They sit in Undeposited Funds until matched to processor payments
    and included in a Deposit.
    """

    # Scoring constants (points, not percentages)
    AMOUNT_POINTS = 40          # Required gate - must match exactly
    DATE_POINTS_MAX = 25        # Maximum date proximity points
    EMAIL_POINTS = 10           # Email match bonus (IDENTITY SIGNAL)
    IDENTITY_POINTS_DETERMINISTIC = 25  # Deterministic ID match (Transaction ID → REF NO)
    UNIQUENESS_BONUS = 10       # Capped bonus, only when identity_present
    MAX_SCORE = 100             # Cap total score at this value

    # Confidence tier thresholds (score-based, not percentages)
    # WHY 80: This is amount(40) + date(25) + email(20) - minimum for identity match
    # Identity gate prevents false confidence regardless of numeric score
    HIGH_CONFIDENCE = 80
    MEDIUM_CONFIDENCE = 50

    def __init__(
        self,
        qbo_client=None,
        db_conn: Optional[sqlite3.Connection] = None,
        workspace_id: Optional[str] = None,
        processor_type: str = 'stripe'
    ):
        """
        Initialize the matcher.

        Args:
            qbo_client: Authenticated QuickBooks client (optional for testing)
            db_conn: Database connection for prior pair lookups (optional)
            workspace_id: Workspace ID for data isolation (optional)
            processor_type: Processor type for pair lookups (default: 'stripe')
        """
        self.qbo_client = qbo_client
        self.db_conn = db_conn
        self.workspace_id = workspace_id
        self.processor_type = processor_type
        self._customer_cache: Dict[str, Dict] = {}

    def fetch_unlinked_payments(self, since_date: Optional[date] = None, include_linked: bool = False) -> List[Dict]:
        """
        Fetch QBO Payments that haven't been linked to a deposit yet.

        A Payment is "unlinked" if it doesn't have a LinkedTxn pointing
        to a Deposit. These are payments sitting in Undeposited Funds.

        Args:
            since_date: Only fetch payments after this date
            include_linked: If True, include payments already linked to deposits.
                           Used by retry-match to find candidates regardless of
                           their QBO deposit status.

        Returns:
            List of QBO Payment objects with customer email
        """
        # Build query
        if since_date:
            date_str = since_date.strftime('%Y-%m-%d')
            query = f"SELECT * FROM Payment WHERE TxnDate >= '{date_str}' MAXRESULTS 1000"
        else:
            query = "SELECT * FROM Payment MAXRESULTS 1000"

        try:
            from quickbooks.objects.payment import Payment
            payments = Payment.query(query, qb=self.qbo_client)

            # Filter to only unlinked payments (unless include_linked=True)
            unlinked = []
            for payment in payments:
                is_linked = False
                if hasattr(payment, 'LinkedTxn') and payment.LinkedTxn:
                    for linked in payment.LinkedTxn:
                        if hasattr(linked, 'TxnType') and linked.TxnType == 'Deposit':
                            is_linked = True
                            break

                if include_linked or not is_linked:
                    # Skip customer email lookup - it's an N+1 problem that causes
                    # 50+ second delays on first sync. Email not needed for matching.
                    customer_id = payment.CustomerRef.value if payment.CustomerRef else None

                    unlinked.append({
                        'Id': payment.Id,
                        'TotalAmt': str(payment.TotalAmt) if payment.TotalAmt else '0',
                        'TxnDate': payment.TxnDate,
                        'CustomerRef': customer_id,
                        'CustomerEmail': None,  # Skip expensive lookup
                        'PaymentRefNum': getattr(payment, 'PaymentRefNum', None),
                    })

            logger.info(f"Found {len(unlinked)} unlinked QBO Payments out of {len(payments)} total")

            # DEBUG_ACH_MATCH: Log target QBO payments and their PaymentRefNum status
            target_qbo_ids = ['50617', '50618', '50619', '50620', '50621']
            for p in unlinked:
                if p['Id'] in target_qbo_ids:
                    logger.info(f"DEBUG_ACH_MATCH: fetch_unlinked_payments returned QBO {p['Id']}: PaymentRefNum='{p.get('PaymentRefNum')}', Amount=${p.get('TotalAmt')}")

            return unlinked

        except Exception as e:
            logger.error(f"Error fetching QBO Payments: {e}")
            raise

    def resolve_qbo_payments_by_ref_no(
        self,
        trans_ids: List[str],
        since_date: Optional[date] = None
    ) -> Tuple[Dict[str, Dict], List[str]]:
        """
        Resolve QBO Payments by PaymentRefNum (deterministic matching).

        Per AUTHORIZE_NET_ACH_MANUAL_DEPOSIT_CONTRACT §4:
        - Query QBO Payments where Payment.RefNo IN (transId list)
        - Returns mapping of RefNo → QBO Payment for resolved payments
        - Returns list of unresolved transIds

        This implements deterministic resolution: transId == PaymentRefNum
        is a verified 1:1 relationship per the contract.

        Args:
            trans_ids: List of Authorize.net transId values to resolve
            since_date: Optional date filter for payments

        Returns:
            Tuple of:
            - Dict mapping RefNo → QBO Payment dict (for resolved payments)
            - List of unresolved transIds (not found in QBO)
        """
        # Fetch all unlinked payments
        all_payments = self.fetch_unlinked_payments(since_date)

        # Build RefNo → Payment index
        ref_no_index: Dict[str, Dict] = {}
        for payment in all_payments:
            ref_no = payment.get('PaymentRefNum')
            if ref_no:
                # Normalize to string for comparison
                ref_no_str = str(ref_no).strip()
                ref_no_index[ref_no_str] = payment

        # Resolve transIds
        resolved: Dict[str, Dict] = {}
        unresolved: List[str] = []

        for trans_id in trans_ids:
            trans_id_str = str(trans_id).strip()
            if trans_id_str in ref_no_index:
                resolved[trans_id_str] = ref_no_index[trans_id_str]
            else:
                unresolved.append(trans_id_str)

        logger.info(
            f"Resolved {len(resolved)}/{len(trans_ids)} transIds to QBO Payments, "
            f"{len(unresolved)} unresolved"
        )

        return resolved, unresolved

    def _get_customer_email(self, customer_id: str) -> Optional[str]:
        """Fetch customer email from QBO, with caching."""
        if customer_id in self._customer_cache:
            return self._customer_cache[customer_id].get('email')

        try:
            from quickbooks.objects.customer import Customer
            customer = Customer.get(customer_id, qb=self.qbo_client)
            email = None
            if customer and hasattr(customer, 'PrimaryEmailAddr') and customer.PrimaryEmailAddr:
                email = customer.PrimaryEmailAddr.Address
            self._customer_cache[customer_id] = {'email': email}
            return email
        except Exception as e:
            logger.debug(f"Could not fetch customer {customer_id}: {e}")
            self._customer_cache[customer_id] = {'email': None}
            return None

    def _calculate_date_score(self, stripe_date: date, qbo_date_str: str) -> int:
        """
        Calculate date proximity score with DIRECTIONAL logic.

        Date is a COINCIDENTAL signal, not identity evidence. It helps rank
        candidates but cannot verify the payment is from the correct customer.

        delta_days = qbo_date - stripe_date:
          > 0: QBO occurred AFTER Stripe (forward, normal)
          = 0: Same day
          < 0: QBO occurred BEFORE Stripe (backward, suspicious)

        Forward/Same-day (delta_days >= 0):
          - 0-3 days: 25 points (full) - normal bookkeeping lag
          - 4-7 days: 12 points (half) - delayed but plausible
          - >= 8 days: 0 points

        Backward (delta_days < 0):
          - Penalized because QBO entry before charge is unusual
          - -1 day: 10 points (minor, could be timezone)
          - -2 to -7 days: 5 points (suspicious but allowed)
          - <= -8 days: 0 points

        Backward matches are NOT rejected outright because:
          - Timezone differences can cause apparent 1-day shifts
          - Manual QBO entries may be backdated for accounting reasons
          - Identity gating (email match) still required for auto-match

        Args:
            stripe_date: Date from processor payment
            qbo_date_str: Date string from QBO payment

        Returns:
            Score between 0-25
        """
        try:
            # Parse QBO date
            if isinstance(qbo_date_str, str):
                qbo_date = datetime.strptime(qbo_date_str[:10], '%Y-%m-%d').date()
            elif isinstance(qbo_date_str, date):
                qbo_date = qbo_date_str
            else:
                return 0

            delta_days = (qbo_date - stripe_date).days

            # Forward/Same-day direction (QBO on or after Stripe - normal)
            if delta_days >= 0:
                if delta_days <= 3:
                    return self.DATE_POINTS_MAX  # 25 points
                elif delta_days <= 7:
                    return self.DATE_POINTS_MAX // 2  # 12 points
                else:
                    return 0

            # Backward direction (QBO before Stripe - suspicious)
            # Penalized because QBO entry preceding charge is unusual,
            # but not rejected since timezone/backdating can explain it.
            else:
                if delta_days == -1:
                    return 10  # Minor penalty, likely timezone difference
                elif delta_days >= -7:
                    return 5   # Significant penalty, clearly backward
                else:
                    return 0   # Too far backward, no points

        except Exception as e:
            logger.debug(f"Date scoring error: {e}")
            return 0

    def _calculate_email_score(self, stripe_email: Optional[str], qbo_email: Optional[str]) -> int:
        """
        Calculate email match score.

        Returns EMAIL_POINTS (25) for exact case-insensitive match, 0 otherwise.
        """
        if not stripe_email or not qbo_email:
            return 0

        if stripe_email.lower().strip() == qbo_email.lower().strip():
            return self.EMAIL_POINTS

        return 0

    def _score_candidate(
        self,
        charge: ProcessorPayment,
        qbo_payment: Dict,
        is_single_candidate: bool
    ) -> MatchScoreBreakdown:
        """
        Calculate multi-factor match score for a candidate.

        Args:
            charge: Stripe/processor charge
            qbo_payment: QBO Payment candidate
            is_single_candidate: Whether this is the only amount-matching candidate

        Returns:
            MatchScoreBreakdown with component scores
        """
        breakdown = MatchScoreBreakdown()

        # Amount MUST match exactly (required gate)
        qbo_amount = Decimal(qbo_payment.get('TotalAmt', '0'))
        if charge.amount_gross == qbo_amount:
            breakdown.amount_points = self.AMOUNT_POINTS
        else:
            # No match possible if amounts differ
            return breakdown

        # Date proximity scoring
        if charge.processor_timestamp:
            charge_date = charge.processor_timestamp.date() if hasattr(charge.processor_timestamp, 'date') else charge.processor_timestamp
            breakdown.date_points = self._calculate_date_score(charge_date, qbo_payment.get('TxnDate'))

        # Email matching (IDENTITY SIGNAL) - lazy fetch if not already loaded
        qbo_email = qbo_payment.get('CustomerEmail')
        if qbo_email is None and qbo_payment.get('CustomerRef'):
            # Lazy fetch customer email (only for amount-matching candidates)
            qbo_email = self._get_customer_email(qbo_payment['CustomerRef'])
            # Store the fetched email back so it's available for display
            qbo_payment['CustomerEmail'] = qbo_email
        breakdown.email_points = self._calculate_email_score(
            charge.customer_email,
            qbo_email
        )

        # Uniqueness bonus - ONLY when identity is present.
        # Without identity verification, uniqueness could cause false confidence.
        if is_single_candidate and breakdown.identity_present:
            breakdown.uniqueness_bonus = self.UNIQUENESS_BONUS

        return breakdown

    def find_deterministic_match(
        self,
        processor_payment_id: str,
        processor_amount: Decimal,
        unlinked_qbo_payments: List[Dict[str, Any]],
    ) -> Optional[MatchCandidate]:
        """
        Attempt deterministic matching via Transaction ID → QBO REF NO.

        Per AUTHORIZE_NET_INTEGRATION_PLAN §4.1-4.2:
        - Primary Rule: Match processor Transaction ID to QBO Payment RefNumber
        - If found and amount matches: deterministic match (Level A)
        - If found but amount differs: flag as anomaly (return None, log warning)
        - If not found: return None (caller should proceed to probabilistic matching)

        INV-ANET-06: If transId matches REF NO exactly, confidence is 100
        and match_type is 'auto', regardless of other signals.

        INV-ANET-07: Probabilistic matching is only invoked when deterministic
        ID matching fails (returns None).

        Args:
            processor_payment_id: Transaction ID (Authorize.net transId or Stripe charge ID)
            processor_amount: Payment amount from processor
            unlinked_qbo_payments: List of unlinked QBO payments with PaymentRefNum

        Returns:
            MatchCandidate with confidence 100 if deterministic match found,
            None if no match or amount mismatch
        """
        # Build REF NO index for O(1) lookup
        ref_no_index: Dict[str, Dict] = {}
        for qbo in unlinked_qbo_payments:
            # Get PaymentRefNum - support both production and test formats
            ref_num = qbo.get('PaymentRefNum') or qbo.get('payment_ref_num')
            if ref_num:
                # Normalize REF NO for comparison (strip whitespace)
                ref_no_index[str(ref_num).strip()] = qbo

        # DEBUG_ACH_MATCH: Log ref_no_index contents for target RefNums
        target_refs = ['81416903922', '81416904312', '81416902804', '81416903816', '81416902484']
        for target in target_refs:
            if target in ref_no_index:
                qbo = ref_no_index[target]
                logger.info(f"DEBUG_ACH_MATCH: ref_no_index contains {target} -> QBO {qbo.get('Id')} (${qbo.get('TotalAmt')})")
        logger.info(f"DEBUG_ACH_MATCH: ref_no_index has {len(ref_no_index)} entries total, pool has {len(unlinked_qbo_payments)} payments")

        # Attempt deterministic lookup
        normalized_id = str(processor_payment_id).strip()
        # DEBUG_ACH_MATCH: Log lookup attempt for target IDs
        if normalized_id in target_refs:
            logger.info(f"DEBUG_ACH_MATCH: Looking up {normalized_id} in ref_no_index, found={normalized_id in ref_no_index}")
        if normalized_id not in ref_no_index:
            # No REF NO match - return None to trigger probabilistic matching
            return None

        # Found REF NO match - verify amount
        qbo_payment = ref_no_index[normalized_id]
        qbo_amount = Decimal(str(qbo_payment.get('TotalAmt') or qbo_payment.get('amount', '0')))

        # =================================================================
        # INV-ANET-06 / §4.4 COMPLIANCE:
        # When Transaction ID matches REF NO, the match is DETERMINISTIC.
        # Amount mismatch is logged as anomaly but DOES NOT block the match.
        # Per §4.4: "Anomalies are logged but do not prevent the match."
        # =================================================================
        amount_mismatch = (qbo_amount != processor_amount)
        if amount_mismatch:
            # REF NO matches but amount differs - log anomaly per §4.2
            # CRITICAL: This does NOT block the match per §4.4 and INV-ANET-06
            logger.warning(
                f"ANOMALY: Amount mismatch on deterministic REF NO match: "
                f"processor {processor_payment_id} (${processor_amount}) != "
                f"QBO REF NO (${qbo_amount}) - "
                f"qbo_payment_id={qbo_payment.get('Id') or qbo_payment.get('qbo_payment_id')} - "
                f"MATCH PROCEEDS per INV-ANET-06"
            )

        # Deterministic match found - create MatchCandidate with confidence 100
        # Per INV-ANET-06: ID match means confidence 100, match_type 'auto'
        breakdown = MatchScoreBreakdown(
            amount_points=self.AMOUNT_POINTS,  # 40 points
            date_points=self.DATE_POINTS_MAX,  # 25 points (full)
            email_points=self.IDENTITY_POINTS_DETERMINISTIC,  # 25 points (deterministic ID match)
            uniqueness_bonus=self.UNIQUENESS_BONUS,  # 10 points
            prior_pair_match=True,  # ID match counts as identity
            is_deterministic=True   # Matched via Transaction ID → REF NO
        )
        # Cap at 100 per the scoring algorithm
        # Total: 40 + 25 + 20 + 10 = 95, but we override to 100 for deterministic

        logger.info(
            f"Deterministic REF NO match: processor {processor_payment_id} -> "
            f"QBO Payment {qbo_payment.get('Id') or qbo_payment.get('qbo_payment_id')} "
            f"(amount ${processor_amount})"
        )

        return MatchCandidate(
            qbo_payment=qbo_payment,
            score=breakdown
        )

    def find_match_candidates(
        self,
        stripe_payment_id: str,
        stripe_amount: Decimal,
        stripe_date: date,
        stripe_email: Optional[str],
        unlinked_qbo_payments: List[Dict[str, Any]],
        processor_customer_id: Optional[str] = None,
    ) -> List[MatchCandidate]:
        """
        Find and score all potential QBO payment matches for a single payment.

        This is the core matching logic used by both production sync and tests.
        Accepts QBO payments in either production format (TotalAmt, TxnDate, etc.)
        or test format (amount, date, customer_email).

        PRIOR MATCH CONTINUITY:
        If processor_customer_id is provided and the matcher was initialized with
        db_conn/workspace_id, prior confirmed pairs are checked for each candidate.
        Candidates with confirmed (processor_customer, qbo_customer) pairs get
        prior_pair_match=True, which enables identity_present without email match.

        Args:
            stripe_payment_id: ID of the Stripe payment being matched
            stripe_amount: Amount from Stripe payment
            stripe_date: Date from Stripe payment
            stripe_email: Customer email from Stripe
            unlinked_qbo_payments: List of unlinked QBO payments to consider
            processor_customer_id: Processor-native customer ID for prior pair lookup

        Returns:
            List of MatchCandidate sorted by score descending
        """
        # ===================================================================
        # DETERMINISTIC MATCHING (INV-ANET-06, INV-ANET-07)
        # ===================================================================
        # First, attempt deterministic matching via REF NO.
        # Per AUTHORIZE_NET_INTEGRATION_PLAN §4.1-4.2:
        # - If Transaction ID matches QBO Payment's RefNumber exactly,
        #   this is a Level A (deterministic) match with confidence 100
        # - Probabilistic matching is only invoked when deterministic fails
        # ===================================================================
        deterministic_match = self.find_deterministic_match(
            processor_payment_id=stripe_payment_id,
            processor_amount=stripe_amount,
            unlinked_qbo_payments=unlinked_qbo_payments
        )
        if deterministic_match:
            # Return immediately with deterministic match
            # No need for probabilistic scoring - ID match is authoritative
            return [deterministic_match]

        # ===================================================================
        # PROBABILISTIC MATCHING (fallback when no REF NO match)
        # ===================================================================
        candidates = []

        # PRIOR MATCH CONTINUITY: Get confirmed QBO customers for this processor customer
        # This is pair-level - same processor customer may have confirmed pairs with
        # multiple QBO customers (e.g., office manager paying for multiple businesses)
        confirmed_qbo_customers: set = set()
        if (processor_customer_id and self.db_conn and self.workspace_id):
            confirmed_qbo_customers = set(get_confirmed_pairs_for_processor_customer(
                db_conn=self.db_conn,
                workspace_id=self.workspace_id,
                processor_type=self.processor_type,
                processor_customer_id=processor_customer_id
            ))

        # First pass: find amount matches
        # Support both production format (TotalAmt) and test format (amount)
        amount_matches = []
        for qbo in unlinked_qbo_payments:
            qbo_amount = Decimal(str(qbo.get('TotalAmt') or qbo.get('amount', '0')))
            if qbo_amount == stripe_amount:
                amount_matches.append(qbo)

        # Count candidates within reasonable date window (7 days) for uniqueness
        candidates_in_window = 0
        if stripe_date:
            for qbo in amount_matches:
                qbo_date_str = qbo.get('TxnDate') or qbo.get('date')
                if qbo_date_str:
                    try:
                        if isinstance(qbo_date_str, date):
                            qbo_date = qbo_date_str
                        else:
                            qbo_date = datetime.strptime(str(qbo_date_str)[:10], '%Y-%m-%d').date()
                        days_diff = abs((qbo_date - stripe_date).days)
                        if days_diff <= 7:
                            candidates_in_window += 1
                    except (ValueError, TypeError):
                        candidates_in_window += 1
                else:
                    candidates_in_window += 1
        else:
            candidates_in_window = len(amount_matches)

        is_unique_in_window = candidates_in_window == 1

        # Score each candidate
        for qbo in amount_matches:
            qbo_amount = Decimal(str(qbo.get('TotalAmt') or qbo.get('amount', '0')))

            # Parse QBO date - support both formats
            qbo_date_str = qbo.get('TxnDate') or qbo.get('date')
            if qbo_date_str:
                if isinstance(qbo_date_str, date):
                    qbo_date = qbo_date_str
                else:
                    try:
                        qbo_date = datetime.strptime(str(qbo_date_str)[:10], '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        qbo_date = stripe_date
            else:
                qbo_date = stripe_date

            # Get QBO email - support both formats
            qbo_email = qbo.get('CustomerEmail') or qbo.get('customer_email')

            # Get QBO customer ID - support both formats
            # This is used for prior pair lookup
            qbo_customer_id = qbo.get('CustomerRef') or qbo.get('qbo_customer_id')

            # Calculate score breakdown
            breakdown = MatchScoreBreakdown()
            breakdown.amount_points = self.AMOUNT_POINTS

            # Date scoring
            if stripe_date and qbo_date:
                breakdown.date_points = self._calculate_date_score(stripe_date, qbo_date)

            # Email scoring (IDENTITY SIGNAL)
            breakdown.email_points = self._calculate_email_score(stripe_email, qbo_email)

            # PRIOR MATCH CONTINUITY: Check if this (processor_customer, qbo_customer) pair
            # has been previously confirmed. This enables identity_present without email match.
            # Note: Does NOT add to score - only contributes to identity gating.
            if qbo_customer_id and qbo_customer_id in confirmed_qbo_customers:
                breakdown.prior_pair_match = True

            # Uniqueness bonus - ONLY when identity is present.
            # Uniqueness is a coincidental signal that assists ranking but cannot
            # independently enable auto-match. Without identity verification,
            # uniqueness could cause false confidence on wrong-customer matches.
            if is_unique_in_window and breakdown.identity_present:
                breakdown.uniqueness_bonus = self.UNIQUENESS_BONUS

            # Get QBO payment ID - support both formats
            qbo_id = qbo.get('Id') or qbo.get('qbo_payment_id') or qbo.get('id')

            candidates.append(MatchCandidate(
                qbo_payment=qbo,
                score=breakdown
            ))

        # Sort by total score descending
        candidates.sort(key=lambda c: c.score.total, reverse=True)

        return candidates

    def match_payments_to_charges(
        self,
        charges: List[ProcessorPayment],
        unlinked_payments: List[Dict]
    ) -> List[Tuple[ProcessorPayment, Optional[Dict], Optional[MatchScoreBreakdown], List[MatchCandidate]]]:
        """
        Match processor charges to QBO Payments with confidence scoring.

        Uses multi-factor scoring: amount (gate), date proximity, email, uniqueness.

        DETERMINISTIC MATCHING (INV-ANET-06, INV-ANET-07):
        Before probabilistic scoring, attempts deterministic match via REF NO.
        If processor_payment_id matches QBO Payment's PaymentRefNum exactly,
        the match is deterministic with confidence 100 (auto-match).

        Args:
            charges: List of processor charges to match
            unlinked_payments: List of unlinked QBO Payments

        Returns:
            List of tuples: (charge, best_qbo_payment or None, score or None, all_candidates)
            - For HIGH confidence matches, all_candidates will be empty (not needed)
            - For MEDIUM/LOW matches, all_candidates contains all scored options for review
        """
        available_payments = list(unlinked_payments)
        results = []

        # DEBUG_ACH_MATCH: Log initial pool state
        target_qbo_ids = ['50617', '50618', '50619', '50620', '50621']
        logger.info(f"DEBUG_ACH_MATCH: === Starting match_payments_to_charges with {len(available_payments)} payments ===")
        for qbo in available_payments:
            qbo_id = str(qbo.get('Id', ''))
            if qbo_id in target_qbo_ids:
                ref_num = qbo.get('PaymentRefNum') or qbo.get('payment_ref_num') or 'NO_REFNUM'
                logger.info(f"DEBUG_ACH_MATCH: Pool contains QBO {qbo_id}, RefNum={ref_num}, Amount=${qbo.get('TotalAmt')}")

        for charge in charges:
            # DEBUG_ACH_MATCH: Log processing of target transactions
            target_trans_ids = ['81416903922', '81416904312', '81416902804', '81416903816', '81416902484']
            if charge.processor_payment_id in target_trans_ids:
                logger.info(f"DEBUG_ACH_MATCH: Processing charge {charge.processor_payment_id} (${charge.amount_gross}), pool has {len(available_payments)} payments")
                # Log which target QBO payments are still in pool
                for qbo in available_payments:
                    qbo_id = str(qbo.get('Id', ''))
                    if qbo_id in target_qbo_ids:
                        ref_num = qbo.get('PaymentRefNum') or qbo.get('payment_ref_num') or 'NO_REFNUM'
                        logger.info(f"DEBUG_ACH_MATCH:   Pool still has QBO {qbo_id} (RefNum={ref_num})")

            # =================================================================
            # DETERMINISTIC MATCHING (INV-ANET-06, INV-ANET-07)
            # =================================================================
            # First, attempt deterministic matching via REF NO.
            # Per AUTHORIZE_NET_INTEGRATION_PLAN §4.1-4.2:
            # - If Transaction ID matches QBO Payment's PaymentRefNum exactly,
            #   this is a Level A (deterministic) match with confidence 100
            # - Probabilistic matching is only invoked when deterministic fails
            # =================================================================
            deterministic_match = self.find_deterministic_match(
                processor_payment_id=charge.processor_payment_id,
                processor_amount=charge.amount_gross,
                unlinked_qbo_payments=available_payments
            )
            if deterministic_match:
                # Remove from available pool
                if deterministic_match.qbo_payment in available_payments:
                    available_payments.remove(deterministic_match.qbo_payment)
                    # DEBUG_ACH_MATCH: Log pool removal
                    removed_id = str(deterministic_match.qbo_payment.get('Id', ''))
                    if removed_id in target_qbo_ids:
                        logger.info(f"DEBUG_ACH_MATCH: REMOVED QBO {removed_id} from pool (deterministic match to {charge.processor_payment_id}), pool now has {len(available_payments)} payments")

                logger.info(
                    f"Deterministic match: charge {charge.processor_payment_id} -> "
                    f"QBO Payment {deterministic_match.qbo_payment_id} via REF NO"
                )
                # HIGH confidence deterministic match - no review needed
                results.append((
                    charge,
                    deterministic_match.qbo_payment,
                    deterministic_match.score,
                    []  # No candidates for review - deterministic
                ))
                continue

            # DEBUG_ACH_MATCH: Log when deterministic fails for target transactions
            if charge.processor_payment_id in target_trans_ids:
                logger.info(f"DEBUG_ACH_MATCH: DETERMINISTIC FAILED for {charge.processor_payment_id} - falling through to probabilistic")

            # =================================================================
            # PROBABILISTIC MATCHING (fallback when no REF NO match)
            # =================================================================
            # Find all amount-matching candidates
            amount_matches = [
                p for p in available_payments
                if Decimal(p.get('TotalAmt', '0')) == charge.amount_gross
            ]

            if not amount_matches:
                logger.warn(
                    EventType.WARNING_DETECTED,
                    f"No QBO Payment match for charge {charge.processor_payment_id} "
                    f"(${charge.amount_gross})"
                )
                results.append((charge, None, None, []))
                continue

            # Count candidates within reasonable date window (7 days) for uniqueness
            # This prevents distant same-amount payments from blocking uniqueness bonus
            charge_date = None
            if charge.processor_timestamp:
                charge_date = charge.processor_timestamp.date() if hasattr(charge.processor_timestamp, 'date') else charge.processor_timestamp

            candidates_in_window = 0
            if charge_date:
                for p in amount_matches:
                    qbo_date_str = p.get('TxnDate')
                    if qbo_date_str:
                        try:
                            qbo_date = datetime.strptime(qbo_date_str[:10], '%Y-%m-%d').date()
                            days_diff = abs((qbo_date - charge_date).days)
                            if days_diff <= 7:  # Within date window that gets points
                                candidates_in_window += 1
                        except (ValueError, TypeError):
                            candidates_in_window += 1  # Count if we can't parse date
                    else:
                        candidates_in_window += 1  # Count if no date
            else:
                candidates_in_window = len(amount_matches)

            # Uniqueness: only one candidate within the date window
            is_unique_in_window = candidates_in_window == 1

            # Score all candidates
            candidates: List[MatchCandidate] = []
            for qbo_payment in amount_matches:
                score = self._score_candidate(charge, qbo_payment, is_unique_in_window)
                candidates.append(MatchCandidate(qbo_payment=qbo_payment, score=score))

            # Sort by score descending
            candidates.sort(key=lambda c: c.score.total, reverse=True)

            # Take the best candidate
            best = candidates[0]

            # Check for tie at top - if multiple candidates have same top score, needs review
            has_tie = len(candidates) > 1 and candidates[1].score.total == best.score.total

            # Remove from available pool (only if not a tie - ties go to review)
            if not has_tie and best.qbo_payment in available_payments:
                available_payments.remove(best.qbo_payment)
                # DEBUG_ACH_MATCH: Log pool removal
                removed_id = str(best.qbo_payment.get('Id', ''))
                if removed_id in target_qbo_ids:
                    logger.info(f"DEBUG_ACH_MATCH: REMOVED QBO {removed_id} from pool (probabilistic match to {charge.processor_payment_id}, score={best.score.total}), pool now has {len(available_payments)} payments")

            if has_tie:
                logger.info(
                    f"Charge {charge.processor_payment_id} (${charge.amount_gross}) has TIE: "
                    f"{len([c for c in candidates if c.score.total == best.score.total])} candidates "
                    f"with score {best.score.total} - needs manual review"
                )
            else:
                logger.info(
                    f"Matched charge {charge.processor_payment_id} (${charge.amount_gross}) "
                    f"to QBO Payment {best.qbo_payment['Id']} with score {best.score.total} "
                    f"({best.score.confidence_level} confidence)"
                )

            # For HIGH confidence with clear winner, we don't need to store all candidates
            # For MEDIUM/LOW or TIES, include all candidates for review UI
            if best.score.confidence_level == 'high' and not has_tie:
                results.append((charge, best.qbo_payment, best.score, []))
            else:
                results.append((charge, best.qbo_payment, best.score, candidates))

        return results


class ProcessorSyncManager:
    """
    Manages syncing payment processor data to the company database.
    
    This class handles:
    - Tracking which payouts have been processed
    - Matching processor charges to QBO Payments
    - Converting processor data to database format
    - Ensuring idempotency (no duplicate processing)
    - Creating proper audit trail (raw_events → canonical_facts → payout_batches)
    """
    
    def __init__(
        self,
        company_conn: sqlite3.Connection,
        processor: ProcessorBase,
        qbo_client=None,
        auto_match_enabled: bool = True,
        workspace_id: str = None,
        company_id: str = None
    ):
        """
        Initialize sync manager.

        Args:
            company_conn: SQLite connection to company's charles.db (Phase 2: consolidated DB)
            processor: Processor implementation (StripeProcessor, etc.)
            qbo_client: Authenticated QuickBooks client (required for matching)
            auto_match_enabled: If False, all matches go to review regardless of score
            workspace_id: Workspace ID (Phase 2+, required for data scoping)
            company_id: Company ID (for idempotency key generation)
        """
        self.conn = company_conn
        self.conn.row_factory = sqlite3.Row
        self.processor = processor
        self.processor_name = processor.get_processor_name()
        self.qbo_client = qbo_client
        self.workspace_id = workspace_id  # Phase 2: required for INSERT operations
        self.company_id = company_id or workspace_id  # Fallback for backward compatibility
        self.auto_match_enabled = auto_match_enabled
        self.matcher = QBOPaymentMatcher(qbo_client) if qbo_client else None

        # Phase A Compliance (A3): Initialize IdempotencyManager for payout sync
        # This ensures exactly-once semantics for all payout processing
        self.idempotency = get_idempotency_manager(
            db_connection=company_conn,
            company_id=self.company_id,
            workspace_id=workspace_id
        )
    
    def sync_payouts(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100
    ) -> Dict:
        """
        Sync payouts from the processor into the database.
        
        Args:
            start_date: Only sync payouts after this date
            end_date: Only sync payouts before this date
            limit: Maximum payouts to fetch
            
        Returns:
            Dict with sync results:
            {
                'payouts_fetched': int,
                'payouts_new': int,
                'payouts_skipped': int,
                'payments_created': int,
                'payments_matched': int,
                'payments_unmatched': int,
                'errors': List[str]
            }
        """
        results = {
            'payouts_fetched': 0,
            'payouts_new': 0,
            'payouts_skipped': 0,
            'payments_created': 0,
            'payments_matched': 0,
            'payments_unmatched': 0,
            'errors': []
        }
        
        # Check if we have QBO client for matching
        if not self.qbo_client:
            results['errors'].append(
                "QBO client not provided - payments will not be matched to QBO"
            )
        
        # Fetch unlinked QBO Payments for matching
        unlinked_qbo_payments = []
        if self.matcher:
            try:
                unlinked_qbo_payments = self.matcher.fetch_unlinked_payments(since_date=start_date)
                logger.info(f"Fetched {len(unlinked_qbo_payments)} unlinked QBO Payments for matching")
            except Exception as e:
                logger.error(f"Failed to fetch QBO Payments: {e}")
                results['errors'].append(f"Failed to fetch QBO Payments: {e}")
        
        # Fetch payouts from processor
        try:
            payouts = self.processor.fetch_payouts(start_date, end_date, limit)
            results['payouts_fetched'] = len(payouts)
        except Exception as e:
            logger.error(f"Failed to fetch payouts: {e}")
            results['errors'].append(f"Failed to fetch payouts: {e}")
            return results
        
        # Process each payout with formal idempotency guarantees
        for payout in payouts:
            try:
                # Skip if payout is not complete (still in transit)
                if not self.processor.is_payout_complete(payout):
                    logger.info(f"Skipping incomplete payout {payout.processor_payout_id} (status: {payout.status})")
                    results['payouts_skipped'] += 1
                    continue

                # =================================================================
                # PHASE A COMPLIANCE (A3): Formal Idempotency Check
                # =================================================================
                # Generate v2 idempotency key for this payout sync operation
                # v2 Key format: payout_sync:ws:{workspace_id}:{processor}:{processor_payout_id}
                # Legacy format: payout_sync:{company_id}:{processor_payout_id}
                # Dual-read ensures backward compatibility with legacy keys
                # =================================================================
                idempotency_key = generate_idempotency_key_v2(
                    operation_type="payout_sync",
                    workspace_id=self.workspace_id,
                    processor=self.processor_name,
                    external_id=payout.processor_payout_id
                )
                # Legacy key for dual-read backward compatibility
                legacy_key = _generate_legacy_key(
                    operation_type="payout_sync",
                    company_id=self.company_id,
                    external_id=payout.processor_payout_id
                )

                # Check idempotency status with dual-read for legacy compatibility
                idem_result = self.idempotency.check_idempotency_dual_read(
                    v2_key=idempotency_key,
                    legacy_key=legacy_key
                )

                if idem_result.status == IdempotencyStatus.EXISTS_COMPLETED:
                    # Already processed successfully - skip
                    logger.info(
                        f"Skipping already-synced payout {payout.processor_payout_id} "
                        f"(idempotency hit, result_id={idem_result.result_id})"
                    )
                    results['payouts_skipped'] += 1
                    continue

                if idem_result.status == IdempotencyStatus.EXISTS_PENDING:
                    # Another process is handling this payout - skip
                    logger.warning(
                        f"Skipping payout {payout.processor_payout_id} - "
                        f"sync operation already in progress"
                    )
                    results['payouts_skipped'] += 1
                    continue

                # EXISTS_FAILED or NOT_EXISTS - proceed with sync
                is_retry = (idem_result.status == IdempotencyStatus.EXISTS_FAILED)
                if is_retry:
                    logger.info(f"Retrying previously failed payout {payout.processor_payout_id}")

                # Mark operation as pending BEFORE starting work
                self.idempotency.start_operation(idempotency_key, "payout_sync")

                try:
                    # Fetch full payout details including payments
                    # Pass full payout object to preserve metadata (settlement_date, etc.)
                    full_payout = self.processor.fetch_payout_details(payout)

                    # Match payments to QBO Payments with multi-factor scoring
                    matched_payments = []
                    if self.matcher and unlinked_qbo_payments:
                        match_results = self.matcher.match_payments_to_charges(
                            full_payout.payments,
                            unlinked_qbo_payments
                        )

                        for charge, qbo_payment, score, all_candidates in match_results:
                            if qbo_payment:
                                # Remove matched payment from available pool
                                if qbo_payment in unlinked_qbo_payments:
                                    unlinked_qbo_payments.remove(qbo_payment)
                                results['payments_matched'] += 1
                            else:
                                results['payments_unmatched'] += 1

                            matched_payments.append((charge, qbo_payment, score, all_candidates))
                    else:
                        # No matching - just pass through charges without QBO IDs
                        matched_payments = [(charge, None, None, []) for charge in full_payout.payments]
                        results['payments_unmatched'] += len(full_payout.payments)

                    # Insert into database
                    payments_created = self._insert_payout_with_matches(full_payout, matched_payments)

                    # Mark idempotency operation as completed
                    self.idempotency.complete_operation(
                        idempotency_key,
                        result_id=payout.processor_payout_id,
                        result_data={
                            'payments_created': payments_created,
                            'processor': self.processor_name
                        }
                    )

                    results['payouts_new'] += 1
                    results['payments_created'] += payments_created

                    logger.info(
                        f"Processed payout {payout.processor_payout_id}: "
                        f"{payments_created} payments, net=${full_payout.net_amount}"
                    )

                except Exception as e:
                    # Mark idempotency operation as failed
                    self.idempotency.fail_operation(idempotency_key, str(e))
                    raise

            except Exception as e:
                logger.error(f"Error processing payout {payout.processor_payout_id}: {e}")
                results['errors'].append(f"Payout {payout.processor_payout_id}: {e}")
        
        return results
    
    def _is_payout_processed(self, processor_payout_id: str) -> bool:
        """
        Check if a payout has already been processed.
        
        Args:
            processor_payout_id: The processor's payout ID
            
        Returns:
            True if payout exists in database
        
        DEPRECATED: Use is_payout_synced() instead, which implements the
        authoritative idempotency rule from the module docstring.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 1 FROM payout_batches 
            WHERE processor = ? AND processor_payout_id = ?
        """, (self.processor_name, processor_payout_id))
        
        return cursor.fetchone() is not None
    
    def is_payout_synced(self, processor_payout_id: str) -> Tuple[bool, Optional[str]]:
        """
        AUTHORITATIVE IDEMPOTENCY CHECK
        
        Determines if a payout should be skipped during sync.
        
        Per the Payout State Model (see module docstring):
        - A payout is "synced" if status IN ('pending', 'complete', 'posted')
        - A payout with status='failed' is NOT synced — it can be retried
        - A payout that doesn't exist is NOT synced
        
        Args:
            processor_payout_id: The processor's payout ID (e.g., po_xxx for Stripe)
            
        Returns:
            Tuple of (is_synced: bool, current_status: str or None)
            - (True, 'complete') - Already synced, skip
            - (True, 'posted') - Already deposited, skip  
            - (True, 'pending') - Sync in progress, skip
            - (False, 'failed') - Previous attempt failed, OK to retry
            - (False, None) - Never seen before, OK to sync
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT status FROM payout_batches 
            WHERE processor = ? AND processor_payout_id = ?
        """, (self.processor_name, processor_payout_id))
        
        row = cursor.fetchone()
        
        if row is None:
            # Never seen this payout
            return (False, None)
        
        status = row['status']
        
        # Only 'failed' status allows retry
        if status == 'failed':
            return (False, 'failed')
        
        # All other statuses mean "already synced"
        return (True, status)
    
    def _insert_payout_with_matches(
        self,
        payout: ProcessorPayout,
        matched_payments: List[Tuple[ProcessorPayment, Optional[Dict], Optional[MatchScoreBreakdown], List[MatchCandidate]]]
    ) -> int:
        """
        Insert a payout and all its matched payments into the database.

        Creates:
        - raw_events for each payment
        - matching_outcomes for each payment (with QBO Payment ID and score if matched)
        - payout_batches for the payout
        - match_reviews for MEDIUM/LOW confidence matches (for manual review UI)

        Args:
            payout: Complete ProcessorPayout
            matched_payments: List of (charge, qbo_payment, score, all_candidates) tuples

        Returns:
            Number of payments inserted
        """
        cursor = self.conn.cursor()

        try:
            canonical_event_ids = []
            match_reviews_to_create = []

            # Insert each payment with its QBO match and score
            for charge, qbo_payment, score, all_candidates in matched_payments:
                # all_candidates is populated when manual review is needed (MEDIUM/LOW or tie)
                needs_review = bool(all_candidates)
                raw_event_id, canonical_event_id = self._insert_payment(
                    cursor, charge, payout.processor_payout_id, qbo_payment, score,
                    needs_review=needs_review
                )
                canonical_event_ids.append(canonical_event_id)

                # Create match_review record if needs review (MEDIUM/LOW confidence OR tie)
                # all_candidates is populated when manual review is needed
                if score and all_candidates:
                    match_reviews_to_create.append({
                        'canonical_event_id': canonical_event_id,
                        'charge': charge,
                        'candidates': all_candidates
                    })

            # =================================================================
            # REFUND PROCESSING (INV-ANET-08, Section 4.5)
            # =================================================================
            # Per AUTHORIZE_NET_INTEGRATION_PLAN:
            # - Refunds link to original payment via refTransId (processor_payment_id)
            # - Refunds do NOT match to QBO Payments directly
            # - Refunds appear as negative lines on the Deposit
            # - Amounts stored positive (INV-ANET-13), sign applied during Deposit
            # =================================================================
            refund_event_ids = []
            if hasattr(payout, 'refunds') and payout.refunds:
                for refund in payout.refunds:
                    raw_event_id, canonical_event_id = self._insert_refund(
                        cursor, refund, payout.processor_payout_id
                    )
                    refund_event_ids.append(canonical_event_id)

                logger.info(
                    f"Inserted {len(refund_event_ids)} refunds for payout {payout.processor_payout_id}"
                )

            # Combine payment and refund IDs for the payout record
            all_event_ids = canonical_event_ids + refund_event_ids

            # Insert payout batch
            # Note: payment_count is payments only; refunds tracked via metadata
            payout_batch_id = str(uuid.uuid4())

            # Enrich metadata with refund info
            payout_metadata = payout.metadata.copy() if payout.metadata else {}
            payout_metadata['refund_count'] = len(refund_event_ids)
            payout_metadata['refund_total'] = str(payout.refund_amount) if hasattr(payout, 'refund_amount') else '0'

            cursor.execute("""
                INSERT INTO payout_batches (
                    id, workspace_id, processor, processor_payout_id,
                    gross_amount, fees, net_amount, currency,
                    settlement_date, arrival_date,
                    payment_count, payment_ids,
                    status, description, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'complete', ?, ?)
            """, (
                payout_batch_id,
                self.workspace_id,  # Phase 2: workspace scoping
                self.processor_name,
                payout.processor_payout_id,
                str(payout.gross_amount),
                str(payout.fees),
                str(payout.net_amount),
                payout.currency,
                payout.settlement_date.isoformat() if payout.settlement_date else None,
                payout.arrival_date.isoformat() if payout.arrival_date else None,
                len(matched_payments),  # Payment count (excludes refunds)
                json.dumps(all_event_ids),  # Includes both payments and refunds
                payout.description,
                json.dumps(payout_metadata)
            ))

            # Phase 3 Remediation: payout linkage via canonical_facts is already handled
            # by Canonicalizer (processor_payout_id set during canonicalization).
            # Legacy canonical_events linkage removed per PHASE3_CANONICAL_EVENTS_CONTRACT §3.3.

            # [TRANSITIONAL] Create match_review records for MEDIUM/LOW confidence matches
            # BINDING DECISION: Failure here MUST NOT roll back authoritative canonical_facts.
            for review_data in match_reviews_to_create:
                try:
                    charge = review_data['charge']
                    candidates = review_data['candidates']

                    # Build candidates JSON with all match info for review UI
                    # Include qbo_customer_id for prior match continuity on manual confirmation
                    candidates_json = json.dumps([
                        {
                            'qbo_payment_id': c.qbo_payment['Id'],
                            'qbo_amount': c.qbo_payment.get('TotalAmt', '0'),
                            'qbo_date': c.qbo_payment.get('TxnDate'),
                            'qbo_customer_email': c.qbo_payment.get('CustomerEmail'),
                            'qbo_customer_id': c.qbo_payment.get('CustomerRef'),  # For prior match continuity
                            'score': c.score.total,
                            'score_breakdown': c.score.to_dict()
                        }
                        for c in candidates
                    ])

                    review_id = str(uuid.uuid4())
                    charge_date = charge.processor_timestamp.date().isoformat() if charge.processor_timestamp else None

                    # =================================================================
                    # IDEMPOTENCY PROTECTION: Match Review Creation
                    # =================================================================
                    # Protected by: Payout-level idempotency via IdempotencyManager
                    # in sync_payouts() (see lines 1016-1056 above).
                    #
                    # Match reviews are created as part of the payout sync transaction:
                    # - If payout already synced (IdempotencyStatus.EXISTS_COMPLETED),
                    #   this code path is never reached
                    # - If payout sync fails, entire transaction rolls back
                    # - canonical_event_id is unique per payment (tied to raw_event)
                    #
                    # This guarantees exactly-once match creation per Phase A compliance.
                    # =================================================================
                    cursor.execute("""
                        INSERT INTO match_reviews (
                            id, workspace_id, canonical_event_id, processor_payment_id,
                            amount, charge_date, customer_email, card_last4,
                            match_candidates, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                    """, (
                        review_id,
                        self.workspace_id,
                        review_data['canonical_event_id'],
                        charge.processor_payment_id,
                        str(charge.amount_gross),
                        charge_date,
                        charge.customer_email,
                        charge.card_last4,
                        candidates_json
                    ))

                    logger.info(
                        f"Created match_review {review_id} for charge {charge.processor_payment_id} "
                        f"with {len(candidates)} candidates"
                    )
                except Exception as e:
                    logger.warning(f"[TRANSITIONAL] Legacy match_review creation failed: {e}")

            self.conn.commit()

            logger.info(
                f"Inserted payout {payout.processor_payout_id} with "
                f"{len(canonical_event_ids)} payments, "
                f"{len(refund_event_ids)} refunds, "
                f"{len(match_reviews_to_create)} pending reviews"
            )

            return len(canonical_event_ids) + len(refund_event_ids)

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert payout: {e}")
            raise

    # =========================================================================
    # PHASE 3: PURE CANONICALIZATION FUNCTIONS
    # =========================================================================
    # These functions implement PURE canonical event creation per
    # PHASE3_CANONICAL_EVENTS_CONTRACT.md. They:
    # - Read ONLY from processor transactions
    # - Write ONLY to raw_events and canonical_facts tables
    # - Have NO QBO side effects
    # - Have NO matching side effects
    # - Are deterministic and idempotent
    # =========================================================================

    def _create_canonical_payment(
        self,
        cursor: sqlite3.Cursor,
        payment: ProcessorPayment,
        processor_payout_id: str,
    ) -> Tuple[str, str, str]:
        """
        Create a PURE canonical payment event.

        Phase 3 (B3 Remediation): Routes through Canonicalizer as the SINGLE
        AUTHORITATIVE canonical creation engine.

        Per PHASE3_CANONICAL_EVENTS_CONTRACT.md:
        - DOES NOT read QBO state
        - DOES NOT perform matching
        - DOES NOT set readiness/status flags
        - DOES NOT write to matching tables
        - IS deterministic and idempotent

        Args:
            cursor: Database cursor (transaction managed by caller)
            payment: ProcessorPayment containing processor data
            processor_payout_id: Parent payout identifier

        Returns:
            Tuple of (raw_event_id, canonical_event_id, canonical_event_id)
            Note: canonical_event_id and canonical_fact_id are now the SAME
            (deterministic SHA256 ID per CE-06)
        """
        raw_event_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # CE-06: Generate DETERMINISTIC canonical event ID
        # This ID will be used in both raw_events and canonical_facts
        canonical_event_id = generate_deterministic_canonical_id(
            workspace_id=self.workspace_id,
            processor=self.processor_name,
            processor_transaction_id=payment.processor_payment_id,
            event_type='payment',
        )

        # Create raw event payload - PURE processor data only
        # Note: NO qbo_payment_id or match_score in canonical payload
        payload = json.dumps({
            'processor_payment_id': payment.processor_payment_id,
            'processor_customer_id': payment.processor_customer_id,
            'amount_gross_cents': int(payment.amount_gross * 100),
            'fees_cents': int(payment.fees * 100),
            'amount_net_cents': int(payment.amount_net * 100),
            'currency': payment.currency,
            'customer_email': payment.customer_email,
            'customer_name': payment.customer_name,
            'payment_method': payment.payment_method,
            'card_brand': payment.card_brand,
            'card_last4': payment.card_last4,
            'description': payment.description,
            'metadata': payment.metadata
        }, default=str)

        checksum = hashlib.sha256(payload.encode()).hexdigest()

        # Insert raw event (audit trail)
        cursor.execute("""
            INSERT INTO raw_events (
                id, workspace_id, processor, processor_event_id, event_type, event_category,
                payload, payload_format, checksum, received_at,
                signature, signature_verified, processed, canonical_event_id
            ) VALUES (?, ?, ?, ?, 'charge.succeeded', 'payment', ?, 'json', ?, ?, '', 1, 1, ?)
        """, (
            raw_event_id,
            self.workspace_id,
            self.processor_name,
            payment.processor_payment_id,
            payload,
            checksum,
            now.isoformat(),
            canonical_event_id
        ))

        # =================================================================
        # B3 REMEDIATION: Route through Canonicalizer (AUTHORITATIVE engine)
        # =================================================================
        # The Canonicalizer is the SINGLE AUTHORITATIVE Phase 3 engine.
        # It generates the same deterministic ID and handles idempotency.
        # =================================================================
        canonicalizer = Canonicalizer(
            workspace_id=self.workspace_id,
            processor=self.processor_name,
        )

        result = canonicalizer.canonicalize_payment(
            cursor=cursor,
            raw_event_id=raw_event_id,
            processor_payment_id=payment.processor_payment_id,
            amount_gross=payment.amount_gross,
            amount_net=payment.amount_net,
            fees=payment.fees,
            currency=payment.currency,
            processor_payout_id=processor_payout_id,
            processor_timestamp=payment.processor_timestamp or now,
            processor_customer_id=payment.processor_customer_id,
            customer_email=payment.customer_email,
            customer_name=payment.customer_name,
            payment_method=payment.payment_method,
            card_brand=payment.card_brand,
            card_last4=payment.card_last4,
            description=payment.description,
            metadata=payment.metadata,
        )

        # The canonical_event_id from Canonicalizer should match what we generated
        # (both use the same deterministic formula)
        assert result.canonical_event_id == canonical_event_id, (
            f"ID mismatch: raw_events has {canonical_event_id[:16]}... "
            f"but Canonicalizer returned {result.canonical_event_id[:16]}..."
        )

        # Return (raw_event_id, canonical_event_id, canonical_fact_id)
        # Note: canonical_event_id == canonical_fact_id now (deterministic)
        return raw_event_id, canonical_event_id, result.canonical_event_id

    def _create_canonical_refund(
        self,
        cursor: sqlite3.Cursor,
        refund: ProcessorRefund,
        processor_payout_id: str,
    ) -> Tuple[str, str, str]:
        """
        Create a PURE canonical refund event.

        Phase 3 (B3 Remediation): Routes through Canonicalizer as the SINGLE
        AUTHORITATIVE canonical creation engine.

        Per PHASE3_CANONICAL_EVENTS_CONTRACT.md:
        - DOES NOT read QBO state
        - DOES NOT perform matching
        - DOES NOT set readiness/status flags
        - DOES NOT write to matching tables
        - IS deterministic and idempotent

        Args:
            cursor: Database cursor (transaction managed by caller)
            refund: ProcessorRefund containing processor data
            processor_payout_id: Parent payout identifier

        Returns:
            Tuple of (raw_event_id, canonical_event_id, canonical_event_id)
            Note: canonical_event_id and canonical_fact_id are now the SAME
            (deterministic SHA256 ID per CE-06)
        """
        raw_event_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # CE-06: Generate DETERMINISTIC canonical event ID
        # This ID will be used in both raw_events and canonical_facts
        canonical_event_id = generate_deterministic_canonical_id(
            workspace_id=self.workspace_id,
            processor=self.processor_name,
            processor_transaction_id=refund.processor_refund_id,
            event_type='refund',
        )

        # Create raw event payload - PURE processor data only
        payload = json.dumps({
            'processor_refund_id': refund.processor_refund_id,
            'processor_payment_id': refund.processor_payment_id,
            'amount_cents': int(refund.amount * 100),
            'currency': refund.currency,
            'refund_date': refund.refund_date.isoformat() if refund.refund_date else None,
            'status': refund.status.value if hasattr(refund.status, 'value') else str(refund.status)
        }, default=str)

        checksum = hashlib.sha256(payload.encode()).hexdigest()

        # Insert raw event
        cursor.execute("""
            INSERT INTO raw_events (
                id, workspace_id, processor, processor_event_id, event_type, event_category,
                payload, payload_format, checksum, received_at,
                signature, signature_verified, processed, canonical_event_id
            ) VALUES (?, ?, ?, ?, 'refund.created', 'refund', ?, 'json', ?, ?, '', 1, 1, ?)
        """, (
            raw_event_id,
            self.workspace_id,
            self.processor_name,
            refund.processor_refund_id,
            payload,
            checksum,
            now.isoformat(),
            canonical_event_id
        ))

        # =================================================================
        # B3 REMEDIATION: Route through Canonicalizer (AUTHORITATIVE engine)
        # =================================================================
        canonicalizer = Canonicalizer(
            workspace_id=self.workspace_id,
            processor=self.processor_name,
        )

        result = canonicalizer.canonicalize_refund(
            cursor=cursor,
            raw_event_id=raw_event_id,
            processor_refund_id=refund.processor_refund_id,
            original_payment_id=refund.processor_payment_id,
            amount=refund.amount,
            currency=refund.currency,
            processor_payout_id=processor_payout_id,
            refund_timestamp=refund.refund_date or now,
            customer_email=refund.customer_email,
            customer_name=refund.customer_name,
            payment_method=refund.payment_method,
            card_brand=refund.card_brand,
            card_last4=refund.card_last4,
            metadata=None,
        )

        # The canonical_event_id from Canonicalizer should match what we generated
        assert result.canonical_event_id == canonical_event_id, (
            f"ID mismatch: raw_events has {canonical_event_id[:16]}... "
            f"but Canonicalizer returned {result.canonical_event_id[:16]}..."
        )

        # Return (raw_event_id, canonical_event_id, canonical_fact_id)
        # Note: canonical_event_id == canonical_fact_id now (deterministic)
        return raw_event_id, canonical_event_id, result.canonical_event_id

    # =========================================================================
    # PUBLIC INSERTION METHODS
    # =========================================================================
    # Phase 3 Remediation: Legacy dual-write methods removed.
    # _write_legacy_canonical_event_payment() — REMOVED
    # _write_legacy_canonical_event_refund() — REMOVED
    # Matching outcomes now written to dedicated matching_outcomes table
    # per PHASE3_CANONICAL_EVENTS_CONTRACT §6.2.
    # =========================================================================

    def _insert_payment(
        self,
        cursor: sqlite3.Cursor,
        payment: ProcessorPayment,
        processor_payout_id: str,
        qbo_payment: Optional[Dict] = None,
        score: Optional[MatchScoreBreakdown] = None,
        needs_review: bool = False
    ) -> Tuple[str, str]:
        """
        Insert a single payment into the database.

        Phase 3 Remediated Architecture:
        Step 1: PURE CANONICALIZATION (via _create_canonical_payment)
            - Creates raw_events record (immutable audit)
            - Creates canonical_facts record (AUTHORITATIVE, QBO-free, match-free)

        Step 2: MATCHING OUTCOME (dedicated matching_outcomes table)
            - Per PHASE3_CANONICAL_EVENTS_CONTRACT §6.2: matching in Matching Table
            - Records match decisions in matching_outcomes (NOT canonical tables)
            - Records confirmed pairs for future matching

        Args:
            cursor: Database cursor
            payment: ProcessorPayment to insert
            processor_payout_id: Parent payout ID
            qbo_payment: Full QBO Payment dict if matched (contains Id, TotalAmt, TxnDate, etc.)
            score: Match score breakdown if matched
            needs_review: If True, don't auto-match even if score is HIGH (e.g., tie scenario)

        Returns:
            Tuple of (raw_event_id, canonical_event_id)
        """
        # =================================================================
        # STEP 1: PURE CANONICALIZATION (Phase 3 - AUTHORITATIVE)
        # =================================================================
        raw_event_id, canonical_event_id, canonical_fact_id = self._create_canonical_payment(
            cursor=cursor,
            payment=payment,
            processor_payout_id=processor_payout_id,
        )

        # Store canonical_fact_id in payment metadata for traceability
        if payment.metadata is None:
            payment.metadata = {}
        payment.metadata['canonical_fact_id'] = canonical_fact_id

        # =================================================================
        # STEP 2: MATCHING OUTCOME (§6.2 — dedicated matching table)
        # Per PHASE3_CANONICAL_EVENTS_CONTRACT: matching data MUST NOT
        # reside in canonical tables. Write to matching_outcomes instead.
        # =================================================================
        qbo_payment_id = qbo_payment['Id'] if qbo_payment else None

        if score and qbo_payment_id:
            match_confidence = score.total
            if score.confidence_level == 'high' and self.auto_match_enabled and not needs_review:
                match_type = 'auto'
                effective_qbo_payment_id = qbo_payment_id
                workflow_status = 'matched'

                # Record confirmed pair for prior match continuity
                qbo_customer_id = qbo_payment.get('CustomerRef') if qbo_payment else None
                if payment.processor_customer_id and qbo_customer_id:
                    record_confirmed_pair(
                        db_conn=self.conn,
                        workspace_id=self.workspace_id,
                        processor_type=self.processor_name,
                        processor_customer_id=payment.processor_customer_id,
                        qbo_customer_id=qbo_customer_id,
                        match_source='auto',
                        canonical_event_id=canonical_event_id
                    )
            else:
                match_type = 'pending'
                effective_qbo_payment_id = None
                workflow_status = 'pending'
                match_confidence = score.total
        elif qbo_payment_id:
            match_type = 'pending'
            match_confidence = 55
            effective_qbo_payment_id = None
            workflow_status = 'pending'
        else:
            match_type = 'unmatched'
            match_confidence = None
            effective_qbo_payment_id = None
            workflow_status = 'pending'

        # Build score breakdown JSON
        score_breakdown_json = None
        if score:
            breakdown_dict = score.to_dict()
            if qbo_payment:
                breakdown_dict['matched_qbo_payment'] = {
                    'qbo_payment_id': qbo_payment.get('Id'),
                    'qbo_amount': qbo_payment.get('TotalAmt'),
                    'qbo_date': qbo_payment.get('TxnDate'),
                    'qbo_customer_email': qbo_payment.get('CustomerEmail'),
                }
            score_breakdown_json = json.dumps(breakdown_dict)

        # INSERT into matching_outcomes (dedicated matching table)
        cursor.execute("""
            INSERT OR IGNORE INTO matching_outcomes (
                id, workspace_id, canonical_fact_id, processor_payment_id,
                qbo_payment_id, match_type, match_confidence,
                match_score_breakdown, workflow_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(uuid.uuid4()),
            self.workspace_id,
            canonical_fact_id,
            payment.processor_payment_id,
            effective_qbo_payment_id,
            match_type,
            match_confidence,
            score_breakdown_json,
            workflow_status,
        ))

        return raw_event_id, canonical_event_id

    def _insert_refund(
        self,
        cursor: sqlite3.Cursor,
        refund: ProcessorRefund,
        processor_payout_id: str
    ) -> Tuple[str, str]:
        """
        Insert a single refund into the database.

        Phase 3 Remediated Architecture:
        Step 1: PURE CANONICALIZATION (via _create_canonical_refund)
            - Creates raw_events record (immutable audit)
            - Creates canonical_facts record (AUTHORITATIVE, QBO-free, match-free)

        Step 2: MATCHING OUTCOME (dedicated matching_outcomes table)
            - Per PHASE3_CANONICAL_EVENTS_CONTRACT §6.2: matching in Matching Table
            - Refunds get 'unmatched' outcome (they don't match to QBO directly)

        Args:
            cursor: Database cursor
            refund: ProcessorRefund to insert
            processor_payout_id: Parent payout ID

        Returns:
            Tuple of (raw_event_id, canonical_event_id)
        """
        # =================================================================
        # STEP 1: PURE CANONICALIZATION (Phase 3 - AUTHORITATIVE)
        # =================================================================
        raw_event_id, canonical_event_id, canonical_fact_id = self._create_canonical_refund(
            cursor=cursor,
            refund=refund,
            processor_payout_id=processor_payout_id,
        )

        # =================================================================
        # STEP 2: MATCHING OUTCOME (§6.2 — dedicated matching table)
        # Refunds don't match to QBO Payments directly. Record as unmatched.
        # =================================================================
        cursor.execute("""
            INSERT OR IGNORE INTO matching_outcomes (
                id, workspace_id, canonical_fact_id, processor_payment_id,
                match_type, workflow_status
            ) VALUES (?, ?, ?, ?, 'unmatched', 'pending')
        """, (
            str(uuid.uuid4()),
            self.workspace_id,
            canonical_fact_id,
            refund.processor_refund_id,
        ))

        logger.info(
            f"Inserted refund {refund.processor_refund_id} "
            f"for original payment {refund.processor_payment_id} "
            f"amount ${refund.amount}"
        )

        return raw_event_id, canonical_event_id

    def get_sync_status(self) -> Dict:
        """
        Get current sync status for this processor.
        
        Returns:
            Dict with status info:
            {
                'processor': str,
                'total_payouts': int,
                'pending_payouts': int,
                'posted_payouts': int,
                'total_payments': int,
                'last_sync': datetime or None
            }
        """
        cursor = self.conn.cursor()
        
        # Count payouts by status
        cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM payout_batches
            WHERE processor = ?
            GROUP BY status
        """, (self.processor_name,))
        
        status_counts = {row['status']: row['count'] for row in cursor.fetchall()}
        
        # Count total payments (Phase 3 Remediation: canonical_facts is authoritative)
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM canonical_facts cf
            WHERE cf.processor = ?
              AND cf.event_type IN ('payment', 'refund', 'ach_return')
        """, (self.processor_name,))
        
        total_payments = cursor.fetchone()['count']
        
        # Get last sync time
        cursor.execute("""
            SELECT MAX(created_at) as last_sync
            FROM payout_batches
            WHERE processor = ?
        """, (self.processor_name,))
        
        last_sync_row = cursor.fetchone()
        last_sync = last_sync_row['last_sync'] if last_sync_row else None
        
        return {
            'processor': self.processor_name,
            'total_payouts': sum(status_counts.values()),
            'pending_payouts': status_counts.get('complete', 0),
            'posted_payouts': status_counts.get('posted', 0),
            'failed_payouts': status_counts.get('failed', 0),
            'total_payments': total_payments,
            'last_sync': last_sync
        }
