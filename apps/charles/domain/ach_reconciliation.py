"""
Module 4: ACH Reconciliation Engine

Implements AUTHORIZE_NET_ACH_RECONCILIATION_PLAN and AUTHORIZE_NET_ACH_RECONCILIATION_INVARIANTS.

KEY PRINCIPLES:
- ACH reconciliation is DEPOSIT-DRIVEN (INV-ACH-01)
- ACH payouts are NEVER auto-matched (INV-ACH-07, INV-ACH-08)
- All ACH items surface in Needs Review (INV-ACH-07)
- Ranking is for PRESENTATION ORDER only (§6)
- User confirmation is ALWAYS required (INV-ACH-08)

This module handles:
1. Finding unmatched QBO deposits that may be ACH transfers
2. Identifying eligible ACH payout batches as candidates
3. Ranking candidates for presentation (NOT matching)
4. Processing user-confirmed ACH reconciliations
"""

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from enum import Enum

from shared.structured_logging import get_logger, EventType


# =============================================================================
# CONSTANTS (per AUTHORIZE_NET_ACH_RECONCILIATION_PLAN)
# =============================================================================

# ACH lookback window in business days (INV-ACH-03)
ACH_LOOKBACK_BUSINESS_DAYS = 10

# Fee tolerance for amount matching (to account for fee variance)
ACH_FEE_TOLERANCE = Decimal('15.00')

# Minimum plausible fee (fees below this are suspicious)
ACH_MIN_PLAUSIBLE_FEE = Decimal('0.00')

# Maximum plausible fee per batch (fees above this are suspicious)
ACH_MAX_PLAUSIBLE_FEE = Decimal('100.00')


class ACHReviewStatus(Enum):
    """Status of an ACH needs review item."""
    PENDING = 'pending'           # Awaiting user review
    CONFIRMED = 'confirmed'       # User confirmed the match
    REJECTED = 'rejected'         # User rejected (no valid batch)
    SKIPPED = 'skipped'           # User deferred decision


@dataclass
class ACHCandidate:
    """
    A candidate ACH payout batch for explaining a deposit.

    Per §6: Ranking exists ONLY for presentation order, not matching.
    """
    # Batch identification
    processor_payout_id: str
    batch_id: str
    settlement_date: date

    # Amounts
    gross_amount: Decimal              # Sum of line items in batch
    implied_fee: Decimal               # batch_gross - deposit_amount

    # Ranking signals (§6.2, §6.3)
    amount_explanation_score: float    # Primary: how well amount explains deposit
    date_proximity_score: float        # Secondary: batch date vs deposit date
    identity_continuity_score: float   # Secondary: customer/email overlap
    uniqueness_score: float            # Secondary: collision avoidance

    # Composite score for ordering (NOT for auto-matching)
    presentation_rank: float = 0.0

    # Eligibility status
    is_eligible: bool = True           # All transactions have QBO payments
    eligibility_reason: str = ''       # Why not eligible (if applicable)

    # Transaction details
    transaction_count: int = 0
    transactions: List[Dict] = field(default_factory=list)

    # Metadata
    metadata: Dict = field(default_factory=dict)

    def __post_init__(self):
        """Calculate composite presentation rank."""
        # Primary signal: amount explanation (weighted 70%)
        # Secondary signals: date proximity (15%), identity (10%), uniqueness (5%)
        self.presentation_rank = (
            self.amount_explanation_score * 0.70 +
            self.date_proximity_score * 0.15 +
            self.identity_continuity_score * 0.10 +
            self.uniqueness_score * 0.05
        )


@dataclass
class ACHNeedsReviewItem:
    """
    An ACH item requiring user review.

    Per INV-ACH-07: All ACH deposit reconciliations MUST surface in Needs Review.
    """
    # Review item identification
    id: str
    workspace_id: str
    created_at: datetime

    # Driver: The unmatched QBO deposit (INV-ACH-01)
    qbo_deposit_id: str
    qbo_deposit_date: date
    qbo_deposit_amount: Decimal
    qbo_deposit_memo: Optional[str] = None

    # Candidates: Ranked ACH batches (§6)
    candidates: List[ACHCandidate] = field(default_factory=list)

    # Status
    status: ACHReviewStatus = ACHReviewStatus.PENDING

    # Resolution (if confirmed)
    confirmed_batch_id: Optional[str] = None
    confirmed_fee_amount: Optional[Decimal] = None
    confirmed_at: Optional[datetime] = None
    confirmed_by: Optional[str] = None


class ACHReconciliationEngine:
    """
    ACH Reconciliation Engine.

    Implements deposit-driven, non-deterministic reconciliation per:
    - AUTHORIZE_NET_ACH_RECONCILIATION_PLAN
    - AUTHORIZE_NET_ACH_RECONCILIATION_INVARIANTS

    CRITICAL INVARIANTS:
    - INV-ACH-01: Deposit-driven reconciliation (deposits are the driver)
    - INV-ACH-02: Individual payments as required evidence
    - INV-ACH-05: Transfer-time fee inference (not attribution)
    - INV-ACH-06: No automatic fee posting
    - INV-ACH-07: Needs Review is mandatory
    - INV-ACH-08: Explicit user action required
    - INV-ACH-10: Determinism over convenience
    """

    def __init__(self, db_connection, workspace_id: str, company_id: str):
        """
        Initialize ACH reconciliation engine.

        Args:
            db_connection: Database connection
            workspace_id: Workspace ID for scoping
            company_id: Company ID
        """
        self.conn = db_connection
        self.workspace_id = workspace_id
        self.company_id = company_id
        self.logger = get_logger(
            service="ach_reconciliation_engine",
            workspace_id=workspace_id,
            company_id=company_id
        )

    # =========================================================================
    # ELIGIBILITY GATE (§5 - Mandatory)
    # =========================================================================

    def check_batch_eligibility(self, processor_payout_id: str) -> Tuple[bool, str, List[Dict]]:
        """
        Check if an ACH batch is eligible for reconciliation.

        Per §5: Before any ACH payout batch may be presented as a candidate,
        EVERY individual ACH transaction in the batch must be associable with
        an existing QBO payment.

        Args:
            processor_payout_id: The ACH batch payout ID

        Returns:
            Tuple of (is_eligible, reason, transactions)
            - is_eligible: True if all transactions have QBO payments
            - reason: Description if not eligible
            - transactions: List of transaction details with match status
        """
        cursor = self.conn.cursor()

        # Get all transactions in this batch
        cursor.execute("""
            SELECT
                cf.id,
                cf.processor_payment_id,
                cf.amount_gross,
                cf.customer_email,
                cf.customer_name,
                mo.qbo_payment_id,
                mo.match_type,
                mo.match_confidence,
                cf.event_type
            FROM canonical_facts cf
            LEFT JOIN matching_outcomes mo ON mo.canonical_fact_id = cf.id
            WHERE cf.processor_payout_id = ?
              AND cf.workspace_id = ?
              AND cf.event_type = 'payment'
            ORDER BY cf.processor_timestamp
        """, (processor_payout_id, self.workspace_id))

        transactions = [dict(row) for row in cursor.fetchall()]

        if not transactions:
            return False, "No transactions found in batch", []

        # Check each transaction for QBO payment match
        unmatched = []
        for txn in transactions:
            if not txn.get('qbo_payment_id'):
                unmatched.append(txn['processor_payment_id'])

        if unmatched:
            reason = f"{len(unmatched)} of {len(transactions)} transactions not matched to QBO payments"
            self.logger.info(
                EventType.PAYOUT_PROCESSING_STARTED,
                f"ACH batch {processor_payout_id} ineligible: {reason}",
                processor_payout_id=processor_payout_id,
                total_transactions=len(transactions),
                unmatched_count=len(unmatched),
                unmatched_ids=unmatched[:5]  # Log first 5
            )
            return False, reason, transactions

        return True, "All transactions matched", transactions

    # =========================================================================
    # CANDIDATE IDENTIFICATION
    # =========================================================================

    def find_ach_batches_in_window(
        self,
        deposit_date: date,
        deposit_amount: Decimal
    ) -> List[Dict]:
        """
        Find ACH payout batches within the lookback window.

        Per INV-ACH-03: Look back up to N business days from deposit date.

        Args:
            deposit_date: QBO deposit date (the driver)
            deposit_amount: QBO deposit amount

        Returns:
            List of ACH batch records with their transactions
        """
        cursor = self.conn.cursor()

        # Calculate lookback window (business days approximation)
        # Add buffer for weekends: 10 business days ≈ 14 calendar days
        calendar_days = int(ACH_LOOKBACK_BUSINESS_DAYS * 1.4)
        window_start = deposit_date - timedelta(days=calendar_days)

        # Find ACH batches (payout_type = 'ach_transfer')
        cursor.execute("""
            SELECT
                pb.id,
                pb.processor_payout_id,
                pb.settlement_date,
                pb.gross_amount,
                pb.net_amount,
                pb.fees,
                pb.payment_count,
                pb.metadata,
                pb.status
            FROM payout_batches pb
            WHERE pb.workspace_id = ?
              AND pb.processor = 'authorize_net'
              AND pb.qbo_deposit_id IS NULL
              AND pb.settlement_date >= ?
              AND pb.settlement_date <= ?
              AND (
                  json_extract(pb.metadata, '$.payout_type') = 'ach_transfer'
                  OR pb.processor_payout_id LIKE 'ach_transfer_%'
              )
            ORDER BY pb.settlement_date DESC
        """, (self.workspace_id, window_start.isoformat(), deposit_date.isoformat()))

        batches = [dict(row) for row in cursor.fetchall()]

        self.logger.info(
            EventType.SYNC_STARTED,
            f"Found {len(batches)} ACH batches in {calendar_days}-day window for deposit {deposit_amount}",
            deposit_date=deposit_date.isoformat(),
            deposit_amount=str(deposit_amount),
            window_start=window_start.isoformat(),
            batch_count=len(batches)
        )

        return batches

    # =========================================================================
    # CANDIDATE RANKING (§6 - Non-Authoritative)
    # =========================================================================

    def rank_candidates(
        self,
        deposit_amount: Decimal,
        deposit_date: date,
        batches: List[Dict]
    ) -> List[ACHCandidate]:
        """
        Rank ACH batch candidates for presentation.

        Per §6: Ranking exists SOLELY to present the most plausible explanations
        first. Ranking does NOT imply correctness and NEVER triggers auto-match.

        Primary signal (§6.2): Amount explanation
        Secondary signals (§6.3): Date proximity, identity continuity, uniqueness

        Args:
            deposit_amount: QBO deposit amount (the driver)
            deposit_date: QBO deposit date
            batches: List of candidate ACH batches

        Returns:
            List of ACHCandidate, ranked by presentation_rank (descending)
        """
        candidates = []

        for batch in batches:
            # Check eligibility (§5)
            is_eligible, reason, transactions = self.check_batch_eligibility(
                batch['processor_payout_id']
            )

            # Parse metadata
            metadata = json.loads(batch.get('metadata', '{}')) if isinstance(batch.get('metadata'), str) else batch.get('metadata', {})

            # Calculate amounts
            gross_amount = Decimal(str(batch['gross_amount']))
            implied_fee = gross_amount - deposit_amount

            # =================================================================
            # PRIMARY RANKING SIGNAL: Amount Explanation (§6.2)
            # =================================================================
            # Score based on how plausible the implied fee is
            amount_score = self._calculate_amount_explanation_score(
                gross_amount, deposit_amount, implied_fee
            )

            # =================================================================
            # SECONDARY SIGNALS (§6.3)
            # =================================================================

            # Date proximity score
            settlement_date = date.fromisoformat(batch['settlement_date']) if isinstance(batch['settlement_date'], str) else batch['settlement_date']
            date_score = self._calculate_date_proximity_score(
                settlement_date, deposit_date
            )

            # Identity continuity score (customer/email overlap)
            identity_score = self._calculate_identity_continuity_score(
                transactions, deposit_amount
            )

            # Uniqueness score (collision avoidance)
            uniqueness_score = self._calculate_uniqueness_score(
                gross_amount, deposit_amount, batches
            )

            candidate = ACHCandidate(
                processor_payout_id=batch['processor_payout_id'],
                batch_id=batch['id'],
                settlement_date=settlement_date,
                gross_amount=gross_amount,
                implied_fee=implied_fee,
                amount_explanation_score=amount_score,
                date_proximity_score=date_score,
                identity_continuity_score=identity_score,
                uniqueness_score=uniqueness_score,
                is_eligible=is_eligible,
                eligibility_reason=reason,
                transaction_count=len(transactions),
                transactions=transactions,
                metadata=metadata
            )

            candidates.append(candidate)

        # Sort by presentation rank (descending)
        # Per §6: Ranking is for PRESENTATION ORDER only
        candidates.sort(key=lambda c: (c.is_eligible, c.presentation_rank), reverse=True)

        self.logger.info(
            EventType.SYNC_STARTED,
            f"Ranked {len(candidates)} ACH candidates for deposit {deposit_amount}",
            deposit_amount=str(deposit_amount),
            eligible_count=sum(1 for c in candidates if c.is_eligible),
            ineligible_count=sum(1 for c in candidates if not c.is_eligible)
        )

        return candidates

    def _calculate_amount_explanation_score(
        self,
        gross_amount: Decimal,
        deposit_amount: Decimal,
        implied_fee: Decimal
    ) -> float:
        """
        Calculate amount explanation score (primary ranking signal).

        Per §6.2: How closely the batch's gross total minus implied fees
        explains the deposit amount.
        """
        # Perfect match: gross exactly explains deposit with plausible fee
        if implied_fee < ACH_MIN_PLAUSIBLE_FEE:
            # Fee too low (negative = deposit > gross, impossible)
            return 0.0

        if implied_fee > ACH_MAX_PLAUSIBLE_FEE:
            # Fee too high (suspicious)
            # Still possible, but lower score
            return max(0.0, 50.0 - float(implied_fee - ACH_MAX_PLAUSIBLE_FEE))

        # Fee is in plausible range
        # Score higher for fees closer to typical values ($1-$10)
        if implied_fee <= Decimal('10.00'):
            return 100.0
        elif implied_fee <= Decimal('25.00'):
            return 90.0
        elif implied_fee <= Decimal('50.00'):
            return 75.0
        else:
            return 60.0

    def _calculate_date_proximity_score(
        self,
        settlement_date: date,
        deposit_date: date
    ) -> float:
        """
        Calculate date proximity score (secondary signal).

        Per §6.3: Batch date relative to deposit date.
        ACH typically has 5-10 business day lag.
        """
        days_diff = (deposit_date - settlement_date).days

        if days_diff < 0:
            # Settlement after deposit (impossible for this deposit)
            return 0.0

        # Typical ACH funding: 5-10 business days ≈ 7-14 calendar days
        if 5 <= days_diff <= 10:
            return 100.0
        elif 3 <= days_diff <= 14:
            return 80.0
        elif 1 <= days_diff <= 20:
            return 50.0
        else:
            return 20.0

    def _calculate_identity_continuity_score(
        self,
        transactions: List[Dict],
        deposit_amount: Decimal
    ) -> float:
        """
        Calculate identity continuity score (secondary signal).

        Per §6.3: Overlap in customers, emails, or identifiers.
        """
        if not transactions:
            return 0.0

        # Count transactions with customer identity info
        with_email = sum(1 for t in transactions if t.get('customer_email'))
        with_name = sum(1 for t in transactions if t.get('customer_name'))

        total = len(transactions)
        identity_ratio = (with_email + with_name) / (total * 2)

        return identity_ratio * 100.0

    def _calculate_uniqueness_score(
        self,
        gross_amount: Decimal,
        deposit_amount: Decimal,
        all_batches: List[Dict]
    ) -> float:
        """
        Calculate uniqueness score (secondary signal).

        Per §6.3: Avoidance of collisions where multiple batches explain
        the same deposit equally well.
        """
        # Count batches with similar gross amounts (within tolerance)
        similar_count = sum(
            1 for b in all_batches
            if abs(Decimal(str(b['gross_amount'])) - gross_amount) <= ACH_FEE_TOLERANCE
        )

        if similar_count == 1:
            return 100.0  # Unique - high confidence
        elif similar_count == 2:
            return 50.0   # One collision
        else:
            return 20.0   # Multiple collisions

    # =========================================================================
    # NEEDS REVIEW ITEMS
    # =========================================================================

    def get_ach_needs_review_items(self) -> List[ACHNeedsReviewItem]:
        """
        Get all ACH items that need review.

        Per INV-ACH-07: All ACH deposit reconciliations MUST surface in
        Needs Review unless deterministic evidence exists.

        Returns:
            List of ACHNeedsReviewItem with ranked candidates
        """
        cursor = self.conn.cursor()

        # Get pending ACH review items
        cursor.execute("""
            SELECT *
            FROM ach_needs_review
            WHERE workspace_id = ?
              AND status = 'pending'
            ORDER BY qbo_deposit_date DESC
        """, (self.workspace_id,))

        items = []
        for row in cursor.fetchall():
            row_dict = dict(row)

            # Parse candidates from JSON
            candidates_json = json.loads(row_dict.get('candidates', '[]'))
            candidates = [
                ACHCandidate(**c) if isinstance(c, dict) else c
                for c in candidates_json
            ]

            item = ACHNeedsReviewItem(
                id=row_dict['id'],
                workspace_id=row_dict['workspace_id'],
                created_at=datetime.fromisoformat(row_dict['created_at']) if isinstance(row_dict['created_at'], str) else row_dict['created_at'],
                qbo_deposit_id=row_dict['qbo_deposit_id'],
                qbo_deposit_date=date.fromisoformat(row_dict['qbo_deposit_date']) if isinstance(row_dict['qbo_deposit_date'], str) else row_dict['qbo_deposit_date'],
                qbo_deposit_amount=Decimal(str(row_dict['qbo_deposit_amount'])),
                qbo_deposit_memo=row_dict.get('qbo_deposit_memo'),
                candidates=candidates,
                status=ACHReviewStatus(row_dict['status'])
            )
            items.append(item)

        return items

    def create_ach_review_item(
        self,
        qbo_deposit_id: str,
        qbo_deposit_date: date,
        qbo_deposit_amount: Decimal,
        qbo_deposit_memo: Optional[str] = None
    ) -> ACHNeedsReviewItem:
        """
        Create a new ACH needs review item for a QBO deposit.

        Per INV-ACH-01: Bank deposits are the sole reconciliation driver.

        Args:
            qbo_deposit_id: QBO deposit ID (the driver)
            qbo_deposit_date: Deposit date
            qbo_deposit_amount: Deposit amount
            qbo_deposit_memo: Optional memo/description

        Returns:
            Created ACHNeedsReviewItem with ranked candidates
        """
        import uuid

        cursor = self.conn.cursor()

        # Check if review item already exists for this deposit
        cursor.execute("""
            SELECT id FROM ach_needs_review
            WHERE qbo_deposit_id = ? AND workspace_id = ?
        """, (qbo_deposit_id, self.workspace_id))

        existing = cursor.fetchone()
        if existing:
            self.logger.info(
                EventType.QBO_OBJECT_CREATION_SKIPPED,
                f"ACH review item already exists for deposit {qbo_deposit_id}",
                qbo_deposit_id=qbo_deposit_id
            )
            # Return existing item
            return self.get_ach_review_item(existing['id'])

        # Find candidate batches
        batches = self.find_ach_batches_in_window(qbo_deposit_date, qbo_deposit_amount)

        # Rank candidates
        candidates = self.rank_candidates(qbo_deposit_amount, qbo_deposit_date, batches)

        # Create review item
        item_id = str(uuid.uuid4())
        now = datetime.utcnow()

        # Serialize candidates for storage
        candidates_json = json.dumps([
            {
                'processor_payout_id': c.processor_payout_id,
                'batch_id': c.batch_id,
                'settlement_date': c.settlement_date.isoformat(),
                'gross_amount': str(c.gross_amount),
                'implied_fee': str(c.implied_fee),
                'amount_explanation_score': c.amount_explanation_score,
                'date_proximity_score': c.date_proximity_score,
                'identity_continuity_score': c.identity_continuity_score,
                'uniqueness_score': c.uniqueness_score,
                'presentation_rank': c.presentation_rank,
                'is_eligible': c.is_eligible,
                'eligibility_reason': c.eligibility_reason,
                'transaction_count': c.transaction_count,
                'transactions': c.transactions,
                'metadata': c.metadata
            }
            for c in candidates
        ])

        cursor.execute("""
            INSERT INTO ach_needs_review (
                id, workspace_id, qbo_deposit_id, qbo_deposit_date,
                qbo_deposit_amount, qbo_deposit_memo, candidates, status,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item_id, self.workspace_id, qbo_deposit_id,
            qbo_deposit_date.isoformat(), str(qbo_deposit_amount),
            qbo_deposit_memo, candidates_json, ACHReviewStatus.PENDING.value,
            now.isoformat(), now.isoformat()
        ))

        self.conn.commit()

        self.logger.info(
            EventType.SYNC_STARTED,
            f"Created ACH review item {item_id} for deposit {qbo_deposit_id}",
            ach_review_id=item_id,
            qbo_deposit_id=qbo_deposit_id,
            qbo_deposit_amount=str(qbo_deposit_amount),
            candidate_count=len(candidates),
            eligible_candidates=sum(1 for c in candidates if c.is_eligible)
        )

        return ACHNeedsReviewItem(
            id=item_id,
            workspace_id=self.workspace_id,
            created_at=now,
            qbo_deposit_id=qbo_deposit_id,
            qbo_deposit_date=qbo_deposit_date,
            qbo_deposit_amount=qbo_deposit_amount,
            qbo_deposit_memo=qbo_deposit_memo,
            candidates=candidates,
            status=ACHReviewStatus.PENDING
        )

    def get_ach_review_item(self, item_id: str) -> Optional[ACHNeedsReviewItem]:
        """Get a single ACH review item by ID."""
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT * FROM ach_needs_review
            WHERE id = ? AND workspace_id = ?
        """, (item_id, self.workspace_id))

        row = cursor.fetchone()
        if not row:
            return None

        row_dict = dict(row)
        candidates_json = json.loads(row_dict.get('candidates', '[]'))

        return ACHNeedsReviewItem(
            id=row_dict['id'],
            workspace_id=row_dict['workspace_id'],
            created_at=datetime.fromisoformat(row_dict['created_at']) if isinstance(row_dict['created_at'], str) else row_dict['created_at'],
            qbo_deposit_id=row_dict['qbo_deposit_id'],
            qbo_deposit_date=date.fromisoformat(row_dict['qbo_deposit_date']) if isinstance(row_dict['qbo_deposit_date'], str) else row_dict['qbo_deposit_date'],
            qbo_deposit_amount=Decimal(str(row_dict['qbo_deposit_amount'])),
            qbo_deposit_memo=row_dict.get('qbo_deposit_memo'),
            candidates=[ACHCandidate(**c) for c in candidates_json] if candidates_json else [],
            status=ACHReviewStatus(row_dict['status']),
            confirmed_batch_id=row_dict.get('confirmed_batch_id'),
            confirmed_fee_amount=Decimal(str(row_dict['confirmed_fee_amount'])) if row_dict.get('confirmed_fee_amount') else None,
            confirmed_at=datetime.fromisoformat(row_dict['confirmed_at']) if row_dict.get('confirmed_at') else None,
            confirmed_by=row_dict.get('confirmed_by')
        )

    # =========================================================================
    # USER CONFIRMATION (INV-ACH-08)
    # =========================================================================

    def confirm_ach_reconciliation(
        self,
        review_item_id: str,
        selected_batch_id: str,
        confirmed_fee: Decimal,
        confirmed_by: str
    ) -> Dict:
        """
        Confirm an ACH reconciliation match.

        Per INV-ACH-08: Explicit user action required.
        The user must explicitly confirm:
        - Which QBO payments explain the deposit
        - The fee amount to be recorded (editable)

        Args:
            review_item_id: ACH review item ID
            selected_batch_id: The processor_payout_id user selected
            confirmed_fee: User-confirmed fee amount (editable per §7.3)
            confirmed_by: User who confirmed

        Returns:
            Dict with confirmation result and next steps
        """
        cursor = self.conn.cursor()

        # Get review item
        item = self.get_ach_review_item(review_item_id)
        if not item:
            raise ValueError(f"ACH review item {review_item_id} not found")

        if item.status != ACHReviewStatus.PENDING:
            raise ValueError(f"ACH review item {review_item_id} is not pending")

        # Find the selected candidate
        selected_candidate = None
        for candidate in item.candidates:
            if candidate.processor_payout_id == selected_batch_id:
                selected_candidate = candidate
                break

        if not selected_candidate:
            raise ValueError(f"Selected batch {selected_batch_id} not in candidates")

        if not selected_candidate.is_eligible:
            raise ValueError(
                f"Selected batch {selected_batch_id} is not eligible: "
                f"{selected_candidate.eligibility_reason}"
            )

        now = datetime.utcnow()

        # Update review item status
        cursor.execute("""
            UPDATE ach_needs_review
            SET status = ?,
                confirmed_batch_id = ?,
                confirmed_fee_amount = ?,
                confirmed_at = ?,
                confirmed_by = ?,
                updated_at = ?
            WHERE id = ? AND workspace_id = ?
        """, (
            ACHReviewStatus.CONFIRMED.value,
            selected_batch_id,
            str(confirmed_fee),
            now.isoformat(),
            confirmed_by,
            now.isoformat(),
            review_item_id,
            self.workspace_id
        ))

        # Get current payout batch metadata and update with confirmed fee
        cursor.execute("""
            SELECT metadata FROM payout_batches
            WHERE processor_payout_id = ? AND workspace_id = ?
        """, (selected_batch_id, self.workspace_id))
        row = cursor.fetchone()
        existing_metadata = json.loads(row['metadata'] if row and row['metadata'] else '{}')

        # Update metadata with confirmed fee (per §7.3: user-confirmed, editable)
        # This fee will be used when creating the QBO expense
        existing_metadata['ach_fees'] = str(confirmed_fee)
        existing_metadata['ach_fee_confirmed'] = True
        existing_metadata['ach_fee_confirmed_at'] = now.isoformat()
        existing_metadata['ach_fee_confirmed_by'] = confirmed_by
        # Per §7.3: Fee description must include this note
        existing_metadata['ach_fee_description'] = "Fees withheld at moment of ACH transfer."

        # Update ACH batch metadata with confirmed fee (ready for deposit creation)
        # Per lifecycle decision: Confirm transitions to Ready for Deposit only
        # qbo_deposit_id is NOT set here - that happens at Create Deposit / Process All
        cursor.execute("""
            UPDATE payout_batches
            SET status = 'ready_for_deposit',
                metadata = ?,
                updated_at = ?
            WHERE processor_payout_id = ? AND workspace_id = ?
        """, (
            json.dumps(existing_metadata),
            now.isoformat(),
            selected_batch_id,
            self.workspace_id
        ))

        # Phase 3 Remediation: update matching_outcomes workflow_status (§6.2)
        cursor.execute("""
            UPDATE matching_outcomes
            SET workflow_status = 'ready_for_deposit',
                updated_at = ?
            WHERE canonical_fact_id IN (
                SELECT id FROM canonical_facts
                WHERE processor_payout_id = ? AND workspace_id = ?
            )
        """, (now.isoformat(), selected_batch_id, self.workspace_id))

        self.conn.commit()

        self.logger.info(
            EventType.PAYOUT_PROCESSING_STARTED,
            f"Confirmed ACH reconciliation: batch {selected_batch_id} ready for deposit",
            ach_review_id=review_item_id,
            bank_feed_deposit_id=item.qbo_deposit_id,
            processor_payout_id=selected_batch_id,
            confirmed_fee=str(confirmed_fee),
            confirmed_by=confirmed_by
        )

        return {
            'status': 'ready_for_deposit',
            'ach_review_id': review_item_id,
            'bank_feed_deposit_id': item.qbo_deposit_id,
            'processor_payout_id': selected_batch_id,
            'confirmed_fee': str(confirmed_fee),
            'gross_amount': str(selected_candidate.gross_amount),
            'transaction_count': selected_candidate.transaction_count,
            'confirmed_at': now.isoformat(),
            'confirmed_by': confirmed_by,
            'fee_description': "Fees withheld at moment of ACH transfer."
        }

    def reject_ach_reconciliation(
        self,
        review_item_id: str,
        rejected_by: str,
        reason: Optional[str] = None
    ) -> Dict:
        """
        Reject an ACH reconciliation (no valid batch matches).

        Args:
            review_item_id: ACH review item ID
            rejected_by: User who rejected
            reason: Optional rejection reason

        Returns:
            Dict with rejection result
        """
        cursor = self.conn.cursor()

        now = datetime.utcnow()

        cursor.execute("""
            UPDATE ach_needs_review
            SET status = ?,
                rejection_reason = ?,
                confirmed_by = ?,
                updated_at = ?
            WHERE id = ? AND workspace_id = ?
        """, (
            ACHReviewStatus.REJECTED.value,
            reason,
            rejected_by,
            now.isoformat(),
            review_item_id,
            self.workspace_id
        ))

        self.conn.commit()

        self.logger.info(
            EventType.WARNING_DETECTED,
            f"Rejected ACH reconciliation: {review_item_id}",
            ach_review_id=review_item_id,
            rejected_by=rejected_by,
            reason=reason
        )

        return {
            'status': 'rejected',
            'ach_review_id': review_item_id,
            'rejected_by': rejected_by,
            'reason': reason
        }
