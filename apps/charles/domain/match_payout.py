"""
Match Payout Engine — per MATCH_PAYOUT_UI_CONTRACT.md

Processor-driven matching: starts from processor payments (canonical_facts),
finds matches among unlinked QBO accounting transactions. Results are grouped
by settlement batch so Ready for Deposit mirrors processor payout batches.

Data sources (unified in canonical_facts):
  - Processor side: canonical_facts WHERE event_type IN (payment, refund, ach_return, payout)
  - Accounting side: canonical_facts WHERE event_type IN (payment_observed, refund_observed, deposit_observed)

Matching (processor → accounting):
  - Level A (Deterministic): processor_payment_id == payment_observed.metadata.ref_no
  - Level B (Probabilistic): Amount(40) + Date(25) + Customer(25) + Uniqueness(10)

Sections (authoritative order, §6):
  1. Ready for Deposit — grouped by settlement batch, identity resolved, amount known
  2. Needs Review — unmatched or low-confidence processor payments
  3. ACH Deposits — matched but amount-unknown, user enters expected deposit amount
  4. Deposits Already Created — read-only, excluded from matching

CONTRACT BOUNDARY — PERMITTED TABLES ONLY:
  READ:  canonical_facts, match_runs, match_run_results, ach_deposit_intents
  WRITE: match_runs, match_run_results, ach_deposit_intents

FORBIDDEN (contract violation if referenced):
  processor_transactions, payout_batches, canonical_events,
  raw_events, qbo_*, accounting system API tables

Authority: PHASE3_CANONICAL_EVENTS_CONTRACT (CERTIFIED & FROZEN),
           PHASE3B-ACCOUNTING_CANONICAL_FACTS_CONTRACT §Phase4,
           MATCH_PAYOUT_UI_CONTRACT §4.2
"""

import hashlib
import json
import logging
import sqlite3
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Scoring constants (reuses QBOPaymentMatcher approach from sync_manager.py)
AMOUNT_POINTS = 40          # Required gate — must match exactly
DATE_POINTS_MAX = 25        # Maximum date proximity points
CUSTOMER_POINTS = 25        # Customer name/email match (IDENTITY SIGNAL)
UNIQUENESS_BONUS = 10       # Only when identity is present
MAX_SCORE = 100

# Confidence tiers (§6.1, §6.2)
HIGH_CONFIDENCE = 80        # → Ready for Deposit
MEDIUM_CONFIDENCE = 50      # → Needs Review


def _generate_id() -> str:
    return str(uuid.uuid4())


def _parse_amount(val: Any) -> Optional[Decimal]:
    """Safely parse an amount string to Decimal."""
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None


def _calculate_date_score(proc_date_str: Optional[str], acct_date_str: Optional[str]) -> int:
    """
    Calculate date proximity score between processor and accounting dates.
    Reuses asymmetric logic from QBOPaymentMatcher._calculate_date_score.

    delta_days = acct_date - proc_date:
      >= 0: accounting on/after processor (normal bookkeeping lag)
      < 0: accounting before processor (suspicious)
    """
    if not proc_date_str or not acct_date_str:
        return 0
    try:
        proc_date = datetime.strptime(str(proc_date_str)[:10], '%Y-%m-%d').date()
        acct_date = datetime.strptime(str(acct_date_str)[:10], '%Y-%m-%d').date()
        delta_days = (acct_date - proc_date).days

        if delta_days >= 0:
            if delta_days <= 3:
                return DATE_POINTS_MAX  # 25 points
            elif delta_days <= 7:
                return DATE_POINTS_MAX // 2  # 12 points
            return 0
        else:
            if delta_days == -1:
                return 10
            elif delta_days >= -7:
                return 5
            return 0
    except Exception:
        return 0


def _calculate_customer_score(
    proc_email: Optional[str],
    proc_name: Optional[str],
    acct_email: Optional[str],
    acct_name: Optional[str],
) -> Tuple[int, bool]:
    """
    Calculate customer identity score.
    Returns (points, identity_present).
    """
    identity_present = False

    # Email match (strongest identity signal)
    if proc_email and acct_email:
        if proc_email.lower().strip() == acct_email.lower().strip():
            return CUSTOMER_POINTS, True

    # Name match (weaker but still useful)
    if proc_name and acct_name:
        pn = proc_name.lower().strip()
        an = acct_name.lower().strip()
        if pn == an:
            return CUSTOMER_POINTS, True
        # Normalized comparison: handle "Last, First" vs "First Last"
        pn_parts = sorted(w.strip() for w in pn.replace(',', ' ').split() if w.strip())
        an_parts = sorted(w.strip() for w in an.replace(',', ' ').split() if w.strip())
        if pn_parts == an_parts and len(pn_parts) >= 2:
            return CUSTOMER_POINTS, True
        # Partial name match
        if pn in an or an in pn:
            return CUSTOMER_POINTS // 2, True

    return 0, identity_present


def _score_match(
    proc_event: Dict,
    acct_fact: Dict,
    is_single_candidate: bool,
) -> Dict:
    """
    Score a candidate match between a processor payment (canonical_facts)
    and an accounting observation (canonical_facts payment_observed).
    Returns scoring breakdown dict.
    """
    breakdown = {
        'amount_points': 0,
        'date_points': 0,
        'customer_points': 0,
        'uniqueness_bonus': 0,
        'total_score': 0,
        'identity_present': False,
    }

    # Amount MUST match exactly (required gate) — §6.1
    proc_amount = _parse_amount(proc_event.get('amount_gross'))
    acct_amount = _parse_amount(acct_fact.get('amount_gross'))
    if proc_amount is None or acct_amount is None:
        return breakdown
    if abs(proc_amount) != abs(acct_amount):
        return breakdown
    breakdown['amount_points'] = AMOUNT_POINTS

    # Transaction ID conflict gate — mismatched IDs are definitive non-match
    # If both sides have IDs and they differ, this cannot be the same transaction
    proc_txn_id = str(proc_event.get('processor_payment_id', '')).strip()
    acct_ref_no = ''
    if acct_fact.get('metadata'):
        try:
            meta = json.loads(acct_fact['metadata']) if isinstance(acct_fact['metadata'], str) else acct_fact.get('metadata', {})
            acct_ref_no = str(meta.get('ref_no', '') or meta.get('PaymentRefNum', '') or '').strip()
        except (json.JSONDecodeError, TypeError):
            pass
    if proc_txn_id and acct_ref_no and proc_txn_id != acct_ref_no:
        breakdown['amount_points'] = 0
        return breakdown

    # Date proximity
    # Processor: processor_timestamp, Accounting: processor_timestamp (txn_date)
    breakdown['date_points'] = _calculate_date_score(
        proc_event.get('processor_timestamp'),
        acct_fact.get('processor_timestamp'),
    )

    # Customer identity
    # Parse accounting metadata for customer info (may be in metadata)
    acct_email = acct_fact.get('customer_email')
    acct_name = acct_fact.get('customer_name')
    if not acct_name and acct_fact.get('metadata'):
        try:
            meta = json.loads(acct_fact['metadata']) if isinstance(acct_fact['metadata'], str) else acct_fact.get('metadata', {})
            acct_name = acct_name or meta.get('customer_name')
        except (json.JSONDecodeError, TypeError):
            pass

    cust_points, identity = _calculate_customer_score(
        proc_event.get('customer_email'), proc_event.get('customer_name'),
        acct_email, acct_name,
    )
    breakdown['customer_points'] = cust_points
    breakdown['identity_present'] = identity

    # Uniqueness bonus only when identity is present
    if is_single_candidate and identity:
        breakdown['uniqueness_bonus'] = UNIQUENESS_BONUS

    total = (
        breakdown['amount_points']
        + breakdown['date_points']
        + breakdown['customer_points']
        + breakdown['uniqueness_bonus']
    )
    breakdown['total_score'] = min(total, MAX_SCORE)
    return breakdown


class MatchPayoutEngine:
    """
    Executes match runs per MATCH_PAYOUT_UI_CONTRACT.

    All matching is user-initiated (§3.1). Each execution creates a new
    immutable MatchRun (§5, §9). No accounting objects are created (§8).
    """

    def __init__(self, conn: sqlite3.Connection, workspace_id: str):
        self.conn = conn
        self.workspace_id = workspace_id

    def execute_match_run(self, user_id: str) -> Dict:
        """
        Execute a processor-driven match run. Starts from processor payments,
        finds matching unlinked QBO accounting transactions.

        Results are grouped by settlement batch. Each result represents a
        processor payment with its matched (or unmatched) QBO counterpart.

        Returns:
            Dict with match_run_id and result summary.
        """
        match_run_id = _generate_id()
        cursor = self.conn.cursor()

        # ── Gather inputs ──

        # Accounting side (QBO observations from canonical_facts)
        acct_payments = self._get_accounting_payments(cursor)
        acct_refunds = self._get_accounting_refunds(cursor)
        existing_deposits = self._get_existing_deposits(cursor)

        # Processor side (from canonical_facts)
        proc_payments = self._get_processor_payments(cursor)
        all_refunds_and_returns = self._get_processor_refunds(cursor)
        # Split: refunds need matching against QBO; ACH returns bypass matching entirely
        proc_refunds = [r for r in all_refunds_and_returns if r['event_type'] == 'refund']
        proc_ach_returns = [r for r in all_refunds_and_returns if r['event_type'] == 'ach_return']

        # Build set of QBO payment IDs already in deposits
        deposited_qbo_ids = self._get_deposited_payment_ids(existing_deposits)

        # Filter to unlinked QBO payments only (not already in a deposit)
        unlinked_acct_payments = [
            p for p in acct_payments
            if str(p.get('processor_payment_id', '')) not in deposited_qbo_ids
        ]

        # Filter to unlinked QBO refunds (same logic — deposit line items can include refunds)
        unlinked_acct_refunds = [
            r for r in acct_refunds
            if str(r.get('processor_payment_id', '')) not in deposited_qbo_ids
        ]

        logger.info(
            f"Match run inputs: {len(proc_payments)} processor payments, "
            f"{len(proc_refunds)} processor refunds, "
            f"{len(proc_ach_returns)} ACH returns, "
            f"{len(unlinked_acct_payments)} unlinked QBO payments "
            f"({len(acct_payments)} total, {len(deposited_qbo_ids)} deposited), "
            f"{len(unlinked_acct_refunds)} unlinked QBO refunds "
            f"({len(acct_refunds)} total), "
            f"{len(existing_deposits)} deposits"
        )

        # Build input hash for MatchRun (§5) — based on processor payments + refunds + returns
        input_ids = sorted([r['id'] for r in proc_payments] + [r['id'] for r in proc_refunds] + [r['id'] for r in proc_ach_returns])
        input_hash = hashlib.sha256(json.dumps(input_ids).encode()).hexdigest()

        # Processor snapshot reference (§5)
        proc_snapshot = None
        if proc_payments:
            latest = max(
                (p.get('created_at', '') for p in proc_payments),
                default=None,
            )
            proc_snapshot = latest

        # ── Classify existing deposits FIRST (§6.4) ──
        # Creates per-component already_created results with processor linkage.
        # Returns set of processor event IDs to skip in matching.
        deposit_results, classified_proc_ids = self._classify_existing_deposits(
            existing_deposits, acct_payments, acct_refunds,
            proc_payments, all_refunds_and_returns,  # Include both refunds + ACH returns
        )

        # ── Classify processor payments by method ──
        card_proc_payments = [
            p for p in proc_payments
            if p.get('payment_method') != 'bank_account'
            and p['id'] not in classified_proc_ids
        ]
        ach_proc_payments = [
            p for p in proc_payments
            if p.get('payment_method') == 'bank_account'
            and p['id'] not in classified_proc_ids
        ]

        # Filter out already-classified refunds too
        remaining_proc_refunds = [
            r for r in proc_refunds
            if r['id'] not in classified_proc_ids
        ]

        # ── Execute matching (processor-driven) ──
        results = list(deposit_results)

        # Match card processor payments → QBO payments
        # Only unclassified processor events; deposited ones handled above.
        card_results = self._match_payments(
            card_proc_payments, acct_payments, unlinked_acct_payments, deposited_qbo_ids,
        )
        results.extend(card_results)

        # Match ACH processor payments → QBO payments (same matching as card)
        # then assign to 'ach' section so they appear in ACH Deposits until
        # the user enters a deposit amount (intent), at which point they move
        # to 'ready'.  Individual per-payment results are needed for deposit
        # creation validation.
        ach_results = self._match_payments(
            ach_proc_payments, acct_payments, unlinked_acct_payments, deposited_qbo_ids,
        )
        for r in ach_results:
            r['section'] = 'ach'
        results.extend(ach_results)

        # Match processor refunds → QBO refunds (same algorithm as payments)
        # All refund results go to 'refund' section; high-confidence matches
        # keep their auto-accepted status, others stay pending
        refund_results = self._match_payments(
            remaining_proc_refunds, acct_refunds, unlinked_acct_refunds, deposited_qbo_ids,
        )
        for r in refund_results:
            r['section'] = 'refund'
            if r.get('resolution_status') != 'accepted':
                r['resolution_status'] = 'pending'
        results.extend(refund_results)

        # ACH returns — no matching needed (no QBO counterpart to match against).
        # Include in their ACH batch with section='ach' so they appear in batch
        # drill-downs and are included in deposit line items.
        for ret in proc_ach_returns:
            if ret['id'] in classified_proc_ids:
                continue
            results.append({
                'id': str(uuid.uuid4()),
                'matched_processor_event_id': ret['id'],
                'matched_payout_batch_id': ret.get('processor_payout_id'),
                'section': 'ach',
                'accounting_fact_id': None,
                'confidence_score': None,
                'match_rationale': 'ACH return - no matching required',
                'resolution_status': 'accepted',
                'exclusion_reason': None,
                'processor_event': ret,
            })

        # ── Count summaries ──
        summary = {'ready': 0, 'needs_review': 0, 'ach': 0, 'refund': 0, 'already_created': 0}
        for r in results:
            summary[r['section']] += 1

        # ── Persist MatchRun (§5) ──
        cursor.execute("""
            INSERT INTO match_runs
            (id, workspace_id, initiated_by, initiated_at,
             input_transaction_ids, input_hash, processor_snapshot_ref,
             status, result_summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'completed', ?)
        """, (
            match_run_id, self.workspace_id, user_id,
            datetime.utcnow().isoformat() + 'Z',
            json.dumps(input_ids), input_hash, proc_snapshot,
            json.dumps(summary),
        ))

        # Persist match results
        for r in results:
            cursor.execute("""
                INSERT INTO match_run_results
                (id, match_run_id, workspace_id, accounting_fact_id,
                 accounting_external_id, section, matched_processor_event_id,
                 matched_payout_batch_id, confidence_score, match_rationale,
                 candidates, resolution_status, exclusion_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                _generate_id(), match_run_id, self.workspace_id,
                r.get('accounting_fact_id', ''),
                r.get('accounting_external_id'),
                r['section'], r.get('matched_processor_event_id'),
                r.get('matched_payout_batch_id'), r.get('confidence_score'),
                json.dumps(r.get('match_rationale')) if r.get('match_rationale') else None,
                json.dumps(r.get('candidates')) if r.get('candidates') else None,
                r.get('resolution_status', 'pending'),
                r.get('exclusion_reason'),
            ))

        self.conn.commit()

        logger.info(
            f"MatchRun {match_run_id} completed: "
            f"ready={summary['ready']}, needs_review={summary['needs_review']}, "
            f"ach={summary['ach']}, refund={summary['refund']}, "
            f"already_created={summary['already_created']}"
        )

        return {
            'match_run_id': match_run_id,
            'summary': summary,
        }

    # ── Data access methods ──

    def _get_accounting_payments(self, cursor) -> List[Dict]:
        """Get QBO payment observations from canonical_facts."""
        cursor.execute("""
            SELECT id, workspace_id, event_type, source_object_type,
                   processor_payment_id, amount_gross, currency,
                   processor_timestamp, customer_email, customer_name,
                   metadata, created_at, observed_at,
                   source_updated_at, description
            FROM canonical_facts
            WHERE workspace_id = ?
              AND event_type = 'payment_observed'
            ORDER BY processor_timestamp DESC
        """, (self.workspace_id,))
        rows = cursor.fetchall()
        return [dict(r) for r in rows]

    def _get_accounting_refunds(self, cursor) -> List[Dict]:
        """Get QBO refund observations from canonical_facts."""
        cursor.execute("""
            SELECT id, workspace_id, event_type, source_object_type,
                   processor_payment_id, amount_gross, currency,
                   processor_timestamp, customer_email, customer_name,
                   metadata, created_at, observed_at, description
            FROM canonical_facts
            WHERE workspace_id = ?
              AND event_type = 'refund_observed'
            ORDER BY processor_timestamp DESC
        """, (self.workspace_id,))
        return [dict(r) for r in rows] if (rows := cursor.fetchall()) else []

    def _get_existing_deposits(self, cursor) -> List[Dict]:
        """Get existing QBO deposit observations (§6.4)."""
        cursor.execute("""
            SELECT id, workspace_id, event_type, source_object_type,
                   processor_payment_id, amount_gross, currency,
                   processor_timestamp, customer_email, customer_name,
                   metadata, created_at, observed_at, description
            FROM canonical_facts
            WHERE workspace_id = ?
              AND event_type = 'deposit_observed'
            ORDER BY processor_timestamp DESC
        """, (self.workspace_id,))
        return [dict(r) for r in rows] if (rows := cursor.fetchall()) else []

    def _get_processor_payments(self, cursor) -> List[Dict]:
        """
        Get processor payment facts from canonical_facts.
        Processor transactions are canonicalized into canonical_facts with
        event_type='payment' and processor='authorize_net'/'stripe'.
        """
        cursor.execute("""
            SELECT id, event_type, amount_gross, amount_net, fees, currency,
                   processor_payment_id, processor_payout_id,
                   processor_customer_id,
                   payment_method, card_brand, card_last4,
                   processor_timestamp, settlement_date,
                   customer_email, customer_name, metadata, created_at
            FROM canonical_facts
            WHERE workspace_id = ?
              AND event_type = 'payment'
            ORDER BY processor_timestamp DESC
        """, (self.workspace_id,))
        return [dict(r) for r in rows] if (rows := cursor.fetchall()) else []

    def _get_processor_refunds(self, cursor) -> List[Dict]:
        """
        Get processor refund/return facts from canonical_facts.
        Includes both refunds (merchant-initiated) and ach_return (bank-initiated).
        """
        cursor.execute("""
            SELECT id, event_type, amount_gross, amount_net, fees, currency,
                   processor_payment_id, processor_payout_id,
                   processor_customer_id,
                   payment_method, card_brand, card_last4,
                   processor_timestamp, settlement_date,
                   customer_email, customer_name, metadata, created_at
            FROM canonical_facts
            WHERE workspace_id = ?
              AND event_type IN ('refund', 'ach_return')
            ORDER BY processor_timestamp DESC
        """, (self.workspace_id,))
        return [dict(r) for r in rows] if (rows := cursor.fetchall()) else []

    def _get_deposited_payment_ids(self, deposits: List[Dict]) -> set:
        """
        Build set of QBO payment IDs that are already part of a deposit.
        Uses deposit_line_items (preferred) to collect only Payment-type IDs,
        excluding RefundReceipts/CreditMemos which need separate matching.
        Falls back to component_payment_ids for old records without line items.
        Returns set of QBO payment ID strings.
        """
        deposited = set()
        for deposit in deposits:
            metadata = {}
            if deposit.get('metadata'):
                try:
                    metadata = json.loads(deposit['metadata']) if isinstance(deposit['metadata'], str) else deposit.get('metadata', {})
                except (json.JSONDecodeError, TypeError):
                    pass
            line_items = metadata.get('deposit_line_items', [])
            if line_items:
                # Use typed line items — only collect Payment IDs
                for item in line_items:
                    if item.get('txn_type') == 'Payment' and item.get('txn_id'):
                        deposited.add(str(item['txn_id']))
            else:
                # Fallback for old records without deposit_line_items
                for pid in metadata.get('component_payment_ids', []):
                    deposited.add(str(pid))
        return deposited

    # ── Matching methods ──

    def _match_payments(
        self,
        proc_payments: List[Dict],
        all_acct_payments: List[Dict],
        unlinked_acct_payments: List[Dict],
        deposited_qbo_ids: set,
    ) -> List[Dict]:
        """
        Processor-driven matching: iterate processor payments, find matching
        QBO accounting transactions.

        Two-tier matching:
          Level A (Deterministic): processor_payment_id == QBO ref_no → score 100
            - Checks ALL QBO payments (including deposited) for deterministic matches
            - If QBO payment is deposited → skip (covered by Already Created section)
            - If QBO payment is unlinked → Ready for Deposit
          Level B (Probabilistic): Amount + Date + Customer + Uniqueness scoring
            - Only searches among unlinked QBO payments

        Each result represents a processor payment with its matched QBO counterpart.
        """
        results = []
        available_acct = list(unlinked_acct_payments)  # Pool for probabilistic matching

        # Build QBO index by ref_no for deterministic matching (ALL payments)
        acct_by_ref_no: Dict[str, Dict] = {}
        for a in all_acct_payments:
            metadata = {}
            if a.get('metadata'):
                try:
                    metadata = json.loads(a['metadata']) if isinstance(a['metadata'], str) else a.get('metadata', {})
                except (json.JSONDecodeError, TypeError):
                    pass
            ref_no = str(metadata.get('ref_no', '')).strip()
            if ref_no:
                acct_by_ref_no[ref_no] = a

        for proc in proc_payments:
            proc_txn_id = str(proc.get('processor_payment_id', '')).strip()
            payout_id = proc.get('processor_payout_id')

            # ── Level A: Deterministic Match ──
            # Processor Transaction ID == QBO PaymentRefNum (ref_no)
            if proc_txn_id and proc_txn_id in acct_by_ref_no:
                acct = acct_by_ref_no[proc_txn_id]
                qbo_id = str(acct.get('processor_payment_id', ''))
                is_deposited = qbo_id in deposited_qbo_ids

                if is_deposited:
                    # QBO payment already in a deposit — skip.
                    # Handled by _classify_existing_deposits() which runs first.
                    continue

                results.append({
                    'accounting_fact_id': acct['id'],
                    'accounting_external_id': acct.get('processor_payment_id'),
                    'section': 'ready',
                    'matched_processor_event_id': proc['id'],
                    'matched_payout_batch_id': payout_id,
                    'confidence_score': 100,
                    'match_rationale': {
                        'type': 'deterministic',
                        'match_key': 'ref_no',
                        'ref_no': proc_txn_id,
                        'processor_payment_id': proc_txn_id,
                        'is_deterministic': True,
                        'amount_points': 40,
                        'date_points': 25,
                        'customer_points': 25,
                        'uniqueness_bonus': 10,
                        'total_score': 100,
                        'identity_present': True,
                    },
                    'resolution_status': 'accepted',
                })

                # Remove from available pool
                if acct in available_acct:
                    available_acct.remove(acct)
                continue

            # ── Level B: Probabilistic Match (unlinked QBO only) ──
            proc_amount = _parse_amount(proc.get('amount_gross'))
            if proc_amount is None:
                results.append({
                    'accounting_fact_id': '',
                    'section': 'needs_review',
                    'matched_processor_event_id': proc['id'],
                    'matched_payout_batch_id': payout_id,
                    'confidence_score': 0,
                    'match_rationale': {'reason': 'no_amount', 'type': 'unmatched'},
                    'resolution_status': 'pending',
                })
                continue

            # Find QBO candidates by amount from unlinked pool
            candidates = []
            for a in available_acct:
                a_amt = _parse_amount(a.get('amount_gross'))
                if a_amt is not None and abs(a_amt) == abs(proc_amount):
                    candidates.append(a)

            if not candidates:
                results.append({
                    'accounting_fact_id': '',
                    'section': 'needs_review',
                    'matched_processor_event_id': proc['id'],
                    'matched_payout_batch_id': payout_id,
                    'confidence_score': 0,
                    'match_rationale': {'reason': 'no_qbo_match', 'type': 'unmatched'},
                    'candidates': [],
                    'resolution_status': 'pending',
                })
                continue

            is_single = len(candidates) == 1

            # Score all candidates
            scored = []
            for acct in candidates:
                breakdown = _score_match(proc, acct, is_single)
                if breakdown['amount_points'] > 0:
                    scored.append({
                        'acct_fact_id': acct['id'],
                        'acct_external_id': acct.get('processor_payment_id'),
                        'customer_name': acct.get('customer_name'),
                        'customer_email': acct.get('customer_email'),
                        'amount': str(acct.get('amount_gross', '0')),
                        'date': acct.get('processor_timestamp'),
                        'score': breakdown['total_score'],
                        'breakdown': breakdown,
                    })

            scored.sort(key=lambda c: c['score'], reverse=True)

            if not scored:
                results.append({
                    'accounting_fact_id': '',
                    'section': 'needs_review',
                    'matched_processor_event_id': proc['id'],
                    'matched_payout_batch_id': payout_id,
                    'confidence_score': 0,
                    'match_rationale': {'reason': 'no_qbo_match', 'type': 'unmatched'},
                    'resolution_status': 'pending',
                })
                continue

            best = scored[0]

            # Classify by confidence (§6.1, §6.2)
            if best['score'] >= HIGH_CONFIDENCE and best['breakdown'].get('identity_present'):
                results.append({
                    'accounting_fact_id': best['acct_fact_id'],
                    'accounting_external_id': best.get('acct_external_id'),
                    'section': 'ready',
                    'matched_processor_event_id': proc['id'],
                    'matched_payout_batch_id': payout_id,
                    'confidence_score': best['score'],
                    'match_rationale': best['breakdown'],
                    'resolution_status': 'accepted',
                })
                matched_acct = next(
                    (a for a in available_acct if a['id'] == best['acct_fact_id']),
                    None,
                )
                if matched_acct:
                    available_acct.remove(matched_acct)
            else:
                results.append({
                    'accounting_fact_id': best['acct_fact_id'],
                    'accounting_external_id': best.get('acct_external_id'),
                    'section': 'needs_review',
                    'matched_processor_event_id': proc['id'],
                    'matched_payout_batch_id': payout_id,
                    'confidence_score': best['score'],
                    'match_rationale': best['breakdown'],
                    'candidates': scored[:5],
                    'resolution_status': 'pending',
                })

        return results

    def _identify_ach_batches(
        self,
        ach_proc_payments: List[Dict],
    ) -> List[Dict]:
        """
        Identify ACH batches — matched but amount-unknown (ACH clarification).

        ACH batches ARE matched during Match. Identity is resolved.
        Only the deposit amount is unknown. User enters expected deposit amount.
        Groups ACH processor payments by processor_payout_id.
        """
        results = []

        # Group ACH processor events by payout batch
        ach_by_batch: Dict[str, List[Dict]] = {}
        for p in ach_proc_payments:
            payout_id = p.get('processor_payout_id')
            if payout_id:
                ach_by_batch.setdefault(payout_id, []).append(p)

        for batch_payout_id, ach_payments in ach_by_batch.items():
            # Calculate sum of linked ACH payments
            payment_sum = sum(
                abs(_parse_amount(p.get('amount_gross')) or Decimal('0'))
                for p in ach_payments
            )

            results.append({
                'accounting_fact_id': batch_payout_id,  # Use payout ID as reference
                'accounting_external_id': batch_payout_id,
                'section': 'ach',
                'matched_payout_batch_id': batch_payout_id,
                'confidence_score': None,
                'match_rationale': {
                    'type': 'ach_batch',
                    'batch_id': batch_payout_id,
                    'payment_count': len(ach_payments),
                    'payment_sum': str(payment_sum),
                    'linked_payment_ids': [p['id'] for p in ach_payments],
                },
                'resolution_status': 'pending',
            })

        return results

    def _classify_existing_deposits(
        self,
        existing_deposits: List[Dict],
        acct_payments: List[Dict],
        acct_refunds: Optional[List[Dict]] = None,
        proc_payments: Optional[List[Dict]] = None,
        proc_refunds: Optional[List[Dict]] = None,
    ) -> Tuple[List[Dict], set]:
        """
        Classify existing QBO deposits as 'already_created' (§6.4).

        Creates one result PER COMPONENT (payment/refund linked to a deposit),
        with full processor linkage so results can be grouped by batch.

        Returns:
            Tuple of (results, classified_proc_ids) where classified_proc_ids
            is the set of processor event IDs already accounted for by deposits.
        """
        results = []
        classified_proc_ids = set()

        # Build QBO fact lookups by processor_payment_id (QBO native ID)
        acct_by_qbo_id: Dict[str, Dict] = {}
        for p in acct_payments:
            qbo_id = str(p.get('processor_payment_id', ''))
            if qbo_id:
                acct_by_qbo_id[qbo_id] = p
        for r in (acct_refunds or []):
            qbo_id = str(r.get('processor_payment_id', ''))
            if qbo_id:
                acct_by_qbo_id[qbo_id] = r

        # Build processor fact lookups
        # By processor_payment_id (transaction ID) for ref_no matching
        proc_by_txn_id: Dict[str, Dict] = {}
        # By (payout_batch_id, normalized_abs_amount) for no-ref_no fallback
        proc_by_batch_amount: Dict[Tuple[str, str], List[Dict]] = {}
        all_proc = list(proc_payments or []) + list(proc_refunds or [])
        for p in all_proc:
            txn_id = str(p.get('processor_payment_id', '')).strip()
            if txn_id:
                proc_by_txn_id[txn_id] = p
            payout_id = p.get('processor_payout_id')
            amt = _parse_amount(p.get('amount_gross'))
            if payout_id and amt is not None:
                key = (str(payout_id), f"{abs(amt):.2f}")
                proc_by_batch_amount.setdefault(key, []).append(p)

        # Track which processor facts have been claimed by a deposit component
        # (to avoid double-assigning in the no-ref_no fallback)
        claimed_proc_ids = set()

        for deposit in existing_deposits:
            metadata = {}
            if deposit.get('metadata'):
                try:
                    metadata = json.loads(deposit['metadata']) if isinstance(deposit['metadata'], str) else deposit.get('metadata', {})
                except (json.JSONDecodeError, TypeError):
                    pass

            # Extract all component QBO IDs from deposit
            component_ids = set()
            line_items = metadata.get('deposit_line_items', [])
            for item in line_items:
                if isinstance(item, dict) and item.get('txn_id'):
                    component_ids.add(str(item['txn_id']))
            for pid in metadata.get('component_payment_ids', []):
                component_ids.add(str(pid))

            if not component_ids:
                continue

            deposit_qbo_id = deposit.get('processor_payment_id')
            deposit_date = deposit.get('processor_timestamp')
            deposit_amount = deposit.get('amount_gross')

            # Pass 1: Match components that have ref_no (deterministic)
            # Also discover which batch(es) this deposit belongs to
            batch_ids_found = set()
            matched_components = {}  # qbo_id -> (acct_fact, proc_fact)
            unmatched_qbo_ids = []

            for qbo_id in component_ids:
                acct_fact = acct_by_qbo_id.get(qbo_id)
                if not acct_fact:
                    continue  # QBO fact not in our universe

                # Extract ref_no from QBO fact metadata
                ref_no = ''
                if acct_fact.get('metadata'):
                    try:
                        meta = json.loads(acct_fact['metadata']) if isinstance(acct_fact['metadata'], str) else acct_fact.get('metadata', {})
                        ref_no = str(meta.get('ref_no', '') or meta.get('PaymentRefNum', '') or '').strip()
                    except (json.JSONDecodeError, TypeError):
                        pass

                if ref_no and ref_no in proc_by_txn_id:
                    proc_fact = proc_by_txn_id[ref_no]
                    matched_components[qbo_id] = (acct_fact, proc_fact)
                    payout_id = proc_fact.get('processor_payout_id')
                    if payout_id:
                        batch_ids_found.add(str(payout_id))
                    claimed_proc_ids.add(proc_fact['id'])
                else:
                    unmatched_qbo_ids.append(qbo_id)

            # Pass 2: No-ref_no fallback — match by amount within discovered batch(es)
            for qbo_id in unmatched_qbo_ids:
                acct_fact = acct_by_qbo_id.get(qbo_id)
                if not acct_fact:
                    continue

                acct_amt = _parse_amount(acct_fact.get('amount_gross'))
                if acct_amt is None:
                    # Can't match without amount — still create result with batch info
                    matched_components[qbo_id] = (acct_fact, None)
                    continue

                # Search for unclaimed processor fact with matching amount in known batch(es)
                found_proc = None
                for batch_id in batch_ids_found:
                    key = (batch_id, f"{abs(acct_amt):.2f}")
                    candidates = proc_by_batch_amount.get(key, [])
                    for c in candidates:
                        if c['id'] not in claimed_proc_ids:
                            found_proc = c
                            break
                    if found_proc:
                        break

                matched_components[qbo_id] = (acct_fact, found_proc)
                if found_proc:
                    claimed_proc_ids.add(found_proc['id'])

            # Create one result per matched component
            for qbo_id, (acct_fact, proc_fact) in matched_components.items():
                payout_id = None
                if proc_fact:
                    payout_id = proc_fact.get('processor_payout_id')
                elif batch_ids_found:
                    # Use any discovered batch (all components of a deposit
                    # belong to the same batch in practice)
                    payout_id = next(iter(batch_ids_found))

                result = {
                    'accounting_fact_id': acct_fact['id'],
                    'accounting_external_id': acct_fact.get('processor_payment_id'),
                    'section': 'already_created',
                    'matched_processor_event_id': proc_fact['id'] if proc_fact else None,
                    'matched_payout_batch_id': payout_id,
                    'confidence_score': 100 if proc_fact else None,
                    'match_rationale': {
                        'type': 'deposit_component',
                        'deposit_qbo_id': deposit_qbo_id,
                        'deposit_date': deposit_date,
                        'deposit_amount': deposit_amount,
                        'match_method': 'ref_no' if (proc_fact and qbo_id not in unmatched_qbo_ids) else 'batch_amount_fallback' if proc_fact else 'batch_inferred',
                    },
                    'exclusion_reason': f'QBO Deposit #{deposit_qbo_id}',
                    'resolution_status': 'accepted',
                }
                results.append(result)

                if proc_fact:
                    classified_proc_ids.add(proc_fact['id'])

        return results, classified_proc_ids

    @staticmethod
    def resolve_result(
        conn: sqlite3.Connection,
        match_run_id: str,
        result_id: str,
        action: str,
        user_id: str,
    ) -> Dict:
        """
        Resolve a needs-review match result (§6.2).

        Actions:
          - 'accept': Accepted → moves to Ready for Deposit
          - 'reject': Rejected → remains in Needs Review
          - 'unresolved': Left unresolved → remains in Needs Review
        """
        valid_actions = ('accept', 'reject', 'unresolved')
        if action not in valid_actions:
            raise ValueError(f"Invalid action: {action}. Must be one of {valid_actions}")

        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, section, resolution_status
            FROM match_run_results
            WHERE id = ? AND match_run_id = ?
        """, (result_id, match_run_id))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Result {result_id} not found in match run {match_run_id}")

        current_section = dict(row)['section']
        if current_section not in ('needs_review', 'refund'):
            raise ValueError("Only needs_review and refund items can be resolved")

        resolution_status = {
            'accept': 'accepted',
            'reject': 'rejected',
            'unresolved': 'unresolved',
        }[action]

        # Accepted needs_review → ready; refund items stay in refund section
        if current_section == 'refund':
            new_section = 'refund'
        else:
            new_section = 'ready' if action == 'accept' else 'needs_review'

        cursor.execute("""
            UPDATE match_run_results
            SET resolution_status = ?,
                section = ?,
                resolved_at = ?,
                resolved_by = ?
            WHERE id = ? AND match_run_id = ?
        """, (
            resolution_status, new_section,
            datetime.utcnow().isoformat() + 'Z', user_id,
            result_id, match_run_id,
        ))
        conn.commit()

        return {
            'result_id': result_id,
            'resolution_status': resolution_status,
            'section': new_section,
        }

    @staticmethod
    def create_ach_intent(
        conn: sqlite3.Connection,
        match_run_id: str,
        workspace_id: str,
        ach_batch_id: str,
        expected_deposit_amount: str,
        linked_payment_ids: List[str],
        user_id: str,
    ) -> Dict:
        """
        Create an ACH deposit intent (§6.3).

        Prepares ACH deposit but does NOT create it.
        Payments are NOT marked consumed — consumption at execution only.
        """
        intent_id = _generate_id()
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM match_runs WHERE id = ?", (match_run_id,))
        if not cursor.fetchone():
            raise ValueError(f"Match run {match_run_id} not found")

        cursor.execute("""
            INSERT INTO ach_deposit_intents
            (id, match_run_id, workspace_id, ach_batch_id,
             expected_deposit_amount, linked_payment_ids, status, created_by)
            VALUES (?, ?, ?, ?, ?, ?, 'prepared', ?)
        """, (
            intent_id, match_run_id, workspace_id,
            ach_batch_id, expected_deposit_amount,
            json.dumps(linked_payment_ids), user_id,
        ))

        # Move ALL individual ACH results for this batch to 'ready' section.
        # Individual results use matched_payout_batch_id as the batch identifier.
        cursor.execute("""
            UPDATE match_run_results
            SET section = 'ready',
                resolution_status = 'accepted',
                resolved_at = ?,
                resolved_by = ?
            WHERE match_run_id = ?
              AND section = 'ach'
              AND matched_payout_batch_id = ?
        """, (
            datetime.utcnow().isoformat() + 'Z', user_id,
            match_run_id, ach_batch_id,
        ))

        conn.commit()

        logger.info(
            f"ACH intent {intent_id} created for batch {ach_batch_id} "
            f"in match run {match_run_id}: amount={expected_deposit_amount}"
        )

        return {
            'intent_id': intent_id,
            'ach_batch_id': ach_batch_id,
            'expected_deposit_amount': expected_deposit_amount,
            'status': 'prepared',
        }
