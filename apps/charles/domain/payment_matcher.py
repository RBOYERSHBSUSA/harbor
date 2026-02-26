"""
Enhanced Payment Matcher Module

Implements multi-factor confidence scoring for matching Stripe payments
to QBO payments. Replaces simple amount-only matching with a scoring
algorithm that considers:
- Amount (40 points, required gate)
- Date proximity (0-25 points)
- Email match (20 points)
- Uniqueness bonus (0 or 25 points)

Raw max: 110 points, capped to 100.
Without email: 40 + 25 + 25 = 90 (exceeds HIGH threshold for auto-match)

Confidence Thresholds:
- HIGH (>= 85): Auto-match without review
- MEDIUM (50-84): Queue for manual review
- LOW (< 50): Queue for manual review with warning

Per MINIMUM_LOGGING_CONTRACT, all match decisions are logged with full
score breakdowns for audit trail reconstruction.

Per CHARLES_PRE_IMPLEMENTATION_COMPLIANCE_CHECKLIST, this module version
is tracked for reproducibility.
"""

# Model version for audit trail and reproducibility
# Per CHARLES_MATCHING_ASSISTANCE_SPEC §Governance
MATCHER_VERSION = "1.0.0"

import json
import sqlite3
import uuid
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Dict, Literal, Any

from shared.structured_logging import StructuredLogger
from shared.idempotency import (
    IdempotencyManager,
    IdempotencyStatus,
    generate_idempotency_key,
    generate_idempotency_key_v2,
    _generate_legacy_key
)
from module4_reconciliation.sync_manager import record_confirmed_pair


@dataclass
class MatchScoreBreakdown:
    """
    Breakdown of match scoring factors.

    Raw max: 110 points, capped to 100.
    Without email: 40 + 25 + 25 = 90 (exceeds HIGH threshold)

    - amount_points: 40 (exact match required, 0 if mismatch)
    - date_points: 0-25 (asymmetric scoring for booking lag)
    - email_points: 0 or 20 (exact email match)
    - uniqueness_bonus: 0 or 25 (single candidate in date window bonus)
    """
    amount_points: int = 0
    date_points: int = 0
    email_points: int = 0
    uniqueness_bonus: int = 0

    @property
    def total(self) -> int:
        """Return total score, capped at 100 (never rounded up)."""
        raw = self.amount_points + self.date_points + self.email_points + self.uniqueness_bonus
        return min(raw, 100)

    def to_dict(self) -> Dict[str, int]:
        return {
            'amount_points': self.amount_points,
            'date_points': self.date_points,
            'email_points': self.email_points,
            'uniqueness_bonus': self.uniqueness_bonus,
            'total': self.total
        }


@dataclass
class MatchCandidate:
    """
    A potential QBO payment match with its score.
    """
    qbo_payment_id: str
    qbo_amount: Decimal
    qbo_date: date
    qbo_customer_email: Optional[str]
    score: MatchScoreBreakdown

    def to_dict(self) -> Dict[str, Any]:
        return {
            'qbo_payment_id': self.qbo_payment_id,
            'qbo_amount': str(self.qbo_amount),
            'qbo_date': self.qbo_date.isoformat() if self.qbo_date else None,
            'qbo_customer_email': self.qbo_customer_email,
            'score': self.score.to_dict()
        }


@dataclass
class MatchResult:
    """
    Result of a payment matching operation.
    """
    status: Literal['matched', 'review_required', 'no_candidates']
    confidence: Literal['high', 'medium', 'low', 'none']
    matched_qbo_payment_id: Optional[str] = None
    match_score: Optional[int] = None
    review_id: Optional[str] = None
    candidates: List[MatchCandidate] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'status': self.status,
            'confidence': self.confidence,
            'matched_qbo_payment_id': self.matched_qbo_payment_id,
            'match_score': self.match_score,
            'review_id': self.review_id,
            'candidate_count': len(self.candidates)
        }


class EnhancedPaymentMatcher:
    """
    Multi-factor payment matcher with confidence scoring.

    Scoring Algorithm (0-100 scale, capped):
    - Amount: 40 points (REQUIRED - no match if amount differs)
    - Date proximity: 0-25 points (asymmetric for booking lag)
    - Email: 20 points (exact match)
    - Uniqueness: 25 bonus points (single candidate in date window)

    Raw max: 110 points, capped to 100.
    Without email: 40 + 25 + 25 = 90 (exceeds HIGH threshold for auto-match)

    Thresholds:
    - HIGH confidence: >= 85 (auto-match)
    - MEDIUM confidence: 50-84 (manual review)
    - LOW confidence: < 50 (manual review with warning)
    """

    # Scoring weights (raw max = 110, capped to 100)
    AMOUNT_POINTS = 40          # Required gate - must match exactly
    DATE_POINTS_MAX = 25        # Maximum date proximity points
    EMAIL_POINTS = 20           # Exact email match bonus
    UNIQUENESS_BONUS = 25       # Single candidate in date window bonus
    MAX_SCORE = 100             # Cap total score at this value

    # Confidence thresholds
    HIGH_CONFIDENCE_THRESHOLD = 85
    MEDIUM_CONFIDENCE_THRESHOLD = 50

    def __init__(self, logger: Optional[StructuredLogger] = None, auto_match_enabled: bool = True):
        """
        Initialize the matcher.

        Args:
            logger: Optional structured logger for audit trail
            auto_match_enabled: If False, all matches go to review regardless of score
        """
        self.logger = logger
        self.auto_match_enabled = auto_match_enabled

    def calculate_date_score(self, stripe_date: date, qbo_date: date) -> int:
        """
        Calculate date proximity score with asymmetric lag handling.

        Per spec:
        - QBO 1-3 days after Stripe: Full 30 points (normal bookkeeping lag)
        - QBO 4-7 days after Stripe: 15 points
        - QBO 8+ days after Stripe: 0 points
        - QBO before Stripe by 1-3 days: Full 30 points
        - QBO before Stripe by 4-7 days: 15 points
        - QBO before Stripe by 8+ days: 0 points
        - Same day: Full 30 points

        Args:
            stripe_date: Date from Stripe payment
            qbo_date: Date from QBO payment

        Returns:
            Score between 0-30
        """
        days_diff = (qbo_date - stripe_date).days

        # Same day = full points
        if days_diff == 0:
            return self.DATE_POINTS_MAX

        abs_diff = abs(days_diff)

        # Within 3 days = full points
        if abs_diff <= 3:
            return self.DATE_POINTS_MAX

        # 4-7 days = half points
        if abs_diff <= 7:
            return self.DATE_POINTS_MAX // 2

        # 8+ days = no points
        return 0

    def calculate_match_score(
        self,
        stripe_amount: Decimal,
        stripe_date: date,
        stripe_email: Optional[str],
        qbo_amount: Decimal,
        qbo_date: date,
        qbo_email: Optional[str],
        is_single_candidate: bool = False
    ) -> MatchScoreBreakdown:
        """
        Calculate multi-factor match score.

        Args:
            stripe_amount: Amount from Stripe payment
            stripe_date: Date from Stripe payment
            stripe_email: Customer email from Stripe
            qbo_amount: Amount from QBO payment
            qbo_date: Date from QBO payment
            qbo_email: Customer email from QBO
            is_single_candidate: Whether this is the only matching candidate

        Returns:
            MatchScoreBreakdown with component scores
        """
        breakdown = MatchScoreBreakdown()

        # Amount MUST match exactly (required gate)
        if stripe_amount == qbo_amount:
            breakdown.amount_points = self.AMOUNT_POINTS
        else:
            # No match possible if amounts differ
            return breakdown

        # Date proximity scoring
        breakdown.date_points = self.calculate_date_score(stripe_date, qbo_date)

        # Email matching (case-insensitive)
        if stripe_email and qbo_email:
            if stripe_email.lower().strip() == qbo_email.lower().strip():
                breakdown.email_points = self.EMAIL_POINTS

        # Uniqueness bonus for single candidate
        if is_single_candidate:
            breakdown.uniqueness_bonus = self.UNIQUENESS_BONUS

        return breakdown

    def find_match_candidates(
        self,
        stripe_payment_id: str,
        stripe_amount: Decimal,
        stripe_date: date,
        stripe_email: Optional[str],
        unlinked_qbo_payments: List[Dict[str, Any]],
    ) -> List[MatchCandidate]:
        """
        Find and score all potential QBO payment matches.

        Args:
            stripe_payment_id: ID of the Stripe payment being matched
            stripe_amount: Amount from Stripe payment
            stripe_date: Date from Stripe payment
            stripe_email: Customer email from Stripe
            unlinked_qbo_payments: List of unlinked QBO payments to consider

        Returns:
            List of MatchCandidate sorted by score descending
        """
        candidates = []

        # First pass: find amount matches
        amount_matches = [
            qbo for qbo in unlinked_qbo_payments
            if Decimal(str(qbo.get('amount', '0'))) == stripe_amount
        ]

        is_single = len(amount_matches) == 1

        # Score each candidate
        for qbo in amount_matches:
            qbo_amount = Decimal(str(qbo.get('amount', '0')))
            qbo_date_str = qbo.get('date')
            qbo_date = self._parse_date(qbo_date_str) if qbo_date_str else stripe_date
            qbo_email = qbo.get('customer_email')

            score = self.calculate_match_score(
                stripe_amount=stripe_amount,
                stripe_date=stripe_date,
                stripe_email=stripe_email,
                qbo_amount=qbo_amount,
                qbo_date=qbo_date,
                qbo_email=qbo_email,
                is_single_candidate=is_single
            )

            candidates.append(MatchCandidate(
                qbo_payment_id=qbo.get('id') or qbo.get('qbo_payment_id'),
                qbo_amount=qbo_amount,
                qbo_date=qbo_date,
                qbo_customer_email=qbo_email,
                score=score
            ))

        # Sort by total score descending
        candidates.sort(key=lambda c: c.score.total, reverse=True)

        return candidates

    def match_payment(
        self,
        canonical_event_id: str,
        stripe_payment_id: str,
        stripe_amount: Decimal,
        stripe_date: date,
        stripe_email: Optional[str],
        stripe_card_last4: Optional[str],
        unlinked_qbo_payments: List[Dict[str, Any]],
        db_conn: sqlite3.Connection,
    ) -> MatchResult:
        """
        Main matching entry point. Attempts to match a Stripe payment
        to a QBO payment.

        LOGGING REQUIREMENTS:
        - MUST log payment_match_attempted
        - MUST log payment_match_high_confidence OR payment_match_low_confidence
        - MUST log payment_match_review_created if review needed
        - MUST log payment_match_no_candidates if no candidates

        IDEMPOTENCY:
        - Checks if canonical_event already has qbo_payment_id set
        - If yes, returns existing match (idempotent behavior)

        Args:
            canonical_event_id: ID of the canonical event being matched
            stripe_payment_id: Stripe payment/charge ID
            stripe_amount: Payment amount
            stripe_date: Payment date
            stripe_email: Customer email
            stripe_card_last4: Last 4 digits of card
            unlinked_qbo_payments: List of unlinked QBO payments
            db_conn: Database connection

        Returns:
            MatchResult with status, confidence, and matched ID or review ID
        """
        cursor = db_conn.cursor()

        # IDEMPOTENCY CHECK: Already matched?
        # Phase 3 Remediation: read from matching_outcomes (§6.2)
        cursor.execute("""
            SELECT qbo_payment_id, match_confidence, match_type
            FROM matching_outcomes
            WHERE canonical_fact_id = ?
        """, (canonical_event_id,))
        row = cursor.fetchone()

        if row and row[0]:  # qbo_payment_id is set
            existing_qbo_id = row[0]
            existing_confidence = row[1] or 0
            confidence_level = self._confidence_level(existing_confidence)

            if self.logger:
                self.logger.log_idempotency_hit(
                    idempotency_key=f"match:{canonical_event_id}",
                    existing_result_id=existing_qbo_id
                )

            return MatchResult(
                status='matched',
                confidence=confidence_level,
                matched_qbo_payment_id=existing_qbo_id,
                match_score=existing_confidence,
                candidates=[]
            )

        # Log match attempt
        candidates = self.find_match_candidates(
            stripe_payment_id=stripe_payment_id,
            stripe_amount=stripe_amount,
            stripe_date=stripe_date,
            stripe_email=stripe_email,
            unlinked_qbo_payments=unlinked_qbo_payments
        )

        if self.logger:
            self.logger.log_match_attempted(
                processor_payment_id=stripe_payment_id,
                amount=str(stripe_amount),
                candidate_count=len(candidates)
            )

        # NO CANDIDATES
        if not candidates:
            if self.logger:
                self.logger.log_match_no_candidates(
                    processor_payment_id=stripe_payment_id,
                    amount=str(stripe_amount)
                )

            # Mark as unmatched in matching_outcomes (§6.2)
            cursor.execute("""
                INSERT INTO matching_outcomes (id, workspace_id, canonical_fact_id, match_type, match_confidence, workflow_status)
                VALUES (lower(hex(randomblob(16))), (SELECT workspace_id FROM canonical_facts WHERE id = ?), ?, 'unmatched', 0, 'pending')
                ON CONFLICT(canonical_fact_id) DO UPDATE SET
                    match_type = 'unmatched',
                    match_confidence = 0,
                    updated_at = CURRENT_TIMESTAMP
            """, (canonical_event_id, canonical_event_id))
            db_conn.commit()

            return MatchResult(
                status='no_candidates',
                confidence='none',
                candidates=[]
            )

        top_candidate = candidates[0]
        top_score = top_candidate.score.total

        # Check for tie at top - if multiple candidates have same top score, send to review
        has_tie = len(candidates) > 1 and candidates[1].score.total == top_score

        # HIGH CONFIDENCE - Auto-match (only if auto_match_enabled AND no tie)
        if top_score >= self.HIGH_CONFIDENCE_THRESHOLD and self.auto_match_enabled and not has_tie:
            if self.logger:
                self.logger.log_match_high_confidence(
                    processor_payment_id=stripe_payment_id,
                    qbo_payment_id=top_candidate.qbo_payment_id,
                    score=top_score,
                    breakdown=top_candidate.score.to_dict()
                )

            # Record auto-match in matching_outcomes (§6.2)
            cursor.execute("""
                INSERT INTO matching_outcomes (id, workspace_id, canonical_fact_id, processor_payment_id,
                    qbo_payment_id, match_type, match_confidence, match_score_breakdown, workflow_status)
                VALUES (lower(hex(randomblob(16))), (SELECT workspace_id FROM canonical_facts WHERE id = ?), ?, ?,
                    ?, 'auto', ?, ?, 'matched')
                ON CONFLICT(canonical_fact_id) DO UPDATE SET
                    qbo_payment_id = excluded.qbo_payment_id,
                    match_type = 'auto',
                    match_confidence = excluded.match_confidence,
                    match_score_breakdown = excluded.match_score_breakdown,
                    workflow_status = 'matched',
                    updated_at = CURRENT_TIMESTAMP
            """, (
                canonical_event_id,
                canonical_event_id,
                stripe_payment_id,
                top_candidate.qbo_payment_id,
                top_score,
                json.dumps(top_candidate.score.to_dict()),
            ))
            db_conn.commit()

            return MatchResult(
                status='matched',
                confidence='high',
                matched_qbo_payment_id=top_candidate.qbo_payment_id,
                match_score=top_score,
                candidates=candidates
            )

        # LOW CONFIDENCE - Create review
        confidence_level = 'medium' if top_score >= self.MEDIUM_CONFIDENCE_THRESHOLD else 'low'

        if self.logger:
            self.logger.log_match_low_confidence(
                processor_payment_id=stripe_payment_id,
                top_score=top_score,
                candidates=[c.to_dict() for c in candidates[:5]]  # Top 5 for log
            )

        # Create match review record
        review_id = f"review_{uuid.uuid4().hex[:12]}"

        cursor.execute("""
            INSERT INTO match_reviews (
                id, canonical_event_id, processor_payment_id, amount,
                event_date, customer_email, customer_name, match_candidates,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (
            review_id,
            canonical_event_id,
            stripe_payment_id,
            str(stripe_amount),
            stripe_date.isoformat(),
            stripe_email,
            None,  # customer_name - not available from Stripe card info
            json.dumps([c.to_dict() for c in candidates])
        ))

        # Record as pending review in matching_outcomes (§6.2)
        cursor.execute("""
            INSERT INTO matching_outcomes (id, workspace_id, canonical_fact_id, processor_payment_id,
                match_type, match_confidence, workflow_status)
            VALUES (lower(hex(randomblob(16))), (SELECT workspace_id FROM canonical_facts WHERE id = ?), ?, ?,
                'pending', ?, 'pending')
            ON CONFLICT(canonical_fact_id) DO UPDATE SET
                match_type = 'pending',
                match_confidence = excluded.match_confidence,
                updated_at = CURRENT_TIMESTAMP
        """, (canonical_event_id, canonical_event_id, stripe_payment_id, top_score))

        db_conn.commit()

        if self.logger:
            self.logger.log_match_review_created(
                review_id=review_id,
                processor_payment_id=stripe_payment_id,
                candidate_count=len(candidates)
            )

        return MatchResult(
            status='review_required',
            confidence=confidence_level,
            review_id=review_id,
            match_score=top_score,
            candidates=candidates
        )

    def _confidence_level(self, score: int) -> Literal['high', 'medium', 'low', 'none']:
        """Map score to confidence level."""
        if score >= self.HIGH_CONFIDENCE_THRESHOLD:
            return 'high'
        elif score >= self.MEDIUM_CONFIDENCE_THRESHOLD:
            return 'medium'
        elif score > 0:
            return 'low'
        return 'none'

    def _parse_date(self, date_str: str) -> date:
        """Parse date string in various formats."""
        if isinstance(date_str, date):
            return date_str

        # Try common formats
        for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ']:
            try:
                return datetime.strptime(date_str[:19], fmt[:len(date_str)]).date()
            except ValueError:
                continue

        # Fallback to today
        return date.today()


def resolve_match_review(
    review_id: str,
    selected_qbo_payment_id: Optional[str],
    resolved_by: str,
    db_conn: sqlite3.Connection,
    idempotency_manager: IdempotencyManager,
    logger: StructuredLogger,
    company_id: str
) -> Dict[str, Any]:
    """
    Manually resolve a match review.

    This function is IDEMPOTENT per IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1:
    - Uses v2 idempotency key: match_resolve:ws:{workspace_id}:{processor}:{review_id}
    - Dual-read checks legacy key: match_resolve:{company_id}:{review_id}
    - Checks before acting
    - Returns cached result if already resolved

    Per MINIMUM_LOGGING_CONTRACT:
    - MUST emit payment_match_manual_resolved

    Args:
        review_id: ID of the match review to resolve
        selected_qbo_payment_id: QBO payment ID to match, or None to mark unmatched
        resolved_by: User identifier who resolved (email or 'system')
        db_conn: Database connection
        idempotency_manager: Idempotency manager for check-before-act
        logger: Structured logger for audit trail
        company_id: Company ID for idempotency key generation

    Returns:
        Dict with resolution result:
        - success: bool
        - review_id: str
        - canonical_event_id: str
        - qbo_payment_id: str or None
        - resolution_type: 'matched' or 'unmatched'
        - was_cached: bool (True if idempotency hit)

    Raises:
        ValueError: If review not found or already resolved with different value
    """
    cursor = db_conn.cursor()

    # Get processor type from review's canonical_fact -> raw_event chain
    # Phase 3 Remediation: read from canonical_facts (§6.2)
    processor_type = 'stripe'
    try:
        cursor.execute("""
            SELECT COALESCE(cf.processor, 'stripe') as processor
            FROM match_reviews mr
            LEFT JOIN canonical_facts cf ON mr.canonical_event_id = cf.id
            WHERE mr.id = ?
        """, (review_id,))
        processor_row = cursor.fetchone()
        if processor_row and processor_row[0]:
            processor_type = processor_row[0]
    except Exception:
        # Table may not exist in test fixtures; default to stripe
        pass

    # Generate v2 idempotency key with workspace scope
    # workspace_id comes from the idempotency_manager (already validated at construction)
    idempotency_key = generate_idempotency_key_v2(
        operation_type="match_resolve",
        workspace_id=idempotency_manager.workspace_id,
        processor=processor_type,
        external_id=review_id
    )
    # Legacy key for dual-read backward compatibility
    legacy_key = _generate_legacy_key(
        operation_type="match_resolve",
        company_id=company_id,
        external_id=review_id
    )

    # IDEMPOTENCY CHECK (v1.1 dual-read for backward compatibility)
    idem_result = idempotency_manager.check_idempotency_dual_read(
        v2_key=idempotency_key,
        legacy_key=legacy_key
    )

    if idem_result.status == IdempotencyStatus.EXISTS_COMPLETED:
        # Already resolved - return cached result
        logger.log_idempotency_hit(
            idempotency_key=idempotency_key,
            existing_result_id=idem_result.result_id or "unmatched"
        )

        # Fetch the review to return full details
        cursor.execute("""
            SELECT canonical_event_id, selected_qbo_payment_id
            FROM match_reviews
            WHERE id = ?
        """, (review_id,))
        row = cursor.fetchone()

        if row:
            return {
                'success': True,
                'review_id': review_id,
                'canonical_event_id': row[0],
                'qbo_payment_id': row[1],
                'resolution_type': 'matched' if row[1] else 'unmatched',
                'was_cached': True
            }

    if idem_result.status == IdempotencyStatus.EXISTS_PENDING:
        raise ValueError(f"Resolution for review {review_id} is already in progress")

    # Fetch the review with additional data for prior match continuity
    # Phase 3 Remediation: JOIN canonical_facts instead of canonical_events
    cursor.execute("""
        SELECT mr.id, mr.canonical_event_id, mr.processor_payment_id, mr.status,
               mr.match_candidates, mr.workspace_id,
               cf.processor_customer_id
        FROM match_reviews mr
        LEFT JOIN canonical_facts cf ON mr.canonical_event_id = cf.id
        WHERE mr.id = ?
    """, (review_id,))
    review_row = cursor.fetchone()

    if not review_row:
        raise ValueError(f"Match review {review_id} not found")

    if review_row[3] == 'resolved':
        # Already resolved - check if same selection
        cursor.execute("""
            SELECT selected_qbo_payment_id FROM match_reviews WHERE id = ?
        """, (review_id,))
        existing = cursor.fetchone()
        if existing and existing[0] != selected_qbo_payment_id:
            raise ValueError(
                f"Review {review_id} already resolved with different selection: "
                f"{existing[0]} vs {selected_qbo_payment_id}"
            )

    canonical_event_id = review_row[1]
    processor_payment_id = review_row[2]
    match_candidates_json = review_row[4]
    workspace_id = review_row[5]
    processor_customer_id = review_row[6]

    # Start idempotency operation
    idempotency_manager.start_operation(idempotency_key, "match_resolve")

    try:
        now = datetime.now().isoformat()

        # Update the match review
        cursor.execute("""
            UPDATE match_reviews
            SET status = 'resolved',
                selected_qbo_payment_id = ?,
                resolved_at = ?,
                resolved_by = ?,
                updated_at = ?
            WHERE id = ?
        """, (selected_qbo_payment_id, now, resolved_by, now, review_id))

        # Record resolution in matching_outcomes (§6.2)
        # Per R-4: MEDIUM/LOW confidence requires human confirmation
        match_type = 'manual' if selected_qbo_payment_id else 'unmatched'
        workflow_status = 'matched' if selected_qbo_payment_id else 'pending'
        cursor.execute("""
            INSERT INTO matching_outcomes (id, workspace_id, canonical_fact_id, processor_payment_id,
                qbo_payment_id, match_type, human_confirmed_at, confirmed_by, workflow_status)
            VALUES (lower(hex(randomblob(16))), ?, ?, ?,
                ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_fact_id) DO UPDATE SET
                qbo_payment_id = excluded.qbo_payment_id,
                match_type = excluded.match_type,
                human_confirmed_at = excluded.human_confirmed_at,
                confirmed_by = excluded.confirmed_by,
                workflow_status = excluded.workflow_status,
                updated_at = CURRENT_TIMESTAMP
        """, (
            workspace_id,
            canonical_event_id,
            processor_payment_id,
            selected_qbo_payment_id,
            match_type,
            now,  # REQUIRED for manual matches per R-4
            resolved_by,  # REQUIRED for manual matches per R-4
            workflow_status,
        ))

        # PRIOR MATCH CONTINUITY: Record confirmed pair for future matches
        # This is written ONLY when a user explicitly confirms a manual match
        if selected_qbo_payment_id and processor_customer_id and workspace_id:
            # Find qbo_customer_id from the selected candidate
            qbo_customer_id = None
            if match_candidates_json:
                try:
                    candidates = json.loads(match_candidates_json)
                    for c in candidates:
                        if c.get('qbo_payment_id') == selected_qbo_payment_id:
                            qbo_customer_id = c.get('qbo_customer_id')
                            break
                except (json.JSONDecodeError, TypeError):
                    pass

            if qbo_customer_id:
                # Determine processor type from canonical_event
                # Default to 'stripe' as it's the most common
                processor_type = 'stripe'
                record_confirmed_pair(
                    db_conn=db_conn,
                    workspace_id=workspace_id,
                    processor_type=processor_type,
                    processor_customer_id=processor_customer_id,
                    qbo_customer_id=qbo_customer_id,
                    match_source='manual',
                    canonical_event_id=canonical_event_id
                )

        db_conn.commit()

        # Log the resolution
        logger.log_match_manual_resolved(
            review_id=review_id,
            processor_payment_id=processor_payment_id,
            qbo_payment_id=selected_qbo_payment_id,
            resolved_by=resolved_by
        )

        # Complete idempotency operation
        result_id = selected_qbo_payment_id or "unmatched"
        idempotency_manager.complete_operation(idempotency_key, result_id)

        return {
            'success': True,
            'review_id': review_id,
            'canonical_event_id': canonical_event_id,
            'qbo_payment_id': selected_qbo_payment_id,
            'resolution_type': 'matched' if selected_qbo_payment_id else 'unmatched',
            'was_cached': False
        }

    except Exception as e:
        db_conn.rollback()
        idempotency_manager.fail_operation(idempotency_key, str(e))
        raise


def get_pending_reviews(
    db_conn: sqlite3.Connection,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Get pending match reviews for manual resolution.

    Args:
        db_conn: Database connection
        limit: Maximum number of reviews to return

    Returns:
        List of pending review dicts with candidates
    """
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT
            mr.id,
            mr.canonical_event_id,
            mr.processor_payment_id,
            mr.amount,
            mr.event_date,
            mr.customer_email,
            mr.customer_name,
            mr.match_candidates,
            mr.created_at
        FROM match_reviews mr
        WHERE mr.status = 'pending'
        ORDER BY mr.created_at ASC
        LIMIT ?
    """, (limit,))

    reviews = []
    for row in cursor.fetchall():
        reviews.append({
            'id': row[0],
            'canonical_event_id': row[1],
            'processor_payment_id': row[2],
            'amount': row[3],
            'event_date': row[4],
            'customer_email': row[5],
            'customer_name': row[6],
            'candidates': json.loads(row[7]) if row[7] else [],
            'created_at': row[8]
        })

    return reviews


def get_review_by_id(
    review_id: str,
    db_conn: sqlite3.Connection
) -> Optional[Dict[str, Any]]:
    """
    Get a single match review by ID.

    Args:
        review_id: Review ID
        db_conn: Database connection

    Returns:
        Review dict or None if not found
    """
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT
            mr.id,
            mr.canonical_event_id,
            mr.processor_payment_id,
            mr.amount,
            mr.event_date,
            mr.customer_email,
            mr.customer_name,
            mr.match_candidates,
            mr.status,
            mr.selected_qbo_payment_id,
            mr.resolved_at,
            mr.resolved_by,
            mr.created_at,
            mr.updated_at
        FROM match_reviews mr
        WHERE mr.id = ?
    """, (review_id,))

    row = cursor.fetchone()
    if not row:
        return None

    return {
        'id': row[0],
        'canonical_event_id': row[1],
        'processor_payment_id': row[2],
        'amount': row[3],
        'event_date': row[4],
        'customer_email': row[5],
        'customer_name': row[6],
        'candidates': json.loads(row[7]) if row[7] else [],
        'status': row[8],
        'selected_qbo_payment_id': row[9],
        'resolved_at': row[10],
        'resolved_by': row[11],
        'created_at': row[12],
        'updated_at': row[13]
    }
