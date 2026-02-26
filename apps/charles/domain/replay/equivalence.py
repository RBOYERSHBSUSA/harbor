"""
Equivalence Verification Module - Phase 3

================================================================================
LAYER 3 COMPONENT — EQUIVALENCE VERIFICATION
================================================================================

This module implements equivalence verification for replay vs original
canonical events per PHASE3_REPLAY_ORCHESTRATION_FORENSICS_PLAN.md §4.

EQUIVALENCE DEFINITION (§4.1):
Replay is equivalent if and only if it produces identical canonical events
given the same raw Stripe objects as input.

COMPARED FIELDS:
- processor_payment_id
- processor_payout_id
- amount_gross
- amount_net
- fees
- event_type
- customer_email
- customer_name

IGNORED FIELDS:
- id (generated UUID)
- created_at (execution time)
- updated_at (execution time)
- sync_run_id (differs by definition)

================================================================================
"""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional


class EquivalenceStatus(Enum):
    """Result status of equivalence check."""
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass
class EventDifference:
    """
    Represents a difference between original and replay events.

    Attributes:
        processor_payment_id: The payment ID where difference was found
        field_name: Name of the differing field
        original_value: Value in original canonical event
        replay_value: Value in replay canonical event
    """
    processor_payment_id: str
    field_name: str
    original_value: Any
    replay_value: Any

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'processor_payment_id': self.processor_payment_id,
            'field_name': self.field_name,
            'original_value': str(self.original_value),
            'replay_value': str(self.replay_value),
        }


@dataclass
class EquivalenceResult:
    """
    Result of equivalence check between original and replay events.

    Attributes:
        status: PASS or FAIL
        original_count: Number of original canonical events
        replay_count: Number of replay canonical events
        matching_count: Number of events that match perfectly
        differing_events: List of events with field differences
        missing_events: Processor payment IDs in original but not replay
        extra_events: Processor payment IDs in replay but not original
        differences: Detailed list of field-level differences
    """
    status: EquivalenceStatus
    original_count: int
    replay_count: int
    matching_count: int
    differing_events: List[str] = field(default_factory=list)
    missing_events: List[str] = field(default_factory=list)
    extra_events: List[str] = field(default_factory=list)
    differences: List[EventDifference] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'result': self.status.value,
            'original_canonical_events': self.original_count,
            'replay_canonical_events': self.replay_count,
            'matching_events': self.matching_count,
            'differing_events': len(self.differing_events),
            'missing_events': len(self.missing_events),
            'extra_events': len(self.extra_events),
            'differing_event_ids': self.differing_events,
            'missing_event_ids': self.missing_events,
            'extra_event_ids': self.extra_events,
            'differences': [d.to_dict() for d in self.differences],
        }


class EquivalenceChecker:
    """
    Field-by-field semantic comparison of canonical events.

    Per PHASE3_REPLAY_ORCHESTRATION_FORENSICS_PLAN.md §4:
    - Compares semantic fields only
    - Ignores IDs, timestamps, logs, execution order
    - Detects missing, extra, and differing events
    """

    # Fields that are compared for semantic equivalence
    # Per §4.1 and §4.2 of Phase 3 plan
    COMPARED_FIELDS = [
        'processor_payment_id',
        'processor_payout_id',
        'processor_refund_id',
        'amount_gross',
        'amount_net',
        'fees',
        'event_type',
        'customer_email',
        'customer_name',
        'processor_customer_id',
        'payment_method',
        'card_brand',
        'card_last4',
        'description',
    ]

    # Fields that are explicitly ignored
    # Per §4.3 of Phase 3 plan
    IGNORED_FIELDS = [
        'id',
        'created_at',
        'updated_at',
        'sync_run_id',
        'qbo_payment_id',
        'qbo_invoice_id',
        'match_confidence',
        'match_type',
        'match_score_breakdown',
        'human_confirmed_at',
        'confirmed_by',
    ]

    def compare_events(
        self,
        original_events: List[Dict[str, Any]],
        replay_events: List[Dict[str, Any]],
    ) -> EquivalenceResult:
        """
        Compare original and replay canonical events for equivalence.

        Args:
            original_events: List of original canonical events (dicts)
            replay_events: List of replay canonical events (dicts)

        Returns:
            EquivalenceResult with PASS/FAIL status and details

        Algorithm:
            1. Index both lists by processor_payment_id
            2. Find missing events (in original, not in replay)
            3. Find extra events (in replay, not in original)
            4. Compare fields for events in both sets
            5. Aggregate results
        """
        # Index by processor_payment_id for fast lookup
        original_by_id = self._index_by_payment_id(original_events)
        replay_by_id = self._index_by_payment_id(replay_events)

        original_ids = set(original_by_id.keys())
        replay_ids = set(replay_by_id.keys())

        # Find missing and extra events
        missing_events = list(original_ids - replay_ids)
        extra_events = list(replay_ids - original_ids)

        # Compare events that exist in both
        common_ids = original_ids & replay_ids
        differences: List[EventDifference] = []
        differing_events: List[str] = []
        matching_count = 0

        for payment_id in common_ids:
            original_event = original_by_id[payment_id]
            replay_event = replay_by_id[payment_id]

            event_diffs = self._compare_single_event(
                payment_id, original_event, replay_event
            )

            if event_diffs:
                differing_events.append(payment_id)
                differences.extend(event_diffs)
            else:
                matching_count += 1

        # Determine overall status
        has_issues = (
            len(missing_events) > 0 or
            len(extra_events) > 0 or
            len(differing_events) > 0
        )
        status = EquivalenceStatus.FAIL if has_issues else EquivalenceStatus.PASS

        return EquivalenceResult(
            status=status,
            original_count=len(original_events),
            replay_count=len(replay_events),
            matching_count=matching_count,
            differing_events=differing_events,
            missing_events=missing_events,
            extra_events=extra_events,
            differences=differences,
        )

    def _index_by_payment_id(
        self,
        events: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Index events by processor_payment_id.

        For refunds and fees, use processor_refund_id or a composite key.
        """
        indexed = {}
        for event in events:
            # Primary key is processor_payment_id
            payment_id = event.get('processor_payment_id')

            # For refunds, use processor_refund_id if payment_id is null
            if not payment_id:
                payment_id = event.get('processor_refund_id')

            # For fees, use a composite of payout_id + fee type
            if not payment_id:
                payout_id = event.get('processor_payout_id', '')
                event_type = event.get('event_type', '')
                payment_id = f"{payout_id}:{event_type}"

            if payment_id:
                indexed[payment_id] = event

        return indexed

    def _compare_single_event(
        self,
        payment_id: str,
        original: Dict[str, Any],
        replay: Dict[str, Any],
    ) -> List[EventDifference]:
        """
        Compare a single pair of events field-by-field.

        Returns list of differences found.
        """
        differences = []

        for field_name in self.COMPARED_FIELDS:
            original_value = original.get(field_name)
            replay_value = replay.get(field_name)

            if not self._values_equal(original_value, replay_value):
                differences.append(EventDifference(
                    processor_payment_id=payment_id,
                    field_name=field_name,
                    original_value=original_value,
                    replay_value=replay_value,
                ))

        return differences

    def _values_equal(self, v1: Any, v2: Any) -> bool:
        """
        Compare two values for semantic equality.

        Handles:
        - None vs empty string (considered equal)
        - Decimal vs string comparison
        - Case-sensitive string comparison
        """
        # Both None or both empty
        if self._is_empty(v1) and self._is_empty(v2):
            return True

        # One empty, one not
        if self._is_empty(v1) != self._is_empty(v2):
            return False

        # Convert to comparable types
        v1_normalized = self._normalize_value(v1)
        v2_normalized = self._normalize_value(v2)

        return v1_normalized == v2_normalized

    def _is_empty(self, value: Any) -> bool:
        """Check if value is considered empty."""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == '':
            return True
        return False

    def _normalize_value(self, value: Any) -> Any:
        """
        Normalize value for comparison.

        - Converts Decimal to string with consistent precision
        - Strips whitespace from strings
        - Returns other types as-is
        """
        if value is None:
            return None

        if isinstance(value, Decimal):
            # Normalize to string with consistent precision
            return str(value.quantize(Decimal('0.01')))

        if isinstance(value, str):
            # Try to parse as Decimal for amount fields
            stripped = value.strip()
            try:
                decimal_val = Decimal(stripped)
                return str(decimal_val.quantize(Decimal('0.01')))
            except Exception:
                return stripped

        return value
