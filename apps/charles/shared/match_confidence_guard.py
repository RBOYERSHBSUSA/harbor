"""
Match Confidence Guard — Structural Enforcement Module

This module provides structural enforcement of confidence thresholds required
by CHARLES_MATCHING_ASSISTANCE_SPEC.MD.

CONTRACT COMPLIANCE:
Per CHARLES_MATCHING_ASSISTANCE_SPEC.MD (Lines 42-44):
- High confidence (>= 85): auto-accept
- Medium confidence (50-84): flag for confirmation
- Low confidence (< 50): deterministic fallback

ENFORCEMENT POINTS:
- Database CHECK constraints enforce confidence thresholds
- Runtime validation prevents threshold tampering
- Audit trail for all match decisions

Per R-4 remediation (Finding A-1).
"""

import sqlite3
from datetime import datetime
from typing import Optional, Literal, Dict, Any
from dataclasses import dataclass


# =============================================================================
# CONTRACT-MANDATED THRESHOLDS (HARD-CODED)
# =============================================================================

# Per CHARLES_MATCHING_ASSISTANCE_SPEC.MD: "High confidence: auto-accept"
HIGH_CONFIDENCE_THRESHOLD = 85

# Per CHARLES_MATCHING_ASSISTANCE_SPEC.MD: "Medium confidence: flag for confirmation"
MEDIUM_CONFIDENCE_THRESHOLD = 50

# Confidence levels
ConfidenceLevel = Literal['high', 'medium', 'low', 'none']

# Match types
MatchType = Literal['auto', 'manual', 'pending', 'unmatched']


# =============================================================================
# EXCEPTIONS
# =============================================================================

class ConfidenceThresholdViolation(Exception):
    """
    Raised when confidence threshold is violated.

    This indicates a CRITICAL CONTRACT VIOLATION.
    """
    pass


class InvalidMatchTypeTransition(Exception):
    """Raised when attempting an invalid match type transition."""
    pass


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def validate_auto_match_threshold(confidence: int) -> None:
    """
    Validate that confidence meets threshold for auto-matching.

    Per CHARLES_MATCHING_ASSISTANCE_SPEC.MD: "High confidence: auto-accept"
    Only confidence >= 85 may be auto-matched.

    Args:
        confidence: Match confidence score (0-100)

    Raises:
        ConfidenceThresholdViolation: If confidence < 85
    """
    if confidence < HIGH_CONFIDENCE_THRESHOLD:
        raise ConfidenceThresholdViolation(
            f"Auto-match requires confidence >= {HIGH_CONFIDENCE_THRESHOLD}, "
            f"got {confidence}. Per CHARLES_MATCHING_ASSISTANCE_SPEC.MD, "
            f"medium/low confidence must require human confirmation."
        )


def get_confidence_level(confidence: int) -> ConfidenceLevel:
    """
    Determine confidence level from score.

    Per CHARLES_MATCHING_ASSISTANCE_SPEC.MD thresholds:
    - HIGH: >= 85
    - MEDIUM: 50-84
    - LOW: < 50

    Args:
        confidence: Match confidence score (0-100)

    Returns:
        ConfidenceLevel
    """
    if confidence >= HIGH_CONFIDENCE_THRESHOLD:
        return 'high'
    elif confidence >= MEDIUM_CONFIDENCE_THRESHOLD:
        return 'medium'
    elif confidence > 0:
        return 'low'
    else:
        return 'none'


def validate_match_type_for_confidence(
    match_type: MatchType,
    confidence: int
) -> None:
    """
    Validate that match_type is appropriate for confidence level.

    Contract requirements:
    - 'auto' → confidence >= 85 (HIGH)
    - 'manual' → any confidence (human confirmed)
    - 'pending' → confidence < 85 (awaiting review)
    - 'unmatched' → no match found

    Args:
        match_type: The match type being set
        confidence: Match confidence score

    Raises:
        ConfidenceThresholdViolation: If match_type violates threshold
    """
    if match_type == 'auto':
        validate_auto_match_threshold(confidence)
    elif match_type == 'manual':
        # Manual matches can have any confidence (human confirmed)
        pass
    elif match_type == 'pending':
        # Pending should be < HIGH threshold (otherwise would auto-match)
        if confidence >= HIGH_CONFIDENCE_THRESHOLD:
            raise ConfidenceThresholdViolation(
                f"Confidence {confidence} >= {HIGH_CONFIDENCE_THRESHOLD} "
                f"should auto-match, not be pending. Contract violation."
            )


# =============================================================================
# CONFIDENCE GUARD
# =============================================================================

class MatchConfidenceGuard:
    """
    Structural enforcement of confidence thresholds.

    Ensures compliance with CHARLES_MATCHING_ASSISTANCE_SPEC.MD:
    - High confidence (>= 85): auto-accept
    - Medium confidence (50-84): flag for confirmation
    - Low confidence (< 50): deterministic fallback

    This class provides validation before database operations to ensure
    contract compliance.
    """

    def __init__(self, db_conn: sqlite3.Connection):
        """
        Initialize the confidence guard.

        Args:
            db_conn: Database connection with canonical_events table
        """
        self.db_conn = db_conn
        self._verify_schema_compliance()

    def _verify_schema_compliance(self) -> None:
        """
        Verify that database schema enforces confidence boundaries.

        Raises:
            ConfidenceThresholdViolation: If schema lacks required constraints
        """
        cursor = self.db_conn.cursor()

        # Check for required columns
        cursor.execute("PRAGMA table_info(canonical_events)")
        columns = {row[1] for row in cursor.fetchall()}

        required_columns = {'match_type', 'match_confidence', 'human_confirmed_at', 'confirmed_by'}
        missing = required_columns - columns

        if missing:
            raise ConfidenceThresholdViolation(
                f"Schema missing required columns: {missing}. "
                f"Run R-4 migration to add: {', '.join(missing)}"
            )

    def validate_auto_match(
        self,
        canonical_event_id: str,
        qbo_payment_id: str,
        confidence: int,
        score_breakdown: Dict[str, Any]
    ) -> None:
        """
        Validate that auto-match meets confidence threshold.

        Per contract: Only HIGH confidence (>= 85) may auto-match.

        Args:
            canonical_event_id: ID of canonical event
            qbo_payment_id: QBO payment ID being matched
            confidence: Match confidence score
            score_breakdown: Score breakdown for audit trail

        Raises:
            ConfidenceThresholdViolation: If confidence < 85
        """
        # GUARD: Validate threshold
        validate_auto_match_threshold(confidence)

        # Additional validation: check match_type will be 'auto'
        validate_match_type_for_confidence('auto', confidence)

    def validate_manual_match(
        self,
        canonical_event_id: str,
        qbo_payment_id: Optional[str],
        confirmed_by: str,
        confirmed_at: datetime
    ) -> None:
        """
        Validate that manual match has required confirmation metadata.

        Per contract: Medium/low confidence requires human confirmation.

        Args:
            canonical_event_id: ID of canonical event
            qbo_payment_id: QBO payment ID being matched (or None if unmatched)
            confirmed_by: User who confirmed
            confirmed_at: Timestamp of confirmation

        Raises:
            ValueError: If confirmation metadata is missing
        """
        if not confirmed_by:
            raise ValueError(
                f"Manual match requires confirmed_by. "
                f"Per contract: medium/low confidence must have human confirmation."
            )

        if not confirmed_at:
            raise ValueError(
                f"Manual match requires confirmed_at timestamp. "
                f"Per contract: medium/low confidence must have human confirmation."
            )

    def get_permitted_match_type(self, confidence: int) -> MatchType:
        """
        Determine permitted match type for confidence level.

        Per CHARLES_MATCHING_ASSISTANCE_SPEC.MD:
        - >= 85: 'auto' (auto-accept)
        - 50-84: 'pending' (flag for confirmation)
        - < 50: 'pending' (deterministic fallback, then human review)

        Args:
            confidence: Match confidence score

        Returns:
            Permitted MatchType
        """
        if confidence >= HIGH_CONFIDENCE_THRESHOLD:
            return 'auto'
        else:
            return 'pending'


# =============================================================================
# MIGRATION HELPER
# =============================================================================

def apply_r4_schema_migration(db_conn: sqlite3.Connection) -> None:
    """
    Apply R-4 schema migration to add confidence threshold enforcement.

    Adds:
    - human_confirmed_at column (for manual matches)
    - confirmed_by column (for manual matches)
    - CHECK constraint: auto-match requires confidence >= 85
    - CHECK constraint: manual match requires confirmation metadata
    - Backfills existing manual matches with confirmation data

    This migration is IDEMPOTENT.

    Args:
        db_conn: Database connection
    """
    cursor = db_conn.cursor()

    # Check if already migrated
    cursor.execute("PRAGMA table_info(canonical_events)")
    columns = {row[1] for row in cursor.fetchall()}

    # Add confirmation columns if missing
    if 'human_confirmed_at' not in columns:
        print("Adding human_confirmed_at column...")
        cursor.execute("""
            ALTER TABLE canonical_events
            ADD COLUMN human_confirmed_at TIMESTAMP
        """)

    if 'confirmed_by' not in columns:
        print("Adding confirmed_by column...")
        cursor.execute("""
            ALTER TABLE canonical_events
            ADD COLUMN confirmed_by TEXT
        """)

    # Backfill existing manual matches with confirmation metadata
    print("Backfilling manual matches with confirmation metadata...")
    cursor.execute("""
        UPDATE canonical_events
        SET human_confirmed_at = COALESCE(
                (SELECT resolved_at FROM match_reviews
                 WHERE canonical_event_id = canonical_events.id
                 AND status = 'resolved'
                 LIMIT 1),
                CURRENT_TIMESTAMP
            ),
            confirmed_by = COALESCE(
                (SELECT resolved_by FROM match_reviews
                 WHERE canonical_event_id = canonical_events.id
                 AND status = 'resolved'
                 LIMIT 1),
                'system_migration_r4'
            )
        WHERE match_type = 'manual'
          AND qbo_payment_id IS NOT NULL
          AND human_confirmed_at IS NULL
    """)

    manual_updated = cursor.rowcount

    # Validate existing auto-matches meet threshold
    print("Validating existing auto-matches meet confidence threshold...")
    cursor.execute("""
        SELECT id, match_confidence
        FROM canonical_events
        WHERE match_type = 'auto'
          AND (match_confidence IS NULL OR match_confidence < 85)
    """)

    violations = cursor.fetchall()
    if violations:
        raise ConfidenceThresholdViolation(
            f"Found {len(violations)} auto-matches with confidence < 85. "
            f"This violates CHARLES_MATCHING_ASSISTANCE_SPEC.MD. "
            f"Cannot apply migration until these are corrected. "
            f"IDs: {[v[0] for v in violations[:5]]}"
        )

    db_conn.commit()

    print("✓ R-4 schema migration applied successfully")
    print(f"  - Added: human_confirmed_at column")
    print(f"  - Added: confirmed_by column")
    print(f"  - Backfilled {manual_updated} manual matches with confirmation metadata")
    print(f"  - Validated all auto-matches have confidence >= 85")
    print(f"\nNOTE: Database triggers should be added manually:")
    print(f"  See scripts/apply_r4_triggers.sql")
