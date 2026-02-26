"""
Raw Data Persistence Module - Phase 2

================================================================================
LAYER 1 BOUNDARY — RAW PROCESSOR DATA PERSISTENCE
================================================================================

This module implements raw processor data persistence at the Layer 1 boundary
per PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md.

CRITICAL ARCHITECTURAL CONSTRAINTS:
- This is a Layer 1 component (no canonical models, no business logic)
- Persistence is append-only and immutable
- Duplicate detection prevents re-persistence
- Feature-flagged via ENABLE_RAW_DATA_PERSISTENCE
- Layer 2 must be completely unaware of this module's existence

FORBIDDEN in this module:
- Canonical payment/payout/refund/fee models
- Amount normalization (Decimal operations for business logic)
- QuickBooks references of any kind
- Scoring, matching, or business logic

ALLOWED in this module:
- Raw JSON storage
- Duplicate detection
- Database writes
- Structured logging

See: PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md
See: PROCESSOR_TRANSACTIONS_CONTRACT.md
================================================================================
"""

import json
import os
import sqlite3
import uuid
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from shared.structured_logging import StructuredLogger, EventType


# =============================================================================
# Feature Flag
# =============================================================================

def is_raw_data_persistence_enabled() -> bool:
    """
    Check if raw data persistence is enabled.

    Per PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md §8.1:
    - Default is false (persistence disabled)
    - When true, persistence is enforced

    Returns:
        True if ENABLE_RAW_DATA_PERSISTENCE=true, False otherwise
    """
    return os.getenv('ENABLE_RAW_DATA_PERSISTENCE', 'false').lower() == 'true'


# =============================================================================
# Persistence Timeout Configuration
# =============================================================================

# Per Phase 2 plan §5.3.1: persistence has a timeout
# If timeout is exceeded, persistence is skipped (not failed)
RAW_DATA_PERSISTENCE_TIMEOUT_MS = int(
    os.getenv('RAW_DATA_PERSISTENCE_TIMEOUT_MS', '5000')
)


# =============================================================================
# Object Type Constants
# =============================================================================

# Valid object types per Phase 2 plan §2.1
OBJECT_TYPES = {
    'payout',
    'balance_txn',
    'charge',
    'refund',
    'customer',
    'payouts_list',
    'balance_transactions_list',
}


# =============================================================================
# Raw Data Persistence Class
# =============================================================================

class RawDataPersistence:
    """
    Raw processor data persistence at Layer 1 boundary.

    This class handles:
    - Persisting raw API responses to database
    - Duplicate detection and cache hits
    - Failure handling per Phase 2 plan

    Thread Safety:
        Each method creates its own database connection.
        Safe for concurrent use.

    Statefulness:
        Stateless - no instance state persists across method calls.
        All state is in the database.
    """

    def __init__(
        self,
        db_path: str,
        workspace_id: str,
        sync_run_id: str,
        logger: StructuredLogger,
    ):
        """
        Initialize raw data persistence.

        Args:
            db_path: Path to SQLite database
            workspace_id: Workspace identifier for isolation
            sync_run_id: Current sync run ID
            logger: Structured logger instance
        """
        self.db_path = db_path
        self.workspace_id = workspace_id
        self.sync_run_id = sync_run_id
        self.logger = logger

    def persist_raw_data(
        self,
        processor: str,
        object_type: str,
        processor_object_id: str,
        raw_json: Dict,
        api_endpoint: str,
        http_status_code: int,
        stripe_request_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Persist raw processor data to database.

        Per PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md:
        - Called after successful HTTP response
        - Before returning data to Layer 2
        - Synchronous operation
        - Feature-flag gated

        Args:
            processor: Processor name ('stripe' or 'authorize_net')
            object_type: Type of object (payout, charge, refund, etc.)
            processor_object_id: Processor's native ID for the object
            raw_json: Verbatim API response (dict)
            api_endpoint: API endpoint called (e.g., '/payouts/po_xxx')
            http_status_code: HTTP response status code
            stripe_request_id: Stripe's Request-Id header if available

        Returns:
            Tuple of (success: bool, raw_data_id: Optional[str])
            - success=True, raw_data_id=<id> if persisted
            - success=True, raw_data_id=<id> if cache hit (already exists)
            - success=False, raw_data_id=None if persistence failed

        Raises:
            Nothing - failures are logged and return (False, None)
        """
        # Check feature flag
        if not is_raw_data_persistence_enabled():
            self.logger.warn(
                EventType.RAW_DATA_PERSISTENCE_SKIPPED,
                f"Raw data persistence disabled for {object_type} {processor_object_id}",
                object_type=object_type,
                processor_object_id=processor_object_id,
                sync_run_id=self.sync_run_id,
                workspace_id=self.workspace_id,
                skip_reason="feature_disabled"
            )
            return (True, None)  # Not an error, just skipped

        # Validate object type
        if object_type not in OBJECT_TYPES:
            self.logger.error(
                EventType.RAW_DATA_PERSISTENCE_FAILED,
                f"Invalid object_type: {object_type}",
                object_type=object_type,
                processor_object_id=processor_object_id,
                sync_run_id=self.sync_run_id,
                workspace_id=self.workspace_id,
                error_code="INVALID_OBJECT_TYPE"
            )
            return (False, None)

        start_time = time.time()

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Check for duplicate (per §4.1)
            existing = self._check_duplicate(
                cursor, processor, object_type, processor_object_id
            )
            if existing:
                conn.close()
                self.logger.info(
                    EventType.RAW_DATA_CACHE_HIT,
                    f"Raw data cache hit for {object_type} {processor_object_id}",
                    object_type=object_type,
                    processor_object_id=processor_object_id,
                    sync_run_id=self.sync_run_id,
                    workspace_id=self.workspace_id,
                    existing_id=existing
                )
                return (True, existing)

            # Check timeout before proceeding
            elapsed_ms = int((time.time() - start_time) * 1000)
            if elapsed_ms > RAW_DATA_PERSISTENCE_TIMEOUT_MS:
                conn.close()
                self.logger.warn(
                    EventType.RAW_DATA_PERSISTENCE_TIMEOUT_SKIPPED,
                    f"Persistence timeout for {object_type} {processor_object_id}",
                    object_type=object_type,
                    processor_object_id=processor_object_id,
                    sync_run_id=self.sync_run_id,
                    workspace_id=self.workspace_id,
                    timeout_ms=elapsed_ms,
                    threshold_ms=RAW_DATA_PERSISTENCE_TIMEOUT_MS
                )
                return (True, None)  # Timeout is skip, not failure

            # Generate ID
            raw_data_id = f"raw_{uuid.uuid4().hex[:16]}"
            captured_at = datetime.now(timezone.utc).isoformat()

            # Insert raw data
            cursor.execute("""
                INSERT INTO raw_processor_data (
                    id,
                    workspace_id,
                    processor,
                    object_type,
                    processor_object_id,
                    sync_run_id,
                    raw_json,
                    api_endpoint,
                    http_status_code,
                    captured_at,
                    stripe_request_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                raw_data_id,
                self.workspace_id,
                processor,
                object_type,
                processor_object_id,
                self.sync_run_id,
                json.dumps(raw_json),
                api_endpoint,
                http_status_code,
                captured_at,
                stripe_request_id
            ))

            conn.commit()
            conn.close()

            duration_ms = int((time.time() - start_time) * 1000)

            self.logger.info(
                EventType.RAW_DATA_PERSISTED,
                f"Persisted raw data for {object_type} {processor_object_id}",
                object_type=object_type,
                processor_object_id=processor_object_id,
                sync_run_id=self.sync_run_id,
                workspace_id=self.workspace_id,
                raw_data_id=raw_data_id,
                duration_ms=duration_ms
            )

            return (True, raw_data_id)

        except sqlite3.IntegrityError:
            # Duplicate detected via unique constraint - this is a cache hit
            # (race condition between check and insert)
            if 'conn' in locals():
                conn.close()

            existing = self._get_existing_id(
                processor, object_type, processor_object_id
            )
            self.logger.info(
                EventType.RAW_DATA_CACHE_HIT,
                f"Raw data cache hit (constraint) for {object_type} {processor_object_id}",
                object_type=object_type,
                processor_object_id=processor_object_id,
                sync_run_id=self.sync_run_id,
                workspace_id=self.workspace_id,
                existing_id=existing
            )
            return (True, existing)

        except Exception as e:
            # Per §5.1: persistence failure = fetch failure
            if 'conn' in locals():
                conn.close()

            duration_ms = int((time.time() - start_time) * 1000)

            self.logger.error(
                EventType.RAW_DATA_PERSISTENCE_FAILED,
                f"Failed to persist raw data for {object_type} {processor_object_id}: {e}",
                object_type=object_type,
                processor_object_id=processor_object_id,
                sync_run_id=self.sync_run_id,
                workspace_id=self.workspace_id,
                error=str(e),
                error_type=type(e).__name__,
                duration_ms=duration_ms
            )
            return (False, None)

    def _check_duplicate(
        self,
        cursor: sqlite3.Cursor,
        processor: str,
        object_type: str,
        processor_object_id: str,
    ) -> Optional[str]:
        """
        Check if raw data already exists for this sync run.

        Per §4.1: duplicate if same workspace_id + processor + object_type +
        processor_object_id + sync_run_id

        Returns:
            Existing raw_data_id if duplicate, None otherwise
        """
        cursor.execute("""
            SELECT id FROM raw_processor_data
            WHERE workspace_id = ?
              AND processor = ?
              AND object_type = ?
              AND processor_object_id = ?
              AND sync_run_id = ?
        """, (
            self.workspace_id,
            processor,
            object_type,
            processor_object_id,
            self.sync_run_id
        ))
        row = cursor.fetchone()
        return row['id'] if row else None

    def _get_existing_id(
        self,
        processor: str,
        object_type: str,
        processor_object_id: str,
    ) -> Optional[str]:
        """
        Get existing raw data ID (for cache hit after constraint violation).

        Returns:
            Existing raw_data_id if found, None otherwise
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id FROM raw_processor_data
                WHERE workspace_id = ?
                  AND processor = ?
                  AND object_type = ?
                  AND processor_object_id = ?
                  AND sync_run_id = ?
            """, (
                self.workspace_id,
                processor,
                object_type,
                processor_object_id,
                self.sync_run_id
            ))
            row = cursor.fetchone()
            conn.close()
            return row['id'] if row else None
        except Exception:
            return None

    def get_raw_data(
        self,
        processor: str,
        object_type: str,
        processor_object_id: str,
        sync_run_id: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Retrieve persisted raw data.

        Used by replay adapter to get stored data instead of making API calls.

        Args:
            processor: Processor name
            object_type: Type of object
            processor_object_id: Processor's native ID
            sync_run_id: Specific sync run ID (defaults to instance sync_run_id)

        Returns:
            Parsed JSON dict if found, None otherwise
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            target_sync_run = sync_run_id or self.sync_run_id

            cursor.execute("""
                SELECT raw_json FROM raw_processor_data
                WHERE workspace_id = ?
                  AND processor = ?
                  AND object_type = ?
                  AND processor_object_id = ?
                  AND sync_run_id = ?
            """, (
                self.workspace_id,
                processor,
                object_type,
                processor_object_id,
                target_sync_run
            ))
            row = cursor.fetchone()
            conn.close()

            if row:
                return json.loads(row['raw_json'])
            return None

        except Exception:
            return None

    def get_raw_data_for_replay(
        self,
        processor: str,
        object_type: str,
        processor_object_id: str,
    ) -> Optional[Dict]:
        """
        Get raw data for replay mode.

        In replay mode, we need the most recent snapshot for this object.
        This is used when the specific sync_run_id is not known.

        Args:
            processor: Processor name
            object_type: Type of object
            processor_object_id: Processor's native ID

        Returns:
            Parsed JSON dict from most recent capture, None if not found
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT raw_json FROM raw_processor_data
                WHERE workspace_id = ?
                  AND processor = ?
                  AND object_type = ?
                  AND processor_object_id = ?
                ORDER BY captured_at DESC
                LIMIT 1
            """, (
                self.workspace_id,
                processor,
                object_type,
                processor_object_id,
            ))
            row = cursor.fetchone()
            conn.close()

            if row:
                return json.loads(row['raw_json'])
            return None

        except Exception:
            return None
