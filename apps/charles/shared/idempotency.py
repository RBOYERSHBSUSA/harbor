"""
Idempotency Module for Charles

Implements IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1

This module provides the single canonical idempotency check function and storage
for all operations that create external state (QBO objects, processor API calls).

Key Principles (§3):
- Idempotency is mandatory for all external state changes
- Check before act: verify key exists BEFORE any external action
- Single canonical check location per operation type
- Idempotency keys are immutable once completed

Key Format v2 (§4.1, v1.1):
    {operation_type}:ws:{workspace_id}:{processor}:{external_id}

Example:
    deposit:ws:ws_abc123:stripe:po_xyz789

Legacy Format (for dual-read compatibility):
    {operation_type}:{company_id}:{external_id}

Example:
    deposit:company_123:po_abc123
"""

import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, Tuple, Dict, Any
from dataclasses import dataclass

from shared.structured_logging import get_logger, EventType


class IdempotencyStatus(Enum):
    """
    Idempotency check outcomes per §6.2
    """
    NOT_EXISTS = "not_exists"           # Safe to proceed
    EXISTS_PENDING = "exists_pending"   # Operation in progress, reject/wait
    EXISTS_COMPLETED = "exists_completed"  # Already done, return cached result
    EXISTS_FAILED = "exists_failed"     # Previous attempt failed, may retry


@dataclass
class IdempotencyResult:
    """
    Result of idempotency check per §5.2 storage schema
    """
    status: IdempotencyStatus
    idempotency_key: str
    operation_type: str
    result_id: Optional[str] = None      # External ID of created object (if any)
    result_data: Optional[Dict] = None   # Additional result metadata
    error_message: Optional[str] = None  # Error details if failed
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class IdempotencyManager:
    """
    Manages idempotency keys and checks per IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1.

    Usage:
        manager = IdempotencyManager(db_connection, company_id, workspace_id)

        # Generate v2 key
        key = generate_idempotency_key_v2("deposit", workspace_id, "stripe", "po_abc123")

        # Check idempotency with dual-read for legacy compatibility
        legacy_key = _generate_legacy_key("deposit", company_id, "po_abc123")
        result = manager.check_idempotency_dual_read(key, legacy_key)

        if result.status == IdempotencyStatus.EXISTS_COMPLETED:
            return result.result_id  # Already done

        if result.status == IdempotencyStatus.EXISTS_PENDING:
            raise Exception("Operation in progress")

        if result.status in (IdempotencyStatus.EXISTS_FAILED, IdempotencyStatus.NOT_EXISTS):
            # Safe to proceed - start operation with v2 key
            manager.start_operation(key, "deposit")

        # Perform external action...

        # Mark complete or failed
        manager.complete_operation(key, "qbo_deposit_456")
        # or
        manager.fail_operation(key, "API Error")
    """

    # Timeout for pending operations (§6.3) - 30 minutes
    PENDING_TIMEOUT_MINUTES = 30

    def __init__(self, db_connection: sqlite3.Connection, company_id: str, workspace_id: str, service: str = "idempotency"):
        """
        Initialize idempotency manager.

        Args:
            db_connection: SQLite connection to company database
            company_id: Company ID for logging metadata and legacy compatibility
            workspace_id: Workspace ID (REQUIRED for scoping per IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1)
            service: Service name for logging

        Raises:
            ValueError: If workspace_id is None or empty (fail closed per contract)
        """
        # CONTRACT v1.1: Fail closed if workspace_id is missing
        if not workspace_id or (isinstance(workspace_id, str) and workspace_id.strip() == ''):
            raise ValueError(
                "IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1 violated: "
                "workspace_id is REQUIRED for IdempotencyManager"
            )

        self.conn = db_connection
        self.company_id = company_id
        self.workspace_id = workspace_id
        self.logger = get_logger(service=service, workspace_id=workspace_id, company_id=company_id)
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """
        Ensure idempotency_keys table exists per §5.2 storage schema.
        Phase 2+ schema includes workspace_id for multitenancy isolation.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS idempotency_keys (
                idempotency_key TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                operation_type TEXT NOT NULL,
                processor_type TEXT NOT NULL,
                external_event_id TEXT NOT NULL,
                company_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'completed', 'failed')),
                result_id TEXT,
                result_data TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                error_message TEXT,
                UNIQUE(workspace_id, processor_type, external_event_id),
                FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
            )
        """)

        # Create index for cleanup queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_idempotency_status_created
            ON idempotency_keys(status, created_at)
        """)

        self.conn.commit()

    def _get_timestamp(self) -> str:
        """Get ISO 8601 UTC timestamp."""
        return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    def _parse_timestamp(self, ts: str) -> datetime:
        """Parse ISO 8601 timestamp."""
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))

    def check_idempotency(self, key: str) -> IdempotencyResult:
        """
        Check idempotency status for a key.

        This is the SINGLE CANONICAL CHECK FUNCTION per §6.1.
        All operations must call this function. Direct database queries
        for idempotency are forbidden.

        Args:
            key: Idempotency key in format {operation_type}:{scope}:{external_id}

        Returns:
            IdempotencyResult with status:
            - NOT_EXISTS: Safe to proceed (call start_operation next)
            - EXISTS_PENDING: Operation in progress, reject/wait
            - EXISTS_COMPLETED: Already done, return cached result
            - EXISTS_FAILED: Previous attempt failed, may retry
        """
        # ===================================================================
        # CONTRACT ASSERTIONS: IDEMPOTENCY_IMPLEMENTATION_CONTRACT
        # ===================================================================

        # §4.1: Key format must be canonical
        assert key is not None and key != "", \
            "IDEMPOTENCY_IMPLEMENTATION_CONTRACT §4.1 violated: empty idempotency key"

        # §4.4: Key must be derived from external IDs, contain colon separators
        assert ':' in key, \
            f"IDEMPOTENCY_IMPLEMENTATION_CONTRACT §4.1 violated: key '{key}' not in canonical format {{operation}}:{{scope}}:{{external_id}}"

        parts = key.split(':')
        assert len(parts) >= 3, \
            f"IDEMPOTENCY_IMPLEMENTATION_CONTRACT §4.1 violated: key '{key}' must have at least 3 parts"

        # §4.4: Key must not be derived from internal IDs or timestamps alone
        # Internal IDs typically are just numbers without prefix (auto-increment: 1, 2, 3...)
        # External processor IDs (e.g., Authorize.net batch IDs) are 10+ digits
        external_id = parts[-1]
        assert not external_id.isdigit() or len(external_id) >= 10, \
            f"IDEMPOTENCY_IMPLEMENTATION_CONTRACT §4.4 violated: key appears derived from internal numeric ID"

        cursor = self.conn.cursor()
        # Phase 2: Filter by workspace_id for proper data isolation
        cursor.execute("""
            SELECT idempotency_key, operation_type, status, result_id, result_data,
                   created_at, completed_at, error_message
            FROM idempotency_keys
            WHERE idempotency_key = ? AND workspace_id = ?
        """, (key, self.workspace_id))

        row = cursor.fetchone()

        if row is None:
            # Key doesn't exist - safe to proceed
            self.logger.info(
                EventType.IDEMPOTENCY_CHECK,
                f"Idempotency check: key not found, safe to proceed",
                idempotency_key=key,
                check_result="not_exists"
            )
            return IdempotencyResult(
                status=IdempotencyStatus.NOT_EXISTS,
                idempotency_key=key,
                operation_type=key.split(":")[0] if ":" in key else "unknown"
            )

        status = row[2]
        result_id = row[3]
        result_data = row[4]
        created_at_str = row[5]
        completed_at_str = row[6]
        error_message = row[7]

        created_at = self._parse_timestamp(created_at_str) if created_at_str else None
        completed_at = self._parse_timestamp(completed_at_str) if completed_at_str else None

        # Check for pending timeout (§6.3)
        if status == "pending" and created_at:
            timeout_threshold = datetime.now(timezone.utc) - timedelta(minutes=self.PENDING_TIMEOUT_MINUTES)
            if created_at < timeout_threshold:
                # Pending too long - mark as failed (§6.3)
                self._timeout_pending_operation(key)
                status = "failed"
                error_message = f"Timed out after {self.PENDING_TIMEOUT_MINUTES} minutes"

        if status == "completed":
            self.logger.log_idempotency_hit(
                idempotency_key=key,
                existing_result_id=result_id or "unknown"
            )
            return IdempotencyResult(
                status=IdempotencyStatus.EXISTS_COMPLETED,
                idempotency_key=key,
                operation_type=row[1],
                result_id=result_id,
                result_data=eval(result_data) if result_data else None,
                created_at=created_at,
                completed_at=completed_at
            )

        if status == "pending":
            self.logger.info(
                EventType.IDEMPOTENCY_CHECK,
                f"Idempotency check: operation in progress",
                idempotency_key=key,
                check_result="exists_pending",
                created_at=created_at_str
            )
            return IdempotencyResult(
                status=IdempotencyStatus.EXISTS_PENDING,
                idempotency_key=key,
                operation_type=row[1],
                created_at=created_at
            )

        if status == "failed":
            self.logger.info(
                EventType.IDEMPOTENCY_CHECK,
                f"Idempotency check: previous attempt failed, may retry",
                idempotency_key=key,
                check_result="exists_failed",
                error_message=error_message
            )
            return IdempotencyResult(
                status=IdempotencyStatus.EXISTS_FAILED,
                idempotency_key=key,
                operation_type=row[1],
                error_message=error_message,
                created_at=created_at
            )

        # Shouldn't reach here
        raise ValueError(f"Unknown idempotency status: {status}")

    def check_idempotency_dual_read(
        self,
        v2_key: str,
        legacy_key: Optional[str] = None
    ) -> IdempotencyResult:
        """
        Check idempotency with dual-read for backward compatibility.

        Implements dual-read strategy per IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1:
        1. First check for v2 workspace-scoped key
        2. If not found AND legacy_key provided, check legacy key
        3. Legacy lookup is ALSO workspace-scoped (prevents cross-workspace hits)
        4. If legacy found, return result (no v2 alias written per contract)
        5. If neither found, return NOT_EXISTS

        Args:
            v2_key: V2 format key (required)
            legacy_key: Legacy format key for backward compatibility lookup (optional)

        Returns:
            IdempotencyResult with status indicating whether operation exists
        """
        # === STEP 1: V2 Key Lookup ===
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT idempotency_key, operation_type, status, result_id, result_data,
                   created_at, completed_at, error_message
            FROM idempotency_keys
            WHERE idempotency_key = ? AND workspace_id = ?
        """, (v2_key, self.workspace_id))

        row = cursor.fetchone()
        if row:
            self.logger.info(
                EventType.IDEMPOTENCY_CHECK,
                f"Idempotency dual-read: v2 key found",
                idempotency_key=v2_key,
                check_result="v2_hit"
            )
            return self._parse_idempotency_row(row, v2_key)

        # === STEP 2: Legacy Key Lookup (if provided) ===
        if legacy_key:
            cursor.execute("""
                SELECT idempotency_key, operation_type, status, result_id, result_data,
                       created_at, completed_at, error_message
                FROM idempotency_keys
                WHERE idempotency_key = ? AND workspace_id = ?
            """, (legacy_key, self.workspace_id))

            row = cursor.fetchone()
            if row:
                # Log that we found a legacy key - important for monitoring migration
                self.logger.info(
                    EventType.IDEMPOTENCY_CHECK,
                    f"Idempotency dual-read: legacy key found, v2 key not found",
                    v2_key=v2_key,
                    legacy_key=legacy_key,
                    check_result="legacy_hit"
                )
                # Return result but keep original legacy key in result for tracing
                return self._parse_idempotency_row(row, legacy_key, was_legacy=True)

        # === STEP 3: Not Found ===
        self.logger.info(
            EventType.IDEMPOTENCY_CHECK,
            f"Idempotency dual-read: key not found, safe to proceed",
            v2_key=v2_key,
            legacy_key=legacy_key,
            check_result="not_exists"
        )
        return IdempotencyResult(
            status=IdempotencyStatus.NOT_EXISTS,
            idempotency_key=v2_key,
            operation_type=v2_key.split(":")[0] if ":" in v2_key else "unknown"
        )

    def _parse_idempotency_row(
        self,
        row: tuple,
        key: str,
        was_legacy: bool = False
    ) -> IdempotencyResult:
        """
        Parse a database row into an IdempotencyResult.

        Args:
            row: Database row tuple
            key: The key that was looked up
            was_legacy: True if this was found via legacy key lookup

        Returns:
            IdempotencyResult with appropriate status
        """
        status = row[2]
        result_id = row[3]
        result_data = row[4]
        created_at_str = row[5]
        completed_at_str = row[6]
        error_message = row[7]

        created_at = self._parse_timestamp(created_at_str) if created_at_str else None
        completed_at = self._parse_timestamp(completed_at_str) if completed_at_str else None

        # Check for pending timeout (§6.3)
        if status == "pending" and created_at:
            timeout_threshold = datetime.now(timezone.utc) - timedelta(minutes=self.PENDING_TIMEOUT_MINUTES)
            if created_at < timeout_threshold:
                self._timeout_pending_operation(key)
                status = "failed"
                error_message = f"Timed out after {self.PENDING_TIMEOUT_MINUTES} minutes"

        if status == "completed":
            if was_legacy:
                self.logger.info(
                    EventType.IDEMPOTENCY_CHECK,
                    f"Idempotency hit on legacy key",
                    idempotency_key=key,
                    existing_result_id=result_id or "unknown"
                )
            else:
                self.logger.log_idempotency_hit(
                    idempotency_key=key,
                    existing_result_id=result_id or "unknown"
                )
            return IdempotencyResult(
                status=IdempotencyStatus.EXISTS_COMPLETED,
                idempotency_key=key,
                operation_type=row[1],
                result_id=result_id,
                result_data=eval(result_data) if result_data else None,
                created_at=created_at,
                completed_at=completed_at
            )

        if status == "pending":
            return IdempotencyResult(
                status=IdempotencyStatus.EXISTS_PENDING,
                idempotency_key=key,
                operation_type=row[1],
                created_at=created_at
            )

        if status == "failed":
            return IdempotencyResult(
                status=IdempotencyStatus.EXISTS_FAILED,
                idempotency_key=key,
                operation_type=row[1],
                error_message=error_message,
                created_at=created_at
            )

        raise ValueError(f"Unknown idempotency status: {status}")

    def _timeout_pending_operation(self, key: str) -> None:
        """Mark a pending operation as failed due to timeout."""
        cursor = self.conn.cursor()
        # Phase 2: Filter by workspace_id
        cursor.execute("""
            UPDATE idempotency_keys
            SET status = 'failed',
                error_message = 'Timed out',
                completed_at = ?
            WHERE idempotency_key = ? AND workspace_id = ? AND status = 'pending'
        """, (self._get_timestamp(), key, self.workspace_id))
        self.conn.commit()

        self.logger.warn(
            EventType.WARNING_DETECTED,
            f"Pending operation timed out: {key}",
            idempotency_key=key,
            warning_type="pending_timeout"
        )

    def start_operation(self, key: str, operation_type: str) -> None:
        """
        Start a new operation by creating a pending idempotency record.

        This MUST be called BEFORE any external API call per §3.2 "Check Before Act".

        Args:
            key: Idempotency key
            operation_type: Type of operation (deposit, payment_match, etc.)

        Raises:
            ValueError: If operation already exists and is not in failed state
        """
        cursor = self.conn.cursor()
        now = self._get_timestamp()

        try:
            # Try to insert new record
            # Phase 2: Include workspace_id (NOT NULL in consolidated DB schema)
            # Extract external_event_id from key (format: operation_type:scope:external_id)
            key_parts = key.split(':')
            external_event_id = key_parts[-1] if len(key_parts) >= 3 else key
            # Use operation_type as processor_type for now (deposit operations don't have explicit processor context here)
            processor_type = operation_type if operation_type else 'unknown'

            cursor.execute("""
                INSERT INTO idempotency_keys (idempotency_key, workspace_id, operation_type, company_id, status, created_at, processor_type, external_event_id)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
            """, (key, self.workspace_id, operation_type, self.company_id, now, processor_type, external_event_id))
            self.conn.commit()

            self.logger.info(
                EventType.IDEMPOTENCY_CHECK,
                f"Started operation: {key}",
                idempotency_key=key,
                operation_type=operation_type,
                status="pending"
            )

        except sqlite3.IntegrityError:
            # Key already exists - check if we can retry (failed state)
            # Phase 2: Filter by workspace_id
            cursor.execute("""
                UPDATE idempotency_keys
                SET status = 'pending',
                    created_at = ?,
                    completed_at = NULL,
                    error_message = NULL,
                    result_id = NULL,
                    result_data = NULL
                WHERE idempotency_key = ? AND workspace_id = ? AND status = 'failed'
            """, (now, key, self.workspace_id))

            if cursor.rowcount == 0:
                self.conn.rollback()
                raise ValueError(f"Operation {key} already exists and is not in failed state")

            self.conn.commit()

            self.logger.info(
                EventType.RETRY_ATTEMPTED,
                f"Retrying failed operation: {key}",
                idempotency_key=key,
                operation_type=operation_type,
                status="pending"
            )

    def complete_operation(
        self,
        key: str,
        result_id: str,
        result_data: Optional[Dict] = None
    ) -> None:
        """
        Mark operation as completed successfully.

        Args:
            key: Idempotency key
            result_id: External ID of created object (e.g., QBO deposit ID)
            result_data: Additional result metadata
        """
        # ===================================================================
        # CONTRACT ASSERTION: IDEMPOTENCY_IMPLEMENTATION_CONTRACT §3.4
        # Completed records are immutable - verify we're not re-completing
        # ===================================================================
        cursor = self.conn.cursor()

        # First check current status to enforce immutability
        # Phase 2: Filter by workspace_id
        cursor.execute("""
            SELECT status FROM idempotency_keys WHERE idempotency_key = ? AND workspace_id = ?
        """, (key, self.workspace_id))
        row = cursor.fetchone()

        if row:
            current_status = row[0]
            # §3.4: Idempotency keys are immutable once completed
            assert current_status != 'completed', \
                f"IDEMPOTENCY_IMPLEMENTATION_CONTRACT §3.4 violated: " \
                f"attempted to modify completed idempotency record '{key}'"

        now = self._get_timestamp()

        result_data_str = str(result_data) if result_data else None

        # Phase 2: Filter by workspace_id
        cursor.execute("""
            UPDATE idempotency_keys
            SET status = 'completed',
                result_id = ?,
                result_data = ?,
                completed_at = ?
            WHERE idempotency_key = ? AND workspace_id = ? AND status = 'pending'
        """, (result_id, result_data_str, now, key, self.workspace_id))

        if cursor.rowcount == 0:
            self.conn.rollback()
            raise ValueError(f"Cannot complete operation {key}: not in pending state")

        self.conn.commit()

        self.logger.info(
            EventType.IDEMPOTENCY_CHECK,
            f"Completed operation: {key} -> {result_id}",
            idempotency_key=key,
            result_id=result_id,
            status="completed"
        )

    def fail_operation(self, key: str, error_message: str) -> None:
        """
        Mark operation as failed.

        Args:
            key: Idempotency key
            error_message: Error details
        """
        cursor = self.conn.cursor()
        now = self._get_timestamp()

        # Phase 2: Filter by workspace_id
        cursor.execute("""
            UPDATE idempotency_keys
            SET status = 'failed',
                error_message = ?,
                completed_at = ?
            WHERE idempotency_key = ? AND workspace_id = ? AND status = 'pending'
        """, (error_message, now, key, self.workspace_id))

        if cursor.rowcount == 0:
            self.conn.rollback()
            raise ValueError(f"Cannot fail operation {key}: not in pending state")

        self.conn.commit()

        self.logger.error(
            EventType.ERROR_DETECTED,
            f"Failed operation: {key} - {error_message}",
            idempotency_key=key,
            error_message=error_message,
            status="failed"
        )

    def cleanup_stale_pending(self) -> int:
        """
        Clean up stale pending operations that have timed out.

        Should be called on startup or periodically per §10.1.

        Returns:
            Number of operations marked as failed
        """
        cursor = self.conn.cursor()
        timeout_threshold = datetime.now(timezone.utc) - timedelta(minutes=self.PENDING_TIMEOUT_MINUTES)
        threshold_str = timeout_threshold.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        # Phase 2: Filter by workspace_id to only clean this workspace's operations
        cursor.execute("""
            UPDATE idempotency_keys
            SET status = 'failed',
                error_message = 'Timed out (cleanup)',
                completed_at = ?
            WHERE workspace_id = ? AND status = 'pending' AND created_at < ?
        """, (self._get_timestamp(), self.workspace_id, threshold_str))

        count = cursor.rowcount
        self.conn.commit()

        if count > 0:
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Cleaned up {count} stale pending operations",
                warning_type="stale_pending_cleanup",
                count=count
            )

        return count


def get_idempotency_manager(
    db_connection: sqlite3.Connection,
    company_id: str,
    workspace_id: str
) -> IdempotencyManager:
    """
    Factory function to create an IdempotencyManager.

    Args:
        db_connection: SQLite connection to company database
        company_id: Company ID for legacy compatibility
        workspace_id: Workspace ID (REQUIRED per CONTRACT v1.1)

    Returns:
        Configured IdempotencyManager instance

    Raises:
        ValueError: If workspace_id is None or empty (fail closed per contract)
    """
    return IdempotencyManager(db_connection, company_id, workspace_id)


def generate_idempotency_key(
    operation_type: str,
    company_id: str,
    external_id: str
) -> str:
    """
    Generate canonical idempotency key per §4.1 (DEPRECATED - use generate_idempotency_key_v2).

    Format: {operation_type}:{scope}:{external_id}

    Args:
        operation_type: Type of operation (deposit, match, refund, fee, payout)
        company_id: Company ID (scope)
        external_id: External system's unique identifier

    Returns:
        Canonical idempotency key

    .. deprecated::
        Use :func:`generate_idempotency_key_v2` instead, which requires workspace_id
        for proper multitenancy isolation per IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1.
    """
    return f"{operation_type}:{company_id}:{external_id}"


def generate_idempotency_key_v2(
    operation_type: str,
    workspace_id: str,
    processor: str,
    external_id: str
) -> str:
    """
    Generate canonical idempotency key per IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1.

    Format: {operation_type}:ws:{workspace_id}:{processor}:{external_id}

    The 'ws:' prefix enables unambiguous detection of v2 keys vs legacy keys.

    Args:
        operation_type: Type of operation (deposit, match_resolve, payout_sync, refund, etc.)
        workspace_id: Workspace ID (canonical scope) - REQUIRED, fails if empty
        processor: Processor type (stripe, authorize_net, etc.)
        external_id: External system's unique identifier

    Returns:
        Canonical v2 idempotency key

    Raises:
        ValueError: If workspace_id is None or empty (fail closed per contract)

    Example:
        >>> generate_idempotency_key_v2("deposit", "ws_abc123", "stripe", "po_xyz789")
        'deposit:ws:ws_abc123:stripe:po_xyz789'
    """
    if not workspace_id or (isinstance(workspace_id, str) and workspace_id.strip() == ''):
        raise ValueError(
            "IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1 violated: "
            "workspace_id is REQUIRED for idempotency key generation"
        )

    if not processor or (isinstance(processor, str) and processor.strip() == ''):
        raise ValueError(
            "IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1 violated: "
            "processor is REQUIRED for idempotency key generation"
        )

    return f"{operation_type}:ws:{workspace_id}:{processor}:{external_id}"


def _generate_legacy_key(
    operation_type: str,
    company_id: str,
    external_id: str
) -> str:
    """
    Generate legacy idempotency key for dual-read lookup.

    INTERNAL USE ONLY - for backward compatibility lookups during dual-read.
    New operations MUST use generate_idempotency_key_v2.

    Format: {operation_type}:{company_id}:{external_id}

    Args:
        operation_type: Type of operation
        company_id: Company ID (legacy scope)
        external_id: External system's unique identifier

    Returns:
        Legacy format idempotency key
    """
    return f"{operation_type}:{company_id}:{external_id}"


def is_v2_key(key: str) -> bool:
    """
    Detect whether an idempotency key is v2 format.

    V2 keys have 'ws:' as the second segment:
    - v2: {operation}:ws:{workspace_id}:{processor}:{external_id}
    - legacy: {operation}:{company_id}:{external_id}

    Args:
        key: Idempotency key to check

    Returns:
        True if v2 format, False if legacy format
    """
    if not key or ':' not in key:
        return False
    parts = key.split(':')
    return len(parts) >= 4 and parts[1] == 'ws'
