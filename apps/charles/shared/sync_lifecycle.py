"""
Sync Run Lifecycle Module for Charles

Implements SYNC_RUN_LIFECYCLE_CONTRACT v1.0

A sync run is a bounded, auditable unit of work that attempts to reconcile
one or more processor payouts into QuickBooks Online deposits.

Key Requirements:
- Every sync run has a unique sync_run_id
- Belongs to exactly one company
- Has exactly one terminal state
- Is fully reconstructable from logs and persistent state

State Machine (§4.1):
- initiated → discovering → matching → executing → (terminal)
- Terminal states: completed, partially_completed, completed_with_ambiguity,
                   completed_with_warnings, failed, timed_out, cancelled
"""

import sqlite3
import uuid
import time
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

from shared.structured_logging import get_logger, EventType


class SyncState(Enum):
    """
    Sync run states per §4.1
    """
    INITIATED = "initiated"
    DISCOVERING = "discovering"
    MATCHING = "matching"
    EXECUTING = "executing"
    COMPLETED = "completed"
    PARTIALLY_COMPLETED = "partially_completed"
    COMPLETED_WITH_AMBIGUITY = "completed_with_ambiguity"
    COMPLETED_WITH_WARNINGS = "completed_with_warnings"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


# Terminal states per §4.4
TERMINAL_STATES = {
    SyncState.COMPLETED,
    SyncState.PARTIALLY_COMPLETED,
    SyncState.COMPLETED_WITH_AMBIGUITY,
    SyncState.COMPLETED_WITH_WARNINGS,
    SyncState.FAILED,
    SyncState.TIMED_OUT,
    SyncState.CANCELLED,
}

# Allowed transitions per §4.3
ALLOWED_TRANSITIONS = {
    SyncState.INITIATED: {SyncState.DISCOVERING, SyncState.CANCELLED, SyncState.FAILED},
    SyncState.DISCOVERING: {SyncState.MATCHING, SyncState.COMPLETED, SyncState.FAILED, SyncState.TIMED_OUT, SyncState.CANCELLED},
    SyncState.MATCHING: {SyncState.EXECUTING, SyncState.FAILED, SyncState.TIMED_OUT, SyncState.CANCELLED},
    SyncState.EXECUTING: {
        SyncState.COMPLETED,
        SyncState.PARTIALLY_COMPLETED,
        SyncState.COMPLETED_WITH_AMBIGUITY,
        SyncState.COMPLETED_WITH_WARNINGS,
        SyncState.FAILED,
        SyncState.TIMED_OUT,
        SyncState.CANCELLED,
    },
}

# Timeout limits per phase (§6.1)
PHASE_TIMEOUTS = {
    SyncState.DISCOVERING: timedelta(minutes=5),
    SyncState.MATCHING: timedelta(minutes=10),
    SyncState.EXECUTING: timedelta(minutes=30),
}
TOTAL_SYNC_TIMEOUT = timedelta(minutes=45)

# Lock timeout (§7.2)
LOCK_TIMEOUT = timedelta(minutes=60)


@dataclass
class SyncRunStats:
    """Statistics for a sync run."""
    payouts_total: int = 0
    payouts_processed: int = 0
    payouts_failed: int = 0
    payouts_ambiguous: int = 0
    payouts_skipped: int = 0


@dataclass
class SyncRun:
    """
    Represents a sync run per §3.1
    """
    sync_run_id: str
    company_id: str
    state: SyncState
    started_at: datetime
    phase_started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    failure_reason: Optional[str] = None
    stats: SyncRunStats = field(default_factory=SyncRunStats)
    initiation_source: str = "manual"  # manual, scheduled, api


class SyncRunManager:
    """
    Manages sync run lifecycle per SYNC_RUN_LIFECYCLE_CONTRACT.

    Ensures:
    - Single sync per company (§7.1)
    - State transitions follow allowed paths (§4.3)
    - Timeouts are enforced (§6)
    - All events are logged (§11)

    Usage:
        manager = SyncRunManager(db_connection, workspace_id, company_id)

        # Start a new sync
        sync_run = manager.start_sync(initiation_source="manual")

        # Transition through phases
        manager.transition_to(sync_run.sync_run_id, SyncState.DISCOVERING)
        # ... do discovery work ...
        manager.transition_to(sync_run.sync_run_id, SyncState.MATCHING)
        # ... do matching work ...
        manager.transition_to(sync_run.sync_run_id, SyncState.EXECUTING)
        # ... do execution work ...

        # Complete the sync
        manager.complete_sync(sync_run.sync_run_id, stats)
    """

    def __init__(self, db_connection: sqlite3.Connection, workspace_id: str, company_id: str):
        """
        Initialize sync run manager.

        Args:
            db_connection: SQLite connection to company database
            workspace_id: Workspace ID (used for lock scoping and logging)
            company_id: Company ID
        """
        self.conn = db_connection
        self.workspace_id = workspace_id
        self.company_id = company_id
        self.logger = get_logger(service="sync_lifecycle", workspace_id=workspace_id, company_id=company_id)
        self._ensure_tables_exist()

    def _ensure_tables_exist(self) -> None:
        """Create required tables if they don't exist."""
        cursor = self.conn.cursor()

        # Sync runs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_runs (
                sync_run_id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                company_id TEXT NOT NULL,
                state TEXT NOT NULL,
                started_at TEXT NOT NULL,
                phase_started_at TEXT,
                completed_at TEXT,
                failure_reason TEXT,
                initiation_source TEXT NOT NULL DEFAULT 'manual',
                payouts_total INTEGER DEFAULT 0,
                payouts_processed INTEGER DEFAULT 0,
                payouts_failed INTEGER DEFAULT 0,
                payouts_ambiguous INTEGER DEFAULT 0,
                payouts_skipped INTEGER DEFAULT 0,
                duration_ms INTEGER,
                FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
            )
        """)

        # Workspace locks table for concurrency control (§7)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS workspace_sync_locks (
                workspace_id TEXT PRIMARY KEY,
                sync_run_id TEXT NOT NULL,
                locked_at TEXT NOT NULL,
                FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
                FOREIGN KEY (sync_run_id) REFERENCES sync_runs(sync_run_id) ON DELETE CASCADE
            )
        """)

        # Drop legacy index if present
        cursor.execute("DROP INDEX IF EXISTS idx_sync_runs_company_state")

        # Index for finding active syncs (workspace-scoped)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sync_runs_workspace_state
            ON sync_runs(workspace_id, state)
        """)

        self.conn.commit()

    def _get_timestamp(self) -> str:
        """Get ISO 8601 UTC timestamp."""
        return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    def _parse_timestamp(self, ts: str) -> datetime:
        """Parse ISO 8601 timestamp."""
        if ts is None:
            return None
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))

    def _acquire_lock(self, sync_run_id: str) -> bool:
        """
        Acquire workspace lock per §7.1.

        Returns:
            True if lock acquired, False if workspace already has active sync
        """
        cursor = self.conn.cursor()
        now = self._get_timestamp()

        # Check for existing lock
        cursor.execute("""
            SELECT sync_run_id, locked_at FROM workspace_sync_locks
            WHERE workspace_id = ?
        """, (self.workspace_id,))

        existing = cursor.fetchone()

        if existing:
            existing_sync_id, locked_at_str = existing
            locked_at = self._parse_timestamp(locked_at_str)

            # Check if lock is stale (§7.2)
            if datetime.now(timezone.utc) - locked_at > LOCK_TIMEOUT:
                # Stale lock - release it and mark old sync as failed
                self._release_stale_lock(existing_sync_id)
            else:
                # Active lock exists
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Sync already in progress for workspace {self.workspace_id}",
                    warning_type="concurrent_sync_rejected",
                    existing_sync_run_id=existing_sync_id
                )
                return False

        # Acquire new lock
        try:
            cursor.execute("""
                INSERT INTO workspace_sync_locks (workspace_id, sync_run_id, locked_at)
                VALUES (?, ?, ?)
            """, (self.workspace_id, sync_run_id, now))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Race condition - another sync acquired lock
            self.conn.rollback()
            return False

    def _release_lock(self, sync_run_id: str) -> None:
        """Release workspace lock."""
        cursor = self.conn.cursor()
        cursor.execute("""
            DELETE FROM workspace_sync_locks
            WHERE workspace_id = ? AND sync_run_id = ?
        """, (self.workspace_id, sync_run_id))
        self.conn.commit()

    def _release_stale_lock(self, stale_sync_id: str) -> None:
        """Release stale lock and mark sync as failed per §8.1."""
        cursor = self.conn.cursor()

        # Mark stale sync as failed
        cursor.execute("""
            UPDATE sync_runs
            SET state = ?,
                completed_at = ?,
                failure_reason = 'Interrupted by system restart or timeout'
            WHERE sync_run_id = ? AND state NOT IN (?, ?, ?, ?, ?, ?, ?)
        """, (
            SyncState.FAILED.value,
            self._get_timestamp(),
            stale_sync_id,
            # Terminal states - don't update if already terminal
            SyncState.COMPLETED.value,
            SyncState.PARTIALLY_COMPLETED.value,
            SyncState.COMPLETED_WITH_AMBIGUITY.value,
            SyncState.COMPLETED_WITH_WARNINGS.value,
            SyncState.FAILED.value,
            SyncState.TIMED_OUT.value,
            SyncState.CANCELLED.value,
        ))

        # Release lock
        cursor.execute("""
            DELETE FROM workspace_sync_locks WHERE sync_run_id = ?
        """, (stale_sync_id,))

        self.conn.commit()

        self.logger.warn(
            EventType.WARNING_DETECTED,
            f"Released stale lock for sync {stale_sync_id}",
            warning_type="stale_lock_released",
            stale_sync_run_id=stale_sync_id
        )

    def start_sync(self, initiation_source: str = "manual") -> SyncRun:
        """
        Start a new sync run per §5.1 Initiation Phase.

        Args:
            initiation_source: How sync was initiated (manual, scheduled, api)

        Returns:
            New SyncRun instance

        Raises:
            RuntimeError: If another sync is already in progress for this company
        """
        sync_run_id = f"sync_{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)
        now_str = self._get_timestamp()

        # Acquire lock (§7.1)
        if not self._acquire_lock(sync_run_id):
            raise RuntimeError(
                f"Sync already in progress for company {self.company_id}. "
                "Only one sync per company is allowed."
            )

        # Create sync run record
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO sync_runs (
                sync_run_id, workspace_id, company_id, state, started_at, phase_started_at, initiation_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (sync_run_id, self.workspace_id, self.company_id, SyncState.INITIATED.value, now_str, now_str, initiation_source))
        self.conn.commit()

        sync_run = SyncRun(
            sync_run_id=sync_run_id,
            company_id=self.company_id,
            state=SyncState.INITIATED,
            started_at=now,
            phase_started_at=now,
            initiation_source=initiation_source
        )

        # Update logger with sync_run_id
        self.logger.set_sync_run_id(sync_run_id)

        # Log sync started (§11.1)
        self.logger.log_sync_started(
            payouts_total=0,
            initiation_source=initiation_source
        )

        return sync_run

    def get_sync_run(self, sync_run_id: str) -> Optional[SyncRun]:
        """Get sync run by ID."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT sync_run_id, company_id, state, started_at, phase_started_at,
                   completed_at, failure_reason, initiation_source,
                   payouts_total, payouts_processed, payouts_failed,
                   payouts_ambiguous, payouts_skipped
            FROM sync_runs WHERE sync_run_id = ?
        """, (sync_run_id,))

        row = cursor.fetchone()
        if not row:
            return None

        stats = SyncRunStats(
            payouts_total=row[8] or 0,
            payouts_processed=row[9] or 0,
            payouts_failed=row[10] or 0,
            payouts_ambiguous=row[11] or 0,
            payouts_skipped=row[12] or 0,
        )

        return SyncRun(
            sync_run_id=row[0],
            company_id=row[1],
            state=SyncState(row[2]),
            started_at=self._parse_timestamp(row[3]),
            phase_started_at=self._parse_timestamp(row[4]),
            completed_at=self._parse_timestamp(row[5]),
            failure_reason=row[6],
            initiation_source=row[7],
            stats=stats,
        )

    def transition_to(self, sync_run_id: str, new_state: SyncState) -> SyncRun:
        """
        Transition sync run to a new state per §4.3.

        Args:
            sync_run_id: Sync run ID
            new_state: Target state

        Returns:
            Updated SyncRun

        Raises:
            ValueError: If transition is not allowed
            RuntimeError: If sync run not found or already terminal
        """
        sync_run = self.get_sync_run(sync_run_id)
        if not sync_run:
            raise RuntimeError(f"Sync run {sync_run_id} not found")

        current_state = sync_run.state

        # Check if already terminal (§4.4)
        if current_state in TERMINAL_STATES:
            raise RuntimeError(
                f"Sync run {sync_run_id} is in terminal state {current_state.value}, "
                "no further transitions allowed"
            )

        # Check if transition is allowed (§4.3)
        allowed = ALLOWED_TRANSITIONS.get(current_state, set())
        if new_state not in allowed:
            raise ValueError(
                f"Transition from {current_state.value} to {new_state.value} is not allowed. "
                f"Allowed transitions: {[s.value for s in allowed]}"
            )

        # Check for timeout before transition (§6)
        self._check_timeout(sync_run, new_state)

        # Perform transition
        now_str = self._get_timestamp()
        cursor = self.conn.cursor()

        if new_state in TERMINAL_STATES:
            # Terminal state - set completed_at, release lock
            cursor.execute("""
                UPDATE sync_runs
                SET state = ?, completed_at = ?
                WHERE sync_run_id = ?
            """, (new_state.value, now_str, sync_run_id))
            self._release_lock(sync_run_id)
        else:
            # Non-terminal - update phase_started_at
            cursor.execute("""
                UPDATE sync_runs
                SET state = ?, phase_started_at = ?
                WHERE sync_run_id = ?
            """, (new_state.value, now_str, sync_run_id))

        self.conn.commit()

        # Log state transition
        self.logger.info(
            EventType.SYNC_STARTED,  # Using generic event for transitions
            f"Sync state transition: {current_state.value} -> {new_state.value}",
            previous_state=current_state.value,
            new_state=new_state.value
        )

        return self.get_sync_run(sync_run_id)

    def _check_timeout(self, sync_run: SyncRun, target_state: SyncState) -> None:
        """
        Check for timeouts per §6.

        Raises:
            TimeoutError: If phase or total timeout exceeded
        """
        now = datetime.now(timezone.utc)

        # Check total sync timeout
        if now - sync_run.started_at > TOTAL_SYNC_TIMEOUT:
            raise TimeoutError(
                f"Total sync timeout exceeded ({TOTAL_SYNC_TIMEOUT.total_seconds() / 60} minutes)"
            )

        # Check phase timeout
        current_state = sync_run.state
        if current_state in PHASE_TIMEOUTS and sync_run.phase_started_at:
            phase_timeout = PHASE_TIMEOUTS[current_state]
            if now - sync_run.phase_started_at > phase_timeout:
                raise TimeoutError(
                    f"Phase {current_state.value} timeout exceeded "
                    f"({phase_timeout.total_seconds() / 60} minutes)"
                )

    def check_timeout_active(self, sync_run_id: str) -> None:
        """
        Actively check for timeout during execution per §6.

        This method should be called periodically during long-running operations
        (e.g., between processing each payout) to enforce timeout limits.

        Args:
            sync_run_id: Sync run ID to check

        Raises:
            TimeoutError: If phase or total timeout exceeded
        """
        sync_run = self.get_sync_run(sync_run_id)
        if not sync_run:
            raise RuntimeError(f"Sync run {sync_run_id} not found")

        # Don't check if already terminal
        if sync_run.state in TERMINAL_STATES:
            return

        now = datetime.now(timezone.utc)

        # Check total sync timeout (§6.1: 45 minutes total)
        total_elapsed = now - sync_run.started_at
        if total_elapsed > TOTAL_SYNC_TIMEOUT:
            self.logger.error(
                EventType.SYNC_FAILED,
                f"Total sync timeout exceeded: {total_elapsed.total_seconds() / 60:.1f} minutes > {TOTAL_SYNC_TIMEOUT.total_seconds() / 60} minutes",
                timeout_type="total",
                elapsed_minutes=total_elapsed.total_seconds() / 60,
                limit_minutes=TOTAL_SYNC_TIMEOUT.total_seconds() / 60
            )
            raise TimeoutError(
                f"Total sync timeout exceeded ({TOTAL_SYNC_TIMEOUT.total_seconds() / 60} minutes)"
            )

        # Check phase timeout (§6.1)
        current_state = sync_run.state
        if current_state in PHASE_TIMEOUTS and sync_run.phase_started_at:
            phase_timeout = PHASE_TIMEOUTS[current_state]
            phase_elapsed = now - sync_run.phase_started_at

            if phase_elapsed > phase_timeout:
                self.logger.error(
                    EventType.SYNC_FAILED,
                    f"Phase {current_state.value} timeout exceeded: {phase_elapsed.total_seconds() / 60:.1f} minutes > {phase_timeout.total_seconds() / 60} minutes",
                    timeout_type="phase",
                    phase=current_state.value,
                    elapsed_minutes=phase_elapsed.total_seconds() / 60,
                    limit_minutes=phase_timeout.total_seconds() / 60
                )
                raise TimeoutError(
                    f"Phase {current_state.value} timeout exceeded "
                    f"({phase_timeout.total_seconds() / 60} minutes)"
                )

    def get_remaining_time(self, sync_run_id: str) -> Dict[str, float]:
        """
        Get remaining time for current phase and total sync.

        Args:
            sync_run_id: Sync run ID

        Returns:
            Dict with 'phase_remaining_seconds' and 'total_remaining_seconds'
        """
        sync_run = self.get_sync_run(sync_run_id)
        if not sync_run:
            return {"phase_remaining_seconds": 0, "total_remaining_seconds": 0}

        now = datetime.now(timezone.utc)

        # Total remaining
        total_elapsed = now - sync_run.started_at
        total_remaining = (TOTAL_SYNC_TIMEOUT - total_elapsed).total_seconds()

        # Phase remaining
        phase_remaining = float('inf')
        current_state = sync_run.state
        if current_state in PHASE_TIMEOUTS and sync_run.phase_started_at:
            phase_timeout = PHASE_TIMEOUTS[current_state]
            phase_elapsed = now - sync_run.phase_started_at
            phase_remaining = (phase_timeout - phase_elapsed).total_seconds()

        return {
            "phase_remaining_seconds": max(0, phase_remaining),
            "total_remaining_seconds": max(0, total_remaining)
        }

    def update_stats(self, sync_run_id: str, stats: SyncRunStats) -> None:
        """Update sync run statistics."""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE sync_runs
            SET payouts_total = ?,
                payouts_processed = ?,
                payouts_failed = ?,
                payouts_ambiguous = ?,
                payouts_skipped = ?
            WHERE sync_run_id = ?
        """, (
            stats.payouts_total,
            stats.payouts_processed,
            stats.payouts_failed,
            stats.payouts_ambiguous,
            stats.payouts_skipped,
            sync_run_id
        ))
        self.conn.commit()

    def complete_sync(
        self,
        sync_run_id: str,
        stats: SyncRunStats,
        has_warnings: bool = False
    ) -> SyncRun:
        """
        Complete a sync run with appropriate terminal state.

        Determines terminal state based on stats:
        - All succeeded → completed (or completed_with_warnings)
        - Some failed → partially_completed
        - Some ambiguous → completed_with_ambiguity

        Args:
            sync_run_id: Sync run ID
            stats: Final statistics
            has_warnings: Whether warnings occurred

        Returns:
            Completed SyncRun
        """
        self.update_stats(sync_run_id, stats)

        # Determine terminal state
        if stats.payouts_failed > 0 and stats.payouts_processed > 0:
            terminal_state = SyncState.PARTIALLY_COMPLETED
        elif stats.payouts_failed > 0 and stats.payouts_processed == 0:
            terminal_state = SyncState.FAILED
        elif stats.payouts_ambiguous > 0:
            terminal_state = SyncState.COMPLETED_WITH_AMBIGUITY
        elif has_warnings:
            terminal_state = SyncState.COMPLETED_WITH_WARNINGS
        else:
            terminal_state = SyncState.COMPLETED

        sync_run = self.transition_to(sync_run_id, terminal_state)

        # Calculate duration
        duration_ms = int((sync_run.completed_at - sync_run.started_at).total_seconds() * 1000)

        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE sync_runs SET duration_ms = ? WHERE sync_run_id = ?
        """, (duration_ms, sync_run_id))
        self.conn.commit()

        # Log completion
        if terminal_state == SyncState.COMPLETED:
            self.logger.log_sync_completed(
                payouts_total=stats.payouts_total,
                payouts_processed=stats.payouts_processed,
                payouts_failed=stats.payouts_failed,
                payouts_ambiguous=stats.payouts_ambiguous,
                duration_ms=duration_ms
            )
        elif terminal_state == SyncState.PARTIALLY_COMPLETED:
            self.logger.info(
                EventType.SYNC_PARTIALLY_COMPLETED,
                f"Sync partially completed: {stats.payouts_processed}/{stats.payouts_total} succeeded",
                payouts_total=stats.payouts_total,
                payouts_processed=stats.payouts_processed,
                payouts_failed=stats.payouts_failed,
                payouts_ambiguous=stats.payouts_ambiguous,
                duration_ms=duration_ms
            )
        elif terminal_state == SyncState.FAILED:
            self.logger.log_sync_failed(
                failure_reason="All payouts failed",
                payouts_total=stats.payouts_total,
                payouts_processed=stats.payouts_processed,
                duration_ms=duration_ms
            )

        return sync_run

    def fail_sync(self, sync_run_id: str, reason: str, stats: Optional[SyncRunStats] = None) -> SyncRun:
        """
        Mark sync run as failed.

        Args:
            sync_run_id: Sync run ID
            reason: Failure reason
            stats: Optional final statistics

        Returns:
            Failed SyncRun
        """
        if stats:
            self.update_stats(sync_run_id, stats)

        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE sync_runs SET failure_reason = ? WHERE sync_run_id = ?
        """, (reason, sync_run_id))
        self.conn.commit()

        sync_run = self.transition_to(sync_run_id, SyncState.FAILED)

        duration_ms = int((sync_run.completed_at - sync_run.started_at).total_seconds() * 1000)

        self.logger.log_sync_failed(
            failure_reason=reason,
            payouts_total=sync_run.stats.payouts_total,
            payouts_processed=sync_run.stats.payouts_processed,
            duration_ms=duration_ms
        )

        return sync_run

    def timeout_sync(self, sync_run_id: str, phase: str) -> SyncRun:
        """
        Mark sync run as timed out per §6.2.

        Args:
            sync_run_id: Sync run ID
            phase: Phase that timed out

        Returns:
            Timed out SyncRun
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE sync_runs SET failure_reason = ? WHERE sync_run_id = ?
        """, (f"Timeout in {phase} phase", sync_run_id))
        self.conn.commit()

        sync_run = self.transition_to(sync_run_id, SyncState.TIMED_OUT)

        self.logger.error(
            EventType.SYNC_FAILED,
            f"Sync timed out in {phase} phase",
            timeout_phase=phase,
            sync_run_id=sync_run_id
        )

        return sync_run

    def cancel_sync(self, sync_run_id: str, reason: str = "User requested") -> SyncRun:
        """
        Cancel sync run per §10.

        Args:
            sync_run_id: Sync run ID
            reason: Cancellation reason

        Returns:
            Cancelled SyncRun
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE sync_runs SET failure_reason = ? WHERE sync_run_id = ?
        """, (f"Cancelled: {reason}", sync_run_id))
        self.conn.commit()

        sync_run = self.transition_to(sync_run_id, SyncState.CANCELLED)

        self.logger.info(
            EventType.SYNC_CANCELLED,
            f"Sync cancelled: {reason}",
            cancellation_reason=reason,
            sync_run_id=sync_run_id
        )

        return sync_run

    def cleanup_incomplete_syncs(self) -> int:
        """
        Clean up incomplete syncs on startup per §8.1.

        Returns:
            Number of syncs marked as failed
        """
        cursor = self.conn.cursor()

        # Find non-terminal syncs
        terminal_values = [s.value for s in TERMINAL_STATES]
        placeholders = ','.join('?' * len(terminal_values))

        cursor.execute(f"""
            SELECT sync_run_id FROM sync_runs
            WHERE workspace_id = ? AND state NOT IN ({placeholders})
        """, (self.workspace_id, *terminal_values))

        incomplete = cursor.fetchall()
        count = 0

        for (sync_run_id,) in incomplete:
            cursor.execute("""
                UPDATE sync_runs
                SET state = ?,
                    completed_at = ?,
                    failure_reason = 'Interrupted by system restart'
                WHERE sync_run_id = ?
            """, (SyncState.FAILED.value, self._get_timestamp(), sync_run_id))

            # Release any lock
            cursor.execute("""
                DELETE FROM workspace_sync_locks WHERE sync_run_id = ?
            """, (sync_run_id,))

            count += 1

        self.conn.commit()

        if count > 0:
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Marked {count} incomplete sync runs as failed on startup",
                warning_type="incomplete_sync_cleanup",
                count=count
            )

        return count


def get_sync_manager(db_connection: sqlite3.Connection, workspace_id: str, company_id: str) -> SyncRunManager:
    """
    Factory function to create a SyncRunManager.

    Args:
        db_connection: SQLite connection to company database
        workspace_id: Workspace ID (used for lock scoping and logging)
        company_id: Company ID

    Returns:
        Configured SyncRunManager instance
    """
    return SyncRunManager(db_connection, workspace_id, company_id)
