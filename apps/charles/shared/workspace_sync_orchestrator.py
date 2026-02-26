"""
Workspace-Scoped Sync Orchestration Layer for Charles Phase 4

This module implements a workspace-scoped orchestration layer that sits ABOVE
the existing SyncRunManager (certified Phase 3 infrastructure, LOCKED).

Responsibilities:
- Controls WHEN a sync may start for a workspace
- Enforces workspace-level gating (license, auth, rate limits, concurrent sync)
- Maps workspace_id → company_id at orchestration boundary
- Tracks workspace-level state (ready, gated, running, cooldown)
- Provides workspace-scoped observability and health tracking

Does NOT:
- Modify sync_lifecycle.py (SyncRunManager is untouched)
- Add pause/resume behavior
- Touch OAuth, licensing, or token lifecycle logic
- Migrate sync_runs table from company_id to workspace_id

Per Phase 4 Development Parameters:
- Workspace-scoped (follows Phase 3 patterns)
- Deterministic and testable
- Uses existing error taxonomy (ErrorCategory enum)
- Uses existing logging standards
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any
from dataclasses import dataclass

import sys
import os

# Add src directory to path for imports
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from charles.shared.structured_logging import get_logger, EventType, StructuredLogger
from charles.shared.sync_lifecycle import SyncRunManager, SyncRun, SyncState, TERMINAL_STATES
from charles.shared.error_taxonomy import ErrorCategory
from charles.shared.observability_interface import get_metrics_collector, get_tracing_provider
import charles.database.workspace_helpers as workspace_helpers


# Configuration from environment variables
SYNC_COOLDOWN_MINUTES = int(os.getenv('SYNC_COOLDOWN_MINUTES', '15'))
OAUTH_FAILURE_THRESHOLD = int(os.getenv('OAUTH_FAILURE_THRESHOLD', '5'))


@dataclass
class WorkspaceStatus:
    """Workspace sync status for observability."""
    workspace_id: str
    state: str
    can_start_sync: bool
    gate_reason: Optional[str]
    active_sync_run_id: Optional[str]
    last_sync_started_at: Optional[datetime]
    last_sync_completed_at: Optional[datetime]
    last_sync_status: Optional[str]
    last_sync_failure_classification: Optional[str]
    cooldown_until: Optional[datetime]
    consecutive_failures: int
    last_success_at: Optional[datetime]
    sync_count_total: int
    sync_count_success: int
    sync_count_failed: int
    health_score: float


class WorkspaceSyncOrchestrator:
    """
    Workspace-scoped sync orchestration layer for Charles Phase 4.

    Orchestration States:
    - ready:    Workspace can start sync (default, all gates pass)
    - gated:    Blocked from sync (license invalid, auth failed, etc.)
    - running:  Sync currently executing (non-terminal sync_run state)
    - cooldown: Recently completed, enforcing minimum interval before next sync

    Gate Checks (in order):
    1. License valid (status='active', not expired)
    2. OAuth healthy (consecutive_refresh_failures < threshold)
    3. Not already running a sync
    4. Not in cooldown period
    5. Not explicitly gated

    Usage:
        orchestrator = WorkspaceSyncOrchestrator(db_connection, workspace_id)

        # Check if sync can start
        can_start, reason = orchestrator.can_start_sync()

        # Request a sync
        if can_start:
            sync_run = orchestrator.request_sync(initiation_source="api")

        # Get workspace status
        status = orchestrator.get_workspace_status()
    """

    def __init__(
        self,
        db_connection: sqlite3.Connection,
        workspace_id: str,
        logger: Optional[StructuredLogger] = None
    ):
        """
        Initialize orchestrator for a workspace.

        Args:
            db_connection: SQLite connection to consolidated charles.db
            workspace_id: Workspace identifier
            logger: Optional structured logger (creates one if not provided)
        """
        self.conn = db_connection
        self.workspace_id = workspace_id
        self.logger = logger or get_logger(service="workspace_sync_orchestrator", workspace_id=workspace_id)
        self._ensure_workspace_state_exists()

    def _ensure_workspace_state_exists(self) -> None:
        """
        Ensure workspace_sync_state row exists for this workspace.
        Creates with default 'ready' state if missing.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO workspace_sync_state (workspace_id, state, state_updated_at)
            VALUES (?, 'ready', CURRENT_TIMESTAMP)
        """, (self.workspace_id,))
        self.conn.commit()

    def can_start_sync(self) -> Tuple[bool, Optional[str]]:
        """
        Check if workspace passes all gates for sync start.

        Gate checks (in order):
        1. License valid (status='active', not expired)
        2. OAuth credentials exist and healthy (not revoked, < threshold failures)
        3a. Not already running a sync (workspace state check)
        3b. No active jobs queued (Phase 6.7 - prevents sync bypassing async jobs)
        4. Not in cooldown period
        5. Not in gated state

        Phase 6.7 Extension (Gate 3b):
        Checks sync_jobs table for QUEUED/CLAIMED/RUNNING jobs to prevent
        synchronous sync requests from bypassing accepted async jobs.

        Returns:
            (can_start: bool, reason: Optional[str])
            - (True, None) if all gates pass
            - (False, "reason") if any gate fails
        """
        cursor = self.conn.cursor()

        # Single efficient query to gather all gate check data
        cursor.execute("""
            SELECT
                ws.state,
                ws.cooldown_until,
                ws.gate_reason,
                ws.active_sync_run_id,
                l.status AS license_status,
                l.expires_at AS license_expires_at,
                qc.status AS oauth_status,
                qc.consecutive_refresh_failures AS oauth_failures
            FROM workspace_sync_state ws
            JOIN workspaces w ON ws.workspace_id = w.workspace_id
            JOIN licenses l ON w.workspace_id = l.workspace_id
            LEFT JOIN qbo_credentials qc ON w.workspace_id = qc.workspace_id AND qc.status = 'active'
            WHERE ws.workspace_id = ?
        """, (self.workspace_id,))

        row = cursor.fetchone()
        if not row:
            return (False, "Workspace not found")

        # Extract gate check data
        (state, cooldown_until, gate_reason, active_sync_run_id,
         license_status, license_expires_at, oauth_status, oauth_failures) = row

        # Gate 1: License validation
        if license_status != 'active':
            reason = f"License {license_status}"
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Sync request denied for workspace {self.workspace_id}: {reason}",
                workspace_id=self.workspace_id,
                gate_type="license_validation",
                gate_reason=reason
            )
            return (False, reason)

        if license_expires_at:
            expires_dt = datetime.fromisoformat(license_expires_at.replace('Z', '+00:00'))
            if expires_dt <= datetime.now(timezone.utc):
                reason = f"License expired: {license_expires_at}"
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Sync request denied for workspace {self.workspace_id}: {reason}",
                    workspace_id=self.workspace_id,
                    gate_type="license_validation",
                    gate_reason=reason
                )
                return (False, reason)

        # Gate 2: OAuth health
        if oauth_status is None:
            reason = "OAuth credentials not configured"
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Sync request denied for workspace {self.workspace_id}: {reason}",
                workspace_id=self.workspace_id,
                gate_type="oauth_health",
                gate_reason=reason
            )
            return (False, reason)

        if oauth_failures is not None and oauth_failures >= OAUTH_FAILURE_THRESHOLD:
            reason = f"OAuth token refresh failing ({oauth_failures}+ consecutive failures)"
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Sync request denied for workspace {self.workspace_id}: {reason}",
                workspace_id=self.workspace_id,
                gate_type="oauth_health",
                gate_reason=reason
            )
            return (False, reason)

        # Gate 3a: Concurrent sync prevention (workspace state check)
        if state == 'running':
            reason = f"Sync already in progress (sync_run_id: {active_sync_run_id})"
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Sync request denied for workspace {self.workspace_id}: {reason}",
                workspace_id=self.workspace_id,
                gate_type="concurrent_sync",
                gate_reason=reason
            )
            return (False, reason)

        # Gate 3b: Active job check (Phase 6.7.1 - Remediation Step 3.4: Fail-Closed)
        # Prevents sync requests from bypassing queued async jobs
        # This check ensures that if an async job has been accepted (QUEUED),
        # subsequent sync requests cannot bypass it by acquiring the lock first
        # Phase 6.7.1: FAIL-CLOSED refinement (Critical Finding H3)
        try:
            active_job_count = cursor.execute("""
                SELECT COUNT(*) as count
                FROM sync_jobs
                WHERE workspace_id = ?
                  AND status IN ('QUEUED', 'CLAIMED', 'RUNNING')
            """, (self.workspace_id,)).fetchone()['count']

            if active_job_count > 0:
                reason = "Sync already queued or running"
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Sync request denied for workspace {self.workspace_id}: {reason}",
                    workspace_id=self.workspace_id,
                    gate_type="concurrent_sync",
                    gate_reason=reason,
                    active_job_count=active_job_count
                )
                return (False, reason)

        except sqlite3.OperationalError as e:
            # sync_jobs table doesn't exist (pre-Phase 6 deployment)
            # This is the ONLY exception we allow to bypass the gate
            # All other exceptions MUST fail closed (reject request)
            error_msg = str(e).lower()
            if 'no such table' in error_msg or 'no such column' in error_msg:
                # Table/column missing - pre-Phase 6 deployment, allow bypass
                self.logger.info(
                    EventType.WARNING_DETECTED,
                    f"Gate 3b bypassed for workspace {self.workspace_id}: sync_jobs table not found (pre-Phase 6)",
                    workspace_id=self.workspace_id,
                    gate_type="concurrent_sync"
                )
                pass  # Skip Gate 3b check - rely on Gate 3a (workspace state) instead
            else:
                # Database error (connection, syntax, etc.) - FAIL CLOSED
                self.logger.error(
                    EventType.ERROR_DETECTED,
                    f"Gate 3b failed for workspace {self.workspace_id}: database error: {str(e)}",
                    workspace_id=self.workspace_id,
                    gate_type="concurrent_sync",
                    error=str(e)
                )
                raise RuntimeError(f"Gate 3b check failed: {str(e)}") from e

        except Exception as e:
            # Any other exception - FAIL CLOSED (reject request)
            # This includes: connection errors, query errors, unexpected exceptions
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Gate 3b failed for workspace {self.workspace_id}: {str(e)}",
                workspace_id=self.workspace_id,
                gate_type="concurrent_sync",
                error=str(e)
            )
            raise RuntimeError(f"Gate 3b check failed: {str(e)}") from e

        # Gate 4: Cooldown enforcement
        if cooldown_until:
            cooldown_dt = datetime.fromisoformat(cooldown_until.replace('Z', '+00:00'))
            if cooldown_dt > datetime.now(timezone.utc):
                reason = f"Rate limit: cooldown until {cooldown_until}"
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Sync request denied for workspace {self.workspace_id}: {reason}",
                    workspace_id=self.workspace_id,
                    gate_type="rate_limit",
                    gate_reason=reason
                )
                return (False, reason)

        # Gate 5: Explicit gating
        if state == 'gated':
            reason = f"Workspace gated: {gate_reason}"
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Sync request denied for workspace {self.workspace_id}: {reason}",
                workspace_id=self.workspace_id,
                gate_type="explicit_gate",
                gate_reason=reason
            )
            return (False, reason)

        # All gates pass
        return (True, None)

    def request_sync(self, initiation_source: str = "manual") -> SyncRun:
        """
        Request a sync for this workspace.

        Flow:
        1. Check gates via can_start_sync()
        2. Resolve company_id from workspace_id
        3. Update workspace_sync_state to 'running'
        4. Create SyncRunManager(db, company_id)
        5. Call sync_manager.start_sync()
        6. Update workspace_sync_state with active_sync_run_id
        7. Return SyncRun

        Args:
            initiation_source: How sync was initiated (manual, scheduled, api)

        Returns:
            SyncRun instance from SyncRunManager

        Raises:
            RuntimeError: If gates fail or sync start fails
            ValueError: If workspace → company_id resolution fails
        """
        # Gate check
        can_start, reason = self.can_start_sync()
        if not can_start:
            raise RuntimeError(f"Sync blocked: {reason}")

        # Resolve company_id from workspace_id (orchestration boundary)
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT id FROM companies WHERE workspace_id = ?",
                (self.workspace_id,)
            )
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"No company found for workspace_id={self.workspace_id}")
            company_id = row[0]
        except Exception as e:
            raise ValueError(f"Failed to resolve company_id from workspace_id={self.workspace_id}: {e}")

        # Update workspace state to 'running'
        cursor = self.conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()
        cursor.execute("""
            UPDATE workspace_sync_state
            SET state = 'running',
                state_updated_at = ?,
                last_sync_started_at = ?
            WHERE workspace_id = ?
        """, (now_iso, now_iso, self.workspace_id))
        self.conn.commit()

        self.logger.info(
            EventType.SYNC_STARTED,
            f"Workspace {self.workspace_id} sync orchestration: transitioning to 'running'",
            workspace_id=self.workspace_id,
            state_transition="ready→running",
            initiation_source=initiation_source
        )

        # Delegate to SyncRunManager (LOCKED Phase 3 infrastructure)
        try:
            sync_manager = SyncRunManager(self.conn, company_id)
            sync_run = sync_manager.start_sync(initiation_source)

            # Update workspace_sync_state with active sync_run_id
            cursor.execute("""
                UPDATE workspace_sync_state
                SET active_sync_run_id = ?
                WHERE workspace_id = ?
            """, (sync_run.sync_run_id, self.workspace_id))
            self.conn.commit()

            self.logger.info(
                EventType.SYNC_STARTED,
                f"Workspace {self.workspace_id} sync started: {sync_run.sync_run_id}",
                workspace_id=self.workspace_id,
                sync_run_id=sync_run.sync_run_id,
                company_id=company_id
            )

            # Phase 5.3: Instrumentation hook - sync started
            metrics = get_metrics_collector()
            metrics.increment_counter(
                "sync_started",
                self.workspace_id,
                {"initiation_source": initiation_source}
            )

            return sync_run

        except Exception as e:
            # Sync start failed - revert workspace state to 'ready' or 'gated'
            # If lock acquisition failed, it's a concurrency issue - revert to 'ready'
            # If other error, may need to gate
            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = 'ready',
                    state_updated_at = ?
                WHERE workspace_id = ?
            """, (datetime.now(timezone.utc).isoformat(), self.workspace_id))
            self.conn.commit()

            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Failed to start sync for workspace {self.workspace_id}: {e}",
                workspace_id=self.workspace_id,
                error=str(e)
            )
            raise RuntimeError(f"Failed to start sync: {e}") from e

    def update_workspace_state_on_completion(
        self,
        sync_run_id: str,
        terminal_state: SyncState,
        failure_classification: Optional[str] = None
    ) -> None:
        """
        Update workspace state after sync reaches terminal state.

        Called by sync completion handler or monitoring process.

        Args:
            sync_run_id: Completed sync run ID
            terminal_state: Terminal state from SyncRunManager
            failure_classification: Error category if failed (authorization_error, etc.)

        State Transitions:
        - completed/partially_completed → cooldown (if configured) or ready
        - failed with authorization_error → gated (requires intervention)
        - failed with infrastructure_error → ready (can retry)
        - timed_out → ready (can retry)
        - cancelled → ready
        """
        cursor = self.conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()

        # Determine next state based on terminal_state and failure_classification
        success_states = [SyncState.COMPLETED, SyncState.PARTIALLY_COMPLETED,
                          SyncState.COMPLETED_WITH_AMBIGUITY, SyncState.COMPLETED_WITH_WARNINGS]

        if terminal_state in success_states:
            # Success path
            if SYNC_COOLDOWN_MINUTES > 0:
                next_state = 'cooldown'
                cooldown_until = (datetime.now(timezone.utc) +
                                  timedelta(minutes=SYNC_COOLDOWN_MINUTES)).isoformat()
            else:
                next_state = 'ready'
                cooldown_until = None

            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = ?,
                    state_updated_at = ?,
                    active_sync_run_id = NULL,
                    last_sync_completed_at = ?,
                    last_sync_status = ?,
                    last_sync_failure_classification = NULL,
                    gate_reason = NULL,
                    gate_set_at = NULL,
                    cooldown_until = ?,
                    consecutive_failures = 0,
                    last_success_at = ?,
                    sync_count_total = sync_count_total + 1,
                    sync_count_success = sync_count_success + 1,
                    updated_at = ?
                WHERE workspace_id = ?
            """, (next_state, now_iso, now_iso, terminal_state.value, cooldown_until,
                  now_iso, now_iso, self.workspace_id))

            self.logger.info(
                EventType.SYNC_COMPLETED,
                f"Workspace {self.workspace_id} sync completed: {sync_run_id}",
                workspace_id=self.workspace_id,
                sync_run_id=sync_run_id,
                terminal_state=terminal_state.value,
                next_state=next_state
            )

            # Phase 5.3: Instrumentation hook - sync completed successfully
            metrics = get_metrics_collector()
            metrics.increment_counter(
                "sync_completed",
                self.workspace_id,
                {"status": terminal_state.value, "next_state": next_state}
            )

        elif terminal_state == SyncState.FAILED:
            # Failure path - determine if should gate based on failure classification
            if failure_classification == ErrorCategory.AUTHORIZATION.value:
                # Authorization failures gate the workspace
                next_state = 'gated'
                gate_reason = 'OAuth authorization failed - requires re-authentication'
                gate_set_at = now_iso
            elif failure_classification == ErrorCategory.CONFIGURATION.value:
                # Configuration failures gate the workspace
                next_state = 'gated'
                gate_reason = 'Configuration error - requires correction'
                gate_set_at = now_iso
            else:
                # Infrastructure or data integrity errors allow retry
                next_state = 'ready'
                gate_reason = None
                gate_set_at = None

            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = ?,
                    state_updated_at = ?,
                    active_sync_run_id = NULL,
                    last_sync_completed_at = ?,
                    last_sync_status = 'failed',
                    last_sync_failure_classification = ?,
                    gate_reason = ?,
                    gate_set_at = ?,
                    cooldown_until = NULL,
                    consecutive_failures = consecutive_failures + 1,
                    sync_count_total = sync_count_total + 1,
                    sync_count_failed = sync_count_failed + 1,
                    updated_at = ?
                WHERE workspace_id = ?
            """, (next_state, now_iso, now_iso, failure_classification,
                  gate_reason, gate_set_at, now_iso, self.workspace_id))

            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Workspace {self.workspace_id} sync failed: {sync_run_id}",
                workspace_id=self.workspace_id,
                sync_run_id=sync_run_id,
                failure_classification=failure_classification,
                next_state=next_state
            )

            # Phase 5.3: Instrumentation hook - sync failed
            metrics = get_metrics_collector()
            metrics.increment_counter(
                "sync_completed",
                self.workspace_id,
                {
                    "status": "failed",
                    "failure_classification": failure_classification or "unknown",
                    "next_state": next_state
                }
            )

        else:
            # Cancelled or timed_out - ready for retry
            next_state = 'ready'
            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = 'ready',
                    state_updated_at = ?,
                    active_sync_run_id = NULL,
                    last_sync_completed_at = ?,
                    last_sync_status = ?,
                    cooldown_until = NULL,
                    sync_count_total = sync_count_total + 1,
                    updated_at = ?
                WHERE workspace_id = ?
            """, (now_iso, now_iso, terminal_state.value, now_iso, self.workspace_id))

            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Workspace {self.workspace_id} sync {terminal_state.value}: {sync_run_id}",
                workspace_id=self.workspace_id,
                sync_run_id=sync_run_id,
                terminal_state=terminal_state.value,
                next_state=next_state
            )

        self.conn.commit()

    def get_workspace_status(self) -> Dict[str, Any]:
        """
        Provide workspace-level sync observability.

        Returns:
            Dict with workspace sync status, health metrics, and observability data
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                state,
                active_sync_run_id,
                last_sync_started_at,
                last_sync_completed_at,
                last_sync_status,
                last_sync_failure_classification,
                gate_reason,
                cooldown_until,
                consecutive_failures,
                last_success_at,
                sync_count_total,
                sync_count_success,
                sync_count_failed
            FROM workspace_sync_state
            WHERE workspace_id = ?
        """, (self.workspace_id,))

        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Workspace state not found for workspace_id={self.workspace_id}")

        (state, active_sync_run_id, last_sync_started_at, last_sync_completed_at,
         last_sync_status, last_sync_failure_classification, gate_reason, cooldown_until,
         consecutive_failures, last_success_at, sync_count_total, sync_count_success,
         sync_count_failed) = row

        # Calculate health_score (0.0 - 1.0)
        if sync_count_total == 0:
            health_score = 1.0  # No history yet
        else:
            success_rate = sync_count_success / sync_count_total

            # Recency penalty (if no success in last 24 hours)
            recency_penalty = 0.0
            if last_success_at:
                last_success_dt = datetime.fromisoformat(last_success_at.replace('Z', '+00:00'))
                hours_since_success = (datetime.now(timezone.utc) - last_success_dt).total_seconds() / 3600
                if hours_since_success > 24:
                    recency_penalty = 0.2

            # Failure penalty (consecutive failures)
            failure_penalty = min(0.3, consecutive_failures * 0.1)

            health_score = max(0.0, success_rate - recency_penalty - failure_penalty)

        # Check if can start sync now
        can_start, _ = self.can_start_sync()

        return {
            'workspace_id': self.workspace_id,
            'state': state,
            'can_start_sync': can_start,
            'gate_reason': gate_reason,
            'active_sync_run_id': active_sync_run_id,
            'last_sync_started_at': last_sync_started_at,
            'last_sync_completed_at': last_sync_completed_at,
            'last_sync_status': last_sync_status,
            'last_sync_failure_classification': last_sync_failure_classification,
            'cooldown_until': cooldown_until,
            'consecutive_failures': consecutive_failures,
            'last_success_at': last_success_at,
            'sync_count_total': sync_count_total,
            'sync_count_success': sync_count_success,
            'sync_count_failed': sync_count_failed,
            'health_score': round(health_score, 2)
        }

    def clear_gate(self, reason: str = "Manual clearance") -> None:
        """
        Manually clear gated state for workspace (admin action).

        Use case: Admin resolves issue (re-authenticates OAuth, renews license)
        and wants to allow syncs to resume.

        Args:
            reason: Reason for clearing gate (for audit log)
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE workspace_sync_state
            SET state = 'ready',
                gate_reason = NULL,
                gate_set_at = NULL,
                state_updated_at = ?
            WHERE workspace_id = ? AND state = 'gated'
        """, (datetime.now(timezone.utc).isoformat(), self.workspace_id))

        rows_affected = cursor.rowcount
        self.conn.commit()

        if rows_affected > 0:
            self.logger.info(
                EventType.WARNING_DETECTED,
                f"Gate cleared for workspace {self.workspace_id}: {reason}",
                workspace_id=self.workspace_id,
                action="clear_gate",
                reason=reason
            )

    def set_gate(self, reason: str) -> None:
        """
        Manually gate workspace to prevent syncs (admin action or automated policy).

        Use case: Admin intervention, license suspension, abuse detection.

        Args:
            reason: Human-readable reason (stored in gate_reason)
        """
        cursor = self.conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()
        cursor.execute("""
            UPDATE workspace_sync_state
            SET state = 'gated',
                gate_reason = ?,
                gate_set_at = ?,
                state_updated_at = ?
            WHERE workspace_id = ?
        """, (reason, now_iso, now_iso, self.workspace_id))

        self.conn.commit()

        self.logger.warn(
            EventType.WARNING_DETECTED,
            f"Workspace {self.workspace_id} gated: {reason}",
            workspace_id=self.workspace_id,
            action="set_gate",
            gate_reason=reason
        )

    # ========================================================================
    # Phase 6.7.1 Remediation: Async Job Lifecycle Methods
    # ========================================================================

    def transition_to_running(self, job_id: str) -> None:
        """
        Transition workspace to 'running' state when async job execution starts.

        Phase 6.7.1 (Remediation Step 3.1): Called by worker AFTER version
        validation passes and BEFORE sync execution begins. Ensures workspace
        state changes only when execution is guaranteed (Timing Correctness).

        Args:
            job_id: Job ID being executed (for audit trail)

        Raises:
            RuntimeError: If workspace not in valid state for transition

        Contract:
            - Workspace must be in 'ready' or 'cooldown' state
            - Transition is atomic
            - Fail-closed: raises exception if validation fails
        """
        cursor = self.conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()

        # Validate current state and transition to 'running'
        cursor.execute("""
            UPDATE workspace_sync_state
            SET state = 'running',
                state_updated_at = ?,
                last_sync_started_at = ?
            WHERE workspace_id = ?
              AND state IN ('ready', 'cooldown')
        """, (now_iso, now_iso, self.workspace_id))

        if cursor.rowcount == 0:
            # Workspace not in valid state for transition
            current_state_row = cursor.execute("""
                SELECT state FROM workspace_sync_state WHERE workspace_id = ?
            """, (self.workspace_id,)).fetchone()

            current_state = current_state_row['state'] if current_state_row else 'unknown'

            raise RuntimeError(
                f"Cannot transition workspace {self.workspace_id} to running: "
                f"current state is '{current_state}', expected 'ready' or 'cooldown'"
            )

        self.conn.commit()

        self.logger.info(
            EventType.SYNC_STARTED,
            f"Workspace {self.workspace_id} transitioned to 'running' for job {job_id}",
            workspace_id=self.workspace_id,
            job_id=job_id,
            state_transition="ready/cooldown→running"
        )

    def reconcile_workspace_after_async_job(
        self,
        job_id: str,
        job_status: str,
        sync_run_id: Optional[str]
    ) -> None:
        """
        Reconcile workspace state after async job reaches terminal state.

        Phase 6.7.1 (Remediation Step 3.2): Called by worker when job completes
        or fails. Ensures workspace state converges with job state atomically
        (Critical Finding C1 - Workspace State Convergence).

        Args:
            job_id: Job ID that completed
            job_status: Terminal job status ('COMPLETED' or 'FAILED')
            sync_run_id: Sync run ID (for validation and audit trail)

        Raises:
            RuntimeError: If validation fails (non-terminal job, missing sync_run, etc.)

        State Transitions:
            - COMPLETED → cooldown (if configured) or ready
            - FAILED → gated (with failure reason)

        Contract:
            - Job must be in terminal state
            - sync_run must exist and be in terminal state (auditable proof)
            - Workspace must be in 'running' state
            - Transition is atomic
            - Idempotent (safe to call multiple times for same job)
        """
        cursor = self.conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()

        # Validate job is terminal
        if job_status not in ('COMPLETED', 'FAILED'):
            raise RuntimeError(
                f"Cannot reconcile workspace {self.workspace_id}: "
                f"job {job_id} status is '{job_status}', expected 'COMPLETED' or 'FAILED'"
            )

        # Validate sync_run exists and is terminal (auditable proof - Step 2.6)
        if sync_run_id:
            sync_run_row = cursor.execute("""
                SELECT state FROM sync_runs WHERE sync_run_id = ?
            """, (sync_run_id,)).fetchone()

            if not sync_run_row:
                raise RuntimeError(
                    f"Cannot reconcile workspace {self.workspace_id}: "
                    f"sync_run {sync_run_id} not found (auditable proof missing)"
                )

            sync_run_state = sync_run_row['state']
            terminal_sync_states = ['completed', 'failed', 'cancelled', 'timed_out',
                                     'partially_completed', 'completed_with_ambiguity',
                                     'completed_with_warnings']

            if sync_run_state not in terminal_sync_states:
                raise RuntimeError(
                    f"Cannot reconcile workspace {self.workspace_id}: "
                    f"sync_run {sync_run_id} state is '{sync_run_state}', not terminal"
                )

        # Determine next workspace state based on job outcome
        if job_status == 'COMPLETED':
            # Success path
            if SYNC_COOLDOWN_MINUTES > 0:
                next_state = 'cooldown'
                cooldown_until = (datetime.now(timezone.utc) +
                                  timedelta(minutes=SYNC_COOLDOWN_MINUTES)).isoformat()
            else:
                next_state = 'ready'
                cooldown_until = None

            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = ?,
                    state_updated_at = ?,
                    active_sync_run_id = NULL,
                    last_sync_completed_at = ?,
                    last_sync_status = 'completed',
                    last_sync_failure_classification = NULL,
                    gate_reason = NULL,
                    gate_set_at = NULL,
                    cooldown_until = ?,
                    consecutive_failures = 0,
                    last_success_at = ?,
                    sync_count_total = sync_count_total + 1,
                    sync_count_success = sync_count_success + 1,
                    updated_at = ?
                WHERE workspace_id = ?
                  AND state = 'running'
            """, (next_state, now_iso, now_iso, cooldown_until,
                  now_iso, now_iso, self.workspace_id))

            if cursor.rowcount > 0:
                self.logger.info(
                    EventType.SYNC_COMPLETED,
                    f"Workspace {self.workspace_id} reconciled after job {job_id} completion",
                    workspace_id=self.workspace_id,
                    job_id=job_id,
                    sync_run_id=sync_run_id,
                    next_state=next_state
                )
            else:
                # Workspace already reconciled (idempotency) - log but don't error
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Workspace {self.workspace_id} already reconciled for job {job_id} (idempotent call)",
                    workspace_id=self.workspace_id,
                    job_id=job_id
                )

        elif job_status == 'FAILED':
            # Failure path - gate workspace with reason
            cursor.execute("""
                SELECT failure_reason FROM sync_jobs WHERE job_id = ?
            """, (job_id,)).fetchone()

            gate_reason = f"Async sync job {job_id} failed"

            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = 'gated',
                    state_updated_at = ?,
                    active_sync_run_id = NULL,
                    last_sync_completed_at = ?,
                    last_sync_status = 'failed',
                    gate_reason = ?,
                    gate_set_at = ?,
                    cooldown_until = NULL,
                    consecutive_failures = consecutive_failures + 1,
                    sync_count_total = sync_count_total + 1,
                    sync_count_failed = sync_count_failed + 1,
                    updated_at = ?
                WHERE workspace_id = ?
                  AND state = 'running'
            """, (now_iso, now_iso, gate_reason, now_iso,
                  now_iso, self.workspace_id))

            if cursor.rowcount > 0:
                self.logger.error(
                    EventType.ERROR_DETECTED,
                    f"Workspace {self.workspace_id} gated after job {job_id} failure",
                    workspace_id=self.workspace_id,
                    job_id=job_id,
                    sync_run_id=sync_run_id,
                    gate_reason=gate_reason
                )
            else:
                # Workspace already reconciled (idempotency) - log but don't error
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Workspace {self.workspace_id} already reconciled for job {job_id} (idempotent call)",
                    workspace_id=self.workspace_id,
                    job_id=job_id
                )

        self.conn.commit()
