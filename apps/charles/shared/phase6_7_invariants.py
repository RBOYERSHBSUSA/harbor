"""
Phase 6.8: Invariant Assertions and Certification Tripwires

This module contains runtime assertions that enforce Phase 6.7 certified guarantees.
These assertions MUST NOT change behavior - they only detect violations and fail-loud.

Certified Guarantees (Phase 6.7):
  - Guarantee 1 (At-Most-Once): Ownership fence prevents duplicate execution
  - Guarantee 5 (Self-Healing): No workspace remains running > 20 minutes
  - Guarantee 6 (Deployment Safety): Sentinel token fencing prevents version skew

Author: Claude Code
Date: 2026-01-03
Phase: 6.8 (Post-Certification Hardening)
"""

import os
import sqlite3
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from charles.shared.structured_logging import get_logger, EventType


# ============================================================================
# Configuration
# ============================================================================

def _load_fail_loud_mode() -> bool:
    """
    Load FAIL_LOUD_MODE from environment with production safeguard.

    Environment Variable: CHARLES_FAIL_LOUD_MODE
    - "true" or "1" = Enable (raises InvariantViolationError on violations)
    - Any other value or unset = Disable (logs CRITICAL only)

    Production Safeguard:
    - If CHARLES_ENV=production AND FAIL_LOUD_MODE=true, raises RuntimeError at startup
    - This prevents misconfiguration from crashing production workers

    Intended Usage:
    - Production: CHARLES_FAIL_LOUD_MODE unset (default: False)
    - Development/Testing: CHARLES_FAIL_LOUD_MODE=true (enables crash-on-violation)

    Returns:
        bool: True if fail-loud enabled, False otherwise

    Raises:
        RuntimeError: If FAIL_LOUD_MODE=true in production environment
    """
    env = os.environ.get('CHARLES_ENV', 'development').lower()
    fail_loud_str = os.environ.get('CHARLES_FAIL_LOUD_MODE', 'false').lower()
    fail_loud_enabled = fail_loud_str in ('true', '1')

    # Production safeguard - prevent FAIL_LOUD_MODE=true in production
    if env == 'production' and fail_loud_enabled:
        raise RuntimeError(
            "CRITICAL CONFIGURATION ERROR: CHARLES_FAIL_LOUD_MODE=true is NOT allowed in production. "
            "This would cause production workers to crash on invariant violations instead of logging. "
            "Set CHARLES_FAIL_LOUD_MODE=false or unset it entirely for production deployments."
        )

    return fail_loud_enabled


# Load FAIL_LOUD_MODE at module initialization
# In production: False (logs CRITICAL only)
# In development/test: Configurable via CHARLES_FAIL_LOUD_MODE env var
FAIL_LOUD_MODE = _load_fail_loud_mode()


class InvariantViolationError(Exception):
    """Raised when a certified Phase 6.7 invariant is violated."""
    pass


# ============================================================================
# Invariant: Workspace State Consistency (INV-WS-1, INV-WS-2)
# ============================================================================

def assert_workspace_running_has_active_job(
    db: sqlite3.Connection,
    workspace_id: str,
    context: str = "unknown"
) -> None:
    """
    Invariant: Every workspace in 'running' state MUST have exactly one active job.

    Phase 6.7 Guarantee 5 enforcement.

    Args:
        db: Database connection
        workspace_id: Workspace to check
        context: Where this assertion is called from (for debugging)

    Raises:
        InvariantViolationError: If invariant violated and FAIL_LOUD_MODE=True

    Logs:
        CRITICAL: If invariant violated (always logs, even if not raising)
    """
    logger = get_logger(service='invariants', workspace_id=workspace_id)
    cursor = db.cursor()

    try:
        # Check workspace state
        ws_row = cursor.execute("""
            SELECT state, active_sync_run_id
            FROM workspace_sync_state
            WHERE workspace_id = ?
        """, [workspace_id]).fetchone()

        if not ws_row:
            # Workspace state doesn't exist - not a violation (might be new workspace)
            return

        ws_state = ws_row['state']
        active_sync_run_id = ws_row['active_sync_run_id']

        if ws_state != 'running':
            # Not in running state - invariant doesn't apply
            return

        # Workspace is 'running' - count active jobs
        active_job_count = cursor.execute("""
            SELECT COUNT(*) as count
            FROM sync_jobs
            WHERE workspace_id = ?
              AND status IN ('QUEUED', 'CLAIMED', 'RUNNING')
        """, [workspace_id]).fetchone()['count']

        # INV-WS-1: Exactly one active job required
        if active_job_count != 1:
            violation_msg = (
                f"INVARIANT VIOLATION (INV-WS-1): Workspace {workspace_id} in 'running' state "
                f"has {active_job_count} active jobs (expected exactly 1). "
                f"Context: {context}. "
                f"Active sync_run_id: {active_sync_run_id}"
            )

            logger.error(
                EventType.ERROR_DETECTED,
                violation_msg,
                workspace_id=workspace_id,
                invariant='INV-WS-1',
                workspace_state=ws_state,
                active_job_count=active_job_count,
                expected_job_count=1,
                context=context
            )

            if FAIL_LOUD_MODE:
                raise InvariantViolationError(violation_msg)

    except sqlite3.OperationalError:
        # Table doesn't exist (pre-Phase 6 deployment) - skip check
        pass


def assert_workspace_not_running_has_no_active_jobs(
    db: sqlite3.Connection,
    workspace_id: str,
    context: str = "unknown"
) -> None:
    """
    Invariant: If workspace is NOT 'running', it MUST have zero active jobs.

    Phase 6.7 Guarantee 5 enforcement (reverse direction).

    Args:
        db: Database connection
        workspace_id: Workspace to check
        context: Where this assertion is called from

    Raises:
        InvariantViolationError: If invariant violated and FAIL_LOUD_MODE=True
    """
    logger = get_logger(service='invariants', workspace_id=workspace_id)
    cursor = db.cursor()

    try:
        # Check workspace state
        ws_row = cursor.execute("""
            SELECT state
            FROM workspace_sync_state
            WHERE workspace_id = ?
        """, [workspace_id]).fetchone()

        if not ws_row:
            return

        ws_state = ws_row['state']

        if ws_state == 'running':
            # Workspace is running - this invariant doesn't apply
            return

        # Workspace is NOT running - count active jobs
        active_job_count = cursor.execute("""
            SELECT COUNT(*) as count
            FROM sync_jobs
            WHERE workspace_id = ?
              AND status IN ('QUEUED', 'CLAIMED', 'RUNNING')
        """, [workspace_id]).fetchone()['count']

        # INV-WS-2: Zero active jobs required when not running
        if active_job_count > 0:
            violation_msg = (
                f"INVARIANT VIOLATION (INV-WS-2): Workspace {workspace_id} in '{ws_state}' state "
                f"has {active_job_count} active jobs (expected 0). "
                f"Context: {context}"
            )

            logger.error(
                EventType.ERROR_DETECTED,
                violation_msg,
                workspace_id=workspace_id,
                invariant='INV-WS-2',
                workspace_state=ws_state,
                active_job_count=active_job_count,
                expected_job_count=0,
                context=context
            )

            if FAIL_LOUD_MODE:
                raise InvariantViolationError(violation_msg)

    except sqlite3.OperationalError:
        pass


# ============================================================================
# Invariant: Ownership Fence (INV-OWN-1)
# ============================================================================

def assert_job_ownership_before_terminal_transition(
    db: sqlite3.Connection,
    job_id: str,
    expected_worker_id: str,
    context: str = "unknown"
) -> bool:
    """
    Invariant: Worker MUST own job before marking it terminal (COMPLETED/FAILED).

    Phase 6.7 Guarantee 1 enforcement (At-Most-Once Execution).

    Args:
        db: Database connection
        job_id: Job identifier
        expected_worker_id: Worker attempting the transition
        context: Where this assertion is called from

    Returns:
        True if worker owns job, False if ownership lost

    Raises:
        InvariantViolationError: Never raises (ownership loss is expected in crash scenarios)

    Logs:
        CRITICAL: If ownership fence violated (worker proceeds despite lost ownership)
    """
    logger = get_logger(service='invariants', workspace_id='system')
    cursor = db.cursor()

    try:
        job_row = cursor.execute("""
            SELECT status, claimed_by, workspace_id
            FROM sync_jobs
            WHERE job_id = ?
        """, [job_id]).fetchone()

        if not job_row:
            # Job doesn't exist - ownership definitively lost
            logger.error(
                EventType.ERROR_DETECTED,
                f"INVARIANT VIOLATION (INV-OWN-1): Job {job_id} not found during ownership check. "
                f"Worker {expected_worker_id} attempting terminal transition. Context: {context}",
                job_id=job_id,
                invariant='INV-OWN-1',
                expected_worker_id=expected_worker_id,
                context=context
            )
            return False

        job_status = job_row['status']
        claimed_by = job_row['claimed_by']
        workspace_id = job_row['workspace_id']

        # Check ownership match
        if claimed_by != expected_worker_id:
            logger.error(
                EventType.ERROR_DETECTED,
                f"INVARIANT VIOLATION (INV-OWN-1): Ownership mismatch for job {job_id}. "
                f"Expected: {expected_worker_id}, Actual: {claimed_by}. Context: {context}",
                job_id=job_id,
                workspace_id=workspace_id,
                invariant='INV-OWN-1',
                expected_worker_id=expected_worker_id,
                actual_claimed_by=claimed_by,
                job_status=job_status,
                context=context
            )
            return False

        # Check job status (must be RUNNING for terminal transition)
        if job_status != 'RUNNING':
            logger.error(
                EventType.ERROR_DETECTED,
                f"INVARIANT VIOLATION (INV-OWN-1): Job {job_id} status is '{job_status}' "
                f"(expected 'RUNNING') during terminal transition attempt. Context: {context}",
                job_id=job_id,
                workspace_id=workspace_id,
                invariant='INV-OWN-1',
                worker_id=expected_worker_id,
                job_status=job_status,
                expected_status='RUNNING',
                context=context
            )
            return False

        # Ownership validated
        return True

    except sqlite3.OperationalError:
        # Table doesn't exist - can't validate ownership, fail open
        return True


# ============================================================================
# Invariant: Deployment Safety (INV-DEPLOY-1)
# ============================================================================

def assert_schema_compatibility_token_match(
    db: sqlite3.Connection,
    worker_token: Optional[str],
    context: str = "unknown"
) -> None:
    """
    Invariant: Worker schema_compatibility_token MUST match deployment_metadata token.

    Phase 6.7 Guarantee 6 enforcement (Deployment Safety).

    Tripwire: If token mismatch detected mid-execution, logs CRITICAL error.
    In FAIL_LOUD_MODE, halts worker immediately to prevent schema incompatibility.

    Args:
        db: Database connection
        worker_token: Worker's loaded schema_compatibility_token
        context: Where this assertion is called from

    Raises:
        InvariantViolationError: If mismatch and FAIL_LOUD_MODE=True

    Logs:
        CRITICAL: If token mismatch detected (always logs, even if not raising)
    """
    logger = get_logger(service='invariants', workspace_id='system')
    cursor = db.cursor()

    try:
        # Load current deployment token
        token_row = cursor.execute("""
            SELECT value FROM deployment_metadata
            WHERE key = 'schema_compatibility_token'
        """).fetchone()

        if not token_row:
            # No token in database - deployment metadata missing
            # This should never happen after Phase 6.7.1 migration
            violation_msg = (
                f"INVARIANT VIOLATION (INV-DEPLOY-1): Schema compatibility token missing "
                f"from deployment_metadata. Worker token: {worker_token}. Context: {context}"
            )

            logger.error(
                EventType.ERROR_DETECTED,
                violation_msg,
                invariant='INV-DEPLOY-1',
                worker_token=worker_token,
                context=context
            )

            if FAIL_LOUD_MODE:
                raise InvariantViolationError(violation_msg)
            return

        db_token = token_row['value']

        # Check token match
        if worker_token != db_token:
            violation_msg = (
                f"INVARIANT VIOLATION (INV-DEPLOY-1): Schema compatibility token mismatch. "
                f"Worker token: {worker_token}, Database token: {db_token}. "
                f"CRITICAL: Worker is running against incompatible schema. "
                f"Context: {context}"
            )

            logger.error(
                EventType.ERROR_DETECTED,
                violation_msg,
                invariant='INV-DEPLOY-1',
                worker_token=worker_token,
                database_token=db_token,
                context=context
            )

            if FAIL_LOUD_MODE:
                raise InvariantViolationError(violation_msg)

    except sqlite3.OperationalError:
        # Table doesn't exist - pre-Phase 6.7.1 deployment
        # Worker with token shouldn't run against pre-6.7.1 schema
        if worker_token:
            violation_msg = (
                f"INVARIANT VIOLATION (INV-DEPLOY-1): Worker has token {worker_token} "
                f"but deployment_metadata table missing. Schema version mismatch. Context: {context}"
            )

            logger.error(
                EventType.ERROR_DETECTED,
                violation_msg,
                invariant='INV-DEPLOY-1',
                worker_token=worker_token,
                context=context
            )

            if FAIL_LOUD_MODE:
                raise InvariantViolationError(violation_msg)


# ============================================================================
# Invariant: Recovery Convergence (INV-REC-1)
# ============================================================================

def assert_no_orphaned_running_workspaces(
    db: sqlite3.Connection,
    context: str = "recovery_post_check"
) -> None:
    """
    Invariant: After recovery, NO workspace can be 'running' without an active job.

    Phase 6.7 Guarantee 5 enforcement (Self-Healing).

    This assertion verifies that recovery correctly reconciles all orphaned states.

    Args:
        db: Database connection
        context: Where this assertion is called from

    Raises:
        InvariantViolationError: If orphaned workspaces found after recovery

    Logs:
        CRITICAL: If orphaned workspaces detected
    """
    logger = get_logger(service='invariants', workspace_id='system')
    cursor = db.cursor()

    try:
        orphaned_workspaces = cursor.execute("""
            SELECT WS.workspace_id, WS.active_sync_run_id
            FROM workspace_sync_state WS
            WHERE WS.state = 'running'
              AND NOT EXISTS (
                  SELECT 1 FROM sync_jobs J
                  WHERE J.workspace_id = WS.workspace_id
                  AND J.status IN ('QUEUED', 'CLAIMED', 'RUNNING')
              )
        """).fetchall()

        if orphaned_workspaces:
            orphan_count = len(orphaned_workspaces)
            orphan_ids = [row['workspace_id'] for row in orphaned_workspaces]

            violation_msg = (
                f"INVARIANT VIOLATION (INV-REC-1): Found {orphan_count} orphaned 'running' "
                f"workspaces after recovery. Workspace IDs: {orphan_ids}. "
                f"Recovery MUST reconcile all orphaned states. Context: {context}"
            )

            logger.error(
                EventType.ERROR_DETECTED,
                violation_msg,
                invariant='INV-REC-1',
                orphan_count=orphan_count,
                orphaned_workspace_ids=orphan_ids,
                context=context
            )

            if FAIL_LOUD_MODE:
                raise InvariantViolationError(violation_msg)

    except sqlite3.OperationalError:
        # Table doesn't exist - skip check
        pass


# ============================================================================
# Tripwire: Maximum Running Duration (TRIPWIRE-TIMEOUT)
# ============================================================================

def assert_no_workspace_running_beyond_timeout(
    db: sqlite3.Connection,
    max_running_minutes: int = 20,
    context: str = "periodic_check"
) -> None:
    """
    Tripwire: NO workspace should remain 'running' > max_running_minutes.

    Phase 6.7 Guarantee 5 enforcement (Self-Healing).

    This is a certification tripwire - if triggered, it indicates recovery
    is not converging properly (heartbeat timeout not working).

    Args:
        db: Database connection
        max_running_minutes: Maximum allowed running duration (default: 20)
        context: Where this assertion is called from

    Raises:
        InvariantViolationError: If workspace found running beyond timeout

    Logs:
        CRITICAL: If long-running workspace detected
    """
    logger = get_logger(service='invariants', workspace_id='system')
    cursor = db.cursor()

    try:
        timeout_threshold = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=max_running_minutes)

        long_running_workspaces = cursor.execute("""
            SELECT workspace_id, state, last_sync_started_at, active_sync_run_id
            FROM workspace_sync_state
            WHERE state = 'running'
              AND last_sync_started_at IS NOT NULL
              AND last_sync_started_at < ?
        """, [timeout_threshold.strftime('%Y-%m-%d %H:%M:%S')]).fetchall()

        if long_running_workspaces:
            for ws_row in long_running_workspaces:
                workspace_id = ws_row['workspace_id']
                last_sync_started_at = ws_row['last_sync_started_at']
                active_sync_run_id = ws_row['active_sync_run_id']

                violation_msg = (
                    f"TRIPWIRE VIOLATION (TRIPWIRE-TIMEOUT): Workspace {workspace_id} "
                    f"has been 'running' since {last_sync_started_at} "
                    f"(> {max_running_minutes} minutes). "
                    f"Recovery should have reconciled this. "
                    f"Active sync_run_id: {active_sync_run_id}. Context: {context}"
                )

                logger.error(
                    EventType.ERROR_DETECTED,
                    violation_msg,
                    workspace_id=workspace_id,
                    invariant='TRIPWIRE-TIMEOUT',
                    last_sync_started_at=last_sync_started_at,
                    max_running_minutes=max_running_minutes,
                    active_sync_run_id=active_sync_run_id,
                    context=context
                )

                if FAIL_LOUD_MODE:
                    raise InvariantViolationError(violation_msg)

    except sqlite3.OperationalError:
        # Table doesn't exist - skip check
        pass


# ============================================================================
# Audit Helper: Collect Invariant Violations
# ============================================================================

def collect_all_invariant_violations(db: sqlite3.Connection) -> Dict[str, Any]:
    """
    Collect all current invariant violations for audit reporting.

    Used by hostile audit harness to verify system state.

    Returns:
        Dictionary with violation counts and details:
        {
            'total_violations': int,
            'orphaned_workspaces': [workspace_ids],
            'long_running_workspaces': [workspace_ids],
            'ownership_violations': [job_ids]
        }
    """
    violations = {
        'total_violations': 0,
        'orphaned_workspaces': [],
        'long_running_workspaces': [],
        'ownership_violations': []
    }

    cursor = db.cursor()

    try:
        # Check orphaned workspaces
        orphaned = cursor.execute("""
            SELECT WS.workspace_id
            FROM workspace_sync_state WS
            WHERE WS.state = 'running'
              AND NOT EXISTS (
                  SELECT 1 FROM sync_jobs J
                  WHERE J.workspace_id = WS.workspace_id
                  AND J.status IN ('QUEUED', 'CLAIMED', 'RUNNING')
              )
        """).fetchall()

        violations['orphaned_workspaces'] = [row['workspace_id'] for row in orphaned]
        violations['total_violations'] += len(orphaned)

        # Check long-running workspaces (> 20 minutes)
        timeout_threshold = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=20)
        long_running = cursor.execute("""
            SELECT workspace_id
            FROM workspace_sync_state
            WHERE state = 'running'
              AND last_sync_started_at IS NOT NULL
              AND last_sync_started_at < ?
        """, [timeout_threshold.strftime('%Y-%m-%d %H:%M:%S')]).fetchall()

        violations['long_running_workspaces'] = [row['workspace_id'] for row in long_running]
        violations['total_violations'] += len(long_running)

    except sqlite3.OperationalError:
        # Tables don't exist - no violations to report
        pass

    return violations
