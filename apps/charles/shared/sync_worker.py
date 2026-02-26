"""
Phase 6: Sync Worker - Async Job Execution Engine

Responsibilities:
  - Poll job queue for available jobs
  - Claim jobs atomically with round-robin fairness
  - Execute sync via existing SyncRunManager flow
  - Heartbeat mechanism for crash recovery
  - Graceful shutdown handling

Author: Claude Code
Date: 2025-12-30
"""

import os
import time
import signal
import sqlite3
import threading
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

from charles.shared.job_queue import JobQueue, SyncJob, JobStatus
from charles.shared.structured_logging import get_logger, EventType
from charles.shared.metrics import metrics  # Phase 6.6: Observability
from charles.shared.phase6_7_invariants import (  # Phase 6.8: Deployment safety tripwire
    assert_schema_compatibility_token_match
)


# ============================================================================
# Phase 6.7.1 Remediation: Semantic Version Comparison (C-ORG-3)
# ============================================================================

def semantic_version_compare(version_a: str, version_b: str) -> int:
    """
    Compare two semantic version strings.

    Phase 6.7.1 (C-ORG-3): Fixes version comparison to use semantic versioning
    instead of lexicographic string comparison.

    Args:
        version_a: First version string (e.g., "6.7.10")
        version_b: Second version string (e.g., "6.7.2")

    Returns:
        -1 if version_a < version_b
         0 if version_a == version_b
         1 if version_a > version_b

    Examples:
        semantic_version_compare("6.7.10", "6.7.2") → 1 (10 > 2, not "10" < "2")
        semantic_version_compare("6.10.0", "6.9.0") → 1 (10 > 9)
        semantic_version_compare("6.7.1", "6.7.1") → 0

    Note: Supports major.minor.patch format. Non-numeric suffixes ignored.
    """
    try:
        # Parse version strings into (major, minor, patch) tuples
        def parse_version(version_str: str) -> tuple:
            # Remove leading 'v' if present
            version_str = version_str.lstrip('v')
            # Split by '.' and take first 3 parts
            parts = version_str.split('.')[:3]
            # Convert to integers, default to 0 if missing or non-numeric
            parsed = []
            for part in parts:
                try:
                    # Remove any non-numeric suffix (e.g., "2-alpha" → 2)
                    numeric_part = ''.join(c for c in part if c.isdigit())
                    parsed.append(int(numeric_part) if numeric_part else 0)
                except ValueError:
                    parsed.append(0)
            # Pad to 3 parts if needed (e.g., "6.7" → "6.7.0")
            while len(parsed) < 3:
                parsed.append(0)
            return tuple(parsed)

        version_a_tuple = parse_version(version_a)
        version_b_tuple = parse_version(version_b)

        # Compare tuples (lexicographic comparison of integers, not strings)
        if version_a_tuple < version_b_tuple:
            return -1
        elif version_a_tuple > version_b_tuple:
            return 1
        else:
            return 0

    except Exception:
        # Fall back to string comparison if parsing fails
        # This should never happen with well-formed version strings
        if version_a < version_b:
            return -1
        elif version_a > version_b:
            return 1
        else:
            return 0


# ============================================================================
# Worker Configuration
# ============================================================================

class WorkerConfig:
    """
    Worker configuration (NO DEFAULTS - must be explicitly set).

    Raises ConfigurationError if required settings missing.
    """

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize worker configuration.

        Args:
            config_dict: Configuration dictionary (from env vars or settings table)

        Raises:
            ValueError: If required configuration missing
        """
        self.config = config_dict or {}

    def get_required(self, key: str) -> Any:
        """
        Get required configuration value.

        Args:
            key: Configuration key

        Returns:
            Configuration value

        Raises:
            ValueError: If configuration key missing
        """
        if key not in self.config:
            raise ValueError(f"Required configuration missing: {key}")
        return self.config[key]

    def get_optional(self, key: str, default: Any = None) -> Any:
        """
        Get optional configuration value with default.

        Args:
            key: Configuration key
            default: Default value if key missing

        Returns:
            Configuration value or default
        """
        return self.config.get(key, default)


# ============================================================================
# Sync Worker
# ============================================================================

class SyncWorker:
    """
    Async sync execution worker.

    Polls job queue, claims jobs, executes syncs via existing infrastructure.
    Crash-recoverable via heartbeat mechanism.
    Gracefully shuts down on SIGTERM/SIGINT.

    Workflow:
      1. Poll job queue for available jobs
      2. Check global concurrency limit
      3. Claim next job atomically (round-robin fairness)
      4. Execute sync via SyncRunManager (Phase 3 contract)
      5. Update heartbeat every 30 seconds
      6. Mark job terminal on completion/failure
      7. Repeat

    State Management:
      - No in-memory job state (all state in database)
      - Crash recovery via heartbeat timeout detection
      - Graceful shutdown: finish current job before exit
    """

    # Phase 6.7.1: Version Safety (Remediation - Critical Findings C-ORG-2, C-ORG-3)
    WORKER_VERSION = "6.7.1"
    REQUIRED_SCHEMA_VERSION = 671  # Integer version (schema_version.version is INTEGER not TEXT)

    # Phase 6.7 Remediation: Schema compatibility token (C-ORG-2 - sentinel strategy)
    # This token prevents old workers from claiming jobs after schema migration
    # Loaded from deployment_metadata table at startup, passed in claim query
    schema_compatibility_token: Optional[str] = None

    # Poll intervals
    POLL_INTERVAL_IDLE = 5  # seconds (when no jobs available)
    POLL_INTERVAL_LIMIT = 5  # seconds (when global limit reached)

    # Heartbeat interval
    HEARTBEAT_INTERVAL = 30  # seconds

    # Checklist Item 13: Recovery frequency enforcement
    RECOVERY_INTERVAL = 60  # seconds (maximum tolerated delay)

    def __init__(
        self,
        db: sqlite3.Connection,
        config: WorkerConfig,
        worker_id: Optional[str] = None
    ):
        """
        Initialize sync worker.

        Args:
            db: SQLite database connection
            config: Worker configuration (NO DEFAULTS - raises if missing)
            worker_id: Worker identifier (default: hostname:pid)

        Raises:
            ValueError: If required configuration missing
        """
        self.db = db
        self.config = config

        # Phase 6.7: Validate schema version before proceeding (Remediation 3)
        self._validate_schema_version(db)

        # Worker identity
        if worker_id is None:
            hostname = os.uname().nodename
            pid = os.getpid()
            worker_id = f"{hostname}:{pid}"
        self.worker_id = worker_id

        # Get required configuration (NO DEFAULTS - raises if missing)
        self.global_limit = config.get_required('max_concurrent_syncs')
        self.per_workspace_limit = config.get_required('max_concurrent_syncs_per_workspace')

        # Job queue interface
        self.job_queue = JobQueue(db)

        # Worker state
        self.running = False
        self.current_job: Optional[SyncJob] = None
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.heartbeat_stop_event = threading.Event()

        # Checklist Item 13: Recovery frequency enforcement
        self.last_recovery_time: Optional[float] = None  # Timestamp of last recovery

        # Logger (system-level, workspace_id set per job)
        self.logger = get_logger(service='sync_worker', workspace_id='system')

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    # ========================================================================
    # Main Loop
    # ========================================================================

    def run(self):
        """
        Main worker loop.

        Polls job queue and executes jobs until stopped.
        Handles graceful shutdown on SIGTERM/SIGINT.

        Phase 6.5 Checklist Items: 13, 14, 15
        """
        self.running = True

        self.logger.info(
            EventType.WORKER_STARTED,
            f"Worker started: {self.worker_id}",
            worker_id=self.worker_id,
            global_limit=self.global_limit,
            per_workspace_limit=self.per_workspace_limit
        )

        # Phase 6.6: Worker lifecycle metrics
        metrics.increment('charles_workers_total')
        metrics.gauge('charles_workers_active', 1)
        self.worker_start_time = time.time()

        # Checklist Item 14: Recovery on worker startup (allowed trigger)
        self._run_recovery()

        try:
            while self.running:
                # Checklist Item 13: Recovery frequency enforcement (≤60s interval)
                current_time = time.time()
                if (self.last_recovery_time is None or
                    (current_time - self.last_recovery_time) >= self.RECOVERY_INTERVAL):
                    self._run_recovery()

                # 1. Check global concurrency limit
                running_count = self._get_running_job_count()
                if running_count >= self.global_limit:
                    self.logger.info(
                        EventType.WORKER_LIMIT_REACHED,
                        f"Global limit reached: {running_count}/{self.global_limit}",
                        worker_id=self.worker_id,
                        running_count=running_count,
                        global_limit=self.global_limit
                    )
                    time.sleep(self.POLL_INTERVAL_LIMIT)
                    continue

                # 2. Claim next job (round-robin fairness)
                # Phase 6.7.1 (C-ORG-2): Pass schema_compatibility_token for deployment fencing
                # Phase 6.7.1 (Step 2.3): Pass worker_version for audit trail
                job = self.job_queue.claim_next_job(
                    worker_id=self.worker_id,
                    per_workspace_limit=self.per_workspace_limit,
                    worker_version=self.WORKER_VERSION,
                    schema_compatibility_token=self.schema_compatibility_token
                )

                if not job:
                    # Checklist Item 14: Recovery on empty poll
                    self._run_recovery()

                    # No jobs available, sleep and retry
                    time.sleep(self.POLL_INTERVAL_IDLE)
                    continue

                # 3. Execute job
                self._execute_job(job)

        except KeyboardInterrupt:
            self.logger.info(
                EventType.WORKER_STOPPED,
                "Worker interrupted by user",
                worker_id=self.worker_id
            )
        except Exception as e:
            self.logger.error(
                EventType.WORKER_CRASHED,
                f"Worker crashed: {str(e)}",
                worker_id=self.worker_id,
                error=str(e)
            )
            raise
        finally:
            self._shutdown()

    def stop(self):
        """
        Stop worker gracefully.

        Finishes current job before exiting.
        """
        self.running = False
        self.logger.info(
            EventType.WORKER_STOPPING,
            f"Worker stopping gracefully: {self.worker_id}",
            worker_id=self.worker_id
        )

    # ========================================================================
    # Job Execution
    # ========================================================================

    def _execute_job(self, job: SyncJob):
        """
        Execute a single job with Phase 6.7.1 remediation logic.

        Phase 6.7.1 Remediation Workflow:
          1. Start heartbeat thread
          2. [Step 2.2] Version validation - reject if worker too old
          3. [Step 2.5] Transition workspace to 'running' (AFTER version check)
          4. Start sync (SyncRunManager.start_sync) - creates sync_run
          5. Link job to sync_run (job.sync_run_id)
          6. Transition job to RUNNING
          7. [Step 2.6] Enforce completion proof before marking COMPLETED
          8. [Step 2.7] Reconcile workspace state on terminal transition
          9. [Step 2.8] Lock cleanup in finally block

        Critical Findings Addressed:
          - C1: Workspace state convergence (Step 2.7)
          - C2: Version safety (Step 2.2)
          - H2: Completion proof enforcement (Step 2.6)

        Args:
            job: Job to execute
        """
        self.current_job = job

        # Get workspace-specific logger
        logger = get_logger(service='sync_worker', workspace_id=job.workspace_id)

        logger.info(
            EventType.JOB_EXECUTION_STARTED,
            f"Worker {self.worker_id} executing job {job.job_id}",
            worker_id=self.worker_id,
            job_id=job.job_id,
            workspace_id=job.workspace_id,
            priority=job.priority,
            retry_count=job.retry_count,
            worker_version=self.WORKER_VERSION,
            minimum_worker_version=job.minimum_worker_version
        )

        # Start heartbeat thread
        self._start_heartbeat(job.job_id)

        sync_run_id = None  # Track for finally block cleanup
        workspace_transitioned = False  # Track if workspace state changed

        try:
            # ================================================================
            # STEP 2.2: VERSION VALIDATION (Critical Findings C-ORG-2, C-ORG-3)
            # ================================================================
            # Validate worker version before execution to prevent old workers
            # from executing new jobs (prevents silent data corruption)
            # Phase 6.7.1 (C-ORG-3): Use semantic version comparison, not string comparison
            if job.minimum_worker_version:
                # Semantic version comparison: -1 if worker < minimum, 0 if equal, 1 if worker > minimum
                version_comparison = semantic_version_compare(self.WORKER_VERSION, job.minimum_worker_version)

                if version_comparison < 0:  # Worker version is semantically LESS than required
                    error_msg = (
                        f"Worker version {self.WORKER_VERSION} insufficient for job {job.job_id}. "
                        f"Minimum required: {job.minimum_worker_version}. "
                        f"Upgrade worker before executing this job."
                    )
                    logger.error(
                        EventType.JOB_EXECUTION_FAILED,
                        error_msg,
                        job_id=job.job_id,
                        workspace_id=job.workspace_id,
                        worker_version=self.WORKER_VERSION,
                        minimum_worker_version=job.minimum_worker_version,
                        version_comparison_result=version_comparison
                    )
                    # Fail job - do NOT execute
                    self.job_queue.mark_failed(job.job_id, error_msg)
                    return  # Exit early without execution

            # ================================================================
            # Phase 6.8: DEPLOYMENT SAFETY TRIPWIRE (INV-DEPLOY-1)
            # ================================================================
            # Verify schema compatibility token still matches before execution
            # This catches rolling deployments mid-execution
            assert_schema_compatibility_token_match(
                self.db,
                self.schema_compatibility_token,
                context=f"pre_execution_job_{job.job_id}"
            )

            # ================================================================
            # STEP 2.5: WORKSPACE STATE TRANSITION (Timing Correctness)
            # ================================================================
            # Transition workspace to 'running' AFTER version validation passes
            # This ensures workspace state changes only when execution guaranteed
            from charles.shared.workspace_sync_orchestrator import WorkspaceSyncOrchestrator

            orchestrator = WorkspaceSyncOrchestrator(
                db_connection=self.db,
                workspace_id=job.workspace_id,
                logger=logger
            )

            try:
                orchestrator.transition_to_running(job.job_id)
                workspace_transitioned = True
            except RuntimeError as e:
                # Workspace not in valid state - fail job
                error_msg = f"Cannot transition workspace to running: {str(e)}"
                logger.error(
                    EventType.JOB_EXECUTION_FAILED,
                    error_msg,
                    job_id=job.job_id,
                    workspace_id=job.workspace_id
                )
                self.job_queue.mark_failed(job.job_id, error_msg)
                return  # Exit early without execution

            # ================================================================
            # SYNC EXECUTION — FAIL LOUD (NOT IMPLEMENTED)
            # Per SYNC_RUN_LIFECYCLE_CONTRACT: real execution required.
            # Simulation removed during Phase 3 Contract Remediation.
            # ================================================================
            error_msg = (
                f"SYNC EXECUTION NOT IMPLEMENTED: Job {job.job_id} cannot execute. "
                f"Real sync execution must be wired to ProcessorSyncManager. "
                f"(SYNC_RUN_LIFECYCLE_CONTRACT — simulation removed)"
            )
            logger.error(
                EventType.JOB_EXECUTION_FAILED,
                error_msg,
                job_id=job.job_id,
                workspace_id=job.workspace_id
            )
            self.job_queue.mark_failed(job.job_id, error_msg)

            # Reconcile workspace state since we transitioned to running above
            if workspace_transitioned:
                orchestrator.reconcile_workspace_after_async_job(
                    job_id=job.job_id,
                    job_status='FAILED',
                    sync_run_id=None
                )

            return  # No execution — fail loudly, do not proceed

            # ================================================================
            # STEP 2.6: COMPLETION PROOF ENFORCEMENT (Critical Finding H2)
            # ================================================================
            # Before marking job COMPLETED, validate auditable proof exists
            # Proof: sync_run_id is NOT NULL and sync_run is in terminal state

            if not sync_run_id:
                raise RuntimeError(
                    f"Cannot complete job {job.job_id}: sync_run_id is NULL. "
                    f"Auditable proof missing (job must have sync_run_id)."
                )

            # When real execution is wired, validate sync_run terminal state here

            # ================================================================
            # H-ORG-1: OWNERSHIP FENCE BEFORE TERMINAL TRANSITION
            # ================================================================
            # Verify worker still owns job before marking complete
            # Prevents double execution if worker lost ownership during execution
            if not self._verify_job_ownership(job.job_id):
                logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Worker {self.worker_id} lost ownership of job {job.job_id} - discarding silently",
                    worker_id=self.worker_id,
                    job_id=job.job_id,
                    workspace_id=job.workspace_id
                )
                # Silently discard - recovery will handle this job
                return

            # Mark job completed (ownership validated, proof validated)
            self.job_queue.mark_completed(job.job_id)

            logger.info(
                EventType.JOB_EXECUTION_COMPLETED,
                f"Job {job.job_id} completed successfully",
                job_id=job.job_id,
                workspace_id=job.workspace_id,
                sync_run_id=sync_run_id
            )

            # ================================================================
            # STEP 2.7 + H-ORG-3: WORKSPACE STATE RECONCILIATION (FAIL-LOUD)
            # ================================================================
            # Reconcile workspace state after job completion
            # This ensures workspace unblocked and ready for next sync
            # H-ORG-3 (Fail-Loud): Re-raise reconciliation exceptions to gate workspace
            # Prevents silent stuck state where job completes but workspace remains running
            orchestrator.reconcile_workspace_after_async_job(
                job_id=job.job_id,
                job_status='COMPLETED',
                sync_run_id=sync_run_id
            )
            # H-ORG-3: If reconciliation fails, exception propagates to outer try/except
            # Worker will attempt to mark job FAILED and gate workspace
            # This is correct behavior - reconciliation failures indicate orchestration bugs

        except Exception as e:
            # ================================================================
            # FAILURE PATH (H-ORG-1 + H-ORG-2)
            # ================================================================
            # H-ORG-1: Ownership fence before marking failed
            # H-ORG-2: Harden exception handler against cascading failures

            failure_reason = f"Worker exception: {str(e)}"

            # H-ORG-1: Verify ownership before marking failed
            # If worker lost ownership, silently discard (recovery will handle)
            if not self._verify_job_ownership(job.job_id):
                logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Worker {self.worker_id} lost ownership of job {job.job_id} during exception - discarding silently",
                    worker_id=self.worker_id,
                    job_id=job.job_id,
                    workspace_id=job.workspace_id,
                    error=str(e)
                )
                # Silently discard - recovery will handle this job
                # DO NOT raise - prevents cascading failures (H-ORG-2)
                return  # Exit early

            # H-ORG-2: Wrap mark_failed in try/except to prevent cascading failures
            # If mark_failed fails due to state validation or ownership loss, log and exit cleanly
            try:
                self.job_queue.mark_failed(job.job_id, failure_reason)

                logger.error(
                    EventType.JOB_EXECUTION_FAILED,
                    f"Job {job.job_id} failed: {str(e)}",
                    job_id=job.job_id,
                    workspace_id=job.workspace_id,
                    error=str(e),
                    failure_reason=failure_reason
                )
            except Exception as mark_failed_error:
                # H-ORG-2: mark_failed raised exception (likely state validation or ownership lost)
                # Log warning and exit cleanly - DO NOT raise or retry
                logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Cannot mark job {job.job_id} as failed: {str(mark_failed_error)}. "
                    f"Job likely already recovered. Original error: {str(e)}",
                    job_id=job.job_id,
                    workspace_id=job.workspace_id,
                    original_error=str(e),
                    mark_failed_error=str(mark_failed_error)
                )
                # DO NOT raise - prevents cascading failures
                # Recovery will handle this job
                return  # Exit early

            # ================================================================
            # STEP 2.7 + H-ORG-3: WORKSPACE STATE RECONCILIATION (Failure Path - FAIL-LOUD)
            # ================================================================
            # Reconcile workspace state after job failure
            # H-ORG-3 (Fail-Loud): Re-raise reconciliation exceptions
            if workspace_transitioned:
                from charles.shared.workspace_sync_orchestrator import WorkspaceSyncOrchestrator
                orchestrator = WorkspaceSyncOrchestrator(
                    db_connection=self.db,
                    workspace_id=job.workspace_id,
                    logger=logger
                )
                orchestrator.reconcile_workspace_after_async_job(
                    job_id=job.job_id,
                    job_status='FAILED',
                    sync_run_id=sync_run_id
                )
                # H-ORG-3: If reconciliation fails, exception propagates
                # This is fail-loud behavior - reconciliation failures should not be silent
                # Worker will exit cleanly (finally block ensures cleanup)

        finally:
            # ================================================================
            # STEP 2.8: LOCK CLEANUP IN FINALLY BLOCK
            # ================================================================
            # Ensure heartbeat stopped and resources released
            # Workspace state convergence handled in try/except above

            # Stop heartbeat
            self._stop_heartbeat()
            self.current_job = None

            # No additional lock cleanup needed - database transaction handles it

    # ========================================================================
    # Heartbeat
    # ========================================================================

    def _start_heartbeat(self, job_id: str):
        """
        Start heartbeat thread for job.

        Updates job heartbeat_at every 30 seconds.
        Runs until heartbeat_stop_event set.

        Args:
            job_id: Job to heartbeat
        """
        self.heartbeat_stop_event.clear()

        def heartbeat_loop():
            while not self.heartbeat_stop_event.is_set():
                try:
                    self.job_queue.update_heartbeat(job_id, self.worker_id)
                    self.logger.info(
                        EventType.JOB_HEARTBEAT_UPDATED,
                        f"Heartbeat updated for job {job_id}",
                        job_id=job_id,
                        worker_id=self.worker_id
                    )
                except Exception as e:
                    self.logger.error(
                        EventType.HEARTBEAT_FAILED,
                        f"Heartbeat update failed for job {job_id}: {str(e)}",
                        job_id=job_id,
                        worker_id=self.worker_id,
                        error=str(e)
                    )

                # Sleep for interval (interruptible)
                self.heartbeat_stop_event.wait(self.HEARTBEAT_INTERVAL)

        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def _stop_heartbeat(self):
        """
        Stop heartbeat thread.
        """
        if self.heartbeat_thread:
            self.heartbeat_stop_event.set()
            self.heartbeat_thread.join(timeout=5)
            self.heartbeat_thread = None

    # ========================================================================
    # Phase 6.7.1 Remediation: Ownership Fence (H-ORG-1)
    # ========================================================================

    def _verify_job_ownership(self, job_id: str) -> bool:
        """
        Verify worker still owns the job before marking it complete/failed.

        Phase 6.7.1 (H-ORG-1 - Ownership Fence): Prevents double execution
        when worker loses ownership due to heartbeat timeout.

        Worker must validate ownership before writing terminal state to prevent:
        - Recovery re-queuing job while original worker continues
        - Double execution (one by this worker, one by next worker)

        Args:
            job_id: Job identifier

        Returns:
            True if worker still owns job and can proceed with terminal transition
            False if worker lost ownership (job already recovered)

        Ownership validation checks:
            1. Job exists
            2. Job.worker_id == self.worker_id (ownership match)
            3. Job.status == 'RUNNING' (not already recovered)
        """
        cursor = self.db.cursor()

        try:
            job_row = cursor.execute("""
                SELECT status, claimed_by, heartbeat_at
                FROM sync_jobs
                WHERE job_id = ?
            """, [job_id]).fetchone()

            if not job_row:
                # Job deleted or not found - worker lost ownership
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Ownership verification failed for job {job_id}: job not found",
                    job_id=job_id,
                    worker_id=self.worker_id
                )
                return False

            job_status = job_row['status']
            job_claimed_by = job_row['claimed_by']
            job_heartbeat_at = job_row['heartbeat_at']

            # Check 1: Status must be RUNNING (not QUEUED or FAILED or COMPLETED)
            if job_status != JobStatus.RUNNING.value:
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Ownership verification failed for job {job_id}: status is {job_status}, expected RUNNING",
                    job_id=job_id,
                    worker_id=self.worker_id,
                    job_status=job_status
                )
                return False

            # Check 2: claimed_by must match this worker
            if job_claimed_by != self.worker_id:
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Ownership verification failed for job {job_id}: claimed_by mismatch",
                    job_id=job_id,
                    worker_id=self.worker_id,
                    job_claimed_by=job_claimed_by
                )
                return False

            # All checks passed - worker still owns job
            return True

        except Exception as e:
            # Database error during verification - FAIL-CLOSED (assume lost ownership)
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Ownership verification failed for job {job_id} due to exception: {str(e)}",
                job_id=job_id,
                worker_id=self.worker_id,
                error=str(e)
            )
            return False

    # ========================================================================
    # Helpers
    # ========================================================================

    def _get_running_job_count(self) -> int:
        """
        Get count of currently running jobs (global).

        Returns:
            Count of jobs in RUNNING status
        """
        cursor = self.db.cursor()
        result = cursor.execute("""
            SELECT COUNT(*) as count
            FROM sync_jobs
            WHERE status = ?
        """, [JobStatus.RUNNING.value]).fetchone()

        return result['count']

    def _run_recovery(self):
        """
        Run two-phase recovery (job-driven + orphan reconciliation).

        Phase 6.5 Revised Contract - Checklist Items 13, 14, 15:
        - Item 13: Enforces ≤60s interval (updates last_recovery_time)
        - Item 14: Called on empty poll and worker startup
        - Item 15: No selective recovery (no workspace_id parameter)

        Contract:
            - Idempotent (safe to call multiple times)
            - Updates last_recovery_time on completion
            - Never throws exception (logs errors only)
        """
        try:
            stats = self.job_queue.recover_all()

            # Update recovery timestamp (Item 13)
            self.last_recovery_time = time.time()

            # Log recovery results (if any resources recovered/reconciled)
            total_actions = (stats['jobs_recovered'] + stats['jobs_failed'] +
                           stats['locks_reconciled'] + stats['workspaces_reconciled'] +
                           stats['syncs_reconciled'])

            if total_actions > 0:
                self.logger.info(
                    EventType.CRASH_RECOVERY_COMPLETED,
                    f"Recovery: {stats['jobs_recovered']} jobs recovered, {stats['jobs_failed']} jobs failed, "
                    f"{stats['locks_reconciled']} locks reconciled, {stats['workspaces_reconciled']} workspaces reconciled, "
                    f"{stats['syncs_reconciled']} syncs reconciled",
                    worker_id=self.worker_id,
                    jobs_recovered=stats['jobs_recovered'],
                    jobs_failed=stats['jobs_failed'],
                    locks_reconciled=stats['locks_reconciled'],
                    workspaces_reconciled=stats['workspaces_reconciled'],
                    syncs_reconciled=stats['syncs_reconciled']
                )

        except Exception as e:
            # Never crash worker due to recovery failure
            self.logger.error(
                EventType.WORKER_CRASHED,
                f"Recovery failed: {str(e)}",
                worker_id=self.worker_id,
                error=str(e)
            )

    def _handle_shutdown_signal(self, signum, frame):
        """
        Handle shutdown signal (SIGTERM/SIGINT).

        Sets running=False to trigger graceful shutdown.

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        self.logger.info(
            EventType.WORKER_SIGNAL_RECEIVED,
            f"Worker received signal {signum}, shutting down gracefully",
            worker_id=self.worker_id,
            signal=signum
        )
        self.stop()

    def _shutdown(self):
        """
        Cleanup on worker shutdown.

        Stops heartbeat, closes database connection.
        """
        # Stop heartbeat if running
        if self.heartbeat_thread:
            self._stop_heartbeat()

        # Log shutdown
        self.logger.info(
            EventType.WORKER_STOPPED,
            f"Worker stopped: {self.worker_id}",
            worker_id=self.worker_id
        )

        # Phase 6.6: Worker shutdown metrics
        metrics.gauge('charles_workers_active', 0)
        if hasattr(self, 'worker_start_time'):
            uptime = time.time() - self.worker_start_time
            metrics.histogram('charles_worker_uptime_seconds', uptime,
                            tags={'worker_id': self.worker_id})

    def _validate_schema_version(self, db: sqlite3.Connection):
        """
        Validate database schema and load deployment metadata.

        Phase 6.7.1 Remediation (C-ORG-2 - Deployment Safety):
        Prevents version skew where worker deployed before schema migration.
        Worker will refuse to start if required columns or token missing.

        Required for Phase 6.7.1:
        - sync_jobs.worker_version (audit metadata)
        - sync_jobs.minimum_worker_version (version enforcement)
        - sync_jobs.schema_compatibility_token (deployment fencing)
        - deployment_metadata.schema_compatibility_token (sentinel value)

        Args:
            db: Database connection

        Raises:
            RuntimeError: If required schema elements missing (FAIL-CLOSED)
        """
        cursor = db.cursor()

        # ====================================================================
        # STEP 1: Validate Required Columns Exist
        # ====================================================================
        required_columns = ['worker_version', 'minimum_worker_version', 'schema_compatibility_token']
        missing_columns = []

        try:
            # Get table schema
            columns_info = cursor.execute("""
                SELECT name FROM pragma_table_info('sync_jobs')
            """).fetchall()

            existing_columns = [row['name'] for row in columns_info]

            # Check for missing required columns
            for col in required_columns:
                if col not in existing_columns:
                    missing_columns.append(col)

            if missing_columns:
                raise RuntimeError(
                    f"Worker version {self.WORKER_VERSION} cannot start (FAIL-CLOSED): "
                    f"Required columns missing from sync_jobs table: {', '.join(missing_columns)}. "
                    f"REQUIRED ACTION: Run schema migration before starting workers:\n"
                    f"  sqlite3 data/charles.db < scripts/apply_phase6_7_remediation_schema.sql"
                )

        except sqlite3.OperationalError as e:
            # sync_jobs table doesn't exist
            raise RuntimeError(
                f"Worker version {self.WORKER_VERSION} cannot start (FAIL-CLOSED): "
                f"sync_jobs table not found. Database schema is pre-Phase 6. "
                f"REQUIRED ACTION: Deploy all schema migrations before starting workers."
            )

        # ====================================================================
        # STEP 2: Load Schema Compatibility Token (C-ORG-2 Sentinel Strategy)
        # ====================================================================
        try:
            token_row = cursor.execute("""
                SELECT value FROM deployment_metadata
                WHERE key = 'schema_compatibility_token'
            """).fetchone()

            if not token_row:
                raise RuntimeError(
                    f"Worker version {self.WORKER_VERSION} cannot start (FAIL-CLOSED): "
                    f"Schema compatibility token not found in deployment_metadata. "
                    f"REQUIRED ACTION: Run schema migration to populate token:\n"
                    f"  sqlite3 data/charles.db < scripts/apply_phase6_7_remediation_schema.sql"
                )

            # Store token in worker instance (will be passed in claim query)
            self.schema_compatibility_token = token_row['value']
            # Note: Logging deferred to run() since logger/worker_id not yet initialized

        except sqlite3.OperationalError as e:
            # deployment_metadata table doesn't exist
            raise RuntimeError(
                f"Worker version {self.WORKER_VERSION} cannot start (FAIL-CLOSED): "
                f"deployment_metadata table not found. "
                f"REQUIRED ACTION: Run schema migration to create table and populate token:\n"
                f"  sqlite3 data/charles.db < scripts/apply_phase6_7_remediation_schema.sql"
            )

        # ====================================================================
        # STEP 3: Verify Schema Version Table (Optional, Informational)
        # ====================================================================
        try:
            result = cursor.execute("""
                SELECT version FROM schema_version
                WHERE version >= ?
                ORDER BY applied_at DESC
                LIMIT 1
            """, [self.REQUIRED_SCHEMA_VERSION]).fetchone()

            if result:
                # Schema version validated - logging deferred to run()
                pass
            else:
                # Columns exist but schema_version not updated - allow but note for later logging
                pass
        except sqlite3.OperationalError:
            # schema_version table doesn't exist - rely on column check only
            pass
