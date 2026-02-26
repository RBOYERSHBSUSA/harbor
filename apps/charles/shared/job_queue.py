"""
Phase 6: Job Queue Manager

Purpose: Database-backed job queue for async sync execution
Features:
  - Persistent job storage (crash-recoverable)
  - Round-robin fair scheduling (priority-based, no shared state)
  - Atomic job claiming (database transactions)
  - Retry scheduling with configurable delays
  - Workspace isolation enforcement

Contracts Preserved:
  - Phase 3: Sync lifecycle (no changes)
  - Phase 4: Workspace orchestration (gates still enforced)
  - Phase 5: Multitenancy (workspace_id required)

Author: Claude Code
Date: 2025-12-30
"""

import json
import sqlite3
import secrets
import time  # Phase 6.6: For recovery timestamp metrics
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

from charles.shared.structured_logging import get_logger, EventType
from charles.shared.metrics import metrics  # Phase 6.6: Observability
from charles.shared.phase6_7_invariants import (  # Phase 6.8: Runtime invariant assertions
    assert_workspace_not_running_has_no_active_jobs,
    assert_no_orphaned_running_workspaces
)


# ============================================================================
# Job States
# ============================================================================

class JobStatus(Enum):
    """Job state machine states."""
    QUEUED = 'QUEUED'        # Job created, waiting for worker
    CLAIMED = 'CLAIMED'       # Worker has claimed, preparing to run
    RUNNING = 'RUNNING'       # Sync actively executing
    COMPLETED = 'COMPLETED'   # Terminal: sync finished successfully
    FAILED = 'FAILED'         # Terminal: sync failed
    CANCELLED = 'CANCELLED'   # Terminal: job cancelled
    TIMED_OUT = 'TIMED_OUT'   # Terminal: job exceeded deadline

    @classmethod
    def terminal_states(cls) -> set:
        """Return set of terminal (final) states."""
        return {cls.COMPLETED, cls.FAILED, cls.CANCELLED, cls.TIMED_OUT}

    def is_terminal(self) -> bool:
        """Check if this state is terminal."""
        return self in self.terminal_states()


# ============================================================================
# Job Data Model
# ============================================================================

@dataclass
class SyncJob:
    """Represents a sync job in the queue."""
    job_id: str
    workspace_id: str
    status: JobStatus
    priority: int
    scheduled_at: datetime
    payload: Dict[str, Any]
    retry_count: int
    max_retries: int

    # Optional fields (NULL until populated)
    sync_run_id: Optional[str] = None
    claimed_at: Optional[datetime] = None
    claimed_by: Optional[str] = None
    heartbeat_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    parent_job_id: Optional[str] = None
    failure_reason: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Phase 6.7.1 Remediation: Version enforcement fields
    worker_version: Optional[str] = None  # Which worker version executed this job (audit trail)
    minimum_worker_version: Optional[str] = None  # Minimum worker version required (enforcement)

    @classmethod
    def from_db_row(cls, row: sqlite3.Row) -> 'SyncJob':
        """Create SyncJob from database row."""
        # Helper to safely get column value (handles missing columns in older schemas)
        def safe_get(column_name: str, default=None):
            try:
                return row[column_name]
            except (KeyError, IndexError):
                return default

        return cls(
            job_id=row['job_id'],
            workspace_id=row['workspace_id'],
            status=JobStatus(row['status']),
            priority=row['priority'],
            scheduled_at=datetime.fromisoformat(row['scheduled_at']),
            payload=json.loads(row['payload']),
            retry_count=row['retry_count'],
            max_retries=row['max_retries'],
            sync_run_id=row['sync_run_id'],
            claimed_at=datetime.fromisoformat(row['claimed_at']) if row['claimed_at'] else None,
            claimed_by=row['claimed_by'],
            heartbeat_at=datetime.fromisoformat(row['heartbeat_at']) if row['heartbeat_at'] else None,
            completed_at=datetime.fromisoformat(row['completed_at']) if row['completed_at'] else None,
            parent_job_id=row['parent_job_id'],
            failure_reason=row['failure_reason'],
            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
            updated_at=datetime.fromisoformat(row['updated_at']) if row['updated_at'] else None,
            # Phase 6.7.1: Version fields (safe to None if columns don't exist)
            worker_version=safe_get('worker_version'),
            minimum_worker_version=safe_get('minimum_worker_version')
        )


# ============================================================================
# Job Queue Manager
# ============================================================================

class JobQueue:
    """
    Database-backed job queue for async sync execution.

    Thread-safe via database transactions.
    Crash-recoverable via persistent storage.
    Fair scheduling via priority field (round-robin).

    Responsibilities:
      - Enqueue jobs (with deduplication check)
      - Atomic job claiming (transaction-based)
      - Job state transitions
      - Retry scheduling
      - Workspace isolation enforcement
    """

    # Retry delays (exponential backoff)
    RETRY_DELAYS = [
        timedelta(minutes=5),   # First retry
        timedelta(minutes=15),  # Second retry
        timedelta(minutes=30),  # Third retry
    ]

    # Heartbeat timeout
    HEARTBEAT_TIMEOUT = timedelta(minutes=5)

    def __init__(self, db: sqlite3.Connection):
        """
        Initialize JobQueue.

        Args:
            db: SQLite database connection (must have sync_jobs table)
        """
        self.db = db
        self.db.row_factory = sqlite3.Row  # Enable column access by name

    # ========================================================================
    # Job Enqueue
    # ========================================================================

    def enqueue_sync_job(
        self,
        workspace_id: str,
        payload: Dict[str, Any],
        is_retry: bool = False,
        parent_job_id: Optional[str] = None,
        retry_count: int = 0,
        max_retries: int = 3,
        scheduled_at: Optional[datetime] = None,
        minimum_worker_version: Optional[str] = None
    ) -> str:
        """
        Enqueue a sync job for async execution.

        Args:
            workspace_id: Workspace identifier (multitenancy isolation)
            payload: Job parameters (JSON-serializable dict)
                Example: {
                   "initiation_source": "manual",
                    "days_back": 30
                }
            is_retry: True if this is a retry job
            parent_job_id: Previous job ID if retry
            retry_count: Number of retries so far
            max_retries: Maximum retry attempts
            scheduled_at: When job should run (None = immediate)
            minimum_worker_version: Minimum worker version required to execute (Phase 6.7.1)

        Returns:
            job_id: Unique job identifier

        Raises:
            RuntimeError: If active job already exists for workspace

        Contract:
            - Enforces one active job per workspace (QUEUED/CLAIMED/RUNNING)
            - Assigns priority for round-robin fairness
            - Does NOT create sync_run (created by worker)
            - Does NOT check orchestrator gates (caller's responsibility)

        Phase 6.7 (Remediation 2):
            Atomicity guaranteed by UNIQUE partial index (idx_sync_jobs_active_per_workspace).
            Concurrent enqueue attempts will fail with IntegrityError, converted to RuntimeError.

        Phase 6.7.1 Remediation:
            - C-ORG-2 (Deployment Safety): Sets schema_compatibility_token from deployment_metadata
            - Step 3.3: Sets minimum_worker_version to prevent old workers from executing new jobs
        """
        logger = get_logger(service='job_queue', workspace_id=workspace_id)

        # Generate unique job ID
        job_id = self._generate_job_id()

        # Default to immediate execution
        if scheduled_at is None:
            scheduled_at = datetime.now(timezone.utc).replace(tzinfo=None)  # UTC to match SQLite CURRENT_TIMESTAMP

        cursor = self.db.cursor()

        # Phase 6.7.1 (C-ORG-2): Load schema_compatibility_token from deployment_metadata
        # This token ensures only compatible workers can claim this job
        schema_compatibility_token = None
        try:
            token_row = cursor.execute("""
                SELECT value FROM deployment_metadata
                WHERE key = 'schema_compatibility_token'
            """).fetchone()
            if token_row:
                schema_compatibility_token = token_row['value']
        except sqlite3.OperationalError:
            # deployment_metadata table doesn't exist (pre-6.7.1 schema)
            # Token will be NULL - backwards compatible with old workers
            pass

        # Assign priority for round-robin fairness
        priority = cursor.execute("""
            SELECT COALESCE(MAX(priority), -1) + 1 AS next_priority
            FROM sync_jobs
            WHERE workspace_id = ?
        """, [workspace_id]).fetchone()['next_priority']

        try:
            # Insert job (UNIQUE index enforces atomicity)
            # Phase 6.7.1: Include schema_compatibility_token and minimum_worker_version if columns exist
            try:
                cursor.execute("""
                    INSERT INTO sync_jobs (
                        job_id, workspace_id, status, priority, scheduled_at,
                        payload, retry_count, max_retries, parent_job_id,
                        minimum_worker_version, schema_compatibility_token
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    job_id,
                    workspace_id,
                    JobStatus.QUEUED.value,
                    priority,
                    scheduled_at.strftime('%Y-%m-%d %H:%M:%S'),  # SQLite datetime format
                    json.dumps(payload),
                    retry_count,
                    max_retries,
                    parent_job_id,
                    minimum_worker_version,
                    schema_compatibility_token
                ])
            except sqlite3.OperationalError:
                # schema_compatibility_token or minimum_worker_version columns don't exist (pre-6.7.1 schema)
                # Fall back to old query (backwards compatibility)
                cursor.execute("""
                    INSERT INTO sync_jobs (
                        job_id, workspace_id, status, priority, scheduled_at,
                        payload, retry_count, max_retries, parent_job_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    job_id,
                    workspace_id,
                    JobStatus.QUEUED.value,
                    priority,
                    scheduled_at.strftime('%Y-%m-%d %H:%M:%S'),  # SQLite datetime format
                    json.dumps(payload),
                    retry_count,
                    max_retries,
                    parent_job_id
                ])

            self.db.commit()

            # Phase 6.6: Emit metrics AFTER commit (non-blocking, read-only)
            metrics.increment('charles_jobs_total', tags={'workspace_id': workspace_id, 'status': JobStatus.QUEUED.value})
            self._update_job_current_metrics()

            logger.info(
                EventType.JOB_ENQUEUED,
                f"Job enqueued for workspace {workspace_id}",
                job_id=job_id,
                priority=priority,
                retry_count=retry_count,
                scheduled_at=scheduled_at.isoformat()
            )

        except sqlite3.IntegrityError as e:
            # UNIQUE constraint violation on idx_sync_jobs_active_per_workspace
            # This means another transaction already enqueued a job for this workspace
            self.db.rollback()

            # Fetch the existing job_id for error message
            existing = cursor.execute("""
                SELECT job_id FROM sync_jobs
                WHERE workspace_id = ?
                  AND status IN ('QUEUED', 'CLAIMED', 'RUNNING')
                ORDER BY scheduled_at ASC
                LIMIT 1
            """, [workspace_id]).fetchone()

            existing_job_id = existing['job_id'] if existing else 'unknown'

            logger.warn(
                EventType.JOB_FAILED,
                f"Job enqueue failed: active job already exists for workspace {workspace_id}",
                workspace_id=workspace_id,
                existing_job_id=existing_job_id,
                error=str(e)
            )

            raise RuntimeError(
                f"Job already active for workspace {workspace_id}: {existing_job_id}"
            )

        return job_id

    # ========================================================================
    # Job Claim (Atomic)
    # ========================================================================

    def claim_next_job(
        self,
        worker_id: str,
        per_workspace_limit: int = 1,
        worker_version: Optional[str] = None,
        schema_compatibility_token: Optional[str] = None
    ) -> Optional[SyncJob]:
        """
        Atomically claim next job for execution (round-robin fair scheduling).

        Args:
            worker_id: Worker identifier ("hostname:pid")
            per_workspace_limit: Max concurrent jobs per workspace (default: 1)
            worker_version: Worker version claiming the job (Phase 6.7.1 audit metadata)
            schema_compatibility_token: Deployment fencing token (Phase 6.7.1 C-ORG-2)

        Returns:
            SyncJob if claimed, None if no jobs available

        Scheduling:
            - Selects job with lowest priority (round-robin across workspaces)
            - Respects scheduled_at (skips future jobs)
            - Respects per-workspace concurrency limit
            - Atomic via UPDATE...RETURNING (no double-claim)
            - No shared state = no contention

        Phase 6.7.1 Remediation:
            - C-ORG-2 (Deployment Safety): Filters by schema_compatibility_token
              Old workers (v6.7.0) don't pass token → claim returns 0 rows
            - Step 2.3: Records worker_version on claim for audit trail
        """
        logger = get_logger(service='job_queue', workspace_id='system')

        cursor = self.db.cursor()

        # Claim next job with fairness (see plan §2.3)
        # Phase 6.7.1: C-ORG-2 (Deployment Fencing) - filter by schema_compatibility_token
        # Phase 6.7.1: Set worker_version if columns exist (graceful degradation)
        try:
            result = cursor.execute("""
                WITH candidate_jobs AS (
                    -- Jobs from workspaces under per-workspace limit
                    -- Phase 6.7.1 (C-ORG-2): Filter by schema_compatibility_token
                    SELECT j.job_id, j.workspace_id, j.priority, j.scheduled_at
                    FROM sync_jobs j
                    WHERE j.status = 'QUEUED'
                      AND j.scheduled_at <= CURRENT_TIMESTAMP
                      AND (j.schema_compatibility_token IS NULL OR j.schema_compatibility_token = ?)
                      AND (SELECT COUNT(*) FROM sync_jobs
                           WHERE workspace_id = j.workspace_id
                           AND status = 'RUNNING') < ?
                ),
                next_job AS (
                    -- Order by priority (round-robin), then age
                    SELECT job_id FROM candidate_jobs
                    ORDER BY priority ASC, scheduled_at ASC
                    LIMIT 1
                )
                -- Atomic claim
                UPDATE sync_jobs
                SET status = 'CLAIMED',
                    claimed_at = CURRENT_TIMESTAMP,
                    claimed_by = ?,
                    worker_version = ?,
                    heartbeat_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = (SELECT job_id FROM next_job)
                  AND status = 'QUEUED'  -- Prevent double-claim
                RETURNING *
            """, [schema_compatibility_token, per_workspace_limit, worker_id, worker_version]).fetchone()
        except sqlite3.OperationalError:
            # schema_compatibility_token or worker_version column doesn't exist (pre-6.7.1 schema)
            # Fall back to old query (backwards compatibility)
            result = cursor.execute("""
                WITH candidate_jobs AS (
                    -- Jobs from workspaces under per-workspace limit
                    SELECT j.job_id, j.workspace_id, j.priority, j.scheduled_at
                    FROM sync_jobs j
                    WHERE j.status = 'QUEUED'
                      AND j.scheduled_at <= CURRENT_TIMESTAMP
                      AND (SELECT COUNT(*) FROM sync_jobs
                           WHERE workspace_id = j.workspace_id
                           AND status = 'RUNNING') < ?
                ),
                next_job AS (
                    -- Order by priority (round-robin), then age
                    SELECT job_id FROM candidate_jobs
                    ORDER BY priority ASC, scheduled_at ASC
                    LIMIT 1
                )
                -- Atomic claim (without worker_version and token)
                UPDATE sync_jobs
                SET status = 'CLAIMED',
                    claimed_at = CURRENT_TIMESTAMP,
                    claimed_by = ?,
                    heartbeat_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = (SELECT job_id FROM next_job)
                  AND status = 'QUEUED'  -- Prevent double-claim
                RETURNING *
            """, [per_workspace_limit, worker_id]).fetchone()

        self.db.commit()

        # Phase 6.6: Emit metrics AFTER commit
        if result:
            job = SyncJob.from_db_row(result)

            # Metrics: Job state transition QUEUED → CLAIMED
            metrics.increment('charles_job_state_transitions_total',
                            tags={'from': JobStatus.QUEUED.value, 'to': JobStatus.CLAIMED.value})
            self._update_job_current_metrics()

            # Calculate wait time (scheduled_at to claimed_at)
            wait_time = (job.claimed_at - job.scheduled_at).total_seconds()
            metrics.histogram('charles_job_wait_time_seconds', wait_time,
                            tags={'workspace_id': job.workspace_id})

            # Re-get logger with workspace context
            workspace_logger = get_logger(service='job_queue', workspace_id=job.workspace_id)
            workspace_logger.info(
                EventType.JOB_CLAIMED,
                f"Job claimed by worker {worker_id}",
                job_id=job.job_id,
                worker_id=worker_id,
                priority=job.priority
            )
            return job
        else:
            logger.debug(
                EventType.NO_JOBS_AVAILABLE,
                f"No jobs available for worker {worker_id}",
                worker_id=worker_id
            )
            return None

    # ========================================================================
    # Job State Transitions
    # ========================================================================

    def transition_to_running(self, job_id: str, sync_run_id: str) -> None:
        """
        Transition job from CLAIMED → RUNNING.

        Args:
            job_id: Job identifier
            sync_run_id: Sync run identifier (from SyncRunManager.start_sync())

        Contract:
            - Called after SyncRunManager.start_sync() creates sync_run
            - Links job to sync_run
            - Updates heartbeat
        """
        # Get job to retrieve workspace_id for logger
        job = self.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        logger = get_logger(service='job_queue', workspace_id=job.workspace_id)

        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE sync_jobs
            SET status = 'RUNNING',
                sync_run_id = ?,
                heartbeat_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
              AND status = 'CLAIMED'
        """, [sync_run_id, job_id])

        if cursor.rowcount == 0:
            raise RuntimeError(f"Job {job_id} not in CLAIMED state")

        self.db.commit()

        # Phase 6.6: Emit metrics AFTER commit
        metrics.increment('charles_job_state_transitions_total',
                        tags={'from': JobStatus.CLAIMED.value, 'to': JobStatus.RUNNING.value})
        self._update_job_current_metrics()

        # CRITICAL: Check concurrency invariant (≤1 RUNNING per workspace)
        self._check_concurrency_invariant(job.workspace_id)

        logger.info(
            EventType.JOB_RUNNING,
            f"Job transitioned to RUNNING",
            job_id=job_id,
            sync_run_id=sync_run_id
        )

    def mark_completed(self, job_id: str) -> None:
        """
        Mark job as COMPLETED (terminal state).

        Args:
            job_id: Job identifier

        Contract:
            - Called after sync reaches terminal state
            - Records completion timestamp
            - Job cannot transition out of COMPLETED
        """
        # Get job to retrieve workspace_id for logger
        job = self.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        logger = get_logger(service='job_queue', workspace_id=job.workspace_id)

        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE sync_jobs
            SET status = 'COMPLETED',
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
              AND status = 'RUNNING'
        """, [job_id])

        if cursor.rowcount == 0:
            raise RuntimeError(f"Job {job_id} not in RUNNING state")

        self.db.commit()

        # Phase 6.6: Emit metrics AFTER commit
        metrics.increment('charles_job_state_transitions_total',
                        tags={'from': JobStatus.RUNNING.value, 'to': JobStatus.COMPLETED.value})
        metrics.increment('charles_jobs_total',
                        tags={'workspace_id': job.workspace_id, 'status': JobStatus.COMPLETED.value})
        self._update_job_current_metrics()

        # Calculate job duration (claimed_at to completed_at)
        job_after = self.get_job(job_id)
        if job_after and job_after.claimed_at and job_after.completed_at:
            duration = (job_after.completed_at - job_after.claimed_at).total_seconds()
            metrics.histogram('charles_job_duration_seconds', duration,
                            tags={'workspace_id': job.workspace_id, 'status': JobStatus.COMPLETED.value})

        logger.info(
            EventType.JOB_COMPLETED,
            f"Job completed successfully",
            job_id=job_id
        )

        # Phase 6.8: Invariant assertion - workspace consistency check
        # After job completion, workspace should transition away from 'running'
        # This is verified by reconcile_workspace_after_async_job in sync_worker.py
        # Here we just verify no orphaned state remains
        assert_workspace_not_running_has_no_active_jobs(
            self.db,
            job.workspace_id,
            context=f"post_mark_completed_{job_id}"
        )

    def mark_failed(self, job_id: str, failure_reason: str) -> None:
        """
        Mark job as FAILED (terminal state).

        Args:
            job_id: Job identifier
            failure_reason: Error message or classification

        Contract:
            - Called after sync fails
            - Records failure reason
            - Triggers retry scheduling (see schedule_retry)
        """
        # Get job to retrieve workspace_id for logger
        job = self.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        logger = get_logger(service='job_queue', workspace_id=job.workspace_id)

        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE sync_jobs
            SET status = 'FAILED',
                failure_reason = ?,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
              AND status IN ('CLAIMED', 'RUNNING')
        """, [failure_reason, job_id])

        if cursor.rowcount == 0:
            raise RuntimeError(f"Job {job_id} not in CLAIMED or RUNNING state")

        self.db.commit()

        # Phase 6.6: Emit metrics AFTER commit
        prev_status = job.status.value  # Original status (CLAIMED or RUNNING)
        metrics.increment('charles_job_state_transitions_total',
                        tags={'from': prev_status, 'to': JobStatus.FAILED.value})
        metrics.increment('charles_jobs_total',
                        tags={'workspace_id': job.workspace_id, 'status': JobStatus.FAILED.value})
        self._update_job_current_metrics()

        logger.warn(
            EventType.JOB_FAILED,
            f"Job failed: {failure_reason}",
            job_id=job_id,
            failure_reason=failure_reason
        )

        # Phase 6.8: Invariant assertion - workspace consistency check
        # After job failure, workspace should transition away from 'running'
        # This is verified by reconcile_workspace_after_async_job in sync_worker.py
        # Here we just verify no orphaned state remains
        assert_workspace_not_running_has_no_active_jobs(
            self.db,
            job.workspace_id,
            context=f"post_mark_failed_{job_id}"
        )

    # ========================================================================
    # Heartbeat
    # ========================================================================

    def update_heartbeat(self, job_id: str, worker_id: str) -> None:
        """
        Update job heartbeat (worker still alive).

        Args:
            job_id: Job identifier
            worker_id: Worker identifier (verify ownership)

        Contract:
            - Called every 30 seconds by worker
            - Prevents crash recovery from re-queuing job
            - Verifies worker ownership (claimed_by = worker_id)
        """
        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE sync_jobs
            SET heartbeat_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
              AND claimed_by = ?
              AND status IN ('CLAIMED', 'RUNNING')
        """, [job_id, worker_id])

        if cursor.rowcount == 0:
            # Worker no longer owns job (crash recovery claimed it)
            raise RuntimeError(f"Worker {worker_id} no longer owns job {job_id}")

        self.db.commit()

    # ========================================================================
    # Retry Scheduling
    # ========================================================================

    def schedule_retry(self, job_id: str) -> Optional[str]:
        """
        Schedule retry for failed job.

        Args:
            job_id: Failed job identifier

        Returns:
            new_job_id if retry scheduled, None if max retries exceeded

        Contract:
            - Creates new job with incremented retry_count
            - Sets scheduled_at = now + RETRY_DELAYS[retry_count]
            - Links to parent via parent_job_id
            - Bypasses cooldown (is_retry=True flag in enqueue_sync_job)
        """
        # Get failed job details - will use job.workspace_id for logger below
        cursor = self.db.cursor()
        job_row = cursor.execute("""
            SELECT * FROM sync_jobs WHERE job_id = ?
        """, [job_id]).fetchone()

        if not job_row:
            raise ValueError(f"Job {job_id} not found")

        job = SyncJob.from_db_row(job_row)
        logger = get_logger(service='job_queue', workspace_id=job.workspace_id)

        # Check if can retry
        if job.retry_count >= job.max_retries:
            logger.warn(
                EventType.JOB_RETRY_EXHAUSTED,
                f"Retry exhausted for job {job_id}",
                job_id=job_id,
                retry_count=job.retry_count,
                max_retries=job.max_retries
            )
            return None

        # Calculate retry delay
        retry_delay = self.RETRY_DELAYS[job.retry_count]
        scheduled_at = datetime.now(timezone.utc).replace(tzinfo=None) + retry_delay  # UTC to match SQLite CURRENT_TIMESTAMP

        # Enqueue retry job
        new_job_id = self.enqueue_sync_job(
            workspace_id=job.workspace_id,
            payload=job.payload,
            is_retry=True,
            parent_job_id=job_id,
            retry_count=job.retry_count + 1,
            max_retries=job.max_retries,
            scheduled_at=scheduled_at
        )

        logger.info(
            EventType.JOB_RETRY_SCHEDULED,
            f"Retry scheduled for job {job_id}",
            original_job_id=job_id,
            new_job_id=new_job_id,
            retry_count=job.retry_count + 1,
            scheduled_at=scheduled_at.isoformat()
        )

        return new_job_id

    # ========================================================================
    # Crash Recovery
    # ========================================================================

    def recover_all(self) -> Dict[str, int]:
        """
        Two-phase recovery: job-driven + table-driven reconciliation.

        PHASE 1: Recover stale jobs (heartbeat-based detection)
        PHASE 2: Reconcile orphaned resources (cross-table queries)

        Phase 6.5 Revised Contract - Items 1-25

        Returns:
            Dictionary with recovery stats:
                {
                    'jobs_recovered': count of jobs re-queued,
                    'jobs_failed': count of jobs marked FAILED,
                    'locks_reconciled': count of orphaned locks deleted,
                    'workspaces_reconciled': count of orphaned workspace states reset,
                    'syncs_reconciled': count of orphaned sync_runs marked failed
                }

        Contract:
            - Called periodically by workers
            - Safe to call concurrently (idempotent)
            - Atomic recovery per resource (one transaction per resource)
        """
        logger = get_logger(service='job_queue', workspace_id='system')

        cursor = self.db.cursor()

        # Find stale jobs (heartbeat timeout)
        # Checklist Item 1: Stale job detection
        timeout_threshold = datetime.now(timezone.utc).replace(tzinfo=None) - self.HEARTBEAT_TIMEOUT

        stale_jobs = cursor.execute("""
            SELECT job_id, workspace_id, sync_run_id, retry_count, max_retries, claimed_by
            FROM sync_jobs
            WHERE status IN (?, ?)
              AND heartbeat_at < ?
        """, [
            JobStatus.CLAIMED.value,
            JobStatus.RUNNING.value,
            timeout_threshold.strftime('%Y-%m-%d %H:%M:%S')
        ]).fetchall()

        recovered_count = 0
        failed_count = 0

        for job_row in stale_jobs:
            job_id = job_row['job_id']
            workspace_id = job_row['workspace_id']
            sync_run_id = job_row['sync_run_id']
            retry_count = job_row['retry_count']
            max_retries = job_row['max_retries']
            claimed_by = job_row['claimed_by']

            if retry_count < max_retries:
                # Checklist Item 2: Job state transition correctness (CLAIMED/RUNNING → QUEUED)
                cursor.execute("""
                    UPDATE sync_jobs
                    SET status = ?,
                        claimed_at = NULL,
                        claimed_by = NULL,
                        heartbeat_at = NULL,
                        retry_count = retry_count + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = ?
                      AND status IN (?, ?)
                """, [
                    JobStatus.QUEUED.value,
                    job_id,
                    JobStatus.CLAIMED.value,
                    JobStatus.RUNNING.value
                ])

                if cursor.rowcount > 0:
                    # Checklist Item 17: All four tables updated atomically
                    # Phase 6.7.1 (C-ORG-1): ALWAYS reset workspace state, even if sync_run_id is NULL
                    # This prevents workspaces from being stuck in 'running' state forever

                    # Lock cleanup (only if sync_run_id exists)
                    if sync_run_id:
                        # Checklist Item 7: Lock cleanup on recovery
                        cursor.execute("""
                            DELETE FROM company_sync_locks
                            WHERE sync_run_id = ?
                        """, [sync_run_id])

                    # Checklist Items 4, 5, 6: Workspace state cleanup
                    # Phase 6.7.1 (C-ORG-1 FIX): Reset workspace UNCONDITIONALLY
                    # Previously: Only reset if sync_run_id IS NOT NULL
                    # Now: ALWAYS reset, regardless of sync_run_id
                    # Item 4: Workspace state consistency
                    # Item 5: Active sync run ID cleared
                    # Item 6: Workspace state transition to ready
                    try:
                        cursor.execute("""
                            UPDATE workspace_sync_state
                            SET state = 'ready',
                                active_sync_run_id = NULL,
                                last_sync_completed_at = CURRENT_TIMESTAMP
                            WHERE workspace_id = ?
                        """, [workspace_id])  # REMOVED: AND active_sync_run_id = ? condition
                    except sqlite3.OperationalError:
                        # workspace_sync_state table doesn't exist (pre-Phase 4 deployment)
                        # Recovery still succeeds for job state; workspace state will be
                        # reconciled when Phase 4 orchestrator is deployed
                        pass

                    # Mark sync_run as failed (only if sync_run_id exists)
                    if sync_run_id:
                        cursor.execute("""
                            UPDATE sync_runs
                            SET state = 'failed',
                                failure_reason = 'worker_crash_recovery',
                                completed_at = CURRENT_TIMESTAMP
                            WHERE sync_run_id = ?
                              AND state NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
                        """, [sync_run_id])

                    # Checklist Item 16: Single job per transaction
                    self.db.commit()

                    recovered_count += 1
                    logger.warn(
                        EventType.JOB_RECOVERED,
                        f"Job {job_id} recovered after worker crash (workspace reset unconditionally)",
                        job_id=job_id,
                        workspace_id=workspace_id,
                        sync_run_id=sync_run_id,
                        retry_count=retry_count + 1,
                        crashed_worker=claimed_by
                    )
            else:
                # Checklist Item 3: Retry count enforcement (mark FAILED when exhausted)
                cursor.execute("""
                    UPDATE sync_jobs
                    SET status = ?,
                        failure_reason = ?,
                        completed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = ?
                      AND status IN (?, ?)
                """, [
                    JobStatus.FAILED.value,
                    f"Exceeded max retries ({max_retries}) after worker crashes",
                    job_id,
                    JobStatus.CLAIMED.value,
                    JobStatus.RUNNING.value
                ])

                if cursor.rowcount > 0:
                    # Checklist Item 17: All four tables updated atomically
                    # Phase 6.7.1 (C-ORG-1): ALWAYS reset workspace state, even if sync_run_id is NULL

                    # Lock cleanup (only if sync_run_id exists)
                    if sync_run_id:
                        # Checklist Item 7: Lock cleanup on recovery
                        cursor.execute("""
                            DELETE FROM company_sync_locks
                            WHERE sync_run_id = ?
                        """, [sync_run_id])

                    # Checklist Items 4, 5, 6: Workspace state cleanup
                    # Phase 6.7.1 (C-ORG-1 FIX): Reset workspace UNCONDITIONALLY
                    try:
                        cursor.execute("""
                            UPDATE workspace_sync_state
                            SET state = 'gated',
                                gate_reason = 'Job failed after max retries (worker crashes)',
                                gate_set_at = CURRENT_TIMESTAMP,
                                active_sync_run_id = NULL,
                                last_sync_completed_at = CURRENT_TIMESTAMP
                            WHERE workspace_id = ?
                        """, [workspace_id])  # REMOVED: AND active_sync_run_id = ? condition
                    except sqlite3.OperationalError:
                        # workspace_sync_state table doesn't exist (pre-Phase 4 deployment)
                        pass

                    # Mark sync_run as failed (only if sync_run_id exists)
                    if sync_run_id:
                        cursor.execute("""
                            UPDATE sync_runs
                            SET state = 'failed',
                                failure_reason = 'worker_crash_recovery',
                                completed_at = CURRENT_TIMESTAMP
                            WHERE sync_run_id = ?
                              AND state NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
                        """, [sync_run_id])

                    # Checklist Item 16: Single job per transaction
                    self.db.commit()

                    failed_count += 1
                    logger.error(
                        EventType.JOB_RETRY_EXHAUSTED,
                        f"Job {job_id} failed: exceeded max retries after crashes (workspace gated)",
                        job_id=job_id,
                        workspace_id=workspace_id,
                        sync_run_id=sync_run_id,
                        retry_count=retry_count,
                        max_retries=max_retries,
                        crashed_worker=claimed_by
                    )

        # ====================================================================
        # PHASE 2: TABLE-DRIVEN RECONCILIATION (Items 19-25)
        # ====================================================================

        locks_reconciled = 0
        workspaces_reconciled = 0
        syncs_reconciled = 0

        # Phase 2A: Orphaned Lock Reconciliation (Items 19-21)
        # Checklist Item 19: Orphaned locks discovered by cross-table query
        try:
            orphaned_locks = cursor.execute("""
                SELECT L.sync_run_id, L.workspace_id
                FROM company_sync_locks L
                WHERE NOT EXISTS (
                    SELECT 1 FROM sync_jobs J
                    WHERE J.sync_run_id = L.sync_run_id
                    AND J.status = ?
                )
            """, [JobStatus.RUNNING.value]).fetchall()
        except Exception:
            # Table doesn't exist yet (pre-Phase 6.5 deployment), skip reconciliation
            orphaned_locks = []

        for lock_row in orphaned_locks:
            sync_run_id = lock_row['sync_run_id']
            workspace_id = lock_row['workspace_id']

            # Checklist Item 20: Orphaned locks deleted within recovery cycle
            cursor.execute("""
                DELETE FROM company_sync_locks
                WHERE sync_run_id = ?
            """, [sync_run_id])

            # Checklist Item 21: Workspace state reset when orphaned lock cleaned up
            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = 'ready',
                    active_sync_run_id = NULL,
                    last_sync_completed_at = CURRENT_TIMESTAMP
                WHERE workspace_id = ?
                  AND active_sync_run_id = ?
            """, [workspace_id, sync_run_id])

            # Mark orphaned sync_run as failed
            cursor.execute("""
                UPDATE sync_runs
                SET state = 'failed',
                    failure_reason = 'orphaned_sync_run_reconciliation',
                    completed_at = CURRENT_TIMESTAMP
                WHERE sync_run_id = ?
                  AND state NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
            """, [sync_run_id])

            # Atomic per orphan
            self.db.commit()

            locks_reconciled += 1
            logger.warn(
                EventType.ORPHANED_LOCK_RECONCILED,
                f"Orphaned lock reconciled: sync_run_id={sync_run_id}",
                sync_run_id=sync_run_id,
                workspace_id=workspace_id
            )

        # Phase 2B: Orphaned Workspace State Reconciliation (Items 22-23)
        # Checklist Item 22: Orphaned workspace states discovered by cross-table query
        try:
            orphaned_workspaces = cursor.execute("""
                SELECT WS.workspace_id, WS.active_sync_run_id
                FROM workspace_sync_state WS
                WHERE WS.state = 'running'
                  AND WS.active_sync_run_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM sync_jobs J
                      WHERE J.sync_run_id = WS.active_sync_run_id
                      AND J.status = ?
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM company_sync_locks L
                      WHERE L.sync_run_id = WS.active_sync_run_id
                  )
            """, [JobStatus.RUNNING.value]).fetchall()
        except Exception:
            # Table doesn't exist yet (pre-Phase 6.5 deployment), skip reconciliation
            orphaned_workspaces = []

        for ws_row in orphaned_workspaces:
            workspace_id = ws_row['workspace_id']
            sync_run_id = ws_row['active_sync_run_id']

            # Checklist Item 23: Orphaned workspace states reset to 'ready'
            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = 'ready',
                    active_sync_run_id = NULL,
                    last_sync_completed_at = CURRENT_TIMESTAMP
                WHERE workspace_id = ?
                  AND state = 'running'
                  AND active_sync_run_id = ?
            """, [workspace_id, sync_run_id])

            # Mark sync_run failed
            cursor.execute("""
                UPDATE sync_runs
                SET state = 'failed',
                    failure_reason = 'orphaned_workspace_state_reconciliation',
                    completed_at = CURRENT_TIMESTAMP
                WHERE sync_run_id = ?
                  AND state NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
            """, [sync_run_id])

            # Atomic per orphan
            self.db.commit()

            workspaces_reconciled += 1
            logger.warn(
                EventType.ORPHANED_WORKSPACE_RECONCILED,
                f"Orphaned workspace state reconciled: workspace_id={workspace_id}",
                workspace_id=workspace_id,
                sync_run_id=sync_run_id
            )

        # Phase 2C: Orphaned Sync Run Reconciliation (Items 24-25)
        # Checklist Item 24: Orphaned sync_runs discovered by cross-table query
        try:
            orphaned_syncs = cursor.execute("""
                SELECT S.sync_run_id, S.workspace_id
                FROM sync_runs S
                WHERE S.state NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
                  AND NOT EXISTS (
                      SELECT 1 FROM sync_jobs J
                      WHERE J.sync_run_id = S.sync_run_id
                      AND J.status = ?
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM company_sync_locks L
                      WHERE L.sync_run_id = S.sync_run_id
                  )
            """, [JobStatus.RUNNING.value]).fetchall()
        except Exception:
            # Table doesn't exist yet (pre-Phase 6.5 deployment), skip reconciliation
            orphaned_syncs = []

        for sync_row in orphaned_syncs:
            sync_run_id = sync_row['sync_run_id']
            workspace_id = sync_row['workspace_id']

            # Checklist Item 25: Orphaned sync_runs marked 'failed'
            cursor.execute("""
                UPDATE sync_runs
                SET state = 'failed',
                    failure_reason = 'orphaned_sync_run_no_active_job',
                    completed_at = CURRENT_TIMESTAMP
                WHERE sync_run_id = ?
                  AND state NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
            """, [sync_run_id])

            # Ensure workspace state is consistent
            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = 'ready',
                    active_sync_run_id = NULL,
                    last_sync_completed_at = CURRENT_TIMESTAMP
                WHERE workspace_id = ?
                  AND active_sync_run_id = ?
            """, [workspace_id, sync_run_id])

            # Atomic per orphan
            self.db.commit()

            syncs_reconciled += 1
            logger.warn(
                EventType.ORPHANED_SYNC_RUN_RECONCILED,
                f"Orphaned sync_run reconciled: sync_run_id={sync_run_id}",
                sync_run_id=sync_run_id,
                workspace_id=workspace_id
            )

        # ====================================================================
        # Phase 2D: Orphaned 'running' Workspace Cleanup (Phase 6.7.1 - C-ORG-1 Enforcement)
        # ====================================================================
        # Detect workspaces in 'running' state with NO active job
        # This catches cases where:
        # - Worker crashed after workspace.transition_to_running() but before job.transition_to_running()
        # - Job was recovered but workspace reset failed (should not happen after C-ORG-1 fix)
        # - Manual database manipulation left workspace in inconsistent state
        #
        # Enforces INV-WS-1: Every workspace in 'running' state MUST have exactly one active job
        # Enforces INV-WS-2: If no active job exists, workspace MUST be 'ready'/'cooldown'/'gated', NOT 'running'

        try:
            orphaned_running_workspaces = cursor.execute("""
                SELECT WS.workspace_id, WS.active_sync_run_id
                FROM workspace_sync_state WS
                WHERE WS.state = 'running'
                  AND NOT EXISTS (
                      SELECT 1 FROM sync_jobs J
                      WHERE J.workspace_id = WS.workspace_id
                      AND J.status IN (?, ?, ?)
                  )
            """, [JobStatus.QUEUED.value, JobStatus.CLAIMED.value, JobStatus.RUNNING.value]).fetchall()
        except Exception:
            # Table doesn't exist yet (pre-Phase 6.5 deployment), skip reconciliation
            orphaned_running_workspaces = []

        for ws_row in orphaned_running_workspaces:
            workspace_id = ws_row['workspace_id']
            active_sync_run_id = ws_row['active_sync_run_id']

            # Reset workspace to 'ready' (no active job exists)
            cursor.execute("""
                UPDATE workspace_sync_state
                SET state = 'ready',
                    active_sync_run_id = NULL,
                    last_sync_completed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE workspace_id = ?
                  AND state = 'running'
            """, [workspace_id])

            # Mark associated sync_run as failed (if exists)
            if active_sync_run_id:
                cursor.execute("""
                    UPDATE sync_runs
                    SET state = 'failed',
                        failure_reason = 'orphaned_running_workspace_no_active_job',
                        completed_at = CURRENT_TIMESTAMP
                    WHERE sync_run_id = ?
                      AND state NOT IN ('completed', 'failed', 'cancelled', 'timed_out')
                """, [active_sync_run_id])

            # Atomic per orphan
            self.db.commit()

            workspaces_reconciled += 1
            logger.warn(
                EventType.ORPHANED_WORKSPACE_RECONCILED,
                f"Orphaned 'running' workspace reconciled (no active job): workspace_id={workspace_id}",
                workspace_id=workspace_id,
                active_sync_run_id=active_sync_run_id
            )

        # ====================================================================
        # RECOVERY COMPLETE
        # ====================================================================

        logger.info(
            EventType.CRASH_RECOVERY_COMPLETED,
            f"Recovery: {recovered_count} jobs recovered, {failed_count} jobs failed, "
            f"{locks_reconciled} locks reconciled, {workspaces_reconciled} workspaces reconciled, "
            f"{syncs_reconciled} syncs reconciled",
            jobs_recovered=recovered_count,
            jobs_failed=failed_count,
            locks_reconciled=locks_reconciled,
            workspaces_reconciled=workspaces_reconciled,
            syncs_reconciled=syncs_reconciled
        )

        # Phase 6.6: Emit recovery metrics AFTER all phases complete
        metrics.increment('charles_recovery_cycles_total')
        metrics.gauge('charles_recovery_last_run_timestamp', time.time())

        # Recovery convergence metrics (prove L4, R1, R2, R3)
        if recovered_count > 0:
            metrics.increment('charles_recovery_jobs_recovered_total', value=recovered_count)
        if failed_count > 0:
            metrics.increment('charles_recovery_jobs_failed_total', value=failed_count)
        if locks_reconciled > 0:
            metrics.increment('charles_recovery_locks_reconciled_total', value=locks_reconciled)
        if workspaces_reconciled > 0:
            metrics.increment('charles_recovery_workspaces_reconciled_total', value=workspaces_reconciled)
        if syncs_reconciled > 0:
            metrics.increment('charles_recovery_syncs_reconciled_total', value=syncs_reconciled)

        # ====================================================================
        # Phase 6.8: Invariant Assertions (Post-Recovery Verification)
        # ====================================================================
        # Verify recovery convergence - NO orphaned running workspaces should remain
        assert_no_orphaned_running_workspaces(self.db, context="post_recovery_verification")

        return {
            'jobs_recovered': recovered_count,
            'jobs_failed': failed_count,
            'locks_reconciled': locks_reconciled,
            'workspaces_reconciled': workspaces_reconciled,
            'syncs_reconciled': syncs_reconciled
        }

    # ========================================================================
    # Helpers
    # ========================================================================

    @staticmethod
    def _generate_job_id() -> str:
        """Generate unique job ID."""
        timestamp = int(datetime.now().timestamp() * 1000)  # milliseconds
        random = secrets.token_hex(6)  # 12 hex chars
        return f"job_{timestamp}_{random}"

    def get_job(self, job_id: str) -> Optional[SyncJob]:
        """Get job by ID."""
        cursor = self.db.cursor()
        row = cursor.execute("""
            SELECT * FROM sync_jobs WHERE job_id = ?
        """, [job_id]).fetchone()

        return SyncJob.from_db_row(row) if row else None

    # ========================================================================
    # Phase 6.6: Metrics Helpers (Read-Only, Non-Blocking)
    # ========================================================================

    def _update_job_current_metrics(self):
        """
        Update charles_jobs_current gauge (jobs by status).

        Phase 6.6: Read-only query AFTER commit.
        Non-blocking, failure-tolerant (never raises).
        """
        try:
            cursor = self.db.cursor()

            # Query current counts by status
            results = cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM sync_jobs
                GROUP BY status
            """).fetchall()

            # Update gauge for each status
            for row in results:
                metrics.gauge('charles_jobs_current', row['count'],
                            tags={'status': row['status']})

            # Also update per-workspace RUNNING count (critical for concurrency invariant)
            per_workspace = cursor.execute("""
                SELECT workspace_id, COUNT(*) as count
                FROM sync_jobs
                WHERE status = ?
                GROUP BY workspace_id
            """, [JobStatus.RUNNING.value]).fetchall()

            for row in per_workspace:
                metrics.gauge('charles_jobs_per_workspace_running', row['count'],
                            tags={'workspace_id': row['workspace_id']})

        except Exception:
            # Never raise - metrics failures don't impact execution
            pass

    def _check_concurrency_invariant(self, workspace_id: str):
        """
        Check concurrency invariant: ≤1 RUNNING job per workspace.

        Phase 6.6: Diagnostic only - logs violation but does NOT change behavior.
        This is read-only observability, not enforcement.

        Args:
            workspace_id: Workspace to check
        """
        try:
            cursor = self.db.cursor()

            # Count RUNNING jobs for this workspace
            count = cursor.execute("""
                SELECT COUNT(*) as count
                FROM sync_jobs
                WHERE workspace_id = ?
                  AND status = ?
            """, [workspace_id, JobStatus.RUNNING.value]).fetchone()['count']

            if count > 1:
                # CRITICAL: Concurrency invariant violated
                # Log for observability (does NOT change execution)
                logger = get_logger(service='job_queue', workspace_id=workspace_id)
                logger.error(
                    EventType.CONCURRENCY_VIOLATION_DETECTED,
                    f"INVARIANT VIOLATION: {count} RUNNING jobs for workspace {workspace_id}",
                    workspace_id=workspace_id,
                    running_count=count
                )

                # Metric to detect violation
                metrics.increment('charles_concurrency_violations_total',
                                tags={'workspace_id': workspace_id})

        except Exception:
            # Never raise - diagnostic check failures don't impact execution
            pass
