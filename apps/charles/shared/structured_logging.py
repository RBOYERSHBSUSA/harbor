"""
Structured Logging Module for Charles

Implements MINIMUM_LOGGING_CONTRACT v1.1 (Phase 5 Update)

All log entries are structured JSON with required fields:
- log_id: Unique identifier for this log entry
- timestamp: ISO 8601 UTC timestamp
- environment: dev / prod
- severity: DEBUG, INFO, WARN, ERROR, FATAL
- event_type: Stable event type identifier
- service: Component or module name
- workspace_id: Workspace identifier (REQUIRED per Multitenancy Contract v1.0)
- company_id: Internal company identifier (maintained for backward compatibility)
- message: Human-readable description

Additional fields are included where applicable per the contract.
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Union, Tuple
from enum import Enum


class Severity(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


class EventType(Enum):
    # Sync Lifecycle Events (§4.1)
    SYNC_STARTED = "sync_started"
    SYNC_DISCOVERY_COMPLETED = "sync_discovery_completed"
    SYNC_MATCHING_COMPLETED = "sync_matching_completed"
    SYNC_EXECUTION_STARTED = "sync_execution_started"
    SYNC_COMPLETED = "sync_completed"
    SYNC_PARTIALLY_COMPLETED = "sync_partially_completed"
    SYNC_COMPLETED_WITH_AMBIGUITY = "sync_completed_with_ambiguity"
    SYNC_COMPLETED_WITH_WARNINGS = "sync_completed_with_warnings"
    SYNC_FAILED = "sync_failed"
    SYNC_CANCELLED = "sync_cancelled"
    SYNC_TIMED_OUT = "sync_timed_out"

    # Payout-Level Events (§4.2)
    PAYOUT_PROCESSING_STARTED = "payout_processing_started"
    PAYOUT_PROCESSING_COMPLETED = "payout_processing_completed"
    PAYOUT_PROCESSING_FAILED = "payout_processing_failed"
    PAYOUT_PROCESSING_AMBIGUOUS = "payout_processing_ambiguous"

    # Matching Events (§4.3)
    PAYMENT_MATCH_ATTEMPTED = "payment_match_attempted"
    PAYMENT_MATCH_CONFIRMED = "payment_match_confirmed"
    PAYMENT_MATCH_AMBIGUOUS = "payment_match_ambiguous"
    PAYMENT_MATCH_FAILED = "payment_match_failed"

    # Enhanced Matching Events (§4.3.1) - Multi-factor confidence scoring
    PAYMENT_MATCH_HIGH_CONFIDENCE = "payment_match_high_confidence"
    PAYMENT_MATCH_LOW_CONFIDENCE = "payment_match_low_confidence"
    PAYMENT_MATCH_REVIEW_CREATED = "payment_match_review_created"
    PAYMENT_MATCH_MANUAL_RESOLVED = "payment_match_manual_resolved"
    PAYMENT_MATCH_NO_CANDIDATES = "payment_match_no_candidates"

    # Accounting Object Events (§4.4)
    QBO_OBJECT_CREATION_ATTEMPTED = "qbo_object_creation_attempted"
    QBO_OBJECT_CREATED = "qbo_object_created"
    QBO_OBJECT_CREATION_SKIPPED = "qbo_object_creation_skipped"
    QBO_OBJECT_CREATION_FAILED = "qbo_object_creation_failed"

    # OAuth Events (§4.5)
    OAUTH_ACCESS_TOKEN_EXPIRED = "oauth_access_token_expired"
    OAUTH_REFRESH_ATTEMPTED = "oauth_refresh_attempted"
    OAUTH_REFRESH_SUCCEEDED = "oauth_refresh_succeeded"
    OAUTH_REFRESH_FAILED = "oauth_refresh_failed"
    OAUTH_AUTHORIZATION_INVALID = "oauth_authorization_invalid"

    # Error, Ambiguity, Warning Events (§4.6)
    ERROR_DETECTED = "error_detected"
    AMBIGUITY_DETECTED = "ambiguity_detected"
    WARNING_DETECTED = "warning_detected"

    # Retry Events (§4.7)
    RETRY_SCHEDULED = "retry_scheduled"
    RETRY_ATTEMPTED = "retry_attempted"
    RETRY_SUCCEEDED = "retry_succeeded"
    RETRY_EXHAUSTED = "retry_exhausted"

    # Idempotency Events
    IDEMPOTENCY_CHECK = "idempotency_check"
    IDEMPOTENCY_HIT = "idempotency_hit"

    # API Key Events (Architectural Exception - see docs/API_KEY_EXCEPTION.md)
    API_KEY_AUTHENTICATED = "api_key_authenticated"
    API_KEY_REJECTED = "api_key_rejected"
    API_KEY_CREATED = "api_key_created"
    API_KEY_REVOKED = "api_key_revoked"

    # Authentication Events (Phase 7.1 - User Authentication)
    USER_LOGIN_ATTEMPTED = "user_login_attempted"
    USER_LOGIN_SUCCEEDED = "user_login_succeeded"
    USER_LOGIN_FAILED = "user_login_failed"
    USER_LOGOUT = "user_logout"
    USER_SIGNUP_ATTEMPTED = "user_signup_attempted"
    USER_SIGNUP_SUCCEEDED = "user_signup_succeeded"
    USER_SIGNUP_FAILED = "user_signup_failed"
    PASSWORD_CHANGE_ATTEMPTED = "password_change_attempted"
    PASSWORD_CHANGE_SUCCEEDED = "password_change_succeeded"
    PASSWORD_CHANGE_FAILED = "password_change_failed"

    # Cleanup Events (Document 05 - Operational Verification Rules)
    CLEANUP_STARTED = "cleanup_started"
    CLEANUP_COMPLETED = "cleanup_completed"
    CLEANUP_SKIPPED = "cleanup_skipped"
    QBO_OBJECT_DELETED = "qbo_object_deleted"

    # Job Queue Events (Phase 6 - Concurrency & Fairness)
    JOB_ENQUEUED = "job_enqueued"
    JOB_CLAIMED = "job_claimed"
    JOB_RUNNING = "job_running"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    JOB_CANCELLED = "job_cancelled"
    JOB_TIMED_OUT = "job_timed_out"
    JOB_HEARTBEAT_UPDATED = "job_heartbeat_updated"
    JOB_RETRY_SCHEDULED = "job_retry_scheduled"
    JOB_RETRY_EXHAUSTED = "job_retry_exhausted"
    NO_JOBS_AVAILABLE = "no_jobs_available"

    # Worker Events (Phase 6 - Worker Lifecycle)
    WORKER_STARTED = "worker_started"
    WORKER_STOPPED = "worker_stopped"
    WORKER_STOPPING = "worker_stopping"
    WORKER_CRASHED = "worker_crashed"
    WORKER_LIMIT_REACHED = "worker_limit_reached"
    WORKER_SIGNAL_RECEIVED = "worker_signal_received"
    JOB_EXECUTION_STARTED = "job_execution_started"
    JOB_EXECUTION_COMPLETED = "job_execution_completed"
    JOB_EXECUTION_FAILED = "job_execution_failed"
    JOB_EXECUTION_SIMULATED = "job_execution_simulated"  # Temporary for Phase 6.2 skeleton
    HEARTBEAT_FAILED = "heartbeat_failed"
    JOB_RECOVERED = "job_recovered"
    CRASH_RECOVERY_COMPLETED = "crash_recovery_completed"
    ORPHANED_LOCK_RECONCILED = "orphaned_lock_reconciled"  # Phase 6.5 Revised
    ORPHANED_WORKSPACE_RECONCILED = "orphaned_workspace_reconciled"  # Phase 6.5 Revised
    ORPHANED_SYNC_RUN_RECONCILED = "orphaned_sync_run_reconciled"  # Phase 6.5 Revised

    # Lock Lifecycle Events (Phase 6.6 - Observability)
    LOCK_ACQUIRED = "lock_acquired"
    LOCK_RELEASED = "lock_released"
    LOCK_ACQUISITION_FAILED = "lock_acquisition_failed"

    # Workspace State Events (Phase 6.6 - Observability)
    WORKSPACE_STATE_TRANSITION = "workspace_state_transition"

    # Invariant Violation Detection (Phase 6.6 - Observability)
    CONCURRENCY_VIOLATION_DETECTED = "concurrency_violation_detected"
    MISMATCH_DETECTED = "mismatch_detected"

    # QBO Realm Mismatch (Phase 8-Lite - Realm Immutability)
    QBO_REALM_MISMATCH = "qbo_realm_mismatch"

    # Multi-License/Workspace Events (Phase 8-Lite Multi-Workspace)
    LICENSE_CREATED = "license_created"
    WORKSPACE_CREATED = "workspace_created"
    WORKSPACE_SELECTED = "workspace_selected"
    WORKSPACE_REQUIRES_QBO_CONNECTION = "workspace_requires_qbo_connection"
    QBO_CONNECTION_COMPLETED = "qbo_connection_completed"

    # Raw Data Persistence Events (Phase 2 - PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md §6.5)
    RAW_DATA_PERSISTED = "raw_data_persisted"
    RAW_DATA_CACHE_HIT = "raw_data_cache_hit"
    RAW_DATA_PERSISTENCE_FAILED = "raw_data_persistence_failed"
    RAW_DATA_PERSISTENCE_SKIPPED = "raw_data_persistence_skipped"
    RAW_DATA_PERSISTENCE_TIMEOUT_SKIPPED = "raw_data_persistence_timeout_skipped"

    # Replay Events (Phase 3 - PHASE3_REPLAY_ORCHESTRATION_FORENSICS_PLAN.md §8.5)
    REPLAY_STARTED = "replay_started"
    REPLAY_COMPLETED = "replay_completed"
    REPLAY_ABORTED = "replay_aborted"
    REPLAY_EQUIVALENCE_PASS = "replay_equivalence_pass"
    REPLAY_EQUIVALENCE_FAIL = "replay_equivalence_fail"
    REPLAY_RAW_DATA_LOADED = "replay_raw_data_loaded"
    REPLAY_IDEMPOTENCY_HIT = "replay_idempotency_hit"

    # Explainability Events (Phase 4 - PHASE4_PROCESSOR_NATIVE_EXPLAINABILITY_PLAN.md §5.4)
    EXPLAINABILITY_RAW_DATA_ACCESSED = "explainability_raw_data_accessed"
    EXPLAINABILITY_EVENT_EXPLAINED = "explainability_event_explained"
    EXPLAINABILITY_PAYOUT_EXPLAINED = "explainability_payout_explained"
    EXPLAINABILITY_MATCH_EXPLAINED = "explainability_match_explained"
    EXPLAINABILITY_SYNC_RUN_SUMMARIZED = "explainability_sync_run_summarized"
    EXPLAINABILITY_ACCESS_DENIED = "explainability_access_denied"
    EXPLAINABILITY_CROSS_WORKSPACE_BLOCKED = "explainability_cross_workspace_blocked"
    EXPLAINABILITY_PII_ACCESS_GRANTED = "explainability_pii_access_granted"


class StructuredLogger:
    """
    Structured logger that outputs JSON log entries per MINIMUM_LOGGING_CONTRACT.

    Usage:
        logger = StructuredLogger(service="module3_qbo_integration", workspace_id="workspace_abc123")
        logger.info(
            EventType.QBO_OBJECT_CREATED,
            "Created QBO Deposit for payout",
            qbo_object_type="Deposit",
            qbo_object_id="456",
            processor_payout_id="po_abc123"
        )
    """

    def __init__(
        self,
        service: str,
        workspace_id: str,
        company_id: Optional[str] = None,
        sync_run_id: Optional[str] = None,
        request_id: Optional[str] = None,
        output=sys.stdout
    ):
        """
        Initialize structured logger.

        Args:
            service: Component or module name
            workspace_id: Workspace identifier (REQUIRED per Multitenancy Contract v1.0)
            company_id: Internal company identifier (DEPRECATED, for backward compatibility)
            sync_run_id: Unique sync execution ID (if sync-related)
            request_id: Shell-issued request correlation ID (Phase 4D)
            output: Output stream (default: stdout)
        """
        self.service = service
        self.workspace_id = workspace_id
        self.company_id = company_id or workspace_id  # Fallback for backward compatibility
        self.sync_run_id = sync_run_id
        self.request_id = request_id
        self.output = output
        self.environment = os.getenv("ENVIRONMENT", "dev")

    def set_sync_run_id(self, sync_run_id: str) -> None:
        """Set the sync_run_id for correlation."""
        self.sync_run_id = sync_run_id

    def _generate_log_id(self) -> str:
        """Generate unique log entry ID."""
        return f"log_{uuid.uuid4().hex[:12]}"

    def _get_timestamp(self) -> str:
        """Get ISO 8601 UTC timestamp."""
        return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    def _build_entry(
        self,
        severity: Severity,
        event_type: Union[EventType, str],
        message: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Build a log entry with all required and optional fields.

        Required fields (§3, updated per Multitenancy Contract v1.0):
        - log_id, timestamp, environment, severity, event_type, service, workspace_id, company_id, message

        Optional fields are passed via kwargs.
        """
        entry = {
            "log_id": self._generate_log_id(),
            "timestamp": self._get_timestamp(),
            "environment": self.environment,
            "severity": severity.value,
            "event_type": event_type.value if isinstance(event_type, EventType) else event_type,
            "service": self.service,
            "workspace_id": self.workspace_id,
            "company_id": self.company_id,
            "message": message
        }

        # Add sync_run_id if set
        if self.sync_run_id:
            entry["sync_run_id"] = self.sync_run_id

        # Phase 4D: Add request_id if set (shell-issued correlation ID)
        if self.request_id:
            entry["request_id"] = self.request_id

        # Add all optional fields from kwargs
        # These may include: processor, processor_payout_id, processor_payment_id,
        # idempotency_key, qbo_realm_id, qbo_object_type, qbo_object_id, duration_ms, etc.
        for key, value in kwargs.items():
            if value is not None:
                entry[key] = value

        return entry

    def _emit(self, entry: Dict[str, Any]) -> None:
        """Write log entry to output and log file."""
        log_line = json.dumps(entry) + "\n"

        # Write to stdout/configured output
        self.output.write(log_line)
        self.output.flush()

        # Also write to log file for compliance testing
        self._write_to_file(log_line)

    def _write_to_file(self, log_line: str) -> None:
        """Append log entry to file."""
        try:
            # Get logs directory (relative to project root)
            logs_dir = Path(__file__).parent.parent.parent / 'logs'
            logs_dir.mkdir(exist_ok=True)

            log_file = logs_dir / 'charles.log'
            with open(log_file, 'a') as f:
                f.write(log_line)
        except Exception:
            pass  # Don't let logging failures break the app

    def log(
        self,
        severity: Severity,
        event_type: Union[EventType, str],
        message: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Emit a structured log entry.

        Args:
            severity: Log severity level
            event_type: Event type from EventType enum
            message: Human-readable description
            **kwargs: Additional fields (processor_payout_id, qbo_object_id, etc.)

        Returns:
            The log entry dict (for testing/inspection)
        """
        entry = self._build_entry(severity, event_type, message, **kwargs)
        self._emit(entry)
        return entry

    # Convenience methods for each severity level

    def _resolve_event_and_message(
        self,
        default_event_type: str,
        event_type: Union[EventType, str],
        message: Optional[str],
    ) -> Tuple[Union[EventType, str], str]:
        """
        Support both structured and stdlib-style logging calls.

        Accepted patterns:
        - logger.info(EventType.SYNC_STARTED, "Message")
        - logger.info("custom_event_type", "Message")
        - logger.info("Message only")
        """
        if isinstance(event_type, EventType):
            if message is None:
                raise TypeError("message is required when event_type is an EventType")
            return event_type, message

        if message is None:
            return default_event_type, event_type

        return event_type, message

    def debug(self, event_type: Union[EventType, str], message: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Log DEBUG level entry."""
        resolved_event_type, resolved_message = self._resolve_event_and_message("debug_log", event_type, message)
        return self.log(Severity.DEBUG, resolved_event_type, resolved_message, **kwargs)

    def info(self, event_type: Union[EventType, str], message: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Log INFO level entry."""
        resolved_event_type, resolved_message = self._resolve_event_and_message("info_log", event_type, message)
        return self.log(Severity.INFO, resolved_event_type, resolved_message, **kwargs)

    def warn(self, event_type: Union[EventType, str], message: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Log WARN level entry."""
        resolved_event_type, resolved_message = self._resolve_event_and_message("warn_log", event_type, message)
        return self.log(Severity.WARN, resolved_event_type, resolved_message, **kwargs)

    def error(self, event_type: Union[EventType, str], message: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Log ERROR level entry."""
        resolved_event_type, resolved_message = self._resolve_event_and_message("error_log", event_type, message)
        return self.log(Severity.ERROR, resolved_event_type, resolved_message, **kwargs)

    def fatal(self, event_type: Union[EventType, str], message: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Log FATAL level entry."""
        resolved_event_type, resolved_message = self._resolve_event_and_message("fatal_log", event_type, message)
        return self.log(Severity.FATAL, resolved_event_type, resolved_message, **kwargs)

    # Domain-specific logging methods for common operations

    def log_payout_started(
        self,
        processor_payout_id: str,
        gross_amount: str,
        net_amount: str,
        payment_count: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log payout processing started."""
        return self.info(
            EventType.PAYOUT_PROCESSING_STARTED,
            f"Started processing payout {processor_payout_id}",
            processor_payout_id=processor_payout_id,
            gross_amount=gross_amount,
            net_amount=net_amount,
            payment_count=payment_count,
            **kwargs
        )

    def log_payout_completed(
        self,
        processor_payout_id: str,
        qbo_deposit_id: str,
        duration_ms: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log payout processing completed."""
        return self.info(
            EventType.PAYOUT_PROCESSING_COMPLETED,
            f"Completed processing payout {processor_payout_id}, created deposit {qbo_deposit_id}",
            processor_payout_id=processor_payout_id,
            qbo_object_type="Deposit",
            qbo_object_id=qbo_deposit_id,
            duration_ms=duration_ms,
            **kwargs
        )

    def log_payout_failed(
        self,
        processor_payout_id: str,
        error_message: str,
        error_category: str,
        duration_ms: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log payout processing failed."""
        return self.error(
            EventType.PAYOUT_PROCESSING_FAILED,
            f"Failed processing payout {processor_payout_id}: {error_message}",
            processor_payout_id=processor_payout_id,
            error_message=error_message,
            error_category=error_category,
            duration_ms=duration_ms,
            **kwargs
        )

    def log_qbo_creation_attempted(
        self,
        qbo_object_type: str,
        idempotency_key: str,
        processor_payout_id: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Log QBO object creation attempt."""
        return self.info(
            EventType.QBO_OBJECT_CREATION_ATTEMPTED,
            f"Attempting to create {qbo_object_type} for payout {processor_payout_id}",
            qbo_object_type=qbo_object_type,
            idempotency_key=idempotency_key,
            processor_payout_id=processor_payout_id,
            **kwargs
        )

    def log_qbo_created(
        self,
        qbo_object_type: str,
        qbo_object_id: str,
        idempotency_key: str,
        processor_payout_id: str,
        duration_ms: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log QBO object created successfully."""
        return self.info(
            EventType.QBO_OBJECT_CREATED,
            f"Created {qbo_object_type} {qbo_object_id} for payout {processor_payout_id}",
            qbo_object_type=qbo_object_type,
            qbo_object_id=qbo_object_id,
            idempotency_key=idempotency_key,
            processor_payout_id=processor_payout_id,
            duration_ms=duration_ms,
            **kwargs
        )

    def log_qbo_creation_skipped(
        self,
        qbo_object_type: str,
        qbo_object_id: str,
        idempotency_key: str,
        processor_payout_id: str,
        reason: str = "idempotency_hit",
        **kwargs
    ) -> Dict[str, Any]:
        """Log QBO object creation skipped (idempotency hit)."""
        return self.info(
            EventType.QBO_OBJECT_CREATION_SKIPPED,
            f"Skipped creating {qbo_object_type} for payout {processor_payout_id}: {reason}",
            qbo_object_type=qbo_object_type,
            qbo_object_id=qbo_object_id,
            idempotency_key=idempotency_key,
            processor_payout_id=processor_payout_id,
            skip_reason=reason,
            **kwargs
        )

    def log_qbo_creation_failed(
        self,
        qbo_object_type: str,
        idempotency_key: str,
        processor_payout_id: str,
        error_message: str,
        duration_ms: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log QBO object creation failed."""
        return self.error(
            EventType.QBO_OBJECT_CREATION_FAILED,
            f"Failed to create {qbo_object_type} for payout {processor_payout_id}: {error_message}",
            qbo_object_type=qbo_object_type,
            idempotency_key=idempotency_key,
            processor_payout_id=processor_payout_id,
            error_message=error_message,
            duration_ms=duration_ms,
            **kwargs
        )

    def log_idempotency_hit(
        self,
        idempotency_key: str,
        existing_result_id: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Log idempotency check hit (already processed)."""
        return self.info(
            EventType.IDEMPOTENCY_HIT,
            f"Idempotency hit for key {idempotency_key}, existing result: {existing_result_id}",
            idempotency_key=idempotency_key,
            existing_result_id=existing_result_id,
            **kwargs
        )

    def log_sync_started(
        self,
        payouts_total: int = 0,
        **kwargs
    ) -> Dict[str, Any]:
        """Log sync run started."""
        return self.info(
            EventType.SYNC_STARTED,
            f"Sync run started with {payouts_total} payouts to process",
            payouts_total=payouts_total,
            **kwargs
        )

    def log_sync_completed(
        self,
        payouts_total: int,
        payouts_processed: int,
        payouts_failed: int,
        payouts_ambiguous: int,
        duration_ms: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log sync run completed."""
        return self.info(
            EventType.SYNC_COMPLETED,
            f"Sync run completed: {payouts_processed}/{payouts_total} processed, {payouts_failed} failed, {payouts_ambiguous} ambiguous",
            payouts_total=payouts_total,
            payouts_processed=payouts_processed,
            payouts_failed=payouts_failed,
            payouts_ambiguous=payouts_ambiguous,
            duration_ms=duration_ms,
            **kwargs
        )

    def log_sync_failed(
        self,
        failure_reason: str,
        payouts_total: int,
        payouts_processed: int,
        duration_ms: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log sync run failed."""
        return self.error(
            EventType.SYNC_FAILED,
            f"Sync run failed: {failure_reason}",
            failure_reason=failure_reason,
            payouts_total=payouts_total,
            payouts_processed=payouts_processed,
            duration_ms=duration_ms,
            **kwargs
        )

    def log_sync_cancelled(
        self,
        cancellation_reason: str,
        payouts_total: int,
        payouts_processed: int,
        duration_ms: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log sync run cancelled."""
        return self.warn(
            EventType.SYNC_CANCELLED,
            f"Sync run cancelled: {cancellation_reason}",
            cancellation_reason=cancellation_reason,
            payouts_total=payouts_total,
            payouts_processed=payouts_processed,
            duration_ms=duration_ms,
            **kwargs
        )

    def log_sync_timed_out(
        self,
        timeout_reason: str,
        payouts_total: int,
        payouts_processed: int,
        duration_ms: int,
        timeout_threshold_ms: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log sync run timed out."""
        return self.error(
            EventType.SYNC_TIMED_OUT,
            f"Sync run timed out: {timeout_reason}",
            timeout_reason=timeout_reason,
            payouts_total=payouts_total,
            payouts_processed=payouts_processed,
            duration_ms=duration_ms,
            timeout_threshold_ms=timeout_threshold_ms,
            **kwargs
        )

    # OAuth Events (§4.5)

    def log_oauth_refresh_attempted(
        self,
        **kwargs
    ) -> Dict[str, Any]:
        """Log OAuth token refresh attempt."""
        return self.info(
            EventType.OAUTH_REFRESH_ATTEMPTED,
            "Attempting to refresh OAuth access token",
            **kwargs
        )

    def log_oauth_refresh_succeeded(
        self,
        expires_at: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Log OAuth token refresh succeeded."""
        return self.info(
            EventType.OAUTH_REFRESH_SUCCEEDED,
            f"OAuth access token refreshed successfully, expires at {expires_at}",
            expires_at=expires_at,
            **kwargs
        )

    def log_oauth_refresh_failed(
        self,
        error_message: str,
        failure_count: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Log OAuth token refresh failed."""
        return self.error(
            EventType.OAUTH_REFRESH_FAILED,
            f"OAuth token refresh failed (attempt {failure_count}): {error_message}",
            error_message=error_message,
            failure_count=failure_count,
            **kwargs
        )

    def log_oauth_authorization_invalid(
        self,
        reason: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Log OAuth authorization invalid (requires re-authorization)."""
        return self.error(
            EventType.OAUTH_AUTHORIZATION_INVALID,
            f"OAuth authorization invalid: {reason}",
            reason=reason,
            **kwargs
        )

    # Enhanced Payment Matching Events (§4.3.1)

    def log_match_attempted(
        self,
        processor_payment_id: str,
        amount: str,
        candidate_count: int,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Log payment match attempt.

        MUST be called at the start of every matching operation per
        MINIMUM_LOGGING_CONTRACT §4.3.1.
        """
        return self.info(
            EventType.PAYMENT_MATCH_ATTEMPTED,
            f"Attempting match for payment {processor_payment_id} (amount={amount}), found {candidate_count} candidates",
            processor_payment_id=processor_payment_id,
            amount=amount,
            candidate_count=candidate_count,
            **kwargs
        )

    def log_match_high_confidence(
        self,
        processor_payment_id: str,
        qbo_payment_id: str,
        score: int,
        breakdown: Dict[str, int],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Log high-confidence auto-match (score >= 85).

        Emitted when a payment is automatically matched without human review.
        """
        return self.info(
            EventType.PAYMENT_MATCH_HIGH_CONFIDENCE,
            f"High-confidence match: payment {processor_payment_id} -> QBO {qbo_payment_id} (score={score})",
            processor_payment_id=processor_payment_id,
            qbo_payment_id=qbo_payment_id,
            match_score=score,
            score_breakdown=breakdown,
            **kwargs
        )

    def log_match_low_confidence(
        self,
        processor_payment_id: str,
        top_score: int,
        candidates: list,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Log low-confidence match requiring manual review (score < 85).

        Emitted when no candidate meets the auto-match threshold.
        """
        return self.info(
            EventType.PAYMENT_MATCH_LOW_CONFIDENCE,
            f"Low-confidence match: payment {processor_payment_id}, top score={top_score}, {len(candidates)} candidates",
            processor_payment_id=processor_payment_id,
            top_score=top_score,
            candidate_count=len(candidates),
            candidates=candidates,
            **kwargs
        )

    def log_match_review_created(
        self,
        review_id: str,
        processor_payment_id: str,
        candidate_count: int,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Log creation of a match review record for manual resolution.

        Emitted when a payment requires human review to select the correct match.
        """
        return self.info(
            EventType.PAYMENT_MATCH_REVIEW_CREATED,
            f"Created match review {review_id} for payment {processor_payment_id} with {candidate_count} candidates",
            review_id=review_id,
            processor_payment_id=processor_payment_id,
            candidate_count=candidate_count,
            **kwargs
        )

    def log_match_manual_resolved(
        self,
        review_id: str,
        processor_payment_id: str,
        qbo_payment_id: Optional[str],
        resolved_by: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Log manual resolution of a match review.

        qbo_payment_id is None if user marked the payment as unmatched.
        """
        if qbo_payment_id:
            message = f"Review {review_id} resolved: payment {processor_payment_id} -> QBO {qbo_payment_id} by {resolved_by}"
        else:
            message = f"Review {review_id} resolved: payment {processor_payment_id} marked as unmatched by {resolved_by}"

        return self.info(
            EventType.PAYMENT_MATCH_MANUAL_RESOLVED,
            message,
            review_id=review_id,
            processor_payment_id=processor_payment_id,
            qbo_payment_id=qbo_payment_id,
            resolved_by=resolved_by,
            resolution_type="matched" if qbo_payment_id else "unmatched",
            **kwargs
        )

    def log_match_no_candidates(
        self,
        processor_payment_id: str,
        amount: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Log when no matching candidates are found for a payment.

        This occurs when no QBO payments match the exact amount.
        """
        return self.warn(
            EventType.PAYMENT_MATCH_NO_CANDIDATES,
            f"No match candidates found for payment {processor_payment_id} (amount={amount})",
            processor_payment_id=processor_payment_id,
            amount=amount,
            **kwargs
        )


def get_logger(
    service: str,
    workspace_id: str,
    company_id: Optional[str] = None,
    sync_run_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> StructuredLogger:
    """
    Factory function to create a structured logger.

    Args:
        service: Component or module name
        workspace_id: Workspace identifier (REQUIRED per Multitenancy Contract v1.0)
        company_id: Internal company identifier (DEPRECATED, for backward compatibility)
        sync_run_id: Unique sync execution ID (optional)
        request_id: Shell-issued request correlation ID (Phase 4D, optional)

    Returns:
        Configured StructuredLogger instance
    """
    return StructuredLogger(
        service=service,
        workspace_id=workspace_id,
        company_id=company_id,
        sync_run_id=sync_run_id,
        request_id=request_id,
    )
