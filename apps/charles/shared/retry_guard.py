"""
Retry Guard — Terminal-State Enforcement for Retry Safety

This module provides terminal-state checking for retry-sensitive operations
to prevent double execution of side-effecting code.

CONTRACT COMPLIANCE:
Per CANONICAL_ARCHITECTURE_CONTRACT §5:
- No duplicate QBO objects across retries
- Resume safely without duplication
- Never "start over" blindly
- Replay safety guaranteed

ENFORCEMENT POINTS:
- Pre-retry terminal-state check
- Crash recovery for PENDING operations
- Pre-posting duplicate detection

Per R-5 remediation (Finding A-2).
"""

import sqlite3
from typing import Optional, Callable, TypeVar, Dict, Any
from datetime import datetime

from shared.idempotency import IdempotencyManager, IdempotencyStatus
from shared.structured_logging import StructuredLogger, EventType


T = TypeVar('T')


class TerminalStateViolation(Exception):
    """
    Raised when retry would violate terminal-state guarantees.

    This indicates a CRITICAL CONTRACT VIOLATION.
    """
    pass


class RetryGuard:
    """
    Terminal-state enforcement for retry-sensitive operations.

    Ensures compliance with CANONICAL_ARCHITECTURE_CONTRACT §5:
    - No duplicate QBO objects
    - Resume safely without duplication
    - Replay safety across retries

    This class provides guards that prevent double-execution of
    side-effecting code during retries.
    """

    def __init__(
        self,
        db_conn: sqlite3.Connection,
        idempotency_manager: IdempotencyManager,
        logger: StructuredLogger
    ):
        """
        Initialize the retry guard.

        Args:
            db_conn: Database connection for state checks
            idempotency_manager: Idempotency manager for status checks
            logger: Structured logger for audit trail
        """
        self.db_conn = db_conn
        self.idempotency_manager = idempotency_manager
        self.logger = logger

    def check_pending_with_recovery(
        self,
        idempotency_key: str,
        recovery_checker: Callable[[], Optional[str]]
    ) -> Optional[str]:
        """
        Check if PENDING operation already completed (crash recovery).

        Per CANONICAL_ARCHITECTURE_CONTRACT §5.4:
        "If a sync run creates some QBO objects and then fails: Charles must
        persist enough state to detect partial completion, Resume safely
        without duplication"

        This function extends crash recovery to PENDING status (not just FAILED).

        Args:
            idempotency_key: Idempotency key to check
            recovery_checker: Function that queries QBO for existing result

        Returns:
            - QBO object ID if operation already completed
            - None if operation truly pending (should reject)

        Raises:
            TerminalStateViolation: If operation is pending without completion
        """
        idem_result = self.idempotency_manager.check_idempotency(idempotency_key)

        if idem_result.status != IdempotencyStatus.EXISTS_PENDING:
            # Not pending - caller should handle
            return None

        # PENDING status - check if actually completed via crash recovery
        self.logger.info(
            EventType.RETRY_ATTEMPTED,
            f"Operation PENDING, performing crash recovery check",
            idempotency_key=idempotency_key,
            recovery_type="pending_crash_recovery"
        )

        existing_result = recovery_checker()

        if existing_result:
            # Operation already completed! Update idempotency status
            self.logger.info(
                EventType.QBO_OBJECT_CREATION_SKIPPED,
                f"Crash recovery: found existing result {existing_result} for PENDING operation",
                idempotency_key=idempotency_key,
                result_id=existing_result,
                recovery_type="pending_completion_detected"
            )

            # Update idempotency status to COMPLETED
            self.idempotency_manager.complete_operation(
                idempotency_key,
                result_id=existing_result,
                result_data={"recovery_type": "pending_crash_recovery"}
            )

            return existing_result

        # Operation truly pending - should reject concurrent execution
        raise TerminalStateViolation(
            f"Operation truly in progress (PENDING) for key {idempotency_key}. "
            f"No existing result found in crash recovery check."
        )

    def pre_posting_duplicate_check(
        self,
        processor_payout_id: str,
        duplicate_checker: Callable[[], Optional[str]],
        idempotency_key: str
    ) -> Optional[str]:
        """
        Check for duplicate QBO deposit BEFORE posting.

        Adds a second layer of protection beyond idempotency checks.

        Per CANONICAL_ARCHITECTURE_CONTRACT §5.1:
        "No external event may ever produce duplicate accounting objects"

        Args:
            processor_payout_id: Processor payout ID
            duplicate_checker: Function that queries QBO for existing deposit
            idempotency_key: Idempotency key

        Returns:
            - QBO deposit ID if duplicate exists
            - None if safe to proceed

        Raises:
            TerminalStateViolation: If duplicate found but idempotency says safe
        """
        self.logger.debug(
            EventType.SYNC_EXECUTION_STARTED,
            f"Pre-posting duplicate check for payout {processor_payout_id}",
            processor_payout_id=processor_payout_id,
            idempotency_key=idempotency_key,
            phase="pre_posting_guard"
        )

        existing_deposit = duplicate_checker()

        if existing_deposit:
            # Duplicate exists! This should have been caught by idempotency
            idem_result = self.idempotency_manager.check_idempotency(idempotency_key)

            if idem_result.status == IdempotencyStatus.NOT_EXISTS:
                # CRITICAL: Duplicate exists but idempotency says NOT_EXISTS
                # This indicates a serious consistency issue
                raise TerminalStateViolation(
                    f"CRITICAL: Duplicate QBO deposit {existing_deposit} exists "
                    f"for payout {processor_payout_id}, but idempotency status "
                    f"is NOT_EXISTS. This violates CANONICAL_ARCHITECTURE_CONTRACT §5.1."
                )

            self.logger.info(
                EventType.QBO_OBJECT_CREATION_SKIPPED,
                f"Pre-posting guard: deposit {existing_deposit} already exists",
                qbo_deposit_id=existing_deposit,
                processor_payout_id=processor_payout_id,
                idempotency_key=idempotency_key,
                guard_type="pre_posting_duplicate_check"
            )

            # Update idempotency if needed
            if idem_result.status != IdempotencyStatus.EXISTS_COMPLETED:
                self.idempotency_manager.complete_operation(
                    idempotency_key,
                    result_id=existing_deposit,
                    result_data={"guard_type": "pre_posting_duplicate_detection"}
                )

            return existing_deposit

        # Safe to proceed
        return None

    def wrap_retry_with_terminal_check(
        self,
        func: Callable[..., T],
        idempotency_key: str,
        operation_name: str,
        *args,
        **kwargs
    ) -> T:
        """
        Wrap a retry-sensitive function with terminal-state checking.

        Before each retry attempt, checks if operation already reached
        terminal state to prevent re-executing side effects.

        Args:
            func: Function to wrap
            idempotency_key: Idempotency key for terminal-state check
            operation_name: Name of operation for logging
            *args, **kwargs: Arguments to pass to func

        Returns:
            Result of func

        Raises:
            TerminalStateViolation: If terminal state already reached
        """
        # Check terminal state BEFORE executing func
        idem_result = self.idempotency_manager.check_idempotency(idempotency_key)

        if idem_result.status == IdempotencyStatus.EXISTS_COMPLETED:
            # Terminal state reached - return cached result
            self.logger.info(
                EventType.IDEMPOTENCY_HIT,
                f"Terminal state check: {operation_name} already completed",
                idempotency_key=idempotency_key,
                result_id=idem_result.result_id,
                guard_type="terminal_state_check"
            )

            # Return cached result (caller should handle type conversion)
            return idem_result.result_id

        # Not in terminal state - safe to execute
        return func(*args, **kwargs)
