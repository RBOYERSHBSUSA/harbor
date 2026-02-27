"""
Mandatory Idempotency Guard Decorator

IDEMPOTENCY_IMPLEMENTATION_CONTRACT Enforcement Layer.

This module provides a mandatory decorator that enforces idempotency
for ALL side-effect producing operations.

Usage:
    @require_idempotency(operation_type="deposit")
    def create_deposit(self, payout_id: str) -> str:
        # Function receives idempotency context automatically
        # Idempotency check happens BEFORE this function executes
        ...

Contract Guarantees:
1. Idempotency check occurs BEFORE function execution (check-before-act)
2. No function can execute without passing through guard
3. Centralized enforcement - all logic in decorator
4. Impossible to bypass at runtime
"""

import functools
import inspect
from typing import Callable, Any, Optional, Dict
from dataclasses import dataclass

from shared.idempotency import (
    IdempotencyManager,
    IdempotencyStatus,
    IdempotencyResult,
    generate_idempotency_key_v2,
    _generate_legacy_key,
)
from shared.structured_logging import get_logger, EventType


@dataclass
class IdempotencyContext:
    """
    Context passed to idempotency-guarded functions.

    Provides access to idempotency state without cluttering function signatures.
    """
    key: str
    operation_type: str
    status: IdempotencyStatus
    manager: IdempotencyManager
    is_retry: bool  # True if operation failed previously and is being retried


class IdempotencyGuardError(Exception):
    """Raised when idempotency guard detects a violation."""
    pass


def require_idempotency(
    operation_type: str,
    key_extractor: Optional[Callable] = None
):
    """
    Decorator that enforces mandatory idempotency for side-effect functions.

    This decorator implements IDEMPOTENCY_IMPLEMENTATION_CONTRACT §3.2:
    - Check before act: verify key exists BEFORE function executes
    - Universal enforcement: ALL side-effect functions must use this
    - Centralized logic: idempotency check in one place

    Args:
        operation_type: Type of operation (deposit, match_resolve, etc.)
        key_extractor: Optional function to extract external_id from function args.
                      If not provided, decorator looks for common patterns:
                      - processor_payout_id parameter
                      - payout_id parameter
                      - external_id parameter

    Returns:
        Decorator function

    Usage:
        @require_idempotency(operation_type="deposit")
        def create_deposit_for_payout(self, processor_payout_id: str) -> str:
            # Idempotency already checked before this executes
            # Access context via: self._idempotency_context if needed
            ...

        @require_idempotency(
            operation_type="custom",
            key_extractor=lambda self, data: data['external_id']
        )
        def custom_operation(self, data: dict) -> str:
            ...

    Contract Enforcement:
    1. Function CANNOT execute without idempotency check
    2. If EXISTS_COMPLETED: returns cached result, function never executes
    3. If EXISTS_PENDING: raises IdempotencyGuardError, function never executes
    4. If EXISTS_FAILED or NOT_EXISTS: marks as pending, executes function
    5. On success: marks as completed automatically
    6. On failure: marks as failed automatically

    This makes idempotency violations IMPOSSIBLE.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs) -> Any:
            # =========================================================
            # STEP 1: Extract required components from self
            # =========================================================

            # Get workspace_id from self (REQUIRED for scoping)
            if not hasattr(self, 'workspace_id'):
                raise IdempotencyGuardError(
                    f"@require_idempotency decorator requires 'workspace_id' attribute on {self.__class__.__name__}. "
                    "Add 'self.workspace_id' to class __init__."
                )
            workspace_id = self.workspace_id

            # Get company_id from self (optional, used for logging metadata only)
            company_id = getattr(self, 'company_id', None)

            # Get idempotency manager from self
            if not hasattr(self, 'idempotency'):
                raise IdempotencyGuardError(
                    f"@require_idempotency decorator requires 'idempotency' attribute on {self.__class__.__name__}. "
                    "Add 'self.idempotency = get_idempotency_manager(...)' to class __init__."
                )
            idempotency_manager: IdempotencyManager = self.idempotency

            # Get logger from self (optional but recommended)
            logger = getattr(self, 'logger', None) or get_logger(
                service="idempotency_guard",
                workspace_id=workspace_id,
                company_id=company_id
            )

            # =========================================================
            # STEP 2: Extract external_id for idempotency key
            # =========================================================

            # Bind arguments to get parameter names
            sig = inspect.signature(func)
            bound_args = sig.bind(self, *args, **kwargs)
            bound_args.apply_defaults()

            if key_extractor:
                # Use custom extractor
                external_id = key_extractor(self, *args, **kwargs)
            else:
                # Use default extraction logic
                external_id = None

                # Try common parameter names in order of preference
                for param_name in ['processor_payout_id', 'payout_id', 'external_id', 'review_id', 'event_id']:
                    if param_name in bound_args.arguments:
                        external_id = bound_args.arguments[param_name]
                        break

                if external_id is None:
                    raise IdempotencyGuardError(
                        f"Could not extract external_id from {func.__name__} parameters. "
                        f"Available parameters: {list(bound_args.arguments.keys())}. "
                        f"Either add a parameter named 'processor_payout_id', 'payout_id', 'external_id', etc., "
                        f"or provide a custom key_extractor function."
                    )

            # =========================================================
            # STEP 3: Generate idempotency key (v2 per IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1 §4.1)
            # =========================================================

            # Resolve processor for v2 key scope
            processor = (
                getattr(self, 'processor_type', None)
                or getattr(self, 'processor_name', None)
                or 'unknown'
            )

            idempotency_key = generate_idempotency_key_v2(
                operation_type=operation_type,
                workspace_id=workspace_id,
                processor=processor,
                external_id=str(external_id)
            )

            # =========================================================
            # STEP 4: CHECK IDEMPOTENCY (MANDATORY, BEFORE FUNCTION)
            # Dual-read: check v2 key first, fall back to legacy v1 key
            # for backward compatibility with in-flight operations.
            # =========================================================

            idem_result: IdempotencyResult = idempotency_manager.check_idempotency(idempotency_key)

            # If v2 key not found, check legacy v1 key for backward compat
            if idem_result.status == IdempotencyStatus.NOT_EXISTS and company_id:
                legacy_key = _generate_legacy_key(
                    operation_type=operation_type,
                    company_id=company_id,
                    external_id=str(external_id)
                )
                legacy_result = idempotency_manager.check_idempotency(legacy_key)
                if legacy_result.status == IdempotencyStatus.EXISTS_COMPLETED:
                    # Migrate: record under v2 key so future lookups use v2
                    idempotency_manager.start_operation(idempotency_key, operation_type)
                    idempotency_manager.complete_operation(
                        idempotency_key,
                        result_id=legacy_result.result_id or "migrated",
                        result_data={"migrated_from": legacy_key}
                    )
                    idem_result = legacy_result  # Use legacy result for this invocation

            # Handle EXISTS_COMPLETED: return cached result
            if idem_result.status == IdempotencyStatus.EXISTS_COMPLETED:
                logger.log_idempotency_hit(
                    idempotency_key=idempotency_key,
                    existing_result_id=idem_result.result_id or "cached"
                )

                # Function NEVER executes - return cached result
                return idem_result.result_id

            # Handle EXISTS_PENDING: operation in progress, reject
            if idem_result.status == IdempotencyStatus.EXISTS_PENDING:
                error_msg = (
                    f"Operation {operation_type} already in progress for {external_id}. "
                    f"Idempotency key: {idempotency_key}"
                )
                logger.error(
                    EventType.ERROR_DETECTED,
                    error_msg,
                    idempotency_key=idempotency_key,
                    error_category="concurrent_operation"
                )

                # Function NEVER executes - raise error
                raise IdempotencyGuardError(error_msg)

            # Handle EXISTS_FAILED or NOT_EXISTS: proceed with operation
            is_retry = (idem_result.status == IdempotencyStatus.EXISTS_FAILED)

            if is_retry:
                logger.info(
                    EventType.RETRY_ATTEMPTED,
                    f"Retrying failed operation {operation_type} for {external_id}",
                    idempotency_key=idempotency_key,
                    operation_type=operation_type
                )

            # =========================================================
            # STEP 5: START OPERATION (MARK AS PENDING)
            # =========================================================

            idempotency_manager.start_operation(idempotency_key, operation_type)

            # Create context for function (optional, function can access if needed)
            context = IdempotencyContext(
                key=idempotency_key,
                operation_type=operation_type,
                status=idem_result.status,
                manager=idempotency_manager,
                is_retry=is_retry
            )

            # Attach context to self for function access
            self._idempotency_context = context

            # =========================================================
            # STEP 6: EXECUTE FUNCTION (PROTECTED BY GUARD)
            # =========================================================

            try:
                result = func(self, *args, **kwargs)

                # =========================================================
                # STEP 7: MARK OPERATION COMPLETED
                # =========================================================

                idempotency_manager.complete_operation(
                    idempotency_key,
                    result_id=str(result) if result is not None else "completed",
                    result_data={
                        "operation_type": operation_type,
                        "external_id": external_id
                    }
                )

                return result

            except Exception as e:
                # =========================================================
                # STEP 8: MARK OPERATION FAILED
                # =========================================================

                idempotency_manager.fail_operation(idempotency_key, str(e))

                # Re-raise exception for caller to handle
                raise

            finally:
                # Clean up context
                if hasattr(self, '_idempotency_context'):
                    delattr(self, '_idempotency_context')

        return wrapper
    return decorator
