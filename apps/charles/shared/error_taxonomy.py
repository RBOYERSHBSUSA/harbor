"""
Error Handling Module for Charles

Implements ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT v1.1

This module provides:
- Error classification into categories (§3)
- Retry logic with exponential backoff for recoverable errors (§7)
- Clear separation between errors, ambiguity, and warnings (§2)

Error Categories (§3):
- authorization_error (Non-Recoverable): OAuth, 401, token issues
- infrastructure_error (Recoverable): Network, timeout, API outage
- configuration_error (Non-Recoverable): Missing accounts, invalid config
- data_integrity_error (Non-Recoverable): Mismatches, duplicates, constraints

Retry Semantics (§7.1):
- Only infrastructure errors are automatically recoverable
- Maximum 3 attempts per operation
- Backoff schedule: 5s → 30s → 120s
"""

import time
from enum import Enum
from typing import Callable, TypeVar, Optional, Any
from dataclasses import dataclass
from functools import wraps

from shared.structured_logging import get_logger, EventType, StructuredLogger


class ErrorCategory(Enum):
    """
    Error categories per §3.
    """
    AUTHORIZATION = "authorization_error"      # Non-Recoverable (§3.1)
    INFRASTRUCTURE = "infrastructure_error"    # Recoverable (§3.2)
    CONFIGURATION = "configuration_error"      # Non-Recoverable (§3.3)
    DATA_INTEGRITY = "data_integrity_error"    # Non-Recoverable (§3.4)


class AmbiguityType(Enum):
    """
    Ambiguity types per §4.
    """
    PAYMENT_MATCHING = "payment_matching"      # Multiple QBO Payments match (§4.1)
    REFUND_ATTRIBUTION = "refund_attribution"  # Multiple original payments possible (§4.2)
    HISTORICAL = "historical"                  # Legacy data lacks metadata (§4.3)


class WarningType(Enum):
    """
    Warning types per §5.
    """
    ANOMALY = "anomaly"           # Unusual but non-blocking (§5.1)
    DEPRECATION = "deprecation"   # API/config deprecation (§5.2)


# Retry configuration per §7.1
MAX_RETRY_ATTEMPTS = 3
BACKOFF_SCHEDULE_SECONDS = [5, 30, 120]  # 5s, 30s, 120s


@dataclass
class ClassifiedError:
    """
    A classified error with category and recovery info.
    """
    category: ErrorCategory
    message: str
    original_exception: Exception
    is_recoverable: bool
    error_code: Optional[str] = None
    diagnostic_context: Optional[dict] = None

    @property
    def should_retry(self) -> bool:
        """Only infrastructure errors should be retried."""
        return self.category == ErrorCategory.INFRASTRUCTURE


def classify_error(error: Exception) -> ClassifiedError:
    """
    Classify an error per ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT §3.

    Args:
        error: The exception to classify

    Returns:
        ClassifiedError with category and recovery info
    """
    error_str = str(error).lower()
    error_type = type(error).__name__

    # Authorization errors (§3.1) - Non-Recoverable
    # OAuth-specific terminal errors added per Phase 3 OAuth Refactor
    auth_keywords = ['oauth', 'unauthorized', '401', 'token', 'authentication',
                     'access denied', 'forbidden', '403', 'invalid_grant',
                     'refresh token', 'expired token', 'invalid_client',
                     'token_revoked', 'authorization_revoked']
    if any(kw in error_str for kw in auth_keywords):
        return ClassifiedError(
            category=ErrorCategory.AUTHORIZATION,
            message=str(error),
            original_exception=error,
            is_recoverable=False,
            error_code="AUTH_FAILED"
        )

    # Configuration errors (§3.3) - Non-Recoverable
    config_keywords = ['configuration', 'not configured', 'missing required',
                       'account not found', 'invalid account', 'realm not found',
                       'company not found', 'missing company']
    if any(kw in error_str for kw in config_keywords):
        return ClassifiedError(
            category=ErrorCategory.CONFIGURATION,
            message=str(error),
            original_exception=error,
            is_recoverable=False,
            error_code="CONFIG_INVALID"
        )

    # Data integrity errors (§3.4) - Non-Recoverable
    integrity_keywords = ['mismatch', 'duplicate', 'integrity', 'constraint',
                          'validation', 'does not match', 'inconsistent',
                          'already exists', 'violation']
    if any(kw in error_str for kw in integrity_keywords):
        return ClassifiedError(
            category=ErrorCategory.DATA_INTEGRITY,
            message=str(error),
            original_exception=error,
            is_recoverable=False,
            error_code="DATA_INTEGRITY_FAILED"
        )

    # Infrastructure errors (§3.2) - Recoverable
    # This is the default for network, timeout, and transient issues
    infra_keywords = ['timeout', 'connection', 'network', 'unavailable',
                      '500', '502', '503', '504', 'service unavailable',
                      'rate limit', '429', 'temporarily', 'try again']
    if any(kw in error_str for kw in infra_keywords) or \
       error_type in ['TimeoutError', 'ConnectionError', 'ConnectionRefusedError']:
        return ClassifiedError(
            category=ErrorCategory.INFRASTRUCTURE,
            message=str(error),
            original_exception=error,
            is_recoverable=True,
            error_code="INFRA_TRANSIENT"
        )

    # Default to infrastructure error (recoverable) for unknown errors
    # Per §3.2, infrastructure errors are the safest default
    return ClassifiedError(
        category=ErrorCategory.INFRASTRUCTURE,
        message=str(error),
        original_exception=error,
        is_recoverable=True,
        error_code="INFRA_UNKNOWN"
    )


T = TypeVar('T')


def with_retry(
    operation_name: str,
    logger: StructuredLogger,
    max_attempts: int = MAX_RETRY_ATTEMPTS,
    backoff_schedule: list = None
) -> Callable:
    """
    Decorator that adds retry logic with exponential backoff per §7.1.

    Only retries infrastructure errors. Non-recoverable errors are raised immediately.

    Args:
        operation_name: Name of operation for logging
        logger: StructuredLogger instance
        max_attempts: Maximum retry attempts (default: 3)
        backoff_schedule: Backoff delays in seconds (default: [5, 30, 120])

    Usage:
        @with_retry("create_deposit", logger)
        def create_deposit():
            # ... operation that may fail ...
    """
    if backoff_schedule is None:
        backoff_schedule = BACKOFF_SCHEDULE_SECONDS

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_error = None
            attempt = 0

            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    attempt += 1
                    classified = classify_error(e)
                    last_error = classified

                    if not classified.should_retry:
                        # Non-recoverable error - don't retry (§7.2)
                        logger.error(
                            EventType.ERROR_DETECTED,
                            f"{operation_name} failed with non-recoverable error: {classified.message}",
                            error_category=classified.category.value,
                            error_code=classified.error_code,
                            is_recoverable=False,
                            attempt=attempt
                        )
                        raise classified.original_exception

                    if attempt >= max_attempts:
                        # Exhausted retries (§7.1)
                        logger.error(
                            EventType.RETRY_EXHAUSTED,
                            f"{operation_name} failed after {max_attempts} attempts: {classified.message}",
                            error_category=classified.category.value,
                            error_code=classified.error_code,
                            retry_count=attempt,
                            max_retries=max_attempts
                        )
                        raise classified.original_exception

                    # Calculate backoff delay
                    delay_index = min(attempt - 1, len(backoff_schedule) - 1)
                    delay = backoff_schedule[delay_index]

                    # Log retry scheduled
                    logger.info(
                        EventType.RETRY_SCHEDULED,
                        f"{operation_name} failed, scheduling retry {attempt}/{max_attempts} after {delay}s",
                        error_category=classified.category.value,
                        error_code=classified.error_code,
                        retry_count=attempt,
                        max_retries=max_attempts,
                        backoff_delay_ms=delay * 1000
                    )

                    # Wait before retry
                    time.sleep(delay)

                    # Log retry attempted
                    logger.info(
                        EventType.RETRY_ATTEMPTED,
                        f"{operation_name} retry {attempt}/{max_attempts} starting",
                        retry_count=attempt,
                        max_retries=max_attempts
                    )

            # Should not reach here, but just in case
            if last_error:
                raise last_error.original_exception

        return wrapper
    return decorator


def retry_operation(
    func: Callable[..., T],
    operation_name: str,
    logger: StructuredLogger,
    max_attempts: int = MAX_RETRY_ATTEMPTS,
    backoff_schedule: list = None,
    *args,
    **kwargs
) -> T:
    """
    Execute an operation with retry logic.

    Non-decorator version for when you need to wrap a specific call.

    Args:
        func: Function to execute
        operation_name: Name of operation for logging
        logger: StructuredLogger instance
        max_attempts: Maximum retry attempts
        backoff_schedule: Backoff delays in seconds
        *args, **kwargs: Arguments to pass to func

    Returns:
        Result of func

    Raises:
        The original exception after max retries or for non-recoverable errors
    """
    if backoff_schedule is None:
        backoff_schedule = BACKOFF_SCHEDULE_SECONDS

    last_error = None
    attempt = 0

    while attempt < max_attempts:
        try:
            return func(*args, **kwargs)

        except Exception as e:
            attempt += 1
            classified = classify_error(e)
            last_error = classified

            if not classified.should_retry:
                # Non-recoverable error
                logger.error(
                    EventType.ERROR_DETECTED,
                    f"{operation_name} failed with non-recoverable error: {classified.message}",
                    error_category=classified.category.value,
                    error_code=classified.error_code,
                    is_recoverable=False,
                    attempt=attempt
                )
                raise

            if attempt >= max_attempts:
                # Exhausted retries
                logger.error(
                    EventType.RETRY_EXHAUSTED,
                    f"{operation_name} failed after {max_attempts} attempts: {classified.message}",
                    error_category=classified.category.value,
                    error_code=classified.error_code,
                    retry_count=attempt,
                    max_retries=max_attempts
                )
                raise

            # Calculate backoff
            delay_index = min(attempt - 1, len(backoff_schedule) - 1)
            delay = backoff_schedule[delay_index]

            logger.info(
                EventType.RETRY_SCHEDULED,
                f"{operation_name} failed, scheduling retry {attempt}/{max_attempts} after {delay}s",
                error_category=classified.category.value,
                retry_count=attempt,
                backoff_delay_ms=delay * 1000
            )

            time.sleep(delay)

            logger.info(
                EventType.RETRY_ATTEMPTED,
                f"{operation_name} retry {attempt}/{max_attempts} starting",
                retry_count=attempt
            )

    if last_error:
        raise last_error.original_exception


class ConfigurationError(Exception):
    """
    Raised when required configuration is missing or invalid.
    Per CANONICAL_ARCHITECTURE_CONTRACT §7.2, configuration errors must cause
    early, loud failure.
    """
    pass


class AuthorizationError(Exception):
    """
    Raised when OAuth or authorization fails.
    Per ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT §3.1, this is non-recoverable
    and requires user re-authentication.
    """
    pass


class DataIntegrityError(Exception):
    """
    Raised when data integrity validation fails.
    Per ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT §3.4, this requires human
    investigation.
    """
    pass
