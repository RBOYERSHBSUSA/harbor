"""
Stripe Processor Pull Module

================================================================================
LAYER 1 BOUNDARY — PROCESSOR API INGESTION ONLY
================================================================================

This module performs processor API ingestion only.
No canonicalization, matching, or inference is allowed here.

FORBIDDEN in this module:
- ProcessorPayment, ProcessorPayout, or any canonical models
- Amount normalization (cents → dollars)
- Business date interpretation
- QBO references of any kind
- Scoring, matching, or business logic

ALLOWED in this module:
- HTTP requests to Stripe API
- Retry logic with exponential backoff
- Pagination handling
- Raw JSON response parsing
- Structured logging of API calls
- Raw data persistence at Layer 1 boundary (Phase 2)

See: PROCESSOR_TRANSACTIONS_CONTRACT.md
See: PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md

================================================================================
METHOD NAMING CONVENTION (VERB SEMANTICS)
================================================================================

Layer 1 and Layer 2 use DIFFERENT verb prefixes to make boundaries explicit:

LAYER 1 (this module) — "fetch_*" methods:
    - fetch_account(), fetch_payouts(), fetch_payout(), etc.
    - Perform raw HTTP requests to processor APIs
    - Return unmodified JSON responses from the processor
    - NO interpretation, normalization, or business logic
    - Stateless: each call is independent

LAYER 2 (StripeProcessor) — "fetch_*" methods that CALL Layer 1:
    - StripeProcessor.fetch_payouts() calls StripePullClient.fetch_payouts()
    - StripeProcessor methods orchestrate Layer 1 calls
    - StripeProcessor methods canonicalize raw data into ProcessorPayment, etc.
    - Boundary comment marks transition from Layer 1 to Layer 2

FUTURE CONSIDERATION:
    If disambiguation is needed, Layer 1 methods could be renamed to "pull_*"
    (e.g., pull_payouts, pull_charge) to make the layer explicit in the name.
    This is NOT done now to avoid unnecessary churn. The architectural seam
    is enforced by module boundaries and tests, not naming alone.

================================================================================
"""

import sys
import os
import time
import requests
from typing import Dict, Optional, List

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.structured_logging import get_logger, EventType, StructuredLogger
from shared.observability_interface import get_metrics_collector
from shared.error_taxonomy import (
    AuthorizationError,
    ConfigurationError,
    MAX_RETRY_ATTEMPTS,
    BACKOFF_SCHEDULE_SECONDS,
)
from processors.raw_data_persistence import (
    RawDataPersistence,
    is_raw_data_persistence_enabled,
)


class StripePullClient:
    """
    Layer 1 Stripe API client.

    Handles all HTTP communication with Stripe's API.
    Returns raw JSON responses - no canonicalization.

    Implements ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT §7.1 for retry logic.

    ============================================================================
    STATEFULNESS CONTRACT
    ============================================================================

    This client MUST be stateless across API calls. Each method call is
    independent and must not rely on or modify state from previous calls.

    ALLOWED instance attributes (set once in __init__):
        - api_key: Stripe API key (immutable credential)
        - _headers: HTTP headers derived from api_key (immutable)
        - logger: Structured logger reference (immutable)
        - workspace_id: Workspace identifier for metrics (immutable)
        - _db_path: Database path for raw data persistence (immutable, Phase 2)
        - _sync_run_id: Sync run ID for persistence context (immutable, Phase 2)
        - _persistence: RawDataPersistence instance (immutable, Phase 2)

    FORBIDDEN instance attributes:
        - Pagination cursors (must be passed as parameters)
        - Retry counters (must be local to each request)
        - Response caches (no memoization)
        - Rate limit state (handled per-request)
        - Any mutable state that persists across calls

    This contract enables:
        - Safe concurrent usage
        - Predictable behavior in tests
        - Future replay/persistence without state migration

    ============================================================================
    """

    BASE_URL = 'https://api.stripe.com/v1'

    # Allowed instance attributes - enforced by __setattr__
    _ALLOWED_ATTRIBUTES = frozenset({
        'api_key',
        '_headers',
        'logger',
        'workspace_id',
        # Phase 2: Raw data persistence (immutable once set)
        '_db_path',
        '_sync_run_id',
        '_persistence',
    })

    def __init__(
        self,
        api_key: str,
        logger: StructuredLogger,
        workspace_id: str = "",
        db_path: Optional[str] = None,
        sync_run_id: Optional[str] = None,
    ):
        """
        Initialize Stripe pull client.

        Args:
            api_key: Stripe secret key (sk_live_xxx or sk_test_xxx)
            logger: Structured logger instance (from parent processor)
            workspace_id: Workspace ID for metrics
            db_path: Database path for raw data persistence (Phase 2)
            sync_run_id: Sync run ID for persistence context (Phase 2)
        """
        # Use object.__setattr__ to bypass our guard during __init__
        object.__setattr__(self, 'api_key', api_key)
        object.__setattr__(self, '_headers', {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        object.__setattr__(self, 'logger', logger)
        object.__setattr__(self, 'workspace_id', workspace_id)

        # Phase 2: Raw data persistence configuration
        object.__setattr__(self, '_db_path', db_path)
        object.__setattr__(self, '_sync_run_id', sync_run_id)

        # Initialize persistence helper if configured
        if db_path and sync_run_id and workspace_id:
            object.__setattr__(self, '_persistence', RawDataPersistence(
                db_path=db_path,
                workspace_id=workspace_id,
                sync_run_id=sync_run_id,
                logger=logger,
            ))
        else:
            object.__setattr__(self, '_persistence', None)

    def __setattr__(self, name: str, value) -> None:
        """
        Guard against unauthorized state additions.

        Raises AttributeError if attempting to set an attribute not in
        _ALLOWED_ATTRIBUTES. This enforces the statefulness contract.
        """
        if name not in self._ALLOWED_ATTRIBUTES:
            raise AttributeError(
                f"StripePullClient forbids attribute '{name}'. "
                f"Layer 1 pull clients must be stateless. "
                f"Allowed attributes: {sorted(self._ALLOWED_ATTRIBUTES)}"
            )
        object.__setattr__(self, name, value)

    def _make_request_with_retry(
        self,
        endpoint: str,
        params: Dict = None,
        method: str = 'GET'
    ) -> Dict:
        """
        Make a request to Stripe API with retry logic.

        Implements ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT §7.1:
        - Max 3 retry attempts
        - Exponential backoff: 5s, 30s, 120s
        - Only infrastructure errors are retried

        Args:
            endpoint: API endpoint (e.g., '/payouts')
            params: Query parameters
            method: HTTP method

        Returns:
            JSON response as dictionary (raw Stripe data)

        Raises:
            AuthorizationError: For 401/403 (not retried)
            ConfigurationError: For 400 bad request (not retried)
            ConnectionError: For infrastructure errors (after retries exhausted)
        """
        url = f'{self.BASE_URL}{endpoint}'
        last_error = None

        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            start_time = time.time()

            try:
                if method == 'GET':
                    response = requests.get(
                        url,
                        headers=self._headers,
                        params=params or {},
                        timeout=30
                    )
                else:
                    response = requests.post(
                        url,
                        headers=self._headers,
                        data=params or {},
                        timeout=30
                    )

                duration_ms = int((time.time() - start_time) * 1000)

                # Handle response based on status code
                if response.status_code == 200:
                    # Phase 5.3: Instrumentation hook - Stripe API call success
                    metrics = get_metrics_collector()
                    metrics.increment_counter(
                        "stripe_api_calls",
                        self.workspace_id,
                        {"operation": endpoint, "status": "success"}
                    )
                    metrics.record_histogram(
                        "stripe_api_duration",
                        duration_ms / 1000.0,  # Convert to seconds
                        self.workspace_id,
                        {"operation": endpoint, "status": "success"}
                    )
                    return response.json()

                # Classify error based on status code per ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT
                error_text = response.text
                status = response.status_code

                if status in [401, 403]:
                    # Authorization error - non-recoverable (§3.1)
                    raise AuthorizationError(
                        f"Stripe API authorization failed ({status}): {error_text}"
                    )

                elif status == 400:
                    # Bad request - configuration or data issue (§3.3)
                    raise ConfigurationError(
                        f"Stripe API bad request: {error_text}"
                    )

                elif status in [500, 502, 503, 504]:
                    # Server error - infrastructure, recoverable (§3.2)
                    error = ConnectionError(
                        f"Stripe API server error ({status}): {error_text}"
                    )
                    last_error = error

                    if attempt < MAX_RETRY_ATTEMPTS:
                        delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                        self.logger.info(
                            EventType.RETRY_SCHEDULED,
                            f"Stripe API error, scheduling retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
                            retry_count=attempt,
                            max_retries=MAX_RETRY_ATTEMPTS,
                            backoff_delay_ms=delay * 1000,
                            status_code=status
                        )
                        time.sleep(delay)
                        continue
                    else:
                        self.logger.error(
                            EventType.RETRY_EXHAUSTED,
                            f"Stripe API failed after {MAX_RETRY_ATTEMPTS} attempts",
                            retry_count=attempt,
                            max_retries=MAX_RETRY_ATTEMPTS,
                            status_code=status
                        )
                        raise error

                elif status == 429:
                    # Rate limit - recoverable (§3.2)
                    error = ConnectionError(
                        f"Stripe API rate limit exceeded"
                    )
                    last_error = error

                    if attempt < MAX_RETRY_ATTEMPTS:
                        delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                        self.logger.info(
                            EventType.RETRY_SCHEDULED,
                            f"Stripe rate limited, retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
                            retry_count=attempt,
                            backoff_delay_ms=delay * 1000
                        )
                        time.sleep(delay)
                        continue
                    else:
                        raise error

                else:
                    # Unknown error - treat as infrastructure
                    raise ConnectionError(
                        f"Stripe API error ({status}): {error_text}"
                    )

            except requests.exceptions.Timeout as e:
                last_error = ConnectionError(f"Stripe API timeout: {e}")
                if attempt < MAX_RETRY_ATTEMPTS:
                    delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                    self.logger.info(
                        EventType.RETRY_SCHEDULED,
                        f"Stripe timeout, retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
                        retry_count=attempt,
                        backoff_delay_ms=delay * 1000
                    )
                    time.sleep(delay)
                    continue
                else:
                    raise last_error

            except requests.exceptions.ConnectionError as e:
                last_error = ConnectionError(f"Stripe connection failed: {e}")
                if attempt < MAX_RETRY_ATTEMPTS:
                    delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                    self.logger.info(
                        EventType.RETRY_SCHEDULED,
                        f"Stripe connection failed, retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
                        retry_count=attempt,
                        backoff_delay_ms=delay * 1000
                    )
                    time.sleep(delay)
                    continue
                else:
                    raise last_error

            except (AuthorizationError, ConfigurationError):
                # Non-recoverable - don't retry
                raise

        # Should not reach here, but just in case
        if last_error:
            raise last_error
        raise ConnectionError("Stripe API request failed")

    # =========================================================================
    # PHASE 2: RAW DATA PERSISTENCE
    # =========================================================================

    def _persist_raw_data(
        self,
        object_type: str,
        processor_object_id: str,
        raw_json: Dict,
        api_endpoint: str,
        http_status_code: int = 200,
        stripe_request_id: Optional[str] = None,
    ) -> None:
        """
        Persist raw API response to database.

        Per PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md:
        - Called after successful HTTP response
        - Called before returning to Layer 2
        - Synchronous operation
        - Feature-flag gated
        - Persistence failure = fetch failure (when enabled)

        Args:
            object_type: Type of object (payout, charge, refund, etc.)
            processor_object_id: Processor's native ID for the object
            raw_json: Verbatim API response
            api_endpoint: API endpoint called
            http_status_code: HTTP response status code
            stripe_request_id: Stripe's Request-Id header if available

        Raises:
            ConnectionError: If persistence fails and feature is enabled
        """
        # Skip if persistence not configured
        if not self._persistence:
            return

        # Skip if feature flag is disabled
        if not is_raw_data_persistence_enabled():
            return

        success, raw_data_id = self._persistence.persist_raw_data(
            processor='stripe',
            object_type=object_type,
            processor_object_id=processor_object_id,
            raw_json=raw_json,
            api_endpoint=api_endpoint,
            http_status_code=http_status_code,
            stripe_request_id=stripe_request_id,
        )

        # Per §5.1: persistence failure = fetch failure when enabled
        if not success:
            raise ConnectionError(
                f"Raw data persistence failed for {object_type} {processor_object_id}. "
                "Per Phase 2 plan §5.2.1, canonicalization cannot proceed without raw data lineage."
            )

    # =========================================================================
    # LAYER 1 API METHODS — Return raw Stripe JSON
    # =========================================================================

    def fetch_account(self) -> Dict:
        """
        Fetch Stripe account info for credential validation.

        Returns:
            Raw Stripe account JSON
        """
        return self._make_request_with_retry('/account')

    def fetch_payouts(
        self,
        created_gte: Optional[int] = None,
        created_lte: Optional[int] = None,
        limit: int = 100
    ) -> Dict:
        """
        Fetch payouts list from Stripe.

        Args:
            created_gte: Unix timestamp for created[gte] filter
            created_lte: Unix timestamp for created[lte] filter
            limit: Maximum number of payouts (max 100 per Stripe)

        Returns:
            Raw Stripe payouts list JSON
        """
        params = {'limit': min(limit, 100)}

        if created_gte is not None:
            params['created[gte]'] = created_gte

        if created_lte is not None:
            params['created[lte]'] = created_lte

        result = self._make_request_with_retry('/payouts', params)

        # Phase 2: Persist raw data (list responses use synthetic ID)
        self._persist_raw_data(
            object_type='payouts_list',
            processor_object_id=f"list_{created_gte or 0}_{created_lte or 0}_{limit}",
            raw_json=result,
            api_endpoint='/payouts',
        )

        return result

    def fetch_payout(self, payout_id: str) -> Dict:
        """
        Fetch a single payout by ID.

        Args:
            payout_id: Stripe payout ID (po_xxx)

        Returns:
            Raw Stripe payout JSON
        """
        result = self._make_request_with_retry(f'/payouts/{payout_id}')

        # Phase 2: Persist raw data
        self._persist_raw_data(
            object_type='payout',
            processor_object_id=payout_id,
            raw_json=result,
            api_endpoint=f'/payouts/{payout_id}',
        )

        return result

    def fetch_balance_transactions(
        self,
        payout_id: str,
        limit: int = 100
    ) -> Dict:
        """
        Fetch balance transactions for a payout.

        Args:
            payout_id: Stripe payout ID
            limit: Maximum number of transactions

        Returns:
            Raw Stripe balance transactions list JSON
        """
        result = self._make_request_with_retry(
            '/balance_transactions',
            params={'payout': payout_id, 'limit': limit}
        )

        # Phase 2: Persist raw data (list keyed by payout)
        self._persist_raw_data(
            object_type='balance_transactions_list',
            processor_object_id=f"payout_{payout_id}",
            raw_json=result,
            api_endpoint=f'/balance_transactions?payout={payout_id}',
        )

        return result

    def fetch_charge(self, charge_id: str) -> Dict:
        """
        Fetch a single charge by ID.

        Args:
            charge_id: Stripe charge ID (ch_xxx)

        Returns:
            Raw Stripe charge JSON
        """
        result = self._make_request_with_retry(f'/charges/{charge_id}')

        # Phase 2: Persist raw data
        self._persist_raw_data(
            object_type='charge',
            processor_object_id=charge_id,
            raw_json=result,
            api_endpoint=f'/charges/{charge_id}',
        )

        return result

    def fetch_refund(self, refund_id: str) -> Dict:
        """
        Fetch a single refund by ID.

        Args:
            refund_id: Stripe refund ID (re_xxx)

        Returns:
            Raw Stripe refund JSON
        """
        result = self._make_request_with_retry(f'/refunds/{refund_id}')

        # Phase 2: Persist raw data
        self._persist_raw_data(
            object_type='refund',
            processor_object_id=refund_id,
            raw_json=result,
            api_endpoint=f'/refunds/{refund_id}',
        )

        return result

    def fetch_balance_transaction(self, balance_transaction_id: str) -> Dict:
        """
        Fetch a single balance transaction by ID.

        Args:
            balance_transaction_id: Stripe balance transaction ID (txn_xxx)

        Returns:
            Raw Stripe balance transaction JSON
        """
        result = self._make_request_with_retry(f'/balance_transactions/{balance_transaction_id}')

        # Phase 2: Persist raw data
        self._persist_raw_data(
            object_type='balance_txn',
            processor_object_id=balance_transaction_id,
            raw_json=result,
            api_endpoint=f'/balance_transactions/{balance_transaction_id}',
        )

        return result

    def fetch_customer(self, customer_id: str) -> Dict:
        """
        Fetch a single customer by ID.

        Args:
            customer_id: Stripe customer ID (cus_xxx)

        Returns:
            Raw Stripe customer JSON
        """
        result = self._make_request_with_retry(f'/customers/{customer_id}')

        # Phase 2: Persist raw data
        self._persist_raw_data(
            object_type='customer',
            processor_object_id=customer_id,
            raw_json=result,
            api_endpoint=f'/customers/{customer_id}',
        )

        return result
