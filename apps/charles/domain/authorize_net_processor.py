"""
Module 4: Authorize.net Processor Implementation

Implements:
- PROCESSOR_NORMALIZATION_CONTRACT v1.0 (§6 Authorize.net Normalization Rules)
- MINIMUM_LOGGING_CONTRACT v1.1 (Structured logging)
- ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT v1.1 (Error classification, retry)

Handles fetching batch settlement and transaction data from Authorize.net Reporting API.

Authorize.net API Structure:
- getSettledBatchListRequest: List settled batches (equivalent to Stripe payouts)
- getTransactionListRequest: Get transactions in a batch
- getTransactionDetailsRequest: Get transaction details (for customer info)

Key concepts:
- Batch Settlement: A group of transactions settled together (maps to payout)
- Transaction: A charge or refund within a batch
- Batch ID: Unique identifier for grouping (processor_payout_id)
- Transaction ID: Unique identifier for payments (processor_payment_id, matches QBO REF NO)

================================================================================
AUTHORIZE.NET BATCH HANDLING (per AUTHORIZE_NET_INTEGRATION_PLAN §2-3)
================================================================================

A batch settlement contains multiple transaction types:

    transactionType='authCaptureTransaction'        -> ProcessorPayment
    transactionType='captureOnlyTransaction'        -> ProcessorPayment
    transactionType='priorAuthCaptureTransaction'   -> ProcessorPayment
    transactionType='refundTransaction'             -> ProcessorRefund (POSITIVE amount)
    transactionType='voidTransaction'               -> Excluded (not settled)
    transactionType='authOnlyTransaction'           -> Excluded (not settled)

AMOUNT NORMALIZATION (§2):
    - Amounts are in dollars (Decimal), NOT cents
    - Refunds stored as POSITIVE amounts (per §4.3)
    - Fees stored as POSITIVE amounts (per §4.4)
    - net_amount = gross_amount - fee_amount - refund_amount (within ±$0.01)

BATCH STATUS FILTERING (§3.4):
    - settledSuccessfully: Ingest and process
    - refundSettledSuccessfully: Ingest (refund-only batch)
    - settlementError: Log warning, skip
    - pendingSettlement: Skip (not yet settled)
================================================================================
"""

import sys
import os
import time
import requests
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any
from xml.etree import ElementTree as ET

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from module4_reconciliation.reconciliation_engine import (
    ProcessorBase,
    ProcessorPayment,
    ProcessorPayout,
    ProcessorRefund,
    ProcessorFee,
    PaymentType,
    RefundStatus,
    FeeType,
)
from shared.structured_logging import get_logger, EventType
from shared.observability_interface import get_metrics_collector
from shared.error_taxonomy import (
    classify_error,
    AuthorizationError,
    ConfigurationError,
    DataIntegrityError,
    MAX_RETRY_ATTEMPTS,
    BACKOFF_SCHEDULE_SECONDS,
)


# Authorize.net transaction types that represent settled payments
PAYMENT_TRANSACTION_TYPES = {
    'authCaptureTransaction',
    'captureOnlyTransaction',
    'priorAuthCaptureTransaction',
}

# Authorize.net transaction types that represent refunds
REFUND_TRANSACTION_TYPES = {
    'refundTransaction',
}

# Authorize.net settlement states to ingest (per §3.4)
VALID_SETTLEMENT_STATES = {
    'settledSuccessfully',
    'refundSettledSuccessfully',
}

# =============================================================================
# ACH (eCheck) CONSTANTS (per AUTHORIZE_NET_ACH_INVARIANTS)
# =============================================================================

# ACH account type identifier
ACH_ACCOUNT_TYPE = 'echeck'

# ACH fee description patterns (for identifying fee transactions)
ACH_FEE_PATTERNS = [
    'ach fee',
    'echeck fee',
    'processing fee',
    'bank fee',
]


class AuthorizeNetProcessor(ProcessorBase):
    """
    Authorize.net payment processor implementation.

    Implements PROCESSOR_NORMALIZATION_CONTRACT (Authorize.net Normalization Rules).

    Uses Authorize.net Reporting API to fetch batch settlements and their transactions.
    All data is normalized to canonical format before being returned.

    INVARIANTS (per AUTHORIZE_NET_INTEGRATION_PLAN §7):
    - INV-ANET-01: Each batchId produces exactly one payout
    - INV-ANET-02: Each batchId produces exactly one payout_batches row
    - INV-ANET-03: net_amount = gross_amount - fee_amount - refund_amount (±$0.01)
    - INV-ANET-04: Only settledSuccessfully/refundSettledSuccessfully batches ingested
    - INV-ANET-05: Each transId produces exactly one canonical_events row
    - INV-ANET-12: All monetary amounts stored as Decimal
    - INV-ANET-13: Refunds/fees stored as positive values
    - INV-ANET-14: raw_events contains complete API response
    - INV-ANET-15: assert_contract_compliance() must pass before persistence
    """

    PROCESSOR_NAME = 'authorize_net'

    # Authorize.net API endpoints
    PRODUCTION_URL = 'https://api.authorize.net/xml/v1/request.api'
    SANDBOX_URL = 'https://apitest.authorize.net/xml/v1/request.api'

    # XML namespace for Authorize.net API
    ANET_NAMESPACE = 'AnetApi/xml/v1/schema/AnetApiSchema.xsd'

    def __init__(
        self,
        api_key: str,  # API Login ID
        transaction_key: str = None,
        company_id: str = "",
        sync_run_id: str = "",
        sandbox: bool = False,
        workspace_id: str = None,
        **kwargs
    ):
        """
        Initialize Authorize.net processor.

        Args:
            api_key: Authorize.net API Login ID
            transaction_key: Authorize.net Transaction Key (required for API calls)
            company_id: Charles company ID (for canonical model)
            sync_run_id: Current sync run ID (for logging correlation)
            sandbox: If True, use sandbox API endpoint
            workspace_id: Explicit workspace scope (required)
        """
        if not workspace_id:
            raise ValueError(
                "workspace_id is required for AuthorizeNetProcessor. "
                "Caller must provide explicit workspace scope."
            )

        self.api_login_id = api_key
        self.transaction_key = transaction_key or kwargs.get('transaction_key', '')
        self.company_id = company_id
        self.sandbox = sandbox
        self.api_url = self.SANDBOX_URL if sandbox else self.PRODUCTION_URL

        # Initialize structured logger per MINIMUM_LOGGING_CONTRACT
        self.workspace_id = workspace_id
        self.logger = get_logger(
            service="authorize_net_processor",
            workspace_id=workspace_id,
            company_id=company_id
        )
        if sync_run_id:
            self.logger.set_sync_run_id(sync_run_id)

        # Validate credentials on init
        self._validate_credentials()

    def _validate_credentials(self) -> None:
        """
        Validate Authorize.net API credentials by making a test request.

        Uses getMerchantDetailsRequest to verify API Login ID and Transaction Key.

        Raises:
            AuthorizationError: If credentials are invalid (§3.1)
            ConnectionError: If unable to reach Authorize.net (§3.2)
        """
        if not self.api_login_id or not self.transaction_key:
            raise ConfigurationError(
                "Authorize.net API Login ID and Transaction Key are required"
            )

        try:
            # Build merchant details request to validate credentials
            xml_request = self._build_xml_request('getMerchantDetailsRequest', {})
            response = self._make_request_with_retry(xml_request)

            # Check for authentication error
            if self._is_auth_error(response):
                raise AuthorizationError(
                    f"Authorize.net authentication failed: {self._get_error_message(response)}"
                )

            # Extract merchant name for logging
            merchant_name = self._get_text(response, './/merchantName') or 'unknown'

            self.logger.info(
                EventType.SYNC_STARTED,
                f"Authorize.net credentials validated for merchant: {merchant_name}",
                processor="authorize_net",
                merchant_name=merchant_name
            )

        except AuthorizationError:
            self.logger.error(
                EventType.ERROR_DETECTED,
                "Invalid Authorize.net API credentials",
                error_category="authorization_error",
                error_code="ANET_AUTH_FAILED",
                processor="authorize_net"
            )
            raise

    def _build_xml_request(self, request_type: str, params: Dict[str, Any]) -> str:
        """
        Build an XML request for Authorize.net API.

        Args:
            request_type: API request type (e.g., 'getSettledBatchListRequest')
            params: Request parameters

        Returns:
            XML string for the request
        """
        # Build XML with proper namespace
        xml = f'''<?xml version="1.0" encoding="utf-8"?>
<{request_type} xmlns="{self.ANET_NAMESPACE}">
    <merchantAuthentication>
        <name>{self.api_login_id}</name>
        <transactionKey>{self.transaction_key}</transactionKey>
    </merchantAuthentication>
'''
        # Add parameters
        for key, value in params.items():
            if value is not None:
                xml += f'    <{key}>{value}</{key}>\n'

        xml += f'</{request_type}>'
        return xml

    def _make_request_with_retry(
        self,
        xml_request: str,
    ) -> ET.Element:
        """
        Make a request to Authorize.net API with retry logic.

        Implements ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT §7.1:
        - Max 3 retry attempts
        - Exponential backoff: 5s, 30s, 120s
        - Only infrastructure errors are retried

        Args:
            xml_request: XML request body

        Returns:
            Parsed XML response as ElementTree Element

        Raises:
            AuthorizationError: For authentication failures (not retried)
            ConfigurationError: For bad request (not retried)
            ConnectionError: For infrastructure errors (after retries exhausted)
        """
        last_error = None
        headers = {'Content-Type': 'application/xml'}

        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            start_time = time.time()

            try:
                response = requests.post(
                    self.api_url,
                    data=xml_request,
                    headers=headers,
                    timeout=30
                )

                duration_ms = int((time.time() - start_time) * 1000)

                if response.status_code == 200:
                    # Parse XML response
                    # Remove namespace for easier parsing
                    response_text = response.text
                    response_text = response_text.replace(f'xmlns="{self.ANET_NAMESPACE}"', '')
                    root = ET.fromstring(response_text)

                    # Check for API-level errors
                    result_code = self._get_text(root, './/resultCode')

                    if result_code == 'Ok':
                        # Success - record metrics
                        metrics = get_metrics_collector()
                        metrics.increment_counter(
                            "authorize_net_api_calls",
                            self.workspace_id,
                            {"operation": "request", "status": "success"}
                        )
                        metrics.record_histogram(
                            "authorize_net_api_duration",
                            duration_ms / 1000.0,
                            self.workspace_id,
                            {"operation": "request", "status": "success"}
                        )
                        return root

                    # API returned an error
                    # Authorize.net uses <messages><message><code>/<text> format
                    error_code = self._get_text(root, './/messages/message/code') or ''
                    error_text = self._get_text(root, './/messages/message/text') or ''

                    # Check if it's an auth error
                    if error_code in ['E00007', 'E00008', 'E00119']:
                        raise AuthorizationError(
                            f"Authorize.net authentication failed: {error_text}"
                        )

                    # Check if it's a configuration error
                    if error_code in ['E00003', 'E00004', 'E00005']:
                        raise ConfigurationError(
                            f"Authorize.net configuration error: {error_text}"
                        )

                    # Treat other errors as infrastructure (retryable)
                    error = ConnectionError(
                        f"Authorize.net API error ({error_code}): {error_text}"
                    )
                    last_error = error

                    if attempt < MAX_RETRY_ATTEMPTS:
                        delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                        self.logger.info(
                            EventType.RETRY_SCHEDULED,
                            f"Authorize.net API error, scheduling retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
                            retry_count=attempt,
                            max_retries=MAX_RETRY_ATTEMPTS,
                            backoff_delay_ms=delay * 1000,
                            error_code=error_code
                        )
                        time.sleep(delay)
                        continue
                    else:
                        self.logger.error(
                            EventType.RETRY_EXHAUSTED,
                            f"Authorize.net API failed after {MAX_RETRY_ATTEMPTS} attempts",
                            retry_count=attempt,
                            max_retries=MAX_RETRY_ATTEMPTS,
                            error_code=error_code
                        )
                        raise error

                # HTTP error status codes
                status = response.status_code

                if status in [401, 403]:
                    raise AuthorizationError(
                        f"Authorize.net API authorization failed ({status})"
                    )

                elif status == 400:
                    raise ConfigurationError(
                        f"Authorize.net API bad request: {response.text}"
                    )

                elif status in [500, 502, 503, 504]:
                    error = ConnectionError(
                        f"Authorize.net API server error ({status})"
                    )
                    last_error = error

                    if attempt < MAX_RETRY_ATTEMPTS:
                        delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                        self.logger.info(
                            EventType.RETRY_SCHEDULED,
                            f"Authorize.net server error, retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
                            retry_count=attempt,
                            backoff_delay_ms=delay * 1000,
                            status_code=status
                        )
                        time.sleep(delay)
                        continue
                    else:
                        raise error

                elif status == 429:
                    error = ConnectionError("Authorize.net API rate limit exceeded")
                    last_error = error

                    if attempt < MAX_RETRY_ATTEMPTS:
                        delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                        self.logger.info(
                            EventType.RETRY_SCHEDULED,
                            f"Authorize.net rate limited, retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
                            retry_count=attempt,
                            backoff_delay_ms=delay * 1000
                        )
                        time.sleep(delay)
                        continue
                    else:
                        raise error

                else:
                    raise ConnectionError(
                        f"Authorize.net API error ({status}): {response.text}"
                    )

            except requests.exceptions.Timeout as e:
                last_error = ConnectionError(f"Authorize.net API timeout: {e}")
                if attempt < MAX_RETRY_ATTEMPTS:
                    delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                    self.logger.info(
                        EventType.RETRY_SCHEDULED,
                        f"Authorize.net timeout, retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
                        retry_count=attempt,
                        backoff_delay_ms=delay * 1000
                    )
                    time.sleep(delay)
                    continue
                else:
                    raise last_error

            except requests.exceptions.ConnectionError as e:
                last_error = ConnectionError(f"Authorize.net connection failed: {e}")
                if attempt < MAX_RETRY_ATTEMPTS:
                    delay = BACKOFF_SCHEDULE_SECONDS[attempt - 1]
                    self.logger.info(
                        EventType.RETRY_SCHEDULED,
                        f"Authorize.net connection failed, retry {attempt}/{MAX_RETRY_ATTEMPTS} after {delay}s",
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

        # Should not reach here
        if last_error:
            raise last_error
        raise ConnectionError("Authorize.net API request failed")

    def _get_text(self, element: ET.Element, path: str) -> Optional[str]:
        """Extract text from XML element by path."""
        found = element.find(path)
        return found.text if found is not None else None

    def _is_auth_error(self, response: ET.Element) -> bool:
        """Check if response indicates an authentication error."""
        result_code = self._get_text(response, './/resultCode')
        if result_code == 'Error':
            error_code = self._get_text(response, './/error/errorCode')
            return error_code in ['E00007', 'E00008', 'E00119']
        return False

    def _get_error_message(self, response: ET.Element) -> str:
        """Extract error message from response."""
        error_text = self._get_text(response, './/error/errorText')
        return error_text or 'Unknown error'

    def fetch_payouts(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100
    ) -> List[ProcessorPayout]:
        """
        Fetch settled batches from Authorize.net (both card and ACH).

        Implements PROCESSOR_NORMALIZATION_CONTRACT (Authorize.net Payout Mapping).

        Uses getSettledBatchListRequest to fetch batch settlements.
        Also fetches ACH (eCheck) transfer payouts per INV-ANET-ACH-01.

        INV-ANET-04: Only batches with settlementState in ['settledSuccessfully',
        'refundSettledSuccessfully'] are returned.

        Args:
            start_date: Only fetch batches settled after this date
            end_date: Only fetch batches settled before this date
            limit: Maximum number of batches to fetch

        Returns:
            List of ProcessorPayout objects (payments not populated for card batches,
            fully populated for ACH payouts)
        """
        # Build date range parameters
        # IMPORTANT: Authorize.net XSD requires elements in specific order:
        # includeStatistics must come BEFORE date fields
        params = {}

        # Include statistics in response for totals (must be first per XSD)
        params['includeStatistics'] = 'true'

        # Authorize.net API expects firstSettlementDate and lastSettlementDate
        # in format: 2021-01-01T00:00:00Z
        if start_date:
            params['firstSettlementDate'] = start_date.strftime('%Y-%m-%dT00:00:00Z')

        if end_date:
            params['lastSettlementDate'] = end_date.strftime('%Y-%m-%dT23:59:59Z')
        else:
            # Default to current date if not specified
            params['lastSettlementDate'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT23:59:59Z')

        xml_request = self._build_xml_request('getSettledBatchListRequest', params)
        response = self._make_request_with_retry(xml_request)

        payouts = []
        batch_count = 0

        # Parse batch list from response - card batches only (exclude eCheck)
        for batch in response.findall('.//batch'):
            if batch_count >= limit:
                break

            # Skip eCheck batches - these are handled separately as ACH payouts
            payment_method = self._get_text(batch, 'paymentMethod')
            if payment_method and payment_method.lower() == 'echeck':
                continue

            payout = self._parse_batch(batch)
            if payout:
                payouts.append(payout)
                batch_count += 1

        card_payout_count = len(payouts)

        # Fetch ACH (eCheck) payouts per INV-ANET-ACH-01
        # ACH payouts are grouped by transfer date (one per day)
        remaining_limit = max(0, limit - len(payouts))
        if remaining_limit > 0:
            ach_payouts = self.fetch_ach_payouts(start_date, end_date, remaining_limit)
            payouts.extend(ach_payouts)

        self.logger.info(
            EventType.SYNC_STARTED,
            f"Fetched {len(payouts)} payouts from Authorize.net "
            f"({card_payout_count} card batches, {len(payouts) - card_payout_count} ACH transfers)",
            processor="authorize_net",
            payout_count=len(payouts),
            card_payout_count=card_payout_count,
            ach_payout_count=len(payouts) - card_payout_count
        )

        return payouts

    def _parse_batch(self, batch: ET.Element) -> Optional[ProcessorPayout]:
        """
        Parse Authorize.net batch element into canonical ProcessorPayout.

        INV-ANET-04: Only batches with valid settlement states are processed.

        Args:
            batch: XML element containing batch data

        Returns:
            ProcessorPayout or None if batch should be skipped
        """
        # Get settlement state
        settlement_state = self._get_text(batch, 'settlementState')

        # INV-ANET-04: Only process valid settlement states
        if settlement_state not in VALID_SETTLEMENT_STATES:
            batch_id = self._get_text(batch, 'batchId') or 'unknown'
            self.logger.info(
                EventType.PAYOUT_PROCESSING_STARTED,
                f"Skipping batch {batch_id} with state: {settlement_state}",
                processor="authorize_net",
                processor_payout_id=batch_id,
                settlement_state=settlement_state
            )
            return None

        batch_id = self._get_text(batch, 'batchId')
        if not batch_id:
            return None

        # Parse settlement time
        # A truly settled batch MUST have settlementTimeUTC - skip if missing
        settlement_time_str = self._get_text(batch, 'settlementTimeUTC')
        if not settlement_time_str:
            self.logger.info(
                EventType.PAYOUT_PROCESSING_STARTED,
                f"Skipping batch {batch_id}: missing settlementTimeUTC despite state={settlement_state}",
                processor="authorize_net",
                processor_payout_id=batch_id,
                settlement_state=settlement_state
            )
            return None

        settlement_date = None
        arrival_date = None
        created_at = None

        if settlement_time_str:
            try:
                # Parse ISO format: 2024-01-15T10:30:00Z
                settlement_dt = datetime.fromisoformat(
                    settlement_time_str.replace('Z', '+00:00')
                )
                settlement_date = settlement_dt.date()
                arrival_date = settlement_date  # Same day for Authorize.net
                created_at = settlement_dt
            except (ValueError, TypeError):
                settlement_date = date.today()
                arrival_date = settlement_date

        # Get batch statistics for gross amount
        # Per §2.1: gross_amount computed from transactions during detail fetch
        # Here we initialize to 0, will be updated in fetch_payout_details

        # Extract payment method and market type for metadata
        payment_method = self._get_text(batch, 'paymentMethod')
        market_type = self._get_text(batch, 'marketType')

        # Map settlement state to Charles status
        status = 'complete' if settlement_state in VALID_SETTLEMENT_STATES else 'pending'

        payout = ProcessorPayout(
            processor_payout_id=batch_id,
            processor=self.PROCESSOR_NAME,
            company_id=self.company_id,
            gross_amount=Decimal('0'),      # Calculated in fetch_payout_details
            fee_amount=Decimal('0'),        # Calculated in fetch_payout_details
            refund_amount=Decimal('0'),     # Calculated in fetch_payout_details
            net_amount=Decimal('0'),        # Calculated in fetch_payout_details
            currency='USD',                 # Authorize.net is typically USD only
            created_at=created_at,
            arrival_date=arrival_date,
            settlement_date=settlement_date,
            status=status,
            description=f'Authorize.net Batch {batch_id}',
            metadata={
                'authorize_net_batch': {
                    'batchId': batch_id,
                    'settlementState': settlement_state,
                    'settlementTimeUTC': settlement_time_str,
                    'paymentMethod': payment_method,
                    'marketType': market_type,
                }
            }
        )

        return payout

    def fetch_payout_details(self, payout: ProcessorPayout) -> ProcessorPayout:
        """
        Fetch complete batch details including all transactions.

        Implements PROCESSOR_NORMALIZATION_CONTRACT (Authorize.net Transaction Processing).

        For card batches, this method:
        1. Uses metadata from input payout (settlement_date, etc. from fetch_available_payouts)
        2. Fetches all transactions for the batch via getTransactionListRequest
        3. Categorizes transactions into payments and refunds
        4. Calculates gross, fee, refund, and net amounts
        5. Validates the payout balance invariant (INV-ANET-03)

        For ACH payouts (per INV-ANET-ACH-01):
        - Payout is already fully populated by fetch_ach_payouts()
        - Validates ACH-specific balance invariant (net = gross - refunds)

        Args:
            payout: ProcessorPayout with metadata from fetch_available_payouts

        Returns:
            ProcessorPayout enriched with complete payment/refund lists

        Raises:
            DataIntegrityError: If payout balance validation fails (INV-ANET-03)
        """
        # Check if this is an ACH payout (per INV-ANET-ACH-01)
        # ACH payouts are already fully populated by fetch_ach_payouts()
        is_ach_payout = (
            payout.metadata and
            payout.metadata.get('payout_type') == 'ach_transfer'
        )

        if is_ach_payout:
            return self.fetch_payout_details_ach(payout)
        start_time = time.time()
        payout_id = payout.processor_payout_id

        # Use settlement data from input payout (already fetched in fetch_available_payouts)
        settlement_date = payout.settlement_date
        arrival_date = payout.arrival_date
        created_at = payout.created_at
        batch_metadata = payout.metadata or {}

        # Fetch transactions for this batch
        txn_params = {'batchId': payout_id}
        xml_request = self._build_xml_request('getTransactionListRequest', txn_params)
        response = self._make_request_with_retry(xml_request)

        payments = []
        refunds = []
        fees = []
        total_gross = Decimal('0')
        total_fees = Decimal('0')
        total_refunds = Decimal('0')

        # Process each transaction in the batch
        # Note: getTransactionListRequest returns transactionStatus, not transactionType
        # Use transactionStatus to categorize: settledSuccessfully=payment, refundSettledSuccessfully=refund
        for txn in response.findall('.//transaction'):
            txn_status = self._get_text(txn, 'transactionStatus')
            trans_id = self._get_text(txn, 'transId')

            if txn_status == 'settledSuccessfully':
                # Process as payment
                payment = self._process_payment_transaction(txn, payout_id)
                if payment:
                    payments.append(payment)
                    total_gross += payment.amount

            elif txn_status == 'refundSettledSuccessfully':
                # Process as refund (INV-ANET-13: stored as positive)
                refund = self._process_refund_transaction(txn, payout_id)
                if refund:
                    refunds.append(refund)
                    total_refunds += refund.amount

            # Note: Other statuses (voided, declined, etc.) are skipped
            # as they don't represent settled funds

        # Per §2.3: Fees are batch-level in Authorize.net
        # If fee information is available from the batch statistics, use it
        # Otherwise, fee_amount = 0 (handled via separate monthly reconciliation)
        # Note: Authorize.net Reporting API doesn't expose fees directly

        # Calculate net amount: gross - fees - refunds
        net_amount = total_gross - total_fees - total_refunds

        # Build the enriched payout, preserving metadata from input
        enriched_payout = ProcessorPayout(
            processor_payout_id=payout_id,
            processor=self.PROCESSOR_NAME,
            company_id=self.company_id,
            gross_amount=total_gross,
            fee_amount=total_fees,
            fees=total_fees,  # Alias
            refund_amount=total_refunds,
            net_amount=net_amount,
            currency='USD',
            created_at=created_at,
            arrival_date=arrival_date,
            settlement_date=settlement_date,
            status=payout.status,  # Preserve status from input payout
            description=payout.description or f'Authorize.net Batch {payout_id}',
            payments=payments,
            payment_count=len(payments),
            refunds=refunds,
            refund_count=len(refunds),
            fee_details=fees,
            metadata=batch_metadata
        )

        duration_ms = int((time.time() - start_time) * 1000)

        # Validate payout balance per INV-ANET-03
        if not enriched_payout.validate_balance():
            expected = enriched_payout.gross_amount - enriched_payout.fee_amount - enriched_payout.refund_amount
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Payout balance validation failed for batch {payout_id}",
                error_category="data_integrity_error",
                error_code="PAYOUT_BALANCE_MISMATCH",
                processor="authorize_net",
                processor_payout_id=payout_id,
                gross_amount=str(enriched_payout.gross_amount),
                fee_amount=str(enriched_payout.fee_amount),
                refund_amount=str(enriched_payout.refund_amount),
                net_amount=str(enriched_payout.net_amount),
                expected_net=str(expected),
                duration_ms=duration_ms
            )
            raise DataIntegrityError(
                f"Batch {payout_id} balance mismatch: "
                f"expected net={expected}, actual net={enriched_payout.net_amount}"
            )

        # Log successful normalization
        self.logger.info(
            EventType.PAYOUT_PROCESSING_STARTED,
            f"Batch {payout_id} normalized: {len(payments)} payments, "
            f"{len(refunds)} refunds, fees=${total_fees}, net=${enriched_payout.net_amount}",
            processor="authorize_net",
            processor_payout_id=payout_id,
            payment_count=len(payments),
            refund_count=len(refunds),
            gross_amount=str(total_gross),
            fee_amount=str(total_fees),
            refund_amount=str(total_refunds),
            net_amount=str(enriched_payout.net_amount),
            duration_ms=duration_ms
        )

        # ===================================================================
        # CONTRACT ASSERTION: PROCESSOR_NORMALIZATION_CONTRACT
        # INV-ANET-15: Assert compliance before returning
        # ===================================================================
        try:
            enriched_payout.assert_contract_compliance()
        except AssertionError as e:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Contract assertion failed for batch {payout_id}: {e}",
                error_category="data_integrity_error",
                error_code="CONTRACT_VIOLATION",
                processor="authorize_net",
                processor_payout_id=payout_id,
                contract="PROCESSOR_NORMALIZATION_CONTRACT"
            )
            raise DataIntegrityError(str(e))

        return enriched_payout

    def _process_payment_transaction(
        self,
        txn: ET.Element,
        payout_id: str
    ) -> Optional[ProcessorPayment]:
        """
        Process a payment transaction from the transaction list.

        Per §2.2 (Transaction → ProcessorPayment mapping).

        Args:
            txn: XML element containing transaction data
            payout_id: Parent batch ID

        Returns:
            ProcessorPayment or None if processing fails
        """
        try:
            trans_id = self._get_text(txn, 'transId')
            if not trans_id:
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    "Transaction has no transId",
                    warning_type="missing_trans_id"
                )
                return None

            # Get amount - in dollars (not cents like Stripe)
            amount_str = self._get_text(txn, 'settleAmount')
            amount = Decimal(amount_str) if amount_str else Decimal('0')

            # Get submit time for processor_timestamp
            submit_time_str = self._get_text(txn, 'submitTimeUTC')
            processor_timestamp = None
            if submit_time_str:
                try:
                    processor_timestamp = datetime.fromisoformat(
                        submit_time_str.replace('Z', '+00:00')
                    )
                except (ValueError, TypeError):
                    processor_timestamp = datetime.now(timezone.utc)

            # Get customer info directly from transaction
            # Per §6.1: Authorize.net includes email in transaction response
            customer_id = None
            customer_email = None
            customer_name = None

            # Check for customer profile ID
            profile_elem = txn.find('.//profile')
            if profile_elem is not None:
                customer_id = self._get_text(profile_elem, 'customerProfileId')

            # Get email from customer element
            customer_elem = txn.find('.//customer')
            if customer_elem is not None:
                customer_email = self._get_text(customer_elem, 'email')

            # Get name - check billTo (detail response) or direct fields (list response)
            bill_to = txn.find('.//billTo')
            if bill_to is not None:
                first_name = self._get_text(bill_to, 'firstName') or ''
                last_name = self._get_text(bill_to, 'lastName') or ''
                customer_name = f"{first_name} {last_name}".strip() or None
            else:
                # List response has firstName/lastName directly on transaction
                first_name = self._get_text(txn, 'firstName') or ''
                last_name = self._get_text(txn, 'lastName') or ''
                customer_name = f"{first_name} {last_name}".strip() or None

            # Get payment method details
            payment_elem = txn.find('.//payment')
            card_brand = None
            card_last4 = None
            payment_method = 'card'

            if payment_elem is not None:
                credit_card = payment_elem.find('creditCard')
                if credit_card is not None:
                    card_number = self._get_text(credit_card, 'cardNumber')
                    if card_number:
                        # Format: XXXX1234 - extract last 4
                        card_last4 = card_number[-4:] if len(card_number) >= 4 else card_number
                    card_type = self._get_text(credit_card, 'cardType')
                    if card_type:
                        card_brand = card_type.lower()

                bank_account = payment_elem.find('bankAccount')
                if bank_account is not None:
                    payment_method = 'bank_account'
            else:
                # List response has accountType/accountNumber directly on transaction
                account_type = self._get_text(txn, 'accountType')
                account_number = self._get_text(txn, 'accountNumber')
                if account_type:
                    if account_type.lower() == 'echeck':
                        payment_method = 'bank_account'
                    else:
                        card_brand = account_type.lower()
                if account_number:
                    # Format: XXXX1234 - extract last 4
                    card_last4 = account_number[-4:] if len(account_number) >= 4 else account_number

            # Get invoice number for description
            invoice_number = self._get_text(txn, 'invoiceNumber')
            description = f"Transaction {trans_id}"
            if invoice_number:
                description = f"Invoice {invoice_number}"

            return ProcessorPayment(
                processor_payment_id=trans_id,
                processor=self.PROCESSOR_NAME,
                processor_payout_id=payout_id,
                payment_type=PaymentType.CHARGE,
                amount=amount,
                amount_gross=amount,
                fees=Decimal('0'),  # Per-transaction fees not available in Reporting API
                amount_net=amount,   # Same as gross since fees are batch-level
                currency='USD',
                processor_customer_id=customer_id,
                customer_email=customer_email,
                customer_name=customer_name,
                payment_method=payment_method,
                card_brand=card_brand,
                card_last4=card_last4,
                processor_timestamp=processor_timestamp,
                invoice_id=invoice_number,
                description=description,
                metadata={
                    'transaction_type': self._get_text(txn, 'transactionType'),
                    'transaction_status': self._get_text(txn, 'transactionStatus'),
                    'invoice_number': invoice_number,
                }
            )

        except Exception as e:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Error processing payment transaction: {e}",
                error_category="infrastructure_error",
                trans_id=self._get_text(txn, 'transId')
            )
            return None

    def _process_refund_transaction(
        self,
        txn: ET.Element,
        payout_id: str
    ) -> Optional[ProcessorRefund]:
        """
        Process a refund transaction.

        Per §2.4 (Refund Handling):
        - ProcessorRefund.amount stored as POSITIVE (INV-ANET-13)
        - Links to original payment via refTransId field
        - Sign applied during QBO Deposit construction

        Args:
            txn: XML element containing refund transaction data
            payout_id: Parent batch ID

        Returns:
            ProcessorRefund with positive amount, or None if processing fails
        """
        try:
            trans_id = self._get_text(txn, 'transId')
            if not trans_id:
                return None

            # Get amount - store as POSITIVE per INV-ANET-13
            amount_str = self._get_text(txn, 'settleAmount')
            raw_amount = Decimal(amount_str) if amount_str else Decimal('0')
            amount = abs(raw_amount)  # Always positive

            # Get original transaction ID (the payment being refunded)
            ref_trans_id = self._get_text(txn, 'refTransId')

            # Get submit time
            submit_time_str = self._get_text(txn, 'submitTimeUTC')
            refund_date = None
            if submit_time_str:
                try:
                    refund_date = datetime.fromisoformat(
                        submit_time_str.replace('Z', '+00:00')
                    )
                except (ValueError, TypeError):
                    refund_date = datetime.now(timezone.utc)

            self.logger.debug(
                EventType.PAYOUT_PROCESSING_STARTED,
                f"Processing refund {trans_id}: ${amount} (original: {ref_trans_id})",
                processor="authorize_net",
                processor_refund_id=trans_id,
                amount=str(amount),
                original_payment_id=ref_trans_id
            )

            return ProcessorRefund(
                processor_refund_id=trans_id,
                processor=self.PROCESSOR_NAME,
                processor_payout_id=payout_id,
                processor_payment_id=ref_trans_id or "",
                amount=amount,  # POSITIVE per INV-ANET-13
                currency='USD',
                refund_date=refund_date,
                status=RefundStatus.SUCCEEDED,
                reason='refund',
                metadata={
                    'transaction_type': self._get_text(txn, 'transactionType'),
                    'original_trans_id': ref_trans_id,
                }
            )

        except Exception as e:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Error processing refund transaction: {e}",
                error_category="infrastructure_error",
                trans_id=self._get_text(txn, 'transId')
            )
            return None

    def fetch_payment_details(self, payment_id: str) -> ProcessorPayment:
        """
        Fetch details for a single transaction.

        Uses getTransactionDetailsRequest for full transaction data.

        Args:
            payment_id: Authorize.net Transaction ID (transId)

        Returns:
            ProcessorPayment object
        """
        params = {'transId': payment_id}
        xml_request = self._build_xml_request('getTransactionDetailsRequest', params)
        response = self._make_request_with_retry(xml_request)

        # Get the transaction element
        txn = response.find('.//transaction')
        if txn is None:
            raise DataIntegrityError(f"Transaction {payment_id} not found")

        # Get basic info
        trans_id = self._get_text(txn, 'transId')
        settle_amount = self._get_text(txn, 'settleAmount')
        amount = Decimal(settle_amount) if settle_amount else Decimal('0')

        # Get submit time
        submit_time_str = self._get_text(txn, 'submitTimeUTC')
        processor_timestamp = None
        if submit_time_str:
            try:
                processor_timestamp = datetime.fromisoformat(
                    submit_time_str.replace('Z', '+00:00')
                )
            except (ValueError, TypeError):
                processor_timestamp = datetime.now(timezone.utc)

        # Get customer info
        customer_id = self._get_text(txn, './/customer/id')
        customer_email = self._get_text(txn, './/customer/email')

        # Get name from billTo
        first_name = self._get_text(txn, './/billTo/firstName') or ''
        last_name = self._get_text(txn, './/billTo/lastName') or ''
        customer_name = f"{first_name} {last_name}".strip() or None

        # Get payment method details
        card_number = self._get_text(txn, './/payment/creditCard/cardNumber')
        card_last4 = card_number[-4:] if card_number and len(card_number) >= 4 else None
        card_type = self._get_text(txn, './/payment/creditCard/cardType')
        card_brand = card_type.lower() if card_type else None

        # Check for bank account
        payment_method = 'card'
        if txn.find('.//payment/bankAccount') is not None:
            payment_method = 'bank_account'

        # Get invoice number
        invoice_number = self._get_text(txn, './/order/invoiceNumber')
        description = self._get_text(txn, './/order/description') or f"Transaction {trans_id}"

        return ProcessorPayment(
            processor_payment_id=trans_id,
            processor=self.PROCESSOR_NAME,
            processor_payout_id="",  # Not known in this context
            payment_type=PaymentType.CHARGE,
            amount=amount,
            amount_gross=amount,
            fees=Decimal('0'),
            amount_net=amount,
            currency='USD',
            processor_customer_id=customer_id,
            customer_email=customer_email,
            customer_name=customer_name,
            payment_method=payment_method,
            card_brand=card_brand,
            card_last4=card_last4,
            processor_timestamp=processor_timestamp,
            invoice_id=invoice_number,
            description=description,
            metadata={'transaction_details': True}
        )

    def is_payout_complete(self, payout: ProcessorPayout) -> bool:
        """
        Check if an Authorize.net batch is complete.

        Per §3.4, batches with settlementState in ['settledSuccessfully',
        'refundSettledSuccessfully'] are considered complete.
        """
        # Check status mapped from settlement state
        if payout.status == 'complete':
            return True

        # Also check metadata for settlement state
        metadata = payout.metadata or {}
        batch_info = metadata.get('authorize_net_batch', {})
        settlement_state = batch_info.get('settlementState')

        return settlement_state in VALID_SETTLEMENT_STATES

    # =========================================================================
    # ACH (eCheck) SUPPORT METHODS
    # Per AUTHORIZE_NET_ACH_INVARIANTS contract
    # =========================================================================

    def _is_ach_transaction(self, txn: ET.Element) -> bool:
        """
        Check if a transaction is an ACH (eCheck) transaction.

        Per INV-ANET-ACH-01: ACH transactions are identified by accountType='eCheck'.

        Args:
            txn: XML element containing transaction data

        Returns:
            True if transaction is ACH, False otherwise
        """
        account_type = self._get_text(txn, 'accountType')
        if account_type and account_type.lower() == ACH_ACCOUNT_TYPE:
            return True

        # Also check payment element for bankAccount
        payment_elem = txn.find('.//payment')
        if payment_elem is not None:
            if payment_elem.find('bankAccount') is not None:
                return True

        return False

    def _is_ach_fee_transaction(self, txn: ET.Element) -> bool:
        """
        Check if a transaction is an ACH fee transaction.

        Per INV-ANET-ACH-04: ACH fees are separate transactions that
        occur once per day on the same date as the ACH transfer.

        Fee transactions are identified by:
        - Transaction type indicates fee/adjustment
        - Amount is negative (or transactionType indicates fee)
        - Description contains fee-related keywords

        Args:
            txn: XML element containing transaction data

        Returns:
            True if transaction is an ACH fee, False otherwise
        """
        if not self._is_ach_transaction(txn):
            return False

        # Check description for fee patterns
        description = (self._get_text(txn, 'description') or '').lower()
        for pattern in ACH_FEE_PATTERNS:
            if pattern in description:
                return True

        # Check transaction type for fee indicators
        txn_type = (self._get_text(txn, 'transactionType') or '').lower()
        if 'fee' in txn_type or 'adjustment' in txn_type:
            return True

        return False

    # Minimum ACH lookback in calendar days (~10 business days + buffer)
    # ACH deposits arrive ~7-10 business days after batch settlement
    # We need batches from this far back to explain today's QBO deposits
    ACH_MINIMUM_LOOKBACK_DAYS = 14

    def fetch_ach_payouts(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100
    ) -> List[ProcessorPayout]:
        """
        Fetch ACH (eCheck) payouts from Authorize.net.

        Per INV-ANET-ACH-01: ACH payouts originate from Transfer-level reporting.
        Per INV-ANET-ACH-02: Creates exactly one payout per ACH transfer date.

        IMPORTANT: ACH uses an extended lookback window (minimum 14 days) regardless
        of the sync_days setting. This ensures batches are available for deposit-driven
        reconciliation, since ACH deposits arrive ~7-10 business days after settlement.

        This method:
        1. Fetches all settled batches in the date range (with extended ACH lookback)
        2. Filters for eCheck transactions
        3. Groups by transfer date (one payout per day)
        4. Builds ACH payouts with gross-only balance (fees NOT netted)

        Args:
            start_date: Only fetch payouts after this date (extended for ACH if too recent)
            end_date: Only fetch payouts before this date
            limit: Maximum number of payouts to fetch

        Returns:
            List of ProcessorPayout objects for ACH transfers
        """
        # Fetch all settled batches
        params = {'includeStatistics': 'true'}

        # ACH requires extended lookback to ensure batches are synced before deposits arrive
        # Per ACH reconciliation plan: deposits arrive ~7-10 business days after settlement
        today = date.today()
        ach_minimum_start = today - timedelta(days=self.ACH_MINIMUM_LOOKBACK_DAYS)

        # Use the earlier of: provided start_date or ACH minimum lookback
        effective_start = start_date
        if start_date is None or start_date > ach_minimum_start:
            effective_start = ach_minimum_start
            self.logger.info(
                EventType.SYNC_STARTED,
                f"ACH sync: Extended lookback to {effective_start} (minimum {self.ACH_MINIMUM_LOOKBACK_DAYS} days for deposit reconciliation)",
                processor="authorize_net",
                requested_start=start_date.isoformat() if start_date else None,
                effective_start=effective_start.isoformat()
            )

        if effective_start:
            params['firstSettlementDate'] = effective_start.strftime('%Y-%m-%dT00:00:00Z')

        if end_date:
            params['lastSettlementDate'] = end_date.strftime('%Y-%m-%dT23:59:59Z')
        else:
            params['lastSettlementDate'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT23:59:59Z')

        xml_request = self._build_xml_request('getSettledBatchListRequest', params)
        response = self._make_request_with_retry(xml_request)

        # Collect all ACH transactions grouped by transfer date
        ach_transactions_by_date: Dict[date, Dict[str, list]] = {}

        for batch in response.findall('.//batch'):
            settlement_state = self._get_text(batch, 'settlementState')
            if settlement_state not in VALID_SETTLEMENT_STATES:
                continue

            batch_id = self._get_text(batch, 'batchId')
            if not batch_id:
                continue

            # Fetch transactions for this batch
            txn_params = {'batchId': batch_id}
            txn_request = self._build_xml_request('getTransactionListRequest', txn_params)
            txn_response = self._make_request_with_retry(txn_request)

            for txn in txn_response.findall('.//transaction'):
                if not self._is_ach_transaction(txn):
                    continue

                # Get transaction date for grouping
                submit_time_str = self._get_text(txn, 'submitTimeUTC')
                if not submit_time_str:
                    continue

                try:
                    submit_dt = datetime.fromisoformat(submit_time_str.replace('Z', '+00:00'))
                    transfer_date = submit_dt.date()
                except (ValueError, TypeError):
                    continue

                # Initialize date bucket if needed
                if transfer_date not in ach_transactions_by_date:
                    ach_transactions_by_date[transfer_date] = {
                        'payments': [],
                        'refunds': [],
                        'fees': [],
                        'batch_ids': set()
                    }

                ach_transactions_by_date[transfer_date]['batch_ids'].add(batch_id)

                # Classify transaction
                txn_status = self._get_text(txn, 'transactionStatus')

                if self._is_ach_fee_transaction(txn):
                    ach_transactions_by_date[transfer_date]['fees'].append(txn)
                elif txn_status == 'settledSuccessfully':
                    ach_transactions_by_date[transfer_date]['payments'].append(txn)
                elif txn_status == 'refundSettledSuccessfully':
                    ach_transactions_by_date[transfer_date]['refunds'].append(txn)

        # Build ACH payouts (one per transfer date)
        payouts = []
        sorted_dates = sorted(ach_transactions_by_date.keys(), reverse=True)

        for transfer_date in sorted_dates[:limit]:
            txn_data = ach_transactions_by_date[transfer_date]
            payout = self._build_ach_payout(transfer_date, txn_data)
            if payout:
                payouts.append(payout)

        self.logger.info(
            EventType.SYNC_STARTED,
            f"Fetched {len(payouts)} ACH transfer payouts from Authorize.net",
            processor="authorize_net",
            payout_count=len(payouts),
            payout_type="ach_transfer"
        )

        return payouts

    def _build_ach_payout(
        self,
        transfer_date: date,
        txn_data: Dict[str, list]
    ) -> Optional[ProcessorPayout]:
        """
        Build an ACH payout for a single transfer date.

        Per INV-ANET-ACH-02: One ACH payout per transfer date.
        Per INV-ANET-ACH-03: ACH deposits are gross-only (fees NOT netted).
        Per INV-ANET-ACH-04: ACH fees tracked separately, not in deposit.

        Args:
            transfer_date: The ACH transfer date
            txn_data: Dict with 'payments', 'refunds', 'fees', 'batch_ids'

        Returns:
            ProcessorPayout with ACH-specific balance model
        """
        payments_xml = txn_data.get('payments', [])
        refunds_xml = txn_data.get('refunds', [])
        fees_xml = txn_data.get('fees', [])
        batch_ids = txn_data.get('batch_ids', set())

        # Skip if no payments
        if not payments_xml and not refunds_xml:
            return None

        # Generate unique payout ID per INV-ANET-ACH-02
        processor_payout_id = f"ach_transfer_{transfer_date.isoformat()}"

        # Process payments
        payments = []
        total_gross = Decimal('0')

        for txn in payments_xml:
            payment = self._process_payment_transaction(txn, processor_payout_id)
            if payment:
                # Ensure payment_method is set correctly for ACH
                payment.payment_method = 'bank_account'
                payments.append(payment)
                total_gross += payment.amount

        # Process refunds
        refunds = []
        total_refunds = Decimal('0')

        for txn in refunds_xml:
            refund = self._process_refund_transaction(txn, processor_payout_id)
            if refund:
                refunds.append(refund)
                total_refunds += refund.amount

        # Calculate ACH fee total (for metadata, NOT netted from deposit)
        total_ach_fees = Decimal('0')
        fee_details = []

        for txn in fees_xml:
            amount_str = self._get_text(txn, 'settleAmount')
            if amount_str:
                fee_amount = abs(Decimal(amount_str))
                total_ach_fees += fee_amount
                fee_details.append({
                    'trans_id': self._get_text(txn, 'transId'),
                    'amount': str(fee_amount),
                    'description': self._get_text(txn, 'description')
                })

        # Per INV-ANET-ACH-03: net_amount = gross_amount - refund_amount
        # Fees are NOT netted from the deposit amount
        net_amount = total_gross - total_refunds

        # Build metadata with ACH-specific info
        metadata = {
            'payout_type': 'ach_transfer',
            'ach_fees': str(total_ach_fees),  # Track actual fees for reporting
            'ach_fee_details': fee_details,   # Fee breakdown for expense creation
            'source_batch_ids': list(batch_ids),
            'authorize_net_batch': {
                'batchId': processor_payout_id,
                'settlementState': 'settledSuccessfully',
                'settlementTimeUTC': transfer_date.isoformat() + 'T00:00:00Z',
                'paymentMethod': 'eCheck',
            }
        }

        payout = ProcessorPayout(
            processor_payout_id=processor_payout_id,
            processor=self.PROCESSOR_NAME,
            company_id=self.company_id,
            gross_amount=total_gross,
            fee_amount=Decimal('0'),  # Per decision: store 0 to preserve CHECK constraint
            fees=Decimal('0'),        # Alias - actual fees in metadata.ach_fees
            refund_amount=total_refunds,
            net_amount=net_amount,    # gross - refunds (NO fee subtraction)
            currency='USD',
            created_at=datetime.combine(transfer_date, datetime.min.time()).replace(tzinfo=timezone.utc),
            arrival_date=transfer_date,
            settlement_date=transfer_date,
            status='complete',
            description=f'Authorize.net ACH Transfer {transfer_date.isoformat()}',
            payments=payments,
            payment_count=len(payments),
            refunds=refunds,
            refund_count=len(refunds),
            fee_details=[],  # Empty - fees handled via metadata
            metadata=metadata
        )

        self.logger.info(
            EventType.PAYOUT_PROCESSING_STARTED,
            f"Built ACH payout for {transfer_date}: {len(payments)} payments, "
            f"{len(refunds)} refunds, fees=${total_ach_fees} (not netted), net=${net_amount}",
            processor="authorize_net",
            processor_payout_id=processor_payout_id,
            payment_count=len(payments),
            refund_count=len(refunds),
            gross_amount=str(total_gross),
            ach_fees=str(total_ach_fees),
            net_amount=str(net_amount),
            payout_type="ach_transfer"
        )

        return payout

    def fetch_payout_details_ach(self, payout: ProcessorPayout) -> ProcessorPayout:
        """
        Fetch complete details for an ACH payout.

        For ACH payouts, the payout is already fully populated by fetch_ach_payouts()
        since ACH payouts are built by grouping transactions by date.

        This method validates the ACH-specific balance invariant and returns the payout.

        Args:
            payout: ProcessorPayout from fetch_ach_payouts()

        Returns:
            ProcessorPayout with validated balance (unchanged)
        """
        # Validate ACH-specific balance: net = gross - refunds (no fee subtraction)
        expected_net = payout.gross_amount - payout.refund_amount
        if abs(payout.net_amount - expected_net) >= Decimal('0.01'):
            raise DataIntegrityError(
                f"ACH payout {payout.processor_payout_id} balance mismatch: "
                f"expected net={expected_net} (gross={payout.gross_amount} - refunds={payout.refund_amount}), "
                f"actual net={payout.net_amount}"
            )

        # ACH payout is already complete
        return payout

    def get_batch_transaction_ids(self, batch_id: str) -> List[str]:
        """
        Get all transaction IDs from a settlement batch.

        Per AUTHORIZE_NET_ACH_MANUAL_DEPOSIT_CONTRACT §4:
        - Calls getTransactionListRequest(batchId)
        - Extracts all transId values from settled transactions
        - Returns list of transIds for deterministic QBO resolution

        Args:
            batch_id: Authorize.net settlement batch ID

        Returns:
            List of transId strings from the batch

        Raises:
            DataIntegrityError: If batch doesn't exist or has no transactions
        """
        self.logger.info(
            EventType.SYNC_STARTED,
            f"Fetching transaction IDs for batch {batch_id}",
            processor="authorize_net",
            batch_id=batch_id
        )

        # Fetch transactions for this batch
        txn_params = {'batchId': batch_id}
        xml_request = self._build_xml_request('getTransactionListRequest', txn_params)

        try:
            response = self._make_request_with_retry(xml_request)
        except Exception as e:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Failed to fetch transactions for batch {batch_id}: {e}",
                error_category="api_error",
                error_code="BATCH_FETCH_FAILED",
                processor="authorize_net",
                batch_id=batch_id
            )
            raise DataIntegrityError(f"Failed to fetch batch {batch_id}: {e}")

        # Check for API errors
        result_code = self._get_text(response, './/resultCode')
        if result_code == 'Error':
            error_text = self._get_error_message(response)
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Authorize.net API error for batch {batch_id}: {error_text}",
                error_category="api_error",
                error_code="BATCH_NOT_FOUND",
                processor="authorize_net",
                batch_id=batch_id
            )
            raise DataIntegrityError(f"Batch {batch_id} not found or inaccessible: {error_text}")

        # Extract transIds from settled transactions
        trans_ids = []
        for txn in response.findall('.//transaction'):
            txn_status = self._get_text(txn, 'transactionStatus')
            trans_id = self._get_text(txn, 'transId')

            # Per contract: only include settled transactions
            if txn_status in ('settledSuccessfully', 'refundSettledSuccessfully') and trans_id:
                trans_ids.append(trans_id)

        if not trans_ids:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Batch {batch_id} has no settled transactions",
                error_category="data_integrity_error",
                error_code="BATCH_EMPTY",
                processor="authorize_net",
                batch_id=batch_id
            )
            raise DataIntegrityError(f"Batch {batch_id} has no settled transactions")

        self.logger.info(
            EventType.SYNC_COMPLETED,
            f"Found {len(trans_ids)} transaction IDs in batch {batch_id}",
            processor="authorize_net",
            batch_id=batch_id,
            transaction_count=len(trans_ids)
        )

        return trans_ids
