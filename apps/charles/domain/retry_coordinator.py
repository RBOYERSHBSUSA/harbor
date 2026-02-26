"""
Module 4: Stripe Processor Implementation

Implements:
- PROCESSOR_NORMALIZATION_CONTRACT v1.0 (§5 Stripe Normalization Rules)
- MINIMUM_LOGGING_CONTRACT v1.1 (Structured logging)
- ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT v1.1 (Error classification, retry)

Handles fetching payment and payout data from Stripe's API.

Stripe API Structure:
- /v1/payouts - List payouts (money sent to merchant's bank)
- /v1/balance_transactions?payout={id} - Get transactions in a payout
- /v1/charges/{id} - Get charge details (for customer info)
- /v1/customers/{id} - Get customer details (for email)
- /v1/refunds/{id} - Get refund details (for original charge reference)

Key concepts:
- Payout: Transfer of funds from Stripe to merchant's bank account
- Balance Transaction: Entry in the balance ledger (charges, fees, payouts)
- Charge: A payment from a customer
- Refund: Money returned to a customer (reduces the payout)

================================================================================
PAYOUT BREAKDOWN HANDLING (per §5.2)
================================================================================

A Stripe payout contains multiple balance transaction types:

    reporting_category='charge'  → Positive amount, linked to QBO Payment
    reporting_category='refund'  → POSITIVE in canonical (sign applied later)
    reporting_category='fee'     → Standalone fees (not per-charge)
    reporting_category='payout'  → The payout itself (ignored)

AMOUNT NORMALIZATION (§5):
    - Amounts in cents are converted to Decimal (÷100)
    - Refunds stored as POSITIVE amounts (per §4.3)
    - Fees stored as POSITIVE amounts (per §4.4)
    - net_amount: AUTHORITATIVE - comes directly from Stripe payout object

The QBO deposit MUST equal net_amount to match the bank transaction.
================================================================================
"""

import sys
import os
import time
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import List, Optional, Dict, Any, Tuple

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
from shared.structured_logging import get_logger, EventType, StructuredLogger
from shared.error_taxonomy import (
    AuthorizationError,
    DataIntegrityError,
)
from processors.stripe_pull import StripePullClient
from workspace_helpers import get_workspace_id_from_company_id


class StripeProcessor(ProcessorBase):
    """
    Stripe payment processor implementation.

    Implements PROCESSOR_NORMALIZATION_CONTRACT §5 (Stripe Normalization Rules).

    Uses Stripe's REST API to fetch payouts and their constituent payments.
    All data is normalized to canonical format before being returned.
    """

    PROCESSOR_NAME = 'stripe'

    def __init__(
        self,
        api_key: str,
        company_id: str = "",
        sync_run_id: str = "",
        db_path: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Stripe processor.

        Args:
            api_key: Stripe secret key (sk_live_xxx or sk_test_xxx)
            company_id: Charles company ID (for canonical model)
            sync_run_id: Current sync run ID (for logging correlation)
            db_path: Database path for raw data persistence (Phase 2)
        """
        self.api_key = api_key
        self.company_id = company_id

        # Initialize structured logger per MINIMUM_LOGGING_CONTRACT
        # Phase 5.3: Store workspace_id for observability hooks
        workspace_id = get_workspace_id_from_company_id(company_id) or company_id
        self.workspace_id = workspace_id
        self.logger = get_logger(
            service="stripe_processor",
            workspace_id=workspace_id,
            company_id=company_id
        )
        if sync_run_id:
            self.logger.set_sync_run_id(sync_run_id)

        # Initialize Layer 1 pull client
        # Phase 2: Pass db_path and sync_run_id for raw data persistence
        self._pull_client = StripePullClient(
            api_key=api_key,
            logger=self.logger,
            workspace_id=workspace_id,
            db_path=db_path,
            sync_run_id=sync_run_id,
        )

        self._validate_credentials()

    def _validate_credentials(self) -> None:
        """
        Validate Stripe API key by fetching account info.

        Raises:
            AuthorizationError: If API key is invalid (§3.1)
            ConnectionError: If unable to reach Stripe (§3.2)
        """
        try:
            response = self._pull_client.fetch_account()
            # =========================================================================
            # LAYER 1 BOUNDARY REACHED
            # Processor-native Stripe data has been pulled.
            # No canonicalization or matching has occurred yet.
            # Layer 2 canonicalization begins below.
            # =========================================================================
            account_id = response.get('id', 'unknown')

            self.logger.info(
                EventType.SYNC_STARTED,
                f"Stripe credentials validated for account: {account_id}",
                processor="stripe",
                stripe_account_id=account_id
            )

        except AuthorizationError:
            self.logger.error(
                EventType.ERROR_DETECTED,
                "Invalid Stripe API key",
                error_category="authorization_error",
                error_code="STRIPE_AUTH_FAILED",
                processor="stripe"
            )
            raise

    def fetch_payouts(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100
    ) -> List[ProcessorPayout]:
        """
        Fetch payouts from Stripe.

        Implements PROCESSOR_NORMALIZATION_CONTRACT §5.1 (Stripe Payout Mapping).

        Args:
            start_date: Only fetch payouts created after this date
            end_date: Only fetch payouts created before this date
            limit: Maximum number of payouts to fetch (max 100 per Stripe)

        Returns:
            List of ProcessorPayout objects (payments not populated)
        """
        # Convert dates to timestamps for Layer 1 pull
        created_gte = None
        created_lte = None

        if start_date:
            created_gte = int(datetime.combine(start_date, datetime.min.time()).timestamp())

        if end_date:
            created_lte = int(datetime.combine(end_date, datetime.max.time()).timestamp())

        data = self._pull_client.fetch_payouts(
            created_gte=created_gte,
            created_lte=created_lte,
            limit=limit
        )
        # =========================================================================
        # LAYER 1 BOUNDARY REACHED
        # Processor-native Stripe data has been pulled.
        # No canonicalization or matching has occurred yet.
        # Layer 2 canonicalization begins below.
        # =========================================================================

        payouts = []
        for item in data.get('data', []):
            payout = self._parse_payout(item)
            payouts.append(payout)

        self.logger.info(
            EventType.SYNC_STARTED,
            f"Fetched {len(payouts)} payouts from Stripe",
            processor="stripe",
            payout_count=len(payouts)
        )

        return payouts

    def _parse_payout(self, data: Dict) -> ProcessorPayout:
        """
        Parse Stripe payout object into canonical ProcessorPayout.

        Implements PROCESSOR_NORMALIZATION_CONTRACT §5.1 (Stripe Payout Mapping).

        Note: This creates a payout WITHOUT the payments list populated.
        Call fetch_payout_details() to get the complete payout with payments.
        """
        # Amount is in cents - convert per §5.1
        net_amount = Decimal(str(data['amount'])) / 100

        # Parse dates per §5.1
        created_at = datetime.fromtimestamp(data['created']) if data.get('created') else None
        arrival_date = None
        if data.get('arrival_date'):
            arrival_date = datetime.fromtimestamp(data['arrival_date']).date()

        payout = ProcessorPayout(
            processor_payout_id=data['id'],
            processor=self.PROCESSOR_NAME,
            company_id=self.company_id,
            gross_amount=Decimal('0'),      # Calculated when fetching details
            fee_amount=Decimal('0'),        # Calculated when fetching details
            refund_amount=Decimal('0'),     # Calculated when fetching details
            net_amount=net_amount,
            currency=data.get('currency', 'usd').upper(),
            created_at=created_at,
            arrival_date=arrival_date,
            settlement_date=arrival_date,
            status=data.get('status', 'pending'),
            description=data.get('description', ''),
            metadata={'stripe_payout': data}
        )

        return payout

    def fetch_payout_details(self, payout: ProcessorPayout) -> ProcessorPayout:
        """
        Fetch complete payout details including all payments/transactions.

        Implements PROCESSOR_NORMALIZATION_CONTRACT §5.2 (Balance Transaction Processing).

        This method:
        1. Fetches the payout object (re-fetches from Stripe API for freshness)
        2. Fetches all balance transactions for the payout
        3. Categorizes transactions into payments, refunds, fees
        4. Validates the payout balance invariant (§7.1)

        Args:
            payout: ProcessorPayout object (uses processor_payout_id)

        Returns:
            ProcessorPayout with complete payment list

        Raises:
            DataIntegrityError: If payout balance validation fails
        """
        start_time = time.time()
        payout_id = payout.processor_payout_id

        # Get the payout (re-fetch from Stripe for complete data)
        payout_data = self._pull_client.fetch_payout(payout_id)

        # Get balance transactions for this payout (§5.2)
        balance_txns = self._pull_client.fetch_balance_transactions(payout_id, limit=100)
        # =========================================================================
        # LAYER 1 BOUNDARY REACHED
        # Processor-native Stripe data has been pulled.
        # No canonicalization or matching has occurred yet.
        # Layer 2 canonicalization begins below.
        # =========================================================================

        payout = self._parse_payout(payout_data)

        payments = []
        refunds = []
        fees = []
        total_gross = Decimal('0')
        total_fees = Decimal('0')
        total_refunds = Decimal('0')
        fee_index = 0

        for txn in balance_txns.get('data', []):
            reporting_category = txn.get('reporting_category', '')

            if reporting_category == 'charge':
                # Payment - process per §5.3
                payment = self._process_charge_transaction(txn, payout_id)
                if payment:
                    payments.append(payment)
                    total_gross += payment.amount

            elif reporting_category == 'fee':
                # Standalone fee per §5.5
                fee_amount = Decimal(str(abs(txn['net']))) / 100
                total_fees += fee_amount

                fee = ProcessorFee(
                    processor=self.PROCESSOR_NAME,
                    processor_payout_id=payout_id,
                    canonical_fee_id=f"fee_{self.PROCESSOR_NAME}_{payout_id}_{fee_index}",
                    fee_type=FeeType.OTHER,
                    amount=fee_amount,
                    currency=txn.get('currency', 'usd').upper(),
                    description=txn.get('description', 'Standalone fee')
                )
                fees.append(fee)
                fee_index += 1

            elif reporting_category == 'refund':
                # Refund - process per §5.4 (store as POSITIVE amount)
                refund = self._process_refund_transaction(txn, payout_id)
                if refund:
                    refunds.append(refund)
                    total_refunds += refund.amount

            # Skip 'payout' category - that's the payout itself

        # Extract per-charge fees and add to total
        for payment in payments:
            total_fees += payment.fees

        # Update payout with calculated values
        payout.payments = payments
        payout.payment_count = len(payments)
        payout.refunds = refunds
        payout.refund_count = len(refunds)
        payout.fee_details = fees
        payout.gross_amount = total_gross
        payout.fee_amount = total_fees
        payout.fees = total_fees  # Alias
        payout.refund_amount = total_refunds

        duration_ms = int((time.time() - start_time) * 1000)

        # Validate payout balance per §7.1
        if not payout.validate_balance():
            expected = payout.gross_amount - payout.fee_amount - payout.refund_amount
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Payout balance validation failed for {payout_id}",
                error_category="data_integrity_error",
                error_code="PAYOUT_BALANCE_MISMATCH",
                processor="stripe",
                processor_payout_id=payout_id,
                gross_amount=str(payout.gross_amount),
                fee_amount=str(payout.fee_amount),
                refund_amount=str(payout.refund_amount),
                net_amount=str(payout.net_amount),
                expected_net=str(expected),
                duration_ms=duration_ms
            )
            raise DataIntegrityError(
                f"Payout {payout_id} balance mismatch: "
                f"expected net={expected}, actual net={payout.net_amount}"
            )

        # Log successful normalization per §12
        self.logger.info(
            EventType.PAYOUT_PROCESSING_STARTED,
            f"Payout {payout_id} normalized: {len(payments)} payments, "
            f"{len(refunds)} refunds, fees=${total_fees}, net=${payout.net_amount}",
            processor="stripe",
            processor_payout_id=payout_id,
            payment_count=len(payments),
            refund_count=len(refunds),
            gross_amount=str(total_gross),
            fee_amount=str(total_fees),
            refund_amount=str(total_refunds),
            net_amount=str(payout.net_amount),
            duration_ms=duration_ms
        )

        # ===================================================================
        # CONTRACT ASSERTION: PROCESSOR_NORMALIZATION_CONTRACT
        # Assert compliance at normalization boundary before returning
        # ===================================================================
        try:
            payout.assert_contract_compliance()
        except AssertionError as e:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Contract assertion failed for payout {payout_id}: {e}",
                error_category="data_integrity_error",
                error_code="CONTRACT_VIOLATION",
                processor="stripe",
                processor_payout_id=payout_id,
                contract="PROCESSOR_NORMALIZATION_CONTRACT"
            )
            raise DataIntegrityError(str(e))

        return payout

    def _process_charge_transaction(
        self,
        txn: Dict,
        payout_id: str
    ) -> Optional[ProcessorPayment]:
        """
        Process a charge balance transaction and fetch full charge details.

        Implements PROCESSOR_NORMALIZATION_CONTRACT §5.3 (Stripe Payment Mapping).

        Args:
            txn: Balance transaction object from Stripe (raw JSON)
            payout_id: Parent payout ID

        Returns:
            ProcessorPayment or None if processing fails
        """
        try:
            # Get the charge ID from the source field
            charge_id = txn.get('source')
            if not charge_id:
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Balance transaction {txn['id']} has no source",
                    warning_type="missing_source",
                    balance_transaction_id=txn['id']
                )
                return None

            # Fetch charge details via Layer 1
            charge_data = self._pull_client.fetch_charge(charge_id)

            # Get customer info via Layer 1
            customer_email = None
            customer_name = None
            customer_id = charge_data.get('customer')

            if customer_id:
                try:
                    customer_data = self._pull_client.fetch_customer(customer_id)
                    # =========================================================================
                    # LAYER 1 BOUNDARY REACHED
                    # Processor-native Stripe data has been pulled.
                    # No canonicalization or matching has occurred yet.
                    # Layer 2 canonicalization begins below.
                    # =========================================================================
                    customer_email = customer_data.get('email')
                    customer_name = customer_data.get('name')
                except Exception as e:
                    self.logger.warn(
                        EventType.WARNING_DETECTED,
                        f"Could not fetch customer {customer_id}: {e}",
                        warning_type="customer_fetch_failed",
                        processor_customer_id=customer_id
                    )

            # =========================================================================
            # LAYER 1 BOUNDARY REACHED
            # Processor-native Stripe data has been pulled.
            # No canonicalization or matching has occurred yet.
            # Layer 2 canonicalization begins below.
            # =========================================================================

            # Amounts from balance transaction (in cents) - convert per §5.3
            amount_gross = Decimal(str(txn['amount'])) / 100
            fees = Decimal(str(txn['fee'])) / 100
            amount_net = Decimal(str(txn['net'])) / 100

            # Get payment method details
            payment_method = None
            card_brand = None
            card_last4 = None

            payment_method_details = charge_data.get('payment_method_details', {})
            if payment_method_details:
                payment_method = payment_method_details.get('type')
                card_info = payment_method_details.get('card', {})
                if card_info:
                    card_brand = card_info.get('brand')
                    card_last4 = card_info.get('last4')

            # Parse timestamp per §5.3
            created_ts = txn.get('created')
            processor_timestamp = datetime.fromtimestamp(created_ts) if created_ts else None

            return ProcessorPayment(
                processor_payment_id=charge_id,
                processor=self.PROCESSOR_NAME,
                processor_payout_id=payout_id,
                payment_type=PaymentType.CHARGE,
                amount=amount_gross,
                amount_gross=amount_gross,
                fees=fees,
                amount_net=amount_net,
                currency=txn.get('currency', 'usd').upper(),
                processor_customer_id=customer_id,
                customer_email=customer_email,
                customer_name=customer_name,
                payment_method=payment_method,
                card_brand=card_brand,
                card_last4=card_last4,
                processor_timestamp=processor_timestamp,
                description=charge_data.get('description', ''),
                metadata={
                    'balance_transaction_id': txn['id'],
                    'charge_data': charge_data
                }
            )

        except Exception as e:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Error processing charge transaction: {e}",
                error_category="infrastructure_error",
                balance_transaction_id=txn.get('id')
            )
            return None

    def _process_refund_transaction(
        self,
        txn: Dict,
        payout_id: str
    ) -> Optional[ProcessorRefund]:
        """
        Process a refund balance transaction.

        Implements PROCESSOR_NORMALIZATION_CONTRACT §5.4 (Stripe Refund Mapping).

        IMPORTANT: Refunds are stored with POSITIVE amounts per §4.3.
        Sign is applied during deposit construction, not here.

        Args:
            txn: Balance transaction object from Stripe (raw JSON, reporting_category='refund')
            payout_id: Parent payout ID

        Returns:
            ProcessorRefund with positive amount, or None if processing fails
        """
        try:
            # Get the refund ID from the source field
            refund_id = txn.get('source')
            if not refund_id:
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Refund transaction {txn['id']} has no source",
                    warning_type="missing_source",
                    balance_transaction_id=txn['id']
                )
                return None

            # Fetch refund details to get original charge info per §5.4 via Layer 1
            try:
                refund_data = self._pull_client.fetch_refund(refund_id)
                # =========================================================================
                # LAYER 1 BOUNDARY REACHED
                # Processor-native Stripe data has been pulled.
                # No canonicalization or matching has occurred yet.
                # Layer 2 canonicalization begins below.
                # =========================================================================
                original_charge_id = refund_data.get('charge')
                reason = refund_data.get('reason', 'refund')
            except Exception as e:
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"Could not fetch refund details {refund_id}: {e}",
                    warning_type="refund_fetch_failed",
                    processor_refund_id=refund_id
                )
                refund_data = {}
                original_charge_id = None
                reason = 'refund'

            # Amounts from balance transaction (in cents)
            # For refunds, amount is NEGATIVE in Stripe - we store POSITIVE per §4.3
            raw_amount = Decimal(str(txn['amount'])) / 100
            amount = abs(raw_amount)  # Always positive

            # Parse timestamp per §5.4
            created_ts = txn.get('created')
            refund_date = datetime.fromtimestamp(created_ts) if created_ts else None

            self.logger.debug(
                EventType.PAYOUT_PROCESSING_STARTED,
                f"Processing refund {refund_id}: ${amount} (original charge: {original_charge_id})",
                processor="stripe",
                processor_refund_id=refund_id,
                amount=str(amount),
                original_charge_id=original_charge_id
            )

            return ProcessorRefund(
                processor_refund_id=refund_id,
                processor=self.PROCESSOR_NAME,
                processor_payout_id=payout_id,
                processor_payment_id=original_charge_id or "",
                amount=amount,  # POSITIVE per §4.3
                currency=txn.get('currency', 'usd').upper(),
                refund_date=refund_date,
                status=RefundStatus.SUCCEEDED,
                reason=reason,
                metadata={
                    'balance_transaction_id': txn['id'],
                    'refund_data': refund_data,
                    'original_charge_id': original_charge_id
                }
            )

        except Exception as e:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Error processing refund transaction: {e}",
                error_category="infrastructure_error",
                balance_transaction_id=txn.get('id')
            )
            return None

    def fetch_payment_details(self, payment_id: str) -> ProcessorPayment:
        """
        Fetch details for a single charge.

        Implements PROCESSOR_NORMALIZATION_CONTRACT §5.3 (Stripe Payment Mapping).

        Args:
            payment_id: Stripe charge ID (ch_xxx)

        Returns:
            ProcessorPayment object
        """
        charge_data = self._pull_client.fetch_charge(payment_id)

        # Get balance transaction for fee info via Layer 1
        balance_txn_id = charge_data.get('balance_transaction')
        fees = Decimal('0')

        if balance_txn_id:
            try:
                balance_txn = self._pull_client.fetch_balance_transaction(balance_txn_id)
                fees = Decimal(str(balance_txn.get('fee', 0))) / 100
            except Exception:
                pass

        # Get customer info via Layer 1
        customer_email = None
        customer_name = None
        customer_id = charge_data.get('customer')

        if customer_id:
            try:
                customer_data = self._pull_client.fetch_customer(customer_id)
                customer_email = customer_data.get('email')
                customer_name = customer_data.get('name')
            except Exception:
                pass

        # =========================================================================
        # LAYER 1 BOUNDARY REACHED
        # Processor-native Stripe data has been pulled.
        # No canonicalization or matching has occurred yet.
        # Layer 2 canonicalization begins below.
        # =========================================================================

        amount_gross = Decimal(str(charge_data['amount'])) / 100
        amount_net = amount_gross - fees

        # Get payment method details
        payment_method = None
        card_brand = None
        card_last4 = None

        payment_method_details = charge_data.get('payment_method_details', {})
        if payment_method_details:
            payment_method = payment_method_details.get('type')
            card_info = payment_method_details.get('card', {})
            if card_info:
                card_brand = card_info.get('brand')
                card_last4 = card_info.get('last4')

        return ProcessorPayment(
            processor_payment_id=payment_id,
            processor=self.PROCESSOR_NAME,
            processor_payout_id="",  # Not known in this context
            payment_type=PaymentType.CHARGE,
            amount=amount_gross,
            amount_gross=amount_gross,
            fees=fees,
            amount_net=amount_net,
            currency=charge_data.get('currency', 'usd').upper(),
            processor_customer_id=customer_id,
            customer_email=customer_email,
            customer_name=customer_name,
            payment_method=payment_method,
            card_brand=card_brand,
            card_last4=card_last4,
            processor_timestamp=datetime.fromtimestamp(charge_data['created']),
            description=charge_data.get('description', ''),
            metadata={'charge_data': charge_data}
        )

    def is_payout_complete(self, payout: ProcessorPayout) -> bool:
        """
        Check if a Stripe payout is complete.

        Stripe payout statuses:
        - pending: Payout has been created
        - in_transit: Funds are on the way to the bank
        - paid: Funds have arrived
        - failed: Payout failed
        - canceled: Payout was canceled
        """
        return payout.status == 'paid'
