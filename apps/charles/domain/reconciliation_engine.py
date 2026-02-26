"""
Module 4: Processor Base Class
Abstract base class defining the interface for payment processor integrations.

Implements PROCESSOR_NORMALIZATION_CONTRACT v1.0

All processors (Stripe, Authorize.net, etc.) must implement this interface.
This ensures Module 4 can work uniformly with different payment processors.

Canonical Data Models (§4):
- ProcessorPayment: Represents a single charge/payment
- ProcessorRefund: Represents a refund (stored as positive amount)
- ProcessorFee: Represents a processing fee
- ProcessorPayout: Represents a payout/batch to merchant's bank
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import List, Optional, Dict, Any
from enum import Enum


class PaymentType(Enum):
    """Payment type per PROCESSOR_NORMALIZATION_CONTRACT §4.2"""
    CHARGE = "charge"


class RefundStatus(Enum):
    """Refund status per PROCESSOR_NORMALIZATION_CONTRACT §4.3"""
    SUCCEEDED = "succeeded"
    PENDING = "pending"
    FAILED = "failed"


class FeeType(Enum):
    """Fee type per PROCESSOR_NORMALIZATION_CONTRACT §4.4"""
    PROCESSING = "processing"
    REFUND = "refund"
    DISPUTE = "dispute"
    OTHER = "other"


class PayoutStatus(Enum):
    """Payout status per PROCESSOR_NORMALIZATION_CONTRACT §4.1"""
    PENDING = "pending"
    IN_TRANSIT = "in_transit"
    PAID = "paid"
    FAILED = "failed"
    CANCELED = "canceled"


@dataclass
class ProcessorPayment:
    """
    Canonical Payment per PROCESSOR_NORMALIZATION_CONTRACT §4.2

    Represents a single charge/transaction within a payout.
    This is the common format that gets inserted into canonical_events.
    """
    # Required identifiers (§4.2)
    processor_payment_id: str              # Processor's native payment/charge ID
    processor: str = ""                    # 'stripe', 'authorize_net'
    processor_payout_id: str = ""          # Parent payout ID

    # Charles-generated ID (§10.1)
    canonical_payment_id: str = ""         # Format: payment_{processor}_{processor_id}

    # Payment type (§4.2)
    payment_type: PaymentType = PaymentType.CHARGE

    # Amounts (all in Decimal for precision)
    amount: Decimal = Decimal('0')         # Payment amount (always positive)
    amount_gross: Decimal = Decimal('0')   # Alias for amount (legacy compatibility)
    fees: Decimal = Decimal('0')           # Processing fees
    amount_net: Decimal = Decimal('0')     # Net amount (gross - fees)
    currency: str = 'USD'

    # Customer info
    processor_customer_id: Optional[str] = None
    customer_email: Optional[str] = None
    customer_name: Optional[str] = None

    # Payment method
    payment_method: Optional[str] = None   # 'card', 'ach', etc.
    card_brand: Optional[str] = None       # 'visa', 'mastercard', etc.
    card_last4: Optional[str] = None

    # Timing
    processor_timestamp: Optional[datetime] = None
    payment_date: Optional[datetime] = None  # Alias for processor_timestamp

    # Description
    description: str = ''
    invoice_id: Optional[str] = None

    # Raw metadata (processor-specific, preserved per §3.3)
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        # Generate canonical ID if not set (§10.1)
        if not self.canonical_payment_id and self.processor and self.processor_payment_id:
            self.canonical_payment_id = f"payment_{self.processor}_{self.processor_payment_id}"
        # Sync amount aliases
        if self.amount == Decimal('0') and self.amount_gross != Decimal('0'):
            self.amount = self.amount_gross
        elif self.amount_gross == Decimal('0') and self.amount != Decimal('0'):
            self.amount_gross = self.amount
        # Sync timestamp aliases
        if self.payment_date is None and self.processor_timestamp is not None:
            self.payment_date = self.processor_timestamp


@dataclass
class ProcessorRefund:
    """
    Canonical Refund per PROCESSOR_NORMALIZATION_CONTRACT §4.3

    Represents a refund of a previous payment.
    Amount is stored as POSITIVE (sign applied during deposit construction).
    """
    # Required identifiers (§4.3)
    processor_refund_id: str               # Processor's native refund ID
    processor: str = ""                    # 'stripe', 'authorize_net'
    processor_payout_id: str = ""          # Payout containing this refund
    processor_payment_id: str = ""         # Original payment being refunded

    # Charles-generated ID (§10.1)
    canonical_refund_id: str = ""          # Format: refund_{processor}_{processor_id}

    # Amount (ALWAYS POSITIVE per §4.3 note)
    amount: Decimal = Decimal('0')         # Refund amount (positive)
    currency: str = 'USD'

    # Timing
    refund_date: Optional[datetime] = None

    # Status
    status: RefundStatus = RefundStatus.SUCCEEDED

    # Details
    reason: Optional[str] = None

    # Raw metadata
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        # Generate canonical ID if not set (§10.1)
        if not self.canonical_refund_id and self.processor and self.processor_refund_id:
            self.canonical_refund_id = f"refund_{self.processor}_{self.processor_refund_id}"
        # Ensure amount is positive (§4.3 note)
        if self.amount < 0:
            self.amount = abs(self.amount)


@dataclass
class ProcessorFee:
    """
    Canonical Fee per PROCESSOR_NORMALIZATION_CONTRACT §4.4

    Represents processing fees deducted from a payout.
    Amount is stored as POSITIVE (sign applied during deposit construction).
    """
    # Required identifiers (§4.4)
    processor: str = ""                    # 'stripe', 'authorize_net'
    processor_payout_id: str = ""          # Parent payout ID

    # Charles-generated ID (§10.1)
    canonical_fee_id: str = ""             # Format: fee_{processor}_{payout_id}_{index}

    # Fee details
    fee_type: FeeType = FeeType.PROCESSING
    amount: Decimal = Decimal('0')         # Fee amount (POSITIVE)
    currency: str = 'USD'
    description: Optional[str] = None
    associated_payment_id: Optional[str] = None  # Payment this fee relates to

    def __post_init__(self):
        # Ensure amount is positive (§4.4 note)
        if self.amount < 0:
            self.amount = abs(self.amount)


@dataclass
class ProcessorPayout:
    """
    Canonical Payout per PROCESSOR_NORMALIZATION_CONTRACT §4.1

    A payout represents funds being deposited to the merchant's bank account.

    Invariant (§4.1): net_amount = gross_amount - fee_amount - refund_amount
    """
    # Required identifiers (§4.1)
    processor_payout_id: str               # Processor's native payout/batch ID
    processor: str = ""                    # 'stripe', 'authorize_net'
    company_id: str = ""                   # Charles company ID

    # Charles-generated ID (§10.1)
    canonical_payout_id: str = ""          # Format: payout_{processor}_{processor_id}

    # Amounts (§4.1)
    gross_amount: Decimal = Decimal('0')   # Total before fees (sum of payments)
    fee_amount: Decimal = Decimal('0')     # Total processing fees (positive number)
    refund_amount: Decimal = Decimal('0')  # Total refunds (positive number)
    net_amount: Decimal = Decimal('0')     # Amount deposited to bank
    fees: Decimal = Decimal('0')           # Alias for fee_amount (legacy)
    currency: str = 'USD'

    # Status (§4.1)
    status: str = 'pending'                # pending, in_transit, paid, failed, canceled

    # Timing
    created_at: Optional[datetime] = None
    arrival_date: Optional[date] = None    # When funds hit the bank
    settlement_date: Optional[date] = None # Settlement date (for QBO)
    payout_date: Optional[date] = None     # Alias for settlement_date
    fetched_at: Optional[datetime] = None  # When Charles retrieved this data (§4.1)

    # Composition - Payments (charges only, always positive)
    payments: List[ProcessorPayment] = field(default_factory=list)
    payment_count: int = 0

    # Composition - Refunds (separate per §4.3, always positive amounts)
    refunds: List['ProcessorRefund'] = field(default_factory=list)
    refund_count: int = 0

    # Composition - Fees (separate per §4.4)
    fee_details: List['ProcessorFee'] = field(default_factory=list)

    # Description
    description: Optional[str] = None

    # Raw metadata (preserved per §3.3)
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        # Generate canonical ID if not set (§10.1)
        if not self.canonical_payout_id and self.processor and self.processor_payout_id:
            self.canonical_payout_id = f"payout_{self.processor}_{self.processor_payout_id}"
        # Set fetched_at if not set
        if self.fetched_at is None:
            self.fetched_at = datetime.now(timezone.utc)
        # Sync fee aliases
        if self.fee_amount == Decimal('0') and self.fees != Decimal('0'):
            self.fee_amount = self.fees
        elif self.fees == Decimal('0') and self.fee_amount != Decimal('0'):
            self.fees = self.fee_amount
        # Sync date aliases
        if self.payout_date is None and self.settlement_date is not None:
            self.payout_date = self.settlement_date

    def validate_balance(self) -> bool:
        """
        Validate payout balance invariant per §7.1:
        - Card payouts: net_amount = gross_amount - fee_amount - refund_amount
        - ACH payouts: net_amount = gross_amount - refund_amount
          (ACH fees are implied-in-deposit, not withheld at settlement)

        Returns:
            True if balance is valid, False otherwise
        """
        is_ach = (self.metadata or {}).get('payout_type') == 'ach_transfer'
        if is_ach:
            expected_net = self.gross_amount - self.refund_amount
        else:
            expected_net = self.gross_amount - self.fee_amount - self.refund_amount
        # Allow small tolerance for floating point
        return abs(self.net_amount - expected_net) < Decimal('0.01')

    def validate_sums(self) -> Dict[str, bool]:
        """
        Validate that component sums match totals per §7.1

        Returns:
            Dict with validation results for payments, refunds, fees
        """
        payment_sum = sum(p.amount for p in self.payments)
        refund_sum = sum(r.amount for r in self.refunds)
        fee_sum = sum(f.amount for f in self.fee_details)

        return {
            "payments_valid": abs(payment_sum - self.gross_amount) < Decimal('0.01'),
            "refunds_valid": abs(refund_sum - self.refund_amount) < Decimal('0.01'),
            "fees_valid": abs(fee_sum - self.fee_amount) < Decimal('0.01') if self.fee_details else True
        }

    def assert_contract_compliance(self) -> None:
        """
        Assert PROCESSOR_NORMALIZATION_CONTRACT compliance at normalization boundary.

        This method enforces contract invariants and should be called before
        handing off normalized data to the reconciliation layer.

        Contract Requirements:
        - §3.1: Processor boundary - no QBO concepts may leak into processor data
        - §4.1: All required payout fields must be present
        - §7.1: Payout balance invariant must hold

        Raises:
            AssertionError: If any contract invariant is violated
        """
        # ===================================================================
        # §4.1 Required Fields
        # ===================================================================
        assert self.processor_payout_id is not None and self.processor_payout_id != "", \
            "PROCESSOR_NORMALIZATION_CONTRACT §4.1 violated: processor_payout_id missing"

        assert self.processor is not None and self.processor != "", \
            "PROCESSOR_NORMALIZATION_CONTRACT §4.1 violated: processor identifier missing"

        assert self.net_amount is not None, \
            "PROCESSOR_NORMALIZATION_CONTRACT §4.1 violated: net_amount missing"

        assert self.gross_amount is not None, \
            "PROCESSOR_NORMALIZATION_CONTRACT §4.1 violated: gross_amount missing"

        assert self.fee_amount is not None, \
            "PROCESSOR_NORMALIZATION_CONTRACT §4.1 violated: fee_amount missing"

        assert self.currency is not None and len(self.currency) == 3, \
            "PROCESSOR_NORMALIZATION_CONTRACT §4.1 violated: invalid currency code"

        assert self.status is not None, \
            "PROCESSOR_NORMALIZATION_CONTRACT §4.1 violated: status missing"

        assert self.fetched_at is not None, \
            "PROCESSOR_NORMALIZATION_CONTRACT §4.1 violated: fetched_at timestamp missing"

        # ===================================================================
        # §3.1 Processor Boundary - No QBO Concepts
        # ===================================================================
        # Processor adapters must not make accounting decisions or contain QBO data
        assert not hasattr(self, 'qbo_deposit_id'), \
            "PROCESSOR_NORMALIZATION_CONTRACT §3.1 violated: processor leaked QBO deposit ID"

        assert not hasattr(self, 'qbo_realm_id'), \
            "PROCESSOR_NORMALIZATION_CONTRACT §3.1 violated: processor leaked QBO realm ID"

        assert not hasattr(self, 'qbo_payment_ids'), \
            "PROCESSOR_NORMALIZATION_CONTRACT §3.1 violated: processor leaked QBO payment IDs"

        # Ensure metadata doesn't contain QBO concepts
        if self.metadata:
            assert 'qbo_' not in str(self.metadata).lower(), \
                "PROCESSOR_NORMALIZATION_CONTRACT §3.1 violated: processor metadata contains QBO data"

        # ===================================================================
        # §7.1 Balance Invariant
        # ===================================================================
        assert self.validate_balance(), \
            f"PROCESSOR_NORMALIZATION_CONTRACT §7.1 violated: payout balance mismatch. " \
            f"net={self.net_amount}, expected={self.gross_amount - self.fee_amount - self.refund_amount}"

        # ===================================================================
        # §4.2, §4.3, §4.4 Component Validation
        # ===================================================================
        for payment in self.payments:
            assert payment.amount >= 0, \
                f"PROCESSOR_NORMALIZATION_CONTRACT §4.2 violated: payment {payment.processor_payment_id} has negative amount"
            assert payment.processor == self.processor, \
                f"PROCESSOR_NORMALIZATION_CONTRACT §4.2 violated: payment processor mismatch"

        for refund in self.refunds:
            assert refund.amount >= 0, \
                f"PROCESSOR_NORMALIZATION_CONTRACT §4.3 violated: refund {refund.processor_refund_id} stored with negative amount (must be positive)"
            assert refund.processor == self.processor, \
                f"PROCESSOR_NORMALIZATION_CONTRACT §4.3 violated: refund processor mismatch"

        for fee in self.fee_details:
            assert fee.amount >= 0, \
                f"PROCESSOR_NORMALIZATION_CONTRACT §4.4 violated: fee stored with negative amount (must be positive)"


class ProcessorBase(ABC):
    """
    Abstract base class for payment processor integrations.
    
    Each processor implementation (Stripe, Authorize.net) must implement
    all abstract methods to provide a uniform interface for Module 4.
    """
    
    # Processor identifier (used in database records)
    PROCESSOR_NAME: str = 'unknown'
    
    def __init__(self, api_key: str, **kwargs):
        """
        Initialize the processor with credentials.
        
        Args:
            api_key: The API key or secret key for authentication
            **kwargs: Additional processor-specific configuration
        """
        self.api_key = api_key
        self._validate_credentials()
    
    @abstractmethod
    def _validate_credentials(self) -> None:
        """
        Validate that the provided credentials are correct.
        Should make a lightweight API call to verify access.
        
        Raises:
            ValueError: If credentials are invalid
            ConnectionError: If unable to reach the processor
        """
        pass
    
    @abstractmethod
    def fetch_payouts(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100
    ) -> List[ProcessorPayout]:
        """
        Fetch payouts from the processor.
        
        Args:
            start_date: Only fetch payouts after this date
            end_date: Only fetch payouts before this date
            limit: Maximum number of payouts to fetch
            
        Returns:
            List of ProcessorPayout objects (without payments populated)
        """
        pass
    
    @abstractmethod
    def fetch_payout_details(self, payout: ProcessorPayout) -> ProcessorPayout:
        """
        Fetch complete details for a single payout, including all payments.

        The input payout object (from fetch_available_payouts) contains metadata
        like settlement_date that should be preserved. This method enriches it
        with transaction details (payments, refunds, fees).

        Args:
            payout: ProcessorPayout object with metadata (from fetch_available_payouts)

        Returns:
            ProcessorPayout with payments/refunds list populated
        """
        pass
    
    @abstractmethod
    def fetch_payment_details(self, payment_id: str) -> ProcessorPayment:
        """
        Fetch details for a single payment/charge.
        
        Args:
            payment_id: The processor's payment/charge ID
            
        Returns:
            ProcessorPayment object
        """
        pass
    
    def is_payout_complete(self, payout: ProcessorPayout) -> bool:
        """
        Check if a payout is complete and ready for processing.
        A complete payout has arrived in the merchant's bank account.
        
        Args:
            payout: The payout to check
            
        Returns:
            True if payout is complete and can be processed
        """
        # Default implementation - override if processor uses different status values
        return payout.status in ('paid', 'complete', 'completed')
    
    def get_processor_name(self) -> str:
        """Get the processor name for database records"""
        return self.PROCESSOR_NAME
