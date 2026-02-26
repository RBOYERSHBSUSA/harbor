"""
Payment Verification Interface

Per Implementation Plan §D.1 and §D.2:
- Abstract interface for payment verification
- StubPaymentVerifier for Phase 8-Lite (always succeeds)
- Designed for future real payment provider integration

Security Notes:
- Payment verification is server-side only
- Stub provides no real payment processing
- Interface supports future Stripe integration
"""

import logging
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class PaymentStatus(str, Enum):
    """
    Payment verification status.

    Per Implementation Plan §D.2:
    - SUCCESS: Payment verified, proceed with issuance
    - FAILED: Payment declined/invalid, abort
    - PENDING: Async verification needed (future)
    """
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"


@dataclass
class PaymentResult:
    """
    Result of payment verification.

    Per Implementation Plan §D.1:
    - status: PaymentStatus value
    - reference: Payment provider reference ID
    - message: Human-readable status
    """
    status: PaymentStatus
    reference: str
    message: Optional[str] = None


class PaymentVerifier(ABC):
    """
    Abstract interface for payment verification.

    Per Implementation Plan §D.1:
    - Implementations: StubVerifier (Phase 8-Lite), StripeVerifier (future)
    - Server-side only, no client trust
    """

    @abstractmethod
    def verify(
        self,
        amount_cents: int,
        currency: str,
        payment_token: str,
        metadata: Dict[str, Any],
    ) -> PaymentResult:
        """
        Verify a payment.

        Args:
            amount_cents: Total amount in cents
            currency: Currency code (e.g., 'USD')
            payment_token: Provider-specific token
            metadata: Additional context (email, license_type, quantity)

        Returns:
            PaymentResult with status and reference
        """
        pass


class StubPaymentVerifier(PaymentVerifier):
    """
    Phase 8-Lite stub payment verifier.

    Per Implementation Plan §D.1:
    - Always returns SUCCESS
    - No real payment processing
    - Reference includes STUB prefix for identification

    Testing Support (per §M-05 remediation):
    - Set STUB_FORCE_FAILURE=true to test failure paths
    """

    def verify(
        self,
        amount_cents: int,
        currency: str,
        payment_token: str,
        metadata: Dict[str, Any],
    ) -> PaymentResult:
        """
        Stub verification - always succeeds unless STUB_FORCE_FAILURE is set.

        Args:
            amount_cents: Total amount (validated but not charged)
            currency: Currency code
            payment_token: Ignored in stub
            metadata: Logged for debugging

        Returns:
            PaymentResult with SUCCESS status
        """
        # Check for forced failure (testing support per M-05)
        if os.environ.get('STUB_FORCE_FAILURE', '').lower() == 'true':
            logger.info(
                "Stub payment verification forced to fail",
                extra={
                    'amount_cents': amount_cents,
                    'currency': currency,
                }
            )
            return PaymentResult(
                status=PaymentStatus.FAILED,
                reference=f"STUB_FAILED_{uuid.uuid4().hex[:12].upper()}",
                message="Stub payment verification forced to fail (testing)",
            )

        # Generate stub reference
        reference = f"STUB_{uuid.uuid4().hex[:12].upper()}"

        logger.info(
            "Stub payment verification succeeded",
            extra={
                'amount_cents': amount_cents,
                'currency': currency,
                'reference': reference,
                'email': metadata.get('email', 'unknown'),
                'license_type': metadata.get('license_type', 'unknown'),
                'quantity': metadata.get('quantity', 0),
            }
        )

        return PaymentResult(
            status=PaymentStatus.SUCCESS,
            reference=reference,
            message="Stub payment verification succeeded",
        )


# Future implementation placeholder
# class StripePaymentVerifier(PaymentVerifier):
#     """
#     Stripe payment verifier (future implementation).
#
#     Migration path per Implementation Plan §D.3:
#     1. Set PAYMENT_PROVIDER=stripe
#     2. Set STRIPE_SECRET_KEY environment variable
#     3. Deploy and test
#     """
#
#     def __init__(self, api_key: str):
#         self.api_key = api_key
#         # Initialize Stripe client
#
#     def verify(self, amount_cents, currency, payment_token, metadata) -> PaymentResult:
#         # Implement Stripe charge/verification
#         pass
