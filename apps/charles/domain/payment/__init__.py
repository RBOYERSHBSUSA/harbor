"""
Charles Payment Module

Per Implementation Plan §D:
- Provides payment verification interface
- Phase 8-Lite: Stub always succeeds
- Future: Swap in real payment provider (Stripe)

Security Notes:
- Payment verification is server-side only
- No client-side authority
- Interface designed for easy provider swap
"""

import os
from charles.domain.payment.verifier import (
    PaymentVerifier,
    StubPaymentVerifier,
    PaymentResult,
    PaymentStatus,
)


def get_payment_verifier() -> PaymentVerifier:
    """
    Get configured payment verifier.

    Per Implementation Plan §D.3:
    - Configuration-driven selection
    - Default: stub (Phase 8-Lite)
    - Future: stripe

    Returns:
        PaymentVerifier instance
    """
    provider = os.environ.get('PAYMENT_PROVIDER', 'stub')

    if provider == 'stub':
        return StubPaymentVerifier()
    elif provider == 'stripe':
        # Future: StripePaymentVerifier
        raise NotImplementedError("Stripe payment provider not yet implemented")
    else:
        raise ValueError(f"Unknown payment provider: {provider}")


__all__ = [
    'PaymentVerifier',
    'StubPaymentVerifier',
    'PaymentResult',
    'PaymentStatus',
    'get_payment_verifier',
]
