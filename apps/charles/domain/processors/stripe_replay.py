"""
Stripe Replay Client - Phase 2

================================================================================
LAYER 1 BOUNDARY — REPLAY ADAPTER FOR RAW DATA
================================================================================

This module implements the replay adapter for deterministic canonicalization
replay per PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md §3.4.

ARCHITECTURAL CONSTRAINTS (MANDATORY):

1. Layer 1 equivalence: This adapter is a Layer 1 component. It fully
   implements the StripePullClient interface with identical signatures
   and return types.

2. No mode-awareness in upper layers: Layer 2 (StripeProcessor) and all
   higher layers are completely unaware of whether they are operating
   against live APIs or stored data.

3. Injection at Layer 1 boundary: Mode selection (live vs. replay) is
   determined solely by which client is injected into the processor.

4. No HTTP calls: Replay mode makes zero HTTP requests to Stripe.

FORBIDDEN in this module:
- Canonical payment/payout/refund/fee models
- Amount normalization (cents to dollars)
- QuickBooks references of any kind
- Scoring, matching, or business logic
- HTTP requests to Stripe API

ALLOWED in this module:
- Reading raw data from database
- Structured logging
- Error handling for missing data

See: PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md §3.4
See: PROCESSOR_TRANSACTIONS_CONTRACT.md
================================================================================
"""

import sys
import os
from typing import Dict, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.structured_logging import StructuredLogger, EventType
from processors.raw_data_persistence import RawDataPersistence


class StripeReplayClient:
    """
    Replay adapter for Stripe that returns stored raw data.

    This class implements the same interface as StripePullClient but
    reads from the raw_processor_data table instead of making HTTP calls.

    Per PHASE2_PROCESSOR_RAW_DATA_PERSISTENCE_PLAN.md §3.4:
    - Replay mode MUST NOT make any HTTP requests to Stripe
    - Returns stored raw data exactly as captured
    - Layer 2 is unaware this is replay mode

    Thread Safety:
        Each method creates its own database connection.
        Safe for concurrent use.

    Statefulness:
        Stateless - no instance state persists across method calls.
        All state is in the database.
    """

    # Allowed instance attributes - matching StripePullClient contract
    _ALLOWED_ATTRIBUTES = frozenset({
        'logger',
        'workspace_id',
        '_persistence',
        '_sync_run_id',
    })

    def __init__(
        self,
        db_path: str,
        workspace_id: str,
        sync_run_id: str,
        logger: StructuredLogger,
    ):
        """
        Initialize Stripe replay client.

        Args:
            db_path: Path to SQLite database with raw_processor_data
            workspace_id: Workspace identifier for data isolation
            sync_run_id: Sync run ID to replay from
            logger: Structured logger instance
        """
        # Use object.__setattr__ to bypass guard during __init__
        object.__setattr__(self, 'logger', logger)
        object.__setattr__(self, 'workspace_id', workspace_id)
        object.__setattr__(self, '_sync_run_id', sync_run_id)
        object.__setattr__(self, '_persistence', RawDataPersistence(
            db_path=db_path,
            workspace_id=workspace_id,
            sync_run_id=sync_run_id,
            logger=logger,
        ))

    def __setattr__(self, name: str, value) -> None:
        """
        Guard against unauthorized state additions.

        Enforces statefulness contract matching StripePullClient.
        """
        if name not in self._ALLOWED_ATTRIBUTES:
            raise AttributeError(
                f"StripeReplayClient forbids attribute '{name}'. "
                f"Layer 1 clients must be stateless. "
                f"Allowed attributes: {sorted(self._ALLOWED_ATTRIBUTES)}"
            )
        object.__setattr__(self, name, value)

    def _get_or_raise(
        self,
        object_type: str,
        processor_object_id: str,
    ) -> Dict:
        """
        Get raw data or raise error if not found.

        Args:
            object_type: Type of object to retrieve
            processor_object_id: Processor's native ID

        Returns:
            Stored raw JSON

        Raises:
            ConnectionError: If raw data not found (replay cannot proceed)
        """
        result = self._persistence.get_raw_data(
            processor='stripe',
            object_type=object_type,
            processor_object_id=processor_object_id,
        )

        if result is None:
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Replay failed: no raw data for {object_type} {processor_object_id}",
                object_type=object_type,
                processor_object_id=processor_object_id,
                sync_run_id=self._sync_run_id,
                workspace_id=self.workspace_id,
                error_code="REPLAY_DATA_NOT_FOUND"
            )
            raise ConnectionError(
                f"Replay failed: raw data not found for {object_type} {processor_object_id}. "
                f"Ensure the sync run {self._sync_run_id} has persisted raw data."
            )

        self.logger.debug(
            EventType.RAW_DATA_CACHE_HIT,
            f"Replay: retrieved {object_type} {processor_object_id}",
            object_type=object_type,
            processor_object_id=processor_object_id,
            sync_run_id=self._sync_run_id,
            workspace_id=self.workspace_id,
        )

        return result

    # =========================================================================
    # LAYER 1 API METHODS — Mirror StripePullClient interface exactly
    # =========================================================================

    def fetch_account(self) -> Dict:
        """
        Return account info (not typically stored in replay).

        For credential validation during replay, returns a minimal stub.
        Account info is not persisted (per Phase 2 plan §2.1) since it's
        for credential validation only.

        Returns:
            Minimal account stub
        """
        # Account responses are not persisted per Phase 2 plan
        # Return a stub that passes validation
        return {
            'id': 'replay_mode',
            'object': 'account',
            'replay_mode': True,
        }

    def fetch_payouts(
        self,
        created_gte: Optional[int] = None,
        created_lte: Optional[int] = None,
        limit: int = 100
    ) -> Dict:
        """
        Fetch payouts list from stored raw data.

        Returns:
            Stored payouts list JSON
        """
        processor_object_id = f"list_{created_gte or 0}_{created_lte or 0}_{limit}"
        return self._get_or_raise('payouts_list', processor_object_id)

    def fetch_payout(self, payout_id: str) -> Dict:
        """
        Fetch a single payout from stored raw data.

        Args:
            payout_id: Stripe payout ID (po_xxx)

        Returns:
            Stored payout JSON
        """
        return self._get_or_raise('payout', payout_id)

    def fetch_balance_transactions(
        self,
        payout_id: str,
        limit: int = 100
    ) -> Dict:
        """
        Fetch balance transactions from stored raw data.

        Args:
            payout_id: Stripe payout ID
            limit: Ignored in replay mode

        Returns:
            Stored balance transactions list JSON
        """
        processor_object_id = f"payout_{payout_id}"
        return self._get_or_raise('balance_transactions_list', processor_object_id)

    def fetch_charge(self, charge_id: str) -> Dict:
        """
        Fetch a single charge from stored raw data.

        Args:
            charge_id: Stripe charge ID (ch_xxx)

        Returns:
            Stored charge JSON
        """
        return self._get_or_raise('charge', charge_id)

    def fetch_refund(self, refund_id: str) -> Dict:
        """
        Fetch a single refund from stored raw data.

        Args:
            refund_id: Stripe refund ID (re_xxx)

        Returns:
            Stored refund JSON
        """
        return self._get_or_raise('refund', refund_id)

    def fetch_balance_transaction(self, balance_transaction_id: str) -> Dict:
        """
        Fetch a single balance transaction from stored raw data.

        Args:
            balance_transaction_id: Stripe balance transaction ID (txn_xxx)

        Returns:
            Stored balance transaction JSON
        """
        return self._get_or_raise('balance_txn', balance_transaction_id)

    def fetch_customer(self, customer_id: str) -> Dict:
        """
        Fetch a single customer from stored raw data.

        Args:
            customer_id: Stripe customer ID (cus_xxx)

        Returns:
            Stored customer JSON
        """
        return self._get_or_raise('customer', customer_id)
