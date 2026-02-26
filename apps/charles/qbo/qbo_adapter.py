"""
Module 3: QuickBooks Online Integration
Generates posting plans and executes them against QBO API

IMPORTANT: python-quickbooks Library Workaround
==============================================
This module uses DIRECT API CALLS (via requests library) instead of the 
python-quickbooks library for creating Deposits. Here's why:

The Problem:
-----------
The python-quickbooks library's Deposit object always adds `DepositLineDetail` 
with `DetailType: "DepositLineDetail"` to ALL line items, even when using 
LinkedTxn. This causes QBO API to reject the request with:
    "Business Validation Error: Select a bank account for this deposit"

The QBO API Requirement:
-----------------------
Per QBO API documentation, Deposit line items have TWO mutually exclusive formats:

1. Payment lines (linking existing QBO Payments from Undeposited Funds):
   {
       "Amount": 125.00,
       "LinkedTxn": [
           {"TxnId": "190", "TxnType": "Payment", "TxnLineId": "0"}
       ]
   }
   NOTE: NO DepositLineDetail, NO DetailType

2. Fee/adjustment lines (direct account posting):
   {
       "Amount": -3.85,  # Negative for fees
       "DetailType": "DepositLineDetail",
       "DepositLineDetail": {
           "AccountRef": {"value": "87"}
       }
   }

The Solution:
------------
The execute_posting_plan() method bypasses python-quickbooks and makes direct 
HTTP POST requests to the QBO API with correctly structured JSON.

The python-quickbooks library is still used for:
- OAuth token management (via module2_oauth.py)
- Querying accounts and other read operations
- Getting the auth_client.access_token for API calls

================================================================================
DEPOSIT LINE ITEM HANDLING
================================================================================

Deposit lines are built from payout data. The amount sign determines handling:

    amount >= 0 (Charges/Payments):
        - Use LinkedTxn format to link to existing QBO Payment
        - The QBO Payment ID is stored in processor_payment_id field
        - Moves the payment from Undeposited Funds to the bank account

    amount < 0 (Matched Refunds):
        - Use LinkedTxn format to link to existing QBO RefundReceipt/CreditMemo
        - The QBO RefundReceipt ID comes from matching_outcomes.qbo_payment_id
        - TxnType is "RefundReceipt" or "CreditMemo" based on source_object_type

    amount < 0 (Unmatched Refunds — fallback):
        - Use DepositLineDetail format with AccountRef
        - Posts to configured refund account (reduces the deposit)
        - No LinkedTxn because there's no QBO transaction to link to

    Fees (separate handling):
        - Use DepositLineDetail format with AccountRef
        - Posts to Fee Expense account
        - Always negative to reduce deposit total

Example payout breakdown:
    Charge 1:    +$100.00  → LinkedTxn to QBO Payment
    Charge 2:    +$50.00   → LinkedTxn to QBO Payment
    Refund:      -$25.00   → LinkedTxn to QBO RefundReceipt (if matched)
    Fees:        -$4.50    → DepositLineDetail to Fee Expense
    ────────────────────
    Net deposit: $120.50   → Must match actual bank deposit

================================================================================

Database Schema Note:
--------------------
The qbo_artifacts table has FK constraints to posting_plans and raw_events tables
that aren't populated in the current workflow. Artifact recording is skipped.
The payout_batches.posting_plan_id column also has an FK constraint that's avoided.

Date: December 2025
"""

import json
import time
import uuid
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from quickbooks.objects.deposit import Deposit, DepositLine, DepositLineDetail
from quickbooks.objects.account import Account
from module2_database import get_db_manager
from shared.structured_logging import get_logger, StructuredLogger, EventType, Severity
from shared.observability_interface import get_metrics_collector
from shared.idempotency import (
    IdempotencyManager,
    IdempotencyStatus,
    IdempotencyResult,
    get_idempotency_manager,
    generate_idempotency_key,
    generate_idempotency_key_v2,
    _generate_legacy_key
)
from shared.sync_lifecycle import (
    SyncRunManager,
    SyncState,
    SyncRun,
    SyncRunStats,
    get_sync_manager
)
from shared.error_taxonomy import (
    classify_error,
    ClassifiedError,
    ErrorCategory,
    retry_operation,
    with_retry,
    ConfigurationError,
    AuthorizationError,
    DataIntegrityError,
    MAX_RETRY_ATTEMPTS,
    BACKOFF_SCHEDULE_SECONDS
)
from shared.config import get_config
from shared.deposit_stamp import (
    DepositStampManager,
    generate_deposit_memo
)
from apps.charles.qbo.qbo_service_interface import QBOServiceInterface


class QBOIntegrationModule:
    """
    Module 3: QuickBooks Online Integration
    Handles deposit creation from payout batches
    """
    
    def __init__(
        self,
        company_id: str,
        qbo_service: QBOServiceInterface,
        sync_run_id: Optional[str] = None,
        workspace_id: str = None
    ):
        """
        Initialize QBO integration

        Args:
            company_id: Company UUID (legacy)
            qbo_service: Harbor-owned QBO service boundary
            sync_run_id: Optional sync run ID for log correlation
            workspace_id: Workspace ID (Phase 2+, required for proper scoping)
        """
        self.company_id = company_id
        self.qbo_service = qbo_service
        self.sync_run_id = sync_run_id or str(uuid.uuid4())
        self.db_manager = get_db_manager()

        # Phase 3: Resolve workspace_id first (needed for OAuth)
        if workspace_id is None:
            from workspace_helpers import get_workspace_id_from_company_id
            workspace_id = get_workspace_id_from_company_id(company_id)

        self.workspace_id = workspace_id

        # Phase 3: Use workspace_id for OAuth (dual-mode API)
        self.qbo_client = self.qbo_service
        # Defer config loading - will be loaded with correct processor_type in create_deposit_for_payout
        # This allows the adapter to be instantiated before we know which processor we're working with
        self.config = None

        # Initialize structured logger per MINIMUM_LOGGING_CONTRACT
        self.logger = get_logger(
            service="module3_qbo_integration",
            workspace_id=self.workspace_id,
            company_id=company_id,
            sync_run_id=self.sync_run_id
        )

        # Initialize idempotency manager per IDEMPOTENCY_IMPLEMENTATION_CONTRACT
        # Phase 2: Use consolidated DB connection, pass workspace_id
        company_conn = self.db_manager.get_connection()
        self.idempotency = get_idempotency_manager(company_conn, company_id, workspace_id=self.workspace_id)

        # Initialize sync manager per SYNC_RUN_LIFECYCLE_CONTRACT
        self.sync_manager = get_sync_manager(company_conn, company_id)

        # Cleanup on startup per contracts
        # Cleanup stale pending operations (IDEMPOTENCY §10.1)
        stale_idem = self.idempotency.cleanup_stale_pending()

        # Cleanup incomplete syncs (SYNC_RUN_LIFECYCLE §8.1)
        incomplete_syncs = self.sync_manager.cleanup_incomplete_syncs()

        if stale_idem > 0 or incomplete_syncs > 0:
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Startup cleanup: {stale_idem} stale operations, {incomplete_syncs} incomplete syncs",
                warning_type="startup_cleanup",
                stale_operations=stale_idem,
                incomplete_syncs=incomplete_syncs
            )

    def _get_idempotency_key(self, processor_payout_id: str, processor: str = "stripe") -> str:
        """
        Generate v2 idempotency key per IDEMPOTENCY_IMPLEMENTATION_CONTRACT v1.1 §4.1

        v2 Format: {operation_type}:ws:{workspace_id}:{processor}:{external_id}
        """
        return generate_idempotency_key_v2(
            operation_type="deposit",
            workspace_id=self.workspace_id,
            processor=processor,
            external_id=processor_payout_id
        )

    def _get_legacy_idempotency_key(self, processor_payout_id: str) -> str:
        """
        Generate legacy idempotency key for dual-read backward compatibility.

        Legacy Format: {operation_type}:{company_id}:{external_id}
        """
        return _generate_legacy_key(
            operation_type="deposit",
            company_id=self.company_id,
            external_id=processor_payout_id
        )

    def _get_qbo_realm_id(self) -> str:
        """Get QBO realm ID for logging."""
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()
        cursor.execute("SELECT qbo_realm_id FROM companies WHERE id = ?", (self.company_id,))
        row = cursor.fetchone()
        conn.close()
        return row['qbo_realm_id'] if row else "unknown"

    def _get_or_create_deposit_stamp(
        self,
        payout_id: str,
        processor_payout_id: str
    ) -> str:
        """
        Get existing deposit stamp or create a new one (idempotent).

        Per DEPOSIT_STAMP_CONTRACT:
        - The stamp MUST be generated once per logical Deposit
        - The full rendered stamp string MUST be persisted internally
        - Retry, replay, or idempotent execution MUST reuse the exact same stamp
        - Time zone MUST be derived from QBO CompanyInfo configuration

        Args:
            payout_id: Internal payout batch UUID
            processor_payout_id: Processor's payout ID (for logging)

        Returns:
            CHARLES deposit stamp string
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        try:
            # Check for existing stamp (idempotency)
            cursor.execute(
                "SELECT deposit_stamp FROM payout_batches WHERE id = ? AND workspace_id = ?",
                (payout_id, self.workspace_id)
            )
            row = cursor.fetchone()

            if row and row['deposit_stamp']:
                # Return existing stamp unchanged
                self.logger.debug(
                    EventType.SYNC_EXECUTION_STARTED,
                    f"Reusing existing deposit stamp for payout {processor_payout_id}",
                    processor_payout_id=processor_payout_id,
                    stamp_source="persisted"
                )
                return row['deposit_stamp']

            # Generate new stamp using the payout ID
            # Import here to avoid circular imports
            from shared.deposit_stamp import generate_deposit_stamp

            stamp = generate_deposit_stamp(processor_payout_id)

            # Persist stamp atomically (only if not already set - handles race conditions)
            cursor.execute(
                """UPDATE payout_batches
                   SET deposit_stamp = ?, updated_at = CURRENT_TIMESTAMP
                   WHERE id = ? AND workspace_id = ? AND (deposit_stamp IS NULL OR deposit_stamp = '')""",
                (stamp, payout_id, self.workspace_id)
            )
            conn.commit()

            # Verify we got the stamp (handles concurrent writes)
            cursor.execute(
                "SELECT deposit_stamp FROM payout_batches WHERE id = ? AND workspace_id = ?",
                (payout_id, self.workspace_id)
            )
            row = cursor.fetchone()
            final_stamp = row['deposit_stamp'] if row else stamp

            self.logger.info(
                EventType.SYNC_EXECUTION_STARTED,
                f"Generated new deposit stamp for payout {processor_payout_id}",
                processor_payout_id=processor_payout_id,
                deposit_stamp=final_stamp,
                stamp_source="generated"
            )

            return final_stamp

        finally:
            conn.close()

    def _load_configuration(self, processor_type: str = 'stripe') -> Dict:
        """Load company configuration from database

        AUTHORITATIVE SOURCE: processor_config table

        Checks configuration sources in order:
        1. company_configuration table (legacy full config, by workspace_id)
        2. processor_config table (AUTHORITATIVE - by workspace_id OR company_id)
        3. companies table (DEPRECATED fallback - will warn if used)

        Phase 8-Lite Fix: processor_config is the authoritative source for
        QBO account configuration. The companies table fallback is deprecated
        and will be removed post-ship.
        """
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()

        # First try company_configuration table (legacy full config)
        # Phase 2: Filter by workspace_id
        cursor.execute("""
        SELECT * FROM company_configuration WHERE workspace_id = ?
        """, (self.workspace_id,))

        config = cursor.fetchone()

        if config:
            conn.close()
            return dict(config)

        # AUTHORITATIVE SOURCE: processor_config table
        # Try by workspace_id first, then fall back to company_id
        # (handles migration period where workspace_id may be NULL)
        cursor.execute("""
        SELECT qbo_bank_account_id, qbo_fee_expense_account_id, qbo_refund_account_id,
               qbo_ach_settlement_account_id
        FROM processor_config
        WHERE processor_type = ?
          AND (workspace_id = ? OR (workspace_id IS NULL AND company_id = ?))
        ORDER BY workspace_id DESC NULLS LAST
        LIMIT 1
        """, (processor_type, self.workspace_id, self.company_id))

        processor_config = cursor.fetchone()

        if processor_config and processor_config['qbo_bank_account_id']:
            conn.close()
            return {
                'qbo_bank_account_id': processor_config['qbo_bank_account_id'],
                'qbo_fee_expense_account_id': processor_config['qbo_fee_expense_account_id'],
                'qbo_refund_account_id': processor_config['qbo_refund_account_id'],
                'qbo_ach_settlement_account_id': processor_config['qbo_ach_settlement_account_id'],
            }

        # DEPRECATED FALLBACK: companies table
        # This path should not be used - processor_config is authoritative
        cursor.execute("""
        SELECT qbo_bank_account_id, qbo_fee_expense_account_id, qbo_undeposited_funds_account_id
        FROM companies WHERE id = ?
        """, (self.company_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            raise ConfigurationError(f"No company found with id {self.company_id}")

        company = dict(row)

        # GUARDRAIL: If companies table has config but processor_config doesn't,
        # this indicates a configuration inconsistency that should be fixed
        if company.get('qbo_bank_account_id'):
            import logging
            logging.warning(
                f"DEPRECATED: Reading qbo_bank_account_id from companies table for "
                f"company_id={self.company_id}. Configuration should be in processor_config. "
                "This fallback will be removed in a future release."
            )
            return {
                'qbo_bank_account_id': company['qbo_bank_account_id'],
                'qbo_fee_expense_account_id': company['qbo_fee_expense_account_id'],
                'qbo_refund_account_id': company.get('qbo_undeposited_funds_account_id')  # Legacy mapping
            }

        # No configuration found anywhere - fail loudly
        # Format processor name for display (e.g., 'authorize_net' -> 'Authorize.net')
        processor_display = {
            'stripe': 'Stripe',
            'authorize_net': 'Authorize.net'
        }.get(processor_type, processor_type)
        raise ConfigurationError(
            f"Company {self.company_id} missing required QBO account configuration. "
            f"Configure bank account, fee account, and refund account in {processor_display} settings "
            f"(Integrations > {processor_display} > Settings) before syncing."
        )
    
    def find_payouts_ready_for_deposit(self) -> List[Dict]:
        """
        Find payout batches ready to be posted as deposits
        
        Returns:
            List of payout batch records
        """
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT * FROM v_payouts_ready_for_deposit
        ORDER BY settlement_date ASC
        """)
        
        payouts = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return payouts
    
    def get_payments_for_payout(self, processor_payout_id: str) -> List[Dict]:
        """
        Get all payments and refunds in a payout

        Args:
            processor_payout_id: Payout ID from processor

        Returns:
            List of canonical event records (payments and refunds)
        """
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()

        # Phase 3 Remediation: read from canonical_facts + matching_outcomes (§6.2)
        # Include both payments and refunds - refunds are needed for deposit line calculation
        # Subquery fetches the QBO source_object_type (RefundReceipt, CreditMemo, etc.)
        # for matched refunds so we can use the correct LinkedTxn TxnType
        cursor.execute("""
        SELECT cf.*, mo.qbo_payment_id, mo.match_type, mo.match_confidence,
               mo.match_score_breakdown, mo.workflow_status as status,
               (SELECT acct.source_object_type FROM canonical_facts acct
                WHERE acct.processor_payment_id = mo.qbo_payment_id
                AND acct.workspace_id = cf.workspace_id
                AND acct.event_type IN ('payment_observed', 'refund_observed')
                LIMIT 1) as matched_qbo_object_type
        FROM canonical_facts cf
        LEFT JOIN matching_outcomes mo ON mo.canonical_fact_id = cf.id
        WHERE cf.processor_payout_id = ? AND cf.workspace_id = ?
          AND cf.event_type IN ('payment', 'refund', 'ach_return')
        ORDER BY cf.processor_timestamp ASC
        """, (processor_payout_id, self.workspace_id))

        payments = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return payments

    def get_payments_for_payout_group(self, processor_payout_ids: List[str]) -> List[Dict]:
        """
        Get all payments, refunds, and ACH returns across multiple payouts.
        Used for grouped ACH deposits where multiple batches form one deposit.
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()

        placeholders = ','.join('?' * len(processor_payout_ids))
        cursor.execute(f"""
        SELECT cf.*, mo.qbo_payment_id, mo.match_type, mo.match_confidence,
               mo.match_score_breakdown, mo.workflow_status as status,
               (SELECT acct.source_object_type FROM canonical_facts acct
                WHERE acct.processor_payment_id = mo.qbo_payment_id
                AND acct.workspace_id = cf.workspace_id
                AND acct.event_type IN ('payment_observed', 'refund_observed')
                LIMIT 1) as matched_qbo_object_type
        FROM canonical_facts cf
        LEFT JOIN matching_outcomes mo ON mo.canonical_fact_id = cf.id
        WHERE cf.processor_payout_id IN ({placeholders}) AND cf.workspace_id = ?
          AND cf.event_type IN ('payment', 'refund', 'ach_return')
        ORDER BY cf.processor_timestamp ASC
        """, (*processor_payout_ids, self.workspace_id))

        payments = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return payments

    def check_existing_deposit(self, payout_batch_id: str) -> Optional[str]:
        """
        Check if deposit already exists for this payout (idempotency)
        
        Args:
            payout_batch_id: Payout batch UUID
            
        Returns:
            QBO deposit ID if exists, None otherwise
        """
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()
        
        # Check qbo_artifacts table
        cursor.execute("""
        SELECT qbo_id FROM qbo_artifacts
        WHERE canonical_event_id = ? AND qbo_type = 'Deposit'
        LIMIT 1
        """, (payout_batch_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result['qbo_id'] if result else None

    def query_qbo_for_existing_deposit(
        self,
        processor_payout_id: str,
        settlement_date: str,
        net_amount: str
    ) -> Optional[str]:
        """
        Query QBO directly for existing deposit matching payout criteria.

        Implements IDEMPOTENCY_IMPLEMENTATION_CONTRACT §13.2:
        - Used during crash recovery to detect duplicates
        - Matches by: date, amount, memo/reference containing processor payout ID
        - If found, returns existing deposit ID to avoid duplicate creation

        Args:
            processor_payout_id: Processor payout ID (stored in PrivateNote)
            settlement_date: Expected deposit date (YYYY-MM-DD)
            net_amount: Expected deposit amount

        Returns:
            QBO deposit ID if found, None otherwise
        """
        import requests
        import os

        qbo_realm_id = self._get_qbo_realm_id()

        # Determine base URL from fail-fast config (CONFIGURATION_MANAGEMENT_CONTRACT)
        environment = get_config().qbo_environment
        if environment == 'sandbox':
            base_url = 'https://sandbox-quickbooks.api.intuit.com'
        else:
            base_url = 'https://quickbooks.api.intuit.com'

        # Query for deposits on the settlement date
        # We search by TxnDate and then filter by PrivateNote containing payout ID
        query = f"SELECT * FROM Deposit WHERE TxnDate = '{settlement_date}'"
        url = f'{base_url}/v3/company/{qbo_realm_id}/query?query={query}&minorversion=65'

        headers = {
            'Authorization': f'Bearer {self.qbo_client.auth_client.access_token}',
            'Accept': 'application/json'
        }

        try:
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                self.logger.warn(
                    EventType.WARNING_DETECTED,
                    f"QBO query for existing deposit failed: {response.status_code}",
                    warning_type="qbo_query_failed",
                    processor_payout_id=processor_payout_id,
                    status_code=response.status_code
                )
                return None

            data = response.json()
            deposits = data.get('QueryResponse', {}).get('Deposit', [])

            # Search for deposit with matching PrivateNote (contains payout ID)
            # and matching amount
            target_amount = float(net_amount)

            for deposit in deposits:
                private_note = deposit.get('PrivateNote', '')
                deposit_amount = float(deposit.get('TotalAmt', 0))

                # Match by payout ID in memo AND amount within tolerance
                if processor_payout_id in private_note:
                    if abs(deposit_amount - target_amount) < 0.01:
                        existing_id = deposit.get('Id')
                        self.logger.info(
                            EventType.QBO_OBJECT_CREATION_SKIPPED,
                            f"Found existing QBO Deposit {existing_id} for payout {processor_payout_id}",
                            qbo_object_type="Deposit",
                            qbo_object_id=existing_id,
                            processor_payout_id=processor_payout_id,
                            qbo_realm_id=qbo_realm_id,
                            recovery_type="qbo_duplicate_detection"
                        )
                        return existing_id

            return None

        except Exception as e:
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Error querying QBO for existing deposit: {e}",
                warning_type="qbo_query_error",
                processor_payout_id=processor_payout_id
            )
            return None

    def generate_posting_plan(
        self,
        payout: Dict,
        payments: List[Dict],
        deposit_stamp: Optional[str] = None
    ) -> Dict:
        """
        Generate posting plan for payout deposit

        Args:
            payout: Payout batch record
            payments: List of payment records (may include refunds with event_type='refund')
            deposit_stamp: CHARLES deposit stamp (e.g., "CHARLES | Created 2024-12-15 14:30 EST").
                          If provided, the memo will include both stamp and payout ID.
                          If None, falls back to legacy memo format.

        Returns:
            Posting plan dict
        """
        # Build deposit lines
        lines = []

        # =========================================================================
        # GUARD: Check that ALL payments have been matched to QBO Payments
        # Per production logic: every processor payment must have a HIGH confidence
        # match before a deposit can be created.
        # Note: Refunds (event_type='refund') don't need QBO payment matching per INV-ANET-08
        # ACH returns (event_type='ach_return') have no QBO counterpart — bypass matching
        # =========================================================================
        unmatched_payments = [
            p for p in payments
            if p.get('event_type') not in ('refund', 'ach_return') and not p.get('qbo_payment_id')
        ]
        if unmatched_payments:
            unmatched_ids = [p['processor_payment_id'] for p in unmatched_payments]
            raise ValueError(
                f"Cannot create deposit: {len(unmatched_payments)} payment(s) not matched to QBO. "
                f"Unmatched: {', '.join(unmatched_ids)}. "
                f"Please match all payments in QBO before creating a deposit."
            )

        # =========================================================================
        # GUARD: Check that refund account is configured if there are refunds
        # =========================================================================
        has_refunds = any(p.get('event_type') == 'refund' for p in payments)
        if has_refunds and not self.config.get('qbo_refund_account_id'):
            raise ConfigurationError(
                "Cannot create deposit: payout contains refunds but no refund account is configured. "
                "Configure a refund account in processor settings (Integrations > Settings) before creating deposits with refunds."
            )

        # =========================================================================
        # ACH PAYOUT DETECTION (per INV-ANET-ACH-03, INV-ANET-ACH-04)
        # =========================================================================
        payout_metadata = json.loads(payout.get('metadata', '{}')) if isinstance(payout.get('metadata'), str) else payout.get('metadata', {})
        is_ach_payout = payout_metadata.get('payout_type') == 'ach_transfer'

        # Add payment/refund lines
        for payment in payments:
            amount = Decimal(payment['amount_gross'])

            if payment.get('event_type') == 'refund':
                if payment.get('qbo_payment_id'):
                    # Matched refund — link to QBO RefundReceipt/CreditMemo via LinkedTxn
                    qbo_type = payment.get('matched_qbo_object_type', 'RefundReceipt')
                    lines.append({
                        "amount": str(-abs(amount)),
                        "qbo_refund_id": payment['qbo_payment_id'],
                        "qbo_refund_type": qbo_type,
                        "description": f"Refund {payment['processor_payment_id']}"
                    })
                else:
                    # Unmatched refund — account-based fallback
                    lines.append({
                        "amount": str(-abs(amount)),
                        "account_ref": self.config['qbo_refund_account_id'],
                        "description": f"Refund {payment['processor_payment_id']}"
                    })
            elif payment.get('event_type') == 'ach_return':
                # ACH return — negative line to the ACH Settlement account.
                # The settlement adjustment (below) will add a balancing line so
                # the deposit total matches the user-entered expected amount.
                ach_settlement_acct = self.config.get('qbo_ach_settlement_account_id')
                if not ach_settlement_acct:
                    raise ConfigurationError(
                        "Cannot create ACH deposit: ACH Settlement Account not configured. "
                        "Configure it in processor settings (Integrations > Settings)."
                    )
                customer_name = payment.get('customer_name') or 'Unknown'
                lines.append({
                    "amount": str(-abs(amount)),
                    "account_ref": ach_settlement_acct,
                    "description": f"ACH Return - {customer_name} - {payment['processor_payment_id']}"
                })
            else:
                # Regular payment - link to QBO Payment via LinkedTxn
                # qbo_payment_id is the matched QBO Payment ID from the matching process
                lines.append({
                    "amount": payment['amount_gross'],
                    "qbo_payment_id": payment['qbo_payment_id'],  # Matched QBO Payment ID
                    "description": f"Payment {payment['processor_payment_id']}"
                })

        # Add fee line (negative, to Fee Expense account)
        # Per lifecycle decision: ACH fees use implied-fee-in-deposit model
        # Fee = gross - bank deposit amount, appears as negative line on deposit
        if is_ach_payout:
            # ACH: fee comes from user-confirmed metadata.ach_fees
            ach_fee_amount = Decimal(payout_metadata.get('ach_fees', '0'))
            if ach_fee_amount > 0:
                lines.append({
                    "amount": str(-ach_fee_amount),
                    "account_ref": self.config['qbo_fee_expense_account_id'],
                    "description": "Fees withheld at moment of ACH transfer."
                })
        elif Decimal(payout['fees']) > 0:
            # Credit card: fee comes from payout.fees
            lines.append({
                "amount": f"-{payout['fees']}",
                "account_ref": self.config['qbo_fee_expense_account_id'],
                "description": f"Processing fees for {payout['processor_payout_id']}"
            })

        # =========================================================================
        # ACH SETTLEMENT ADJUSTMENT LINE
        # When the user-entered deposit amount differs from the payment sum,
        # add an adjustment line to the ACH Settlement Account so the QBO
        # deposit total matches what actually hit the bank.
        # =========================================================================
        if is_ach_payout:
            expected_deposit = payout_metadata.get('expected_deposit_amount')
            if expected_deposit:
                expected = Decimal(str(expected_deposit))
                line_sum = sum(Decimal(l['amount']) for l in lines)
                adjustment = expected - line_sum
                if abs(adjustment) > Decimal('0.001'):
                    ach_settlement_acct = self.config.get('qbo_ach_settlement_account_id')
                    if not ach_settlement_acct:
                        raise ConfigurationError(
                            "Cannot create ACH deposit: ACH Settlement Account not configured. "
                            "Configure it in processor settings (Integrations > Settings)."
                        )
                    lines.append({
                        "amount": str(adjustment),
                        "account_ref": ach_settlement_acct,
                        "description": "Change to Settlement Account"
                    })

        # Generate memo with CHARLES stamp (required for all Charles-created Deposits)
        # Format: "CHARLES | Created YYYY-MM-DD HH:MM <TZ>\nPayout: {payout_id}"
        payout_metadata = json.loads(payout.get('metadata', '{}')) if isinstance(payout.get('metadata'), str) else payout.get('metadata', {})
        grouped_ids = payout_metadata.get('grouped_batch_ids')
        if deposit_stamp:
            if grouped_ids:
                memo = f"{deposit_stamp}\nPayouts: {', '.join(grouped_ids)}"
            else:
                memo = generate_deposit_memo(payout['processor_payout_id'], deposit_stamp)
        else:
            # Legacy fallback (should not happen in normal operation)
            memo = f"Payout {payout['processor_payout_id']}"

        # Create posting plan
        operations = [{
            "type": "create_deposit",
            "account_ref": self.config['qbo_bank_account_id'],
            "amount": payout['net_amount'],
            "txn_date": payout['settlement_date'],
            "memo": memo,
            "lines": lines
        }]

        # ACH fees are included as negative line on deposit (implied-fee-in-deposit model)
        # No separate fee expense operation needed

        posting_plan = {
            "id": str(uuid.uuid4()),
            "payout_batch_id": payout['id'],
            "processor_payout_id": payout['processor_payout_id'],
            "plan_type": "deposit",
            "line_count": len(lines),
            "total_amount": payout['net_amount'],
            "deposit_stamp": deposit_stamp,  # Persist stamp with plan for audit
            "operations": operations
        }

        return posting_plan

    def persist_posting_plan(self, posting_plan: Dict) -> str:
        """
        Persist posting plan to database before execution.

        Args:
            posting_plan: Posting plan dict from generate_posting_plan

        Returns:
            Posting plan ID
        """
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()

        try:
            # Phase 2: Include workspace_id (NOT NULL in consolidated DB schema)
            cursor.execute("""
                INSERT INTO posting_plans (
                    id, workspace_id, payout_batch_id, processor_payout_id, plan_type,
                    operations, line_count, total_amount, validated, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                posting_plan['id'],
                self.workspace_id,  # Phase 2: workspace scoping
                posting_plan['payout_batch_id'],
                posting_plan['processor_payout_id'],
                posting_plan['plan_type'],
                json.dumps(posting_plan['operations']),
                posting_plan['line_count'],
                posting_plan['total_amount'],
                1,  # validated
                'validated'
            ))
            conn.commit()

            self.logger.debug(
                EventType.SYNC_EXECUTION_STARTED,
                f"Persisted posting plan {posting_plan['id']}",
                posting_plan_id=posting_plan['id'],
                processor_payout_id=posting_plan['processor_payout_id'],
                line_count=posting_plan['line_count']
            )

            return posting_plan['id']
        finally:
            conn.close()

    def update_posting_plan_status(
        self,
        posting_plan_id: str,
        status: str,
        qbo_deposit_id: str = None,
        error_message: str = None
    ) -> None:
        """Update posting plan status after execution."""
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()

        try:
            if status == 'posted':
                cursor.execute("""
                    UPDATE posting_plans
                    SET status = ?, qbo_deposit_id = ?, execution_completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (status, qbo_deposit_id, posting_plan_id))
            elif status == 'failed':
                cursor.execute("""
                    UPDATE posting_plans
                    SET status = ?, error_message = ?, retry_count = retry_count + 1
                    WHERE id = ?
                """, (status, error_message, posting_plan_id))
            else:
                cursor.execute("""
                    UPDATE posting_plans
                    SET status = ?, execution_started_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (status, posting_plan_id))

            conn.commit()
        finally:
            conn.close()
    
    def validate_posting_plan(self, posting_plan: Dict, payout: Dict) -> bool:
        """
        Validate posting plan
        
        Args:
            posting_plan: Posting plan to validate
            payout: Payout batch record
            
        Returns:
            True if valid
            
        Raises:
            ValueError if validation fails
        """
        operation = posting_plan['operations'][0]
        
        # Calculate line total
        line_total = Decimal('0')
        for line in operation['lines']:
            line_total += Decimal(line['amount'])
        
        # Verify total matches payout net amount
        expected_total = Decimal(payout['net_amount'])
        
        if abs(line_total - expected_total) > Decimal('0.01'):
            raise ValueError(
                f"Line total {line_total} does not match payout net {expected_total}"
            )
        
        return True
    
    def execute_posting_plan(self, posting_plan: Dict) -> str:
        """
        Execute posting plan against QBO API

        Implements ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT §7:
        - Retries infrastructure errors with exponential backoff (5s, 30s, 120s)
        - Non-recoverable errors (authorization, configuration) are not retried

        For ACH payouts (per INV-ANET-ACH-04):
        - Creates deposit without fee lines (gross-only)
        - Creates separate expense for ACH fees

        Args:
            posting_plan: Posting plan to execute

        Returns:
            QBO deposit ID
        """
        import requests
        import os

        # ===================================================================
        # CONTRACT ASSERTIONS: ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT
        # ===================================================================

        # §2.2: Ambiguity must never be silently resolved
        # No ambiguous items may reach the execution phase
        assert posting_plan.get('has_ambiguous_items') is not True, \
            "ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT violated: ambiguous items reached execution"

        assert posting_plan.get('ambiguity_count', 0) == 0, \
            "ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT violated: posting plan contains unresolved ambiguity"

        # Verify no individual line items are marked ambiguous
        for op in posting_plan.get('operations', []):
            if op.get('type') != 'create_deposit':
                continue
            for line in op.get('lines', []):
                assert line.get('is_ambiguous') is not True, \
                    f"ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT violated: " \
                    f"ambiguous line item {line.get('processor_payment_id')} reached execution"
                assert line.get('match_status') != 'ambiguous', \
                    f"ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT violated: " \
                    f"line with ambiguous match status reached execution"

        # Find the deposit operation
        deposit_operation = None

        for op in posting_plan.get('operations', []):
            if op.get('type') == 'create_deposit':
                deposit_operation = op

        if not deposit_operation:
            raise ValueError("Posting plan missing deposit operation")

        operation = deposit_operation

        # Build line items per QBO API spec:
        # - Payment lines: LinkedTxn with TxnType "Payment"
        # - Matched refund lines: LinkedTxn with TxnType "RefundReceipt"/"CreditMemo"
        # - Fee/unmatched refund lines: DepositLineDetail with AccountRef
        line_items = []

        for line_spec in operation['lines']:
            amount = Decimal(line_spec['amount'])

            if 'qbo_payment_id' in line_spec:
                # Payment line - use LinkedTxn to link to existing Payment
                line_item = {
                    "Amount": float(abs(amount)),
                    "LinkedTxn": [{
                        "TxnId": str(line_spec['qbo_payment_id']),
                        "TxnType": "Payment",
                        "TxnLineId": "0"
                    }]
                }
            elif 'qbo_refund_id' in line_spec:
                # Matched refund - use LinkedTxn to link to QBO RefundReceipt/CreditMemo
                line_item = {
                    "Amount": float(amount),  # Negative for refunds
                    "LinkedTxn": [{
                        "TxnId": str(line_spec['qbo_refund_id']),
                        "TxnType": line_spec.get('qbo_refund_type', 'RefundReceipt'),
                        "TxnLineId": "0"
                    }]
                }
            else:
                # Fee/unmatched refund - use DepositLineDetail with AccountRef
                # Negative amounts reduce the deposit total
                line_item = {
                    "Amount": float(amount),  # Keep negative for fees
                    "DetailType": "DepositLineDetail",
                    "DepositLineDetail": {
                        "AccountRef": {
                            "value": str(line_spec['account_ref'])
                        }
                    }
                }

            line_items.append(line_item)

        # Build deposit data
        deposit_data = {
            "DepositToAccountRef": {
                "value": str(operation['account_ref'])
            },
            "TxnDate": operation['txn_date'],
            "PrivateNote": operation['memo'],
            "Line": line_items
        }

        # Get realm_id
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()
        cursor.execute("SELECT qbo_realm_id FROM companies WHERE id = ?", (self.company_id,))
        row = cursor.fetchone()
        conn.close()
        realm_id = row['qbo_realm_id']

        # Determine base URL from fail-fast config (CONFIGURATION_MANAGEMENT_CONTRACT)
        environment = get_config().qbo_environment
        if environment == 'sandbox':
            base_url = 'https://sandbox-quickbooks.api.intuit.com'
        else:
            base_url = 'https://quickbooks.api.intuit.com'

        url = f'{base_url}/v3/company/{realm_id}/deposit?minorversion=65'

        def make_api_call() -> str:
            """Inner function for API call with proper error classification."""
            headers = {
                'Authorization': f'Bearer {self.qbo_client.auth_client.access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            response = requests.post(url, headers=headers, json=deposit_data)

            if response.status_code == 200:
                result = response.json()
                return result.get('Deposit', {}).get('Id')

            # Classify error based on status code per ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT
            error_msg = response.text
            status = response.status_code

            if status == 401 or status == 403:
                # Authorization error - non-recoverable
                raise AuthorizationError(f"QBO API authorization error ({status}): {error_msg}")
            elif status == 400:
                # Bad request - likely data integrity or configuration issue
                if 'duplicate' in error_msg.lower():
                    raise DataIntegrityError(f"QBO API duplicate error: {error_msg}")
                elif 'account' in error_msg.lower() or 'invalid' in error_msg.lower():
                    raise ConfigurationError(f"QBO API configuration error: {error_msg}")
                else:
                    raise DataIntegrityError(f"QBO API validation error ({status}): {error_msg}")
            elif status in [500, 502, 503, 504]:
                # Server/infrastructure error - recoverable with retry
                raise ConnectionError(f"QBO API server error ({status}): {error_msg}")
            elif status == 429:
                # Rate limit - recoverable with retry
                raise ConnectionError(f"QBO API rate limit exceeded: {error_msg}")
            else:
                # Unknown error - treat as infrastructure (recoverable)
                raise ConnectionError(f"QBO API error ({status}): {error_msg}")

        # Execute with retry logic per §7.1:
        # - Max 3 attempts
        # - Backoff: 5s, 30s, 120s
        # - Only infrastructure errors are retried
        deposit_id = retry_operation(
            func=make_api_call,
            operation_name="qbo_create_deposit",
            logger=self.logger,
            max_attempts=MAX_RETRY_ATTEMPTS,
            backoff_schedule=BACKOFF_SCHEDULE_SECONDS
        )

        return deposit_id
    
    def record_qbo_artifact(
        self,
        qbo_deposit_id: str,
        posting_plan: Dict,
        payout: Dict,
        qbo_response: Dict
    ) -> None:
        """
        Record QBO artifact in database

        Args:
            qbo_deposit_id: QBO deposit ID
            posting_plan: Posting plan that was executed
            payout: Payout batch record
            qbo_response: Full QBO API response
        """
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()

        try:
            artifact_id = str(uuid.uuid4())
            processor_payout_id = payout.get('processor_payout_id')
            posting_plan_id = posting_plan.get('id') if posting_plan else None

            # Phase 2: Include workspace_id (NOT NULL in consolidated DB schema)
            cursor.execute("""
                INSERT INTO qbo_artifacts (
                    id, workspace_id, qbo_id, qbo_type, posting_plan_id, payout_batch_id,
                    processor_payout_id, amount, account_ref, txn_date, memo,
                    line_items, status, qbo_response
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                artifact_id,
                self.workspace_id,  # Phase 2: workspace scoping
                qbo_deposit_id,
                'Deposit',
                posting_plan_id,
                payout.get('id'),
                processor_payout_id,
                payout.get('net_amount'),
                self.config['qbo_bank_account_id'],
                payout.get('settlement_date'),
                f"Stripe payout {processor_payout_id}",
                json.dumps(posting_plan.get('operations', [{}])[0].get('lines', [])) if posting_plan else None,
                'created',
                json.dumps(qbo_response) if qbo_response else None
            ))

            conn.commit()

            self.logger.info(
                EventType.QBO_OBJECT_CREATED,
                f"Recorded artifact for Deposit {qbo_deposit_id}",
                qbo_object_type="Deposit",
                qbo_object_id=qbo_deposit_id,
                artifact_id=artifact_id,
                posting_plan_id=posting_plan_id,
                processor_payout_id=processor_payout_id
            )
        except Exception as e:
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Failed to record artifact: {e}",
                warning_type="artifact_recording_failed",
                qbo_object_type="Deposit",
                qbo_object_id=qbo_deposit_id,
                processor_payout_id=payout.get('processor_payout_id'),
                error=str(e)
            )
        finally:
            conn.close()
    
    def mark_payout_as_posted(self, payout_id: str, qbo_deposit_id: str, posting_plan_id: str) -> None:
        """
        Mark payout batch as posted and update associated canonical events.

        Args:
            payout_id: Payout batch UUID
            qbo_deposit_id: QBO deposit ID
            posting_plan_id: Posting plan UUID
        """
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()

        # Get the processor_payout_id to update canonical_events
        # Phase 2: Filter by workspace_id
        cursor.execute("SELECT processor_payout_id FROM payout_batches WHERE id = ? AND workspace_id = ?", (payout_id, self.workspace_id))
        row = cursor.fetchone()
        processor_payout_id = row['processor_payout_id'] if row else None

        # Update payout_batches
        cursor.execute("""
            UPDATE payout_batches
            SET qbo_deposit_id = ?,
                posting_plan_id = ?,
                status = 'posted',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (qbo_deposit_id, posting_plan_id, payout_id))

        # Phase 3 Remediation: update matching_outcomes workflow_status (§6.2)
        if processor_payout_id:
            cursor.execute("""
                UPDATE matching_outcomes
                SET workflow_status = 'posted',
                    updated_at = CURRENT_TIMESTAMP
                WHERE canonical_fact_id IN (
                    SELECT id FROM canonical_facts WHERE processor_payout_id = ?
                )
            """, (processor_payout_id,))

        conn.commit()
        conn.close()

    def _atomic_finalization(
        self,
        qbo_deposit_id: str,
        posting_plan: Dict,
        payout: Dict,
        idempotency_key: str,
        qbo_realm_id: str,
        api_duration_ms: int
    ) -> None:
        """
        Atomic finalization of all state updates after posting.

        CANONICAL_ARCHITECTURE_CONTRACT §NC-2 Compliance:
        This function encapsulates ALL state updates that must happen after posting.
        It executes as a single atomic transaction to maintain consistency.

        This function is ONLY called from within _execute_posting_with_finalization(),
        which ensures posting happens first and this happens second, with no
        conditional logic between them.

        Args:
            qbo_deposit_id: QBO deposit ID from posting
            posting_plan: The posting plan that was executed
            payout: Payout batch record
            idempotency_key: Idempotency key for this operation
            qbo_realm_id: QBO realm ID for logging
            api_duration_ms: Duration of API call in milliseconds

        Raises:
            Exception: If atomic finalization fails. This is CRITICAL because posting
                      already succeeded - the QBO deposit exists but our state is inconsistent.
        """
        processor_payout_id = payout['processor_payout_id']

        # ===================================================================
        # ATOMIC STATE UPDATES (single transaction)
        # ===================================================================
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()

        try:
            self.logger.debug(
                EventType.SYNC_EXECUTION_STARTED,
                f"Starting atomic finalization for deposit {qbo_deposit_id}",
                qbo_deposit_id=qbo_deposit_id,
                processor_payout_id=processor_payout_id,
                idempotency_key=idempotency_key,
                phase="finalization"
            )
            # 1. Mark idempotency operation as completed
            cursor.execute("""
                UPDATE idempotency_keys
                SET status = 'completed',
                    result_id = ?,
                    result_data = ?,
                    completed_at = CURRENT_TIMESTAMP
                WHERE idempotency_key = ? AND status = 'pending'
            """, (
                qbo_deposit_id,
                str({
                    "processor_payout_id": processor_payout_id,
                    "amount": payout['net_amount'],
                    "qbo_realm_id": qbo_realm_id
                }),
                idempotency_key
            ))

            # 2. Update posting plan status
            cursor.execute("""
                UPDATE posting_plans
                SET status = 'posted',
                    qbo_deposit_id = ?,
                    execution_completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (qbo_deposit_id, posting_plan['id']))

            # 3. Record QBO artifact
            # Phase 2: Include workspace_id (NOT NULL in consolidated DB schema)
            artifact_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO qbo_artifacts (
                    id, workspace_id, qbo_id, qbo_type, posting_plan_id, payout_batch_id,
                    processor_payout_id, amount, account_ref, txn_date, memo,
                    line_items, status, qbo_response
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                artifact_id,
                self.workspace_id,  # Phase 2: workspace scoping
                qbo_deposit_id,
                'Deposit',
                posting_plan['id'],
                payout['id'],
                processor_payout_id,
                payout['net_amount'],
                self.config['qbo_bank_account_id'],
                payout['settlement_date'],
                posting_plan['operations'][0]['memo'],  # Use actual memo with CHARLES stamp
                json.dumps(posting_plan['operations'][0]['lines']),
                'created',
                json.dumps({"Id": qbo_deposit_id})
            ))

            # 4. Mark payout as posted
            cursor.execute("""
                UPDATE payout_batches
                SET qbo_deposit_id = ?,
                    posting_plan_id = ?,
                    status = 'posted',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (qbo_deposit_id, posting_plan['id'], payout['id']))

            # 5. Phase 3 Remediation: update matching_outcomes workflow_status (§6.2)
            cursor.execute("""
                UPDATE matching_outcomes
                SET workflow_status = 'posted',
                    updated_at = CURRENT_TIMESTAMP
                WHERE canonical_fact_id IN (
                    SELECT id FROM canonical_facts WHERE processor_payout_id = ?
                )
            """, (processor_payout_id,))

            # Commit all updates atomically
            conn.commit()

            self.logger.debug(
                EventType.SYNC_EXECUTION_STARTED,
                f"Atomic finalization committed successfully for deposit {qbo_deposit_id}",
                qbo_deposit_id=qbo_deposit_id,
                processor_payout_id=processor_payout_id,
                phase="finalization_complete"
            )

            # ===================================================================
            # LOGGING (non-transactional, informational only)
            # ===================================================================
            self.logger.log_qbo_created(
                qbo_object_type="Deposit",
                qbo_object_id=qbo_deposit_id,
                idempotency_key=idempotency_key,
                processor_payout_id=processor_payout_id,
                duration_ms=api_duration_ms,
                qbo_realm_id=qbo_realm_id,
                amount=payout['net_amount']
            )

        except Exception as e:
            conn.rollback()

            # CRITICAL ERROR: Posting succeeded but finalization failed
            # This means QBO has the deposit but our database is inconsistent
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"CRITICAL: Atomic finalization failed after successful posting. "
                f"QBO Deposit {qbo_deposit_id} exists but state not recorded. "
                f"Error: {str(e)}",
                error_category="finalization_failure",
                qbo_deposit_id=qbo_deposit_id,
                processor_payout_id=processor_payout_id,
                idempotency_key=idempotency_key,
                qbo_realm_id=qbo_realm_id,
                error_message=str(e),
                phase="finalization",
                recovery_required=True
            )

            # Re-raise with context
            raise RuntimeError(
                f"Finalization failed after successful QBO posting. "
                f"Deposit {qbo_deposit_id} created but not recorded. "
                f"Original error: {str(e)}"
            ) from e

        finally:
            conn.close()

    def _execute_posting_with_finalization(
        self,
        posting_plan: Dict,
        payout: Dict,
        idempotency_key: str,
        qbo_realm_id: str
    ) -> str:
        """
        TERMINAL POSTING BOUNDARY.

        Executes QBO posting and atomic finalization as a single operation.

        CANONICAL_ARCHITECTURE_CONTRACT §NC-2 Compliance:
        - Posting (execute_posting_plan) happens first
        - Atomic finalization happens second (bundled state updates)
        - Function returns immediately after
        - NO conditional logic, branching, or retries after this function returns
        - Calling code MUST return immediately after calling this function

        Per NC-2: "Posting is terminal and irreversible"
        Per NC-2: "No mutation after posting"

        This function encapsulates the terminal boundary. Once posting executes,
        only atomic finalization is allowed. No other code paths may follow.

        Args:
            posting_plan: Validated posting plan
            payout: Payout batch record
            idempotency_key: Idempotency key
            qbo_realm_id: QBO realm ID

        Returns:
            QBO deposit ID

        Raises:
            Exception: If posting or finalization fails
        """
        # Mark posting plan as executing (last state change before posting)
        self.update_posting_plan_status(posting_plan['id'], 'executing')

        # ===================================================================
        # POSTING (TERMINAL OPERATION)
        # ===================================================================
        try:
            api_start_time = time.time()
            qbo_deposit_id = self.execute_posting_plan(posting_plan)
            api_duration_ms = int((time.time() - api_start_time) * 1000)

            self.logger.info(
                EventType.QBO_OBJECT_CREATED,
                f"QBO posting succeeded: Deposit {qbo_deposit_id} created",
                qbo_deposit_id=qbo_deposit_id,
                idempotency_key=idempotency_key,
                phase="posting_complete",
                duration_ms=api_duration_ms
            )

            # Phase 5.3: Instrumentation hook - QBO API call success
            metrics = get_metrics_collector()
            metrics.increment_counter(
                "qbo_api_calls",
                self.workspace_id,
                {"operation": "create_deposit", "status": "success"}
            )
            metrics.record_histogram(
                "qbo_api_duration",
                api_duration_ms / 1000.0,  # Convert to seconds
                self.workspace_id,
                {"operation": "create_deposit", "status": "success"}
            )

        except Exception as e:
            # Posting failed - QBO deposit NOT created
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"QBO posting failed: {str(e)}",
                error_category="posting_failure",
                idempotency_key=idempotency_key,
                error_message=str(e),
                phase="posting"
            )

            # Phase 5.3: Instrumentation hook - QBO API call failure
            metrics = get_metrics_collector()
            metrics.increment_counter(
                "qbo_api_calls",
                self.workspace_id,
                {"operation": "create_deposit", "status": "error"}
            )
            # Re-raise with clear context that posting failed
            raise RuntimeError(f"QBO API posting failed: {str(e)}") from e

        # ===================================================================
        # ATOMIC FINALIZATION (bundled with posting, no conditional logic)
        # ===================================================================
        self._atomic_finalization(
            qbo_deposit_id=qbo_deposit_id,
            posting_plan=posting_plan,
            payout=payout,
            idempotency_key=idempotency_key,
            qbo_realm_id=qbo_realm_id,
            api_duration_ms=api_duration_ms
        )

        # Return immediately (terminal boundary enforced)
        return qbo_deposit_id
    
    def create_deposit_for_payout(self, payout_id: str) -> str:
        """
        Main workflow: Create QBO deposit for a payout batch

        Implements IDEMPOTENCY_IMPLEMENTATION_CONTRACT §7.1 pattern:
        1. Compute idempotency key
        2. Check idempotency status
        3. If EXISTS_COMPLETED -> return existing result
        4. If EXISTS_PENDING -> reject
        5. If EXISTS_FAILED or NOT_EXISTS -> create pending record, proceed
        6. Execute API call
        7. Mark completed or failed

        Args:
            payout_id: Payout batch UUID

        Returns:
            QBO deposit ID
        """
        start_time = time.time()

        # Get payout record
        conn = self.db_manager.get_connection()  # Phase 2: consolidated DB
        cursor = conn.cursor()

        # Phase 2: Filter by workspace_id
        cursor.execute("SELECT * FROM payout_batches WHERE id = ? AND workspace_id = ?", (payout_id, self.workspace_id))
        payout = dict(cursor.fetchone())
        conn.close()

        # Reload configuration with the correct processor type from this payout
        # (QBOAdapter may have been initialized before we knew which processor)
        processor_type = payout.get('processor', 'stripe')
        self.config = self._load_configuration(processor_type)

        processor_payout_id = payout['processor_payout_id']
        # v2 key with workspace scope; legacy key for dual-read backward compatibility
        idempotency_key = self._get_idempotency_key(processor_payout_id, processor=processor_type)
        legacy_key = self._get_legacy_idempotency_key(processor_payout_id)
        qbo_realm_id = self._get_qbo_realm_id()

        # Log payout processing started (§4.2)
        self.logger.log_payout_started(
            processor_payout_id=processor_payout_id,
            gross_amount=payout['gross_amount'],
            net_amount=payout['net_amount'],
            payment_count=payout['payment_count'],
            processor=payout.get('processor', 'stripe'),
            qbo_realm_id=qbo_realm_id
        )

        # ===================================================================
        # IDEMPOTENCY CHECK (§3.2 Check Before Act, v1.1 Dual-Read)
        # ===================================================================
        idempotency_result = self.idempotency.check_idempotency_dual_read(
            v2_key=idempotency_key,
            legacy_key=legacy_key
        )

        if idempotency_result.status == IdempotencyStatus.EXISTS_COMPLETED:
            # Already done - return cached result (§6.2)
            existing_deposit_id = idempotency_result.result_id
            self.logger.log_qbo_creation_skipped(
                qbo_object_type="Deposit",
                qbo_object_id=existing_deposit_id,
                idempotency_key=idempotency_key,
                processor_payout_id=processor_payout_id,
                reason="idempotency_hit_completed",
                qbo_realm_id=qbo_realm_id
            )
            return existing_deposit_id

        if idempotency_result.status == IdempotencyStatus.EXISTS_PENDING:
            # PENDING - check if actually completed via crash recovery
            # Per R-5: Handle finalization failures (posting succeeded but finalization failed)
            self.logger.info(
                EventType.RETRY_ATTEMPTED,
                f"Operation PENDING for {processor_payout_id}, checking for completion",
                processor_payout_id=processor_payout_id,
                idempotency_key=idempotency_key,
                recovery_type="pending_crash_recovery"
            )

            existing_qbo_deposit = self.query_qbo_for_existing_deposit(
                processor_payout_id=processor_payout_id,
                settlement_date=payout['settlement_date'],
                net_amount=payout['net_amount']
            )

            if existing_qbo_deposit:
                # Posting succeeded but finalization failed - complete it now
                self.idempotency.complete_operation(
                    idempotency_key,
                    result_id=existing_qbo_deposit,
                    result_data={
                        "processor_payout_id": processor_payout_id,
                        "amount": payout['net_amount'],
                        "qbo_realm_id": qbo_realm_id,
                        "recovery_type": "pending_completion_detected"
                    }
                )

                # Mark payout as posted
                self.mark_payout_as_posted(payout['id'], existing_qbo_deposit, "recovered")

                self.logger.info(
                    EventType.QBO_OBJECT_CREATION_SKIPPED,
                    f"Recovered PENDING operation: {existing_qbo_deposit}",
                    qbo_deposit_id=existing_qbo_deposit,
                    processor_payout_id=processor_payout_id,
                    idempotency_key=idempotency_key,
                    recovery_type="pending_crash_recovery"
                )

                return existing_qbo_deposit

            # Operation truly pending - reject concurrent execution
            raise RuntimeError(
                f"Operation in progress for payout {processor_payout_id}. "
                f"Idempotency key: {idempotency_key}"
            )

        # ===================================================================
        # CRASH RECOVERY: QBO DUPLICATE DETECTION (§13.2)
        # ===================================================================
        if idempotency_result.status == IdempotencyStatus.EXISTS_FAILED:
            # Previous attempt failed - could be after QBO API call but before recording
            # Query QBO to check if deposit already exists (crash recovery)
            self.logger.info(
                EventType.RETRY_ATTEMPTED,
                f"Previous attempt failed for payout {processor_payout_id}, checking QBO for existing deposit",
                processor_payout_id=processor_payout_id,
                idempotency_key=idempotency_key,
                recovery_type="crash_recovery"
            )

            existing_qbo_deposit = self.query_qbo_for_existing_deposit(
                processor_payout_id=processor_payout_id,
                settlement_date=payout['settlement_date'],
                net_amount=payout['net_amount']
            )

            if existing_qbo_deposit:
                # Deposit already exists in QBO - update our records and return
                self.idempotency.complete_operation(
                    idempotency_key,
                    result_id=existing_qbo_deposit,
                    result_data={
                        "processor_payout_id": processor_payout_id,
                        "amount": payout['net_amount'],
                        "qbo_realm_id": qbo_realm_id,
                        "recovery_type": "qbo_duplicate_detection"
                    }
                )

                # Mark payout as posted
                self.mark_payout_as_posted(payout['id'], existing_qbo_deposit, "recovered")

                self.logger.info(
                    EventType.QBO_OBJECT_CREATION_SKIPPED,
                    f"Recovered existing QBO Deposit {existing_qbo_deposit} for payout {processor_payout_id}",
                    qbo_object_type="Deposit",
                    qbo_object_id=existing_qbo_deposit,
                    idempotency_key=idempotency_key,
                    processor_payout_id=processor_payout_id,
                    qbo_realm_id=qbo_realm_id,
                    recovery_type="crash_recovery"
                )

                return existing_qbo_deposit

        # EXISTS_FAILED (no QBO duplicate found) or NOT_EXISTS: proceed with operation
        # Create pending record BEFORE any API call (§3.2)
        self.idempotency.start_operation(idempotency_key, "deposit")

        # ===================================================================
        # CONTRACT ASSERTIONS
        # ===================================================================

        # IDEMPOTENCY_IMPLEMENTATION_CONTRACT §3.2: Key must exist before mutation
        assert idempotency_key is not None and idempotency_key != "", \
            "IDEMPOTENCY_IMPLEMENTATION_CONTRACT violated: mutation without idempotency key"

        # CANONICAL_ARCHITECTURE_CONTRACT §2.2: QBO module must not receive ambiguity data
        # This ensures no unresolved matching happened upstream
        assert 'candidate_matches' not in payout, \
            "CANONICAL_ARCHITECTURE_CONTRACT violated: QBO module received unresolved candidate_matches"
        assert 'is_ambiguous' not in payout or payout.get('is_ambiguous') is False, \
            "CANONICAL_ARCHITECTURE_CONTRACT violated: QBO module received ambiguous payout"
        assert 'match_confidence' not in payout or payout.get('match_confidence', 1.0) >= 1.0, \
            "CANONICAL_ARCHITECTURE_CONTRACT violated: QBO module received low-confidence match"

        # ===================================================================
        # PRE-POSTING DUPLICATE CHECK (R-5 Guard)
        # ===================================================================
        # Per CANONICAL_ARCHITECTURE_CONTRACT §5.1: No duplicate QBO objects
        # Double-check QBO before posting to prevent duplicates during retries
        existing_duplicate = self.query_qbo_for_existing_deposit(
            processor_payout_id=processor_payout_id,
            settlement_date=payout['settlement_date'],
            net_amount=payout['net_amount']
        )

        if existing_duplicate:
            # Duplicate exists! Should not happen, but handle gracefully
            self.logger.warn(
                EventType.WARNING_DETECTED,
                f"Pre-posting guard detected duplicate: {existing_duplicate}",
                qbo_deposit_id=existing_duplicate,
                processor_payout_id=processor_payout_id,
                idempotency_key=idempotency_key,
                warning_type="pre_posting_duplicate_detected"
            )

            # Complete idempotency operation
            self.idempotency.complete_operation(
                idempotency_key,
                result_id=existing_duplicate,
                result_data={"guard_type": "pre_posting_duplicate_prevention"}
            )

            # Mark payout as posted
            self.mark_payout_as_posted(payout['id'], existing_duplicate, "recovered")

            return existing_duplicate

        # ===================================================================
        # OPERATION EXECUTION
        # ===================================================================
        try:
            # Get payments — check for grouped ACH batches
            payout_metadata = json.loads(payout.get('metadata', '{}')) if isinstance(payout.get('metadata'), str) else payout.get('metadata', {})
            grouped_ids = payout_metadata.get('grouped_batch_ids')
            if grouped_ids:
                payments = self.get_payments_for_payout_group(grouped_ids)
            else:
                payments = self.get_payments_for_payout(processor_payout_id)

            self.logger.debug(
                EventType.PAYOUT_PROCESSING_STARTED,
                f"Found {len(payments)} payments in payout {processor_payout_id}"
                + (f" (grouped: {len(grouped_ids)} batches)" if grouped_ids else ""),
                processor_payout_id=processor_payout_id,
                payment_count=len(payments)
            )

            # ===============================================================
            # DEPOSIT STAMP GENERATION (IDEMPOTENT)
            # ===============================================================
            # Per spec: The stamp MUST be generated once per logical Deposit
            # and reused on retry/replay. Time zone sourced from QBO CompanyInfo.
            deposit_stamp = self._get_or_create_deposit_stamp(
                payout_id=payout['id'],
                processor_payout_id=processor_payout_id
            )

            self.logger.debug(
                EventType.SYNC_EXECUTION_STARTED,
                f"Using deposit stamp for payout {processor_payout_id}",
                processor_payout_id=processor_payout_id,
                deposit_stamp=deposit_stamp
            )

            # Generate posting plan with CHARLES stamp
            posting_plan = self.generate_posting_plan(payout, payments, deposit_stamp)

            # Validate posting plan
            self.validate_posting_plan(posting_plan, payout)

            # Persist posting plan before execution
            self.persist_posting_plan(posting_plan)

            # Log QBO object creation attempt (§4.4)
            self.logger.log_qbo_creation_attempted(
                qbo_object_type="Deposit",
                idempotency_key=idempotency_key,
                processor_payout_id=processor_payout_id,
                qbo_realm_id=qbo_realm_id,
                amount=payout['net_amount']
            )

            # ===================================================================
            # TERMINAL POSTING BOUNDARY (§NC-2)
            # ===================================================================
            # Execute posting with atomic finalization
            # This function MUST be the last operation in this try block
            # No conditional logic, branching, or retries allowed after this call
            qbo_deposit_id = self._execute_posting_with_finalization(
                posting_plan=posting_plan,
                payout=payout,
                idempotency_key=idempotency_key,
                qbo_realm_id=qbo_realm_id
            )

            # Calculate total duration for logging
            total_duration_ms = int((time.time() - start_time) * 1000)

            # Log payout processing completed (§4.2)
            self.logger.log_payout_completed(
                processor_payout_id=processor_payout_id,
                qbo_deposit_id=qbo_deposit_id,
                duration_ms=total_duration_ms,
                processor=payout.get('processor', 'stripe'),
                qbo_realm_id=qbo_realm_id
            )

            # IMMEDIATE RETURN - no logic after terminal posting boundary
            return qbo_deposit_id

        except Exception as e:
            # ===================================================================
            # MARK OPERATION FAILED (§5.3)
            # ===================================================================
            self.idempotency.fail_operation(idempotency_key, str(e))
            raise  # Re-raise to be handled by caller
    
    def process_all_pending_payouts(self, initiation_source: str = "manual") -> Dict:
        """
        Process all payouts ready for deposit.

        Implements SYNC_RUN_LIFECYCLE_CONTRACT state machine:
        initiated → discovering → matching → executing → (terminal)

        Args:
            initiation_source: How sync was initiated (manual, scheduled, api)

        Returns:
            Summary dict with counts and sync_run_id
        """
        # ===================================================================
        # PHASE 1: INITIATION (§5.1)
        # ===================================================================
        try:
            sync_run = self.sync_manager.start_sync(initiation_source=initiation_source)
        except RuntimeError as e:
            # Another sync in progress
            self.logger.error(
                EventType.ERROR_DETECTED,
                f"Cannot start sync: {e}",
                error_category="concurrent_sync"
            )
            return {
                "sync_run_id": None,
                "payouts_found": 0,
                "deposits_created": 0,
                "errors": 1,
                "skipped": 0,
                "error": str(e)
            }

        # Update logger and instance sync_run_id
        self.sync_run_id = sync_run.sync_run_id
        self.logger.set_sync_run_id(sync_run.sync_run_id)

        stats = SyncRunStats()
        has_warnings = False

        try:
            # ===================================================================
            # PHASE 2: DISCOVERY (§5.2)
            # ===================================================================
            self.sync_manager.transition_to(sync_run.sync_run_id, SyncState.DISCOVERING)

            payouts = self.find_payouts_ready_for_deposit()
            stats.payouts_total = len(payouts)

            self.logger.info(
                EventType.SYNC_STARTED,
                f"Discovery complete: found {len(payouts)} payouts ready for deposit",
                payouts_total=len(payouts),
                phase="discovering"
            )

            if len(payouts) == 0:
                # No payouts to process - complete immediately
                self.sync_manager.complete_sync(sync_run.sync_run_id, stats)
                return {
                    "sync_run_id": sync_run.sync_run_id,
                    "payouts_found": 0,
                    "deposits_created": 0,
                    "errors": 0,
                    "skipped": 0
                }

            # ===================================================================
            # PHASE 3: MATCHING (§5.3)
            # For Module 3, matching is done during payout fetch (already matched)
            # ===================================================================
            self.sync_manager.transition_to(sync_run.sync_run_id, SyncState.MATCHING)

            self.logger.info(
                EventType.SYNC_STARTED,
                "Matching phase complete (payments pre-matched in Module 4)",
                phase="matching"
            )

            # ===================================================================
            # PHASE 4: EXECUTION (§5.4)
            # ===================================================================
            self.sync_manager.transition_to(sync_run.sync_run_id, SyncState.EXECUTING)

            for payout in payouts:
                # ---------------------------------------------------------------
                # ACTIVE TIMEOUT CHECK (§6) - Check before processing each payout
                # ---------------------------------------------------------------
                self.sync_manager.check_timeout_active(sync_run.sync_run_id)

                payout_start_time = time.time()
                processor_payout_id = payout['processor_payout_id']
                processor_type = payout.get('processor', 'stripe')
                idempotency_key = self._get_idempotency_key(processor_payout_id, processor=processor_type)

                try:
                    self.create_deposit_for_payout(payout['id'])
                    stats.payouts_processed += 1

                except RuntimeError as e:
                    # Operation in progress (idempotency pending) - skip
                    stats.payouts_skipped += 1
                    self.logger.warn(
                        EventType.WARNING_DETECTED,
                        f"Skipped payout {processor_payout_id}: {e}",
                        warning_type="payout_skipped",
                        processor_payout_id=processor_payout_id
                    )
                    has_warnings = True

                except Exception as e:
                    payout_duration_ms = int((time.time() - payout_start_time) * 1000)

                    # Classify error per ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT
                    error_message = str(e)
                    error_category = self._classify_error(e)

                    # Log payout processing failed (§4.2)
                    self.logger.log_payout_failed(
                        processor_payout_id=processor_payout_id,
                        error_message=error_message,
                        error_category=error_category,
                        duration_ms=payout_duration_ms,
                        idempotency_key=idempotency_key
                    )

                    # Log QBO object creation failed (§4.4)
                    self.logger.log_qbo_creation_failed(
                        qbo_object_type="Deposit",
                        idempotency_key=idempotency_key,
                        processor_payout_id=processor_payout_id,
                        error_message=error_message,
                        duration_ms=payout_duration_ms
                    )

                    stats.payouts_failed += 1

                    # Check if authorization error - should abort entire sync
                    if error_category == "authorization_error":
                        self.sync_manager.fail_sync(
                            sync_run.sync_run_id,
                            reason=f"Authorization error: {error_message}",
                            stats=stats
                        )
                        return {
                            "sync_run_id": sync_run.sync_run_id,
                            "payouts_found": stats.payouts_total,
                            "deposits_created": stats.payouts_processed,
                            "errors": stats.payouts_failed,
                            "skipped": stats.payouts_skipped,
                            "error": error_message
                        }

            # ===================================================================
            # PHASE 5: COMPLETION (§5.4)
            # ===================================================================
            self.sync_manager.complete_sync(sync_run.sync_run_id, stats, has_warnings)

            return {
                "sync_run_id": sync_run.sync_run_id,
                "payouts_found": stats.payouts_total,
                "deposits_created": stats.payouts_processed,
                "errors": stats.payouts_failed,
                "skipped": stats.payouts_skipped
            }

        except TimeoutError as e:
            # Timeout during any phase (§6)
            current_phase = self.sync_manager.get_sync_run(sync_run.sync_run_id).state.value
            self.sync_manager.timeout_sync(sync_run.sync_run_id, current_phase)
            return {
                "sync_run_id": sync_run.sync_run_id,
                "payouts_found": stats.payouts_total,
                "deposits_created": stats.payouts_processed,
                "errors": stats.payouts_failed,
                "skipped": stats.payouts_skipped,
                "error": f"Timeout in {current_phase} phase"
            }

        except Exception as e:
            # Unexpected error - fail the sync
            self.sync_manager.fail_sync(
                sync_run.sync_run_id,
                reason=str(e),
                stats=stats
            )
            return {
                "sync_run_id": sync_run.sync_run_id,
                "payouts_found": stats.payouts_total,
                "deposits_created": stats.payouts_processed,
                "errors": stats.payouts_failed + 1,
                "skipped": stats.payouts_skipped,
                "error": str(e)
            }

    def _classify_error(self, error: Exception) -> str:
        """
        Classify error per ERROR_VS_AMBIGUITY_TAXONOMY_CONTRACT §3.

        Uses the canonical classify_error function from error_handling module.

        Returns one of:
        - authorization_error (Non-Recoverable)
        - infrastructure_error (Recoverable)
        - configuration_error (Non-Recoverable)
        - data_integrity_error (Non-Recoverable)
        """
        classified = classify_error(error)
        return classified.category.value
