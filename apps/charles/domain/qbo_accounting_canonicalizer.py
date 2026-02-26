"""
QBO Accounting Canonicalizer

Phase 3B: Accounting Canonical Facts — QuickBooks Online Implementation

This module provides the PURE, ISOLATED canonicalization layer for transforming
QuickBooks Online accounting objects into canonical facts.

Per PHASE3B-ACCOUNTING_CANONICAL_FACTS_CONTRACT.md:
- ACF-01 (Observation Only): Facts assert "QBO contained object O at time T"
- ACF-02 (No Correctness Assertion): Facts do not assert economic correctness
- ACF-03 (Immutability): Facts are immutable once created
- ACF-04 (Append-Only): New observations create new facts, never mutate
- ACF-05 (Deterministic): Same input MUST produce same canonical output
- ACF-06 (Idempotent): Re-canonicalization MUST NOT create duplicates

HARD CONSTRAINTS (NON-NEGOTIABLE):
During accounting canonical fact creation, this module MUST NOT:
- Perform matching between processor and accounting facts
- Infer relationships, correctness, or reconciliation state
- Create, update, or delete QBO objects
- Write to matching tables
- Read processor canonical facts
- Encode system-specific accounting rules (Undeposited Funds, etc.)

This module writes ONLY to the canonical_facts table.
"""

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal
from enum import Enum

# Current normalization version for accounting facts
ACCOUNTING_NORMALIZATION_VERSION = '3.0.0'

logger = logging.getLogger(__name__)


# =============================================================================
# ACCOUNTING CANONICAL EVENT TYPES (Per Phase 3B Contract)
# =============================================================================

class AccountingCanonicalEventType(Enum):
    """
    Accounting canonical event types per PHASE3B-ACCOUNTING_CANONICAL_FACTS_CONTRACT.md.

    These types represent observations of accounting-system objects.
    They are distinct from processor event types (payment, refund, etc.).
    """
    PAYMENT_OBSERVED = 'payment_observed'
    REFUND_OBSERVED = 'refund_observed'
    DEPOSIT_OBSERVED = 'deposit_observed'


class QBOObjectType(Enum):
    """
    QBO object types that can be canonicalized.

    This is the source_object_type value stored in canonical_facts.
    """
    PAYMENT = 'Payment'
    REFUND_RECEIPT = 'RefundReceipt'
    CREDIT_MEMO = 'CreditMemo'
    DEPOSIT = 'Deposit'


# =============================================================================
# CREATION RESULT DATA CLASS
# =============================================================================

@dataclass
class AccountingCanonicalCreationResult:
    """
    Result of accounting canonical fact creation.

    Contains both the canonical_fact_id and audit information
    for logging and debugging purposes.
    """
    canonical_fact_id: str
    workspace_id: str
    source_system: str
    source_object_type: str
    source_object_id: str
    event_type: str
    amount: Decimal
    observed_at: datetime
    normalization_version: str
    created_at: datetime
    was_duplicate: bool = False

    def to_audit_dict(self) -> Dict[str, Any]:
        """Return audit-friendly dictionary for logging."""
        return {
            'canonical_fact_id': self.canonical_fact_id,
            'workspace_id': self.workspace_id,
            'source_system': self.source_system,
            'source_object_type': self.source_object_type,
            'source_object_id': self.source_object_id,
            'event_type': self.event_type,
            'amount': str(self.amount),
            'observed_at': self.observed_at.isoformat() if self.observed_at else None,
            'normalization_version': self.normalization_version,
            'created_at': self.created_at.isoformat(),
            'was_duplicate': self.was_duplicate,
        }


# =============================================================================
# DETERMINISTIC ID GENERATION (Per Phase 3B Contract)
# =============================================================================

def generate_accounting_canonical_id(
    workspace_id: str,
    source_system: str,
    source_object_type: str,
    source_object_id: str,
    observation_type: str = 'observed',
) -> str:
    """
    Generate a deterministic canonical ID for accounting facts.

    Per PHASE3B-ACCOUNTING_CANONICAL_FACTS_CONTRACT.md:
    - canonical_id = SHA256(workspace_id | source_system | source_object_type | source_object_id | observation_type)
    - IDs MUST be deterministic across runs
    - IDs MUST be globally unique per workspace

    Args:
        workspace_id: Workspace identifier
        source_system: Accounting system name ('qbo', 'xero', etc.)
        source_object_type: Object type from source system ('Payment', 'Deposit', etc.)
        source_object_id: Object ID from source system
        observation_type: Type of observation (default 'observed')

    Returns:
        SHA256 hash as canonical fact ID
    """
    key_parts = [
        workspace_id,
        source_system,
        source_object_type.lower(),  # Normalize to lowercase
        source_object_id,
        observation_type,
    ]
    key_string = '|'.join(str(p) for p in key_parts)
    return hashlib.sha256(key_string.encode()).hexdigest()


# =============================================================================
# QBO ACCOUNTING CANONICALIZER CLASS
# =============================================================================

class QBOAccountingCanonicalizer:
    """
    Pure canonicalization engine for QuickBooks Online accounting objects.

    This class provides isolated, deterministic canonicalization
    with NO matching or inference side effects.

    Per PHASE3B-ACCOUNTING_CANONICAL_FACTS_CONTRACT.md:
    - Reads ONLY QBO objects (passed as input, not fetched)
    - Writes ONLY to canonical_facts table
    - Has NO dependency on processor canonical facts
    - Has NO dependency on matching state
    - Is deterministic and idempotent
    """

    SOURCE_SYSTEM = 'qbo'

    def __init__(self, workspace_id: str):
        """
        Initialize the QBO accounting canonicalizer.

        Args:
            workspace_id: Workspace identifier for data isolation
        """
        self.workspace_id = workspace_id
        self.normalization_version = ACCOUNTING_NORMALIZATION_VERSION

    def _generate_canonical_id(
        self,
        source_object_type: str,
        source_object_id: str,
    ) -> str:
        """
        Generate a deterministic canonical fact ID for this workspace.

        Args:
            source_object_type: QBO object type (Payment, Deposit, etc.)
            source_object_id: QBO object Id

        Returns:
            SHA256 hash as canonical fact ID
        """
        return generate_accounting_canonical_id(
            workspace_id=self.workspace_id,
            source_system=self.SOURCE_SYSTEM,
            source_object_type=source_object_type,
            source_object_id=source_object_id,
            observation_type='observed',
        )

    def canonicalize_payment_observed(
        self,
        cursor: sqlite3.Cursor,
        qbo_payment: Dict[str, Any],
        observed_at: Optional[datetime] = None,
    ) -> AccountingCanonicalCreationResult:
        """
        Create a canonical fact from an observed QBO Payment object.

        Per Phase 3B contract, this captures:
        - The existence of a Payment in QBO
        - Its observable properties at observation time
        - NO assertion about correctness or matching

        Args:
            cursor: Database cursor (transaction managed by caller)
            qbo_payment: QBO Payment object as dictionary
            observed_at: When the observation was made (defaults to now)

        Returns:
            AccountingCanonicalCreationResult with fact details and audit info
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        observed_at = observed_at or now
        event_type = AccountingCanonicalEventType.PAYMENT_OBSERVED.value
        source_object_type = QBOObjectType.PAYMENT.value

        # Extract QBO Payment ID
        qbo_id = str(qbo_payment.get('Id', ''))
        if not qbo_id:
            raise ValueError("QBO Payment must have an Id")

        # Generate deterministic canonical ID
        canonical_id = self._generate_canonical_id(source_object_type, qbo_id)

        # Check for idempotency (duplicate detection)
        cursor.execute("SELECT * FROM canonical_facts WHERE id = ?", (canonical_id,))
        existing = cursor.fetchone()
        if existing:
            existing_dict = dict(existing)
            logger.info(
                f"[CANONICAL-QBO] Idempotent skip: Payment {qbo_id} already canonicalized "
                f"as {canonical_id[:16]}..."
            )
            return AccountingCanonicalCreationResult(
                canonical_fact_id=existing_dict['id'],
                workspace_id=self.workspace_id,
                source_system=self.SOURCE_SYSTEM,
                source_object_type=source_object_type,
                source_object_id=qbo_id,
                event_type=event_type,
                amount=Decimal(str(existing_dict.get('amount_gross', '0'))),
                observed_at=datetime.fromisoformat(existing_dict.get('observed_at', now.isoformat())) if existing_dict.get('observed_at') else now,
                normalization_version=existing_dict.get('normalization_version', self.normalization_version),
                created_at=datetime.fromisoformat(existing_dict['created_at']),
                was_duplicate=True,
            )

        # Extract and normalize payment properties
        amount = Decimal(str(qbo_payment.get('TotalAmt', '0')))
        currency = qbo_payment.get('CurrencyRef', {}).get('value', 'USD') if isinstance(qbo_payment.get('CurrencyRef'), dict) else 'USD'
        txn_date = qbo_payment.get('TxnDate', '')

        # Extract customer name from CustomerRef
        customer_ref = qbo_payment.get('CustomerRef')
        customer_name = customer_ref.get('name', '') if isinstance(customer_ref, dict) else ''

        # Extract account info
        acct_id, acct_name = self._extract_deposit_account_info(qbo_payment)

        # Build normalized payload (Phase 3B contract requirement)
        normalized_payload = {
            'qbo_id': qbo_id,
            'amount': str(amount),
            'currency': currency,
            'txn_date': txn_date,
            'customer_name': customer_name,
            'ref_no': qbo_payment.get('PaymentRefNum') or '',
            'deposit_account_id': acct_id,
            'deposit_account_name': acct_name,
            'linked_txn_ids': self._extract_linked_txn_ids(qbo_payment),
            'raw_status': qbo_payment.get('PrivateNote', ''),
        }

        # Extract timestamps from MetaData
        source_updated_at = None
        create_time = None
        metadata_obj = qbo_payment.get('MetaData', {})
        if metadata_obj.get('LastUpdatedTime'):
            try:
                source_updated_at = datetime.fromisoformat(
                    metadata_obj['LastUpdatedTime'].replace('Z', '+00:00')
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass
        if metadata_obj.get('CreateTime'):
            try:
                create_time = datetime.fromisoformat(
                    metadata_obj['CreateTime'].replace('Z', '+00:00')
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass

        # Create raw_event_id reference (synthetic for QBO observations)
        raw_event_id = f"qbo:payment:{qbo_id}"

        # Insert canonical fact
        self._insert_accounting_canonical_fact(
            cursor=cursor,
            canonical_id=canonical_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            source_object_type=source_object_type,
            amount=amount,
            currency=currency,
            txn_date=txn_date,
            normalized_payload=normalized_payload,
            observed_at=observed_at,
            source_updated_at=source_updated_at,
            created_at=now,
            customer_name=customer_name,
            create_time=create_time,
        )

        result = AccountingCanonicalCreationResult(
            canonical_fact_id=canonical_id,
            workspace_id=self.workspace_id,
            source_system=self.SOURCE_SYSTEM,
            source_object_type=source_object_type,
            source_object_id=qbo_id,
            event_type=event_type,
            amount=amount,
            observed_at=observed_at,
            normalization_version=self.normalization_version,
            created_at=now,
            was_duplicate=False,
        )

        self._log_creation(result)
        return result

    def canonicalize_refund_observed(
        self,
        cursor: sqlite3.Cursor,
        qbo_refund: Dict[str, Any],
        refund_type: str,  # 'RefundReceipt' or 'CreditMemo'
        observed_at: Optional[datetime] = None,
    ) -> AccountingCanonicalCreationResult:
        """
        Create a canonical fact from an observed QBO RefundReceipt or CreditMemo.

        Per Phase 3B contract, this captures:
        - The existence of a refund object in QBO
        - Its observable properties at observation time
        - NO assertion about correctness or matching

        Args:
            cursor: Database cursor (transaction managed by caller)
            qbo_refund: QBO RefundReceipt or CreditMemo object as dictionary
            refund_type: 'RefundReceipt' or 'CreditMemo'
            observed_at: When the observation was made (defaults to now)

        Returns:
            AccountingCanonicalCreationResult with fact details and audit info
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        observed_at = observed_at or now
        event_type = AccountingCanonicalEventType.REFUND_OBSERVED.value

        # Validate refund_type
        if refund_type not in ('RefundReceipt', 'CreditMemo'):
            raise ValueError(f"Invalid refund_type: {refund_type}. Must be 'RefundReceipt' or 'CreditMemo'")

        source_object_type = refund_type

        # Extract QBO ID
        qbo_id = str(qbo_refund.get('Id', ''))
        if not qbo_id:
            raise ValueError(f"QBO {refund_type} must have an Id")

        # Generate deterministic canonical ID
        canonical_id = self._generate_canonical_id(source_object_type, qbo_id)

        # Check for idempotency
        cursor.execute("SELECT * FROM canonical_facts WHERE id = ?", (canonical_id,))
        existing = cursor.fetchone()
        if existing:
            existing_dict = dict(existing)
            logger.info(
                f"[CANONICAL-QBO] Idempotent skip: {refund_type} {qbo_id} already canonicalized "
                f"as {canonical_id[:16]}..."
            )
            return AccountingCanonicalCreationResult(
                canonical_fact_id=existing_dict['id'],
                workspace_id=self.workspace_id,
                source_system=self.SOURCE_SYSTEM,
                source_object_type=source_object_type,
                source_object_id=qbo_id,
                event_type=event_type,
                amount=Decimal(str(existing_dict.get('amount_gross', '0'))),
                observed_at=datetime.fromisoformat(existing_dict.get('observed_at', now.isoformat())) if existing_dict.get('observed_at') else now,
                normalization_version=existing_dict.get('normalization_version', self.normalization_version),
                created_at=datetime.fromisoformat(existing_dict['created_at']),
                was_duplicate=True,
            )

        # Extract and normalize properties
        amount = Decimal(str(qbo_refund.get('TotalAmt', '0')))
        currency = qbo_refund.get('CurrencyRef', {}).get('value', 'USD') if isinstance(qbo_refund.get('CurrencyRef'), dict) else 'USD'
        txn_date = qbo_refund.get('TxnDate', '')

        # Extract customer name from CustomerRef
        customer_ref = qbo_refund.get('CustomerRef')
        customer_name = customer_ref.get('name', '') if isinstance(customer_ref, dict) else ''

        # Extract ref_no: RefundReceipt uses PaymentRefNum, CreditMemo uses DocNumber
        if refund_type == 'RefundReceipt':
            ref_no = qbo_refund.get('PaymentRefNum') or ''
        else:
            ref_no = qbo_refund.get('DocNumber') or ''

        # Extract account info (RefundReceipts have DepositToAccountRef)
        acct_id, acct_name = self._extract_deposit_account_info(qbo_refund)

        # Build normalized payload
        normalized_payload = {
            'qbo_id': qbo_id,
            'amount': str(amount),
            'currency': currency,
            'txn_date': txn_date,
            'customer_name': customer_name,
            'ref_no': ref_no,
            'deposit_account_id': acct_id,
            'deposit_account_name': acct_name,
            'related_payment_ids': self._extract_related_payment_ids(qbo_refund),
            'raw_type': refund_type,
        }

        # Extract timestamps from MetaData
        source_updated_at = None
        create_time = None
        metadata_obj = qbo_refund.get('MetaData', {})
        if metadata_obj.get('LastUpdatedTime'):
            try:
                source_updated_at = datetime.fromisoformat(
                    metadata_obj['LastUpdatedTime'].replace('Z', '+00:00')
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass
        if metadata_obj.get('CreateTime'):
            try:
                create_time = datetime.fromisoformat(
                    metadata_obj['CreateTime'].replace('Z', '+00:00')
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass

        raw_event_id = f"qbo:{refund_type.lower()}:{qbo_id}"

        # Insert canonical fact
        self._insert_accounting_canonical_fact(
            cursor=cursor,
            canonical_id=canonical_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            source_object_type=source_object_type,
            amount=amount,
            currency=currency,
            txn_date=txn_date,
            normalized_payload=normalized_payload,
            observed_at=observed_at,
            source_updated_at=source_updated_at,
            created_at=now,
            customer_name=customer_name,
            create_time=create_time,
        )

        result = AccountingCanonicalCreationResult(
            canonical_fact_id=canonical_id,
            workspace_id=self.workspace_id,
            source_system=self.SOURCE_SYSTEM,
            source_object_type=source_object_type,
            source_object_id=qbo_id,
            event_type=event_type,
            amount=amount,
            observed_at=observed_at,
            normalization_version=self.normalization_version,
            created_at=now,
            was_duplicate=False,
        )

        self._log_creation(result)
        return result

    def canonicalize_deposit_observed(
        self,
        cursor: sqlite3.Cursor,
        qbo_deposit: Dict[str, Any],
        observed_at: Optional[datetime] = None,
    ) -> AccountingCanonicalCreationResult:
        """
        Create a canonical fact from an observed QBO Deposit object.

        Per Phase 3B contract, this captures:
        - The existence of a Deposit in QBO
        - Its observable properties at observation time
        - NO assertion about correctness, origin, or matching

        Args:
            cursor: Database cursor (transaction managed by caller)
            qbo_deposit: QBO Deposit object as dictionary
            observed_at: When the observation was made (defaults to now)

        Returns:
            AccountingCanonicalCreationResult with fact details and audit info
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        observed_at = observed_at or now
        event_type = AccountingCanonicalEventType.DEPOSIT_OBSERVED.value
        source_object_type = QBOObjectType.DEPOSIT.value

        # Extract QBO Deposit ID
        qbo_id = str(qbo_deposit.get('Id', ''))
        if not qbo_id:
            raise ValueError("QBO Deposit must have an Id")

        # Generate deterministic canonical ID
        canonical_id = self._generate_canonical_id(source_object_type, qbo_id)

        # Check for idempotency
        cursor.execute("SELECT * FROM canonical_facts WHERE id = ?", (canonical_id,))
        existing = cursor.fetchone()
        if existing:
            existing_dict = dict(existing)
            logger.info(
                f"[CANONICAL-QBO] Idempotent skip: Deposit {qbo_id} already canonicalized "
                f"as {canonical_id[:16]}..."
            )
            return AccountingCanonicalCreationResult(
                canonical_fact_id=existing_dict['id'],
                workspace_id=self.workspace_id,
                source_system=self.SOURCE_SYSTEM,
                source_object_type=source_object_type,
                source_object_id=qbo_id,
                event_type=event_type,
                amount=Decimal(str(existing_dict.get('amount_gross', '0'))),
                observed_at=datetime.fromisoformat(existing_dict.get('observed_at', now.isoformat())) if existing_dict.get('observed_at') else now,
                normalization_version=existing_dict.get('normalization_version', self.normalization_version),
                created_at=datetime.fromisoformat(existing_dict['created_at']),
                was_duplicate=True,
            )

        # Extract and normalize properties
        total_amount = Decimal(str(qbo_deposit.get('TotalAmt', '0')))
        currency = qbo_deposit.get('CurrencyRef', {}).get('value', 'USD') if isinstance(qbo_deposit.get('CurrencyRef'), dict) else 'USD'
        txn_date = qbo_deposit.get('TxnDate', '')

        # Extract component payment IDs from deposit lines (backward compat)
        component_payment_ids = self._extract_deposit_component_ids(qbo_deposit)
        # Extract ALL line items with types and amounts
        deposit_line_items = self._extract_deposit_line_items(qbo_deposit)
        line_count = len(qbo_deposit.get('Line', []))

        # Extract deposit account
        acct_id, acct_name = self._extract_deposit_account_info(qbo_deposit)

        # Build normalized payload
        normalized_payload = {
            'qbo_id': qbo_id,
            'total_amount': str(total_amount),
            'txn_date': txn_date,
            'ref_no': qbo_deposit.get('DocNumber') or '',
            'deposit_account_id': acct_id,
            'deposit_account_name': acct_name,
            'component_payment_ids': component_payment_ids,
            'deposit_line_items': deposit_line_items,
            'raw_line_count': line_count,
        }

        # Extract timestamps from MetaData
        source_updated_at = None
        create_time = None
        metadata_obj = qbo_deposit.get('MetaData', {})
        if metadata_obj.get('LastUpdatedTime'):
            try:
                source_updated_at = datetime.fromisoformat(
                    metadata_obj['LastUpdatedTime'].replace('Z', '+00:00')
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass
        if metadata_obj.get('CreateTime'):
            try:
                create_time = datetime.fromisoformat(
                    metadata_obj['CreateTime'].replace('Z', '+00:00')
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass

        raw_event_id = f"qbo:deposit:{qbo_id}"

        # Insert canonical fact
        self._insert_accounting_canonical_fact(
            cursor=cursor,
            canonical_id=canonical_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            source_object_type=source_object_type,
            amount=total_amount,
            currency=currency,
            txn_date=txn_date,
            normalized_payload=normalized_payload,
            observed_at=observed_at,
            source_updated_at=source_updated_at,
            created_at=now,
            create_time=create_time,
        )

        result = AccountingCanonicalCreationResult(
            canonical_fact_id=canonical_id,
            workspace_id=self.workspace_id,
            source_system=self.SOURCE_SYSTEM,
            source_object_type=source_object_type,
            source_object_id=qbo_id,
            event_type=event_type,
            amount=total_amount,
            observed_at=observed_at,
            normalization_version=self.normalization_version,
            created_at=now,
            was_duplicate=False,
        )

        self._log_creation(result)
        return result

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _extract_deposit_account_info(self, qbo_obj: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """Extract deposit account ID and name from QBO object's DepositToAccountRef."""
        deposit_ref = qbo_obj.get('DepositToAccountRef', {})
        if isinstance(deposit_ref, dict):
            return deposit_ref.get('value'), deposit_ref.get('name')
        return None, None

    def _extract_linked_txn_ids(self, qbo_payment: Dict[str, Any]) -> List[str]:
        """Extract linked transaction IDs from QBO Payment."""
        linked_ids = []
        for line in qbo_payment.get('Line', []):
            for linked_txn in line.get('LinkedTxn', []):
                txn_id = linked_txn.get('TxnId')
                if txn_id:
                    linked_ids.append(str(txn_id))
        return linked_ids

    def _extract_related_payment_ids(self, qbo_refund: Dict[str, Any]) -> List[str]:
        """Extract related payment IDs from QBO RefundReceipt or CreditMemo."""
        related_ids = []
        for line in qbo_refund.get('Line', []):
            for linked_txn in line.get('LinkedTxn', []):
                if linked_txn.get('TxnType') == 'Payment':
                    txn_id = linked_txn.get('TxnId')
                    if txn_id:
                        related_ids.append(str(txn_id))
        return related_ids

    def _extract_deposit_component_ids(self, qbo_deposit: Dict[str, Any]) -> List[str]:
        """Extract component transaction IDs from QBO Deposit lines.
        Includes Payments, RefundReceipts, and CreditMemos — all linked transactions."""
        component_ids = []
        for line in qbo_deposit.get('Line', []):
            for linked_txn in line.get('LinkedTxn', []):
                txn_id = linked_txn.get('TxnId')
                if txn_id:
                    component_ids.append(str(txn_id))
        return component_ids

    def _extract_deposit_line_items(self, qbo_deposit: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract ALL line items from QBO Deposit, preserving type and amount info."""
        line_items = []
        for line in qbo_deposit.get('Line', []):
            linked_txns = line.get('LinkedTxn', [])
            amount = str(line.get('Amount', '0'))
            description = line.get('Description', '')

            if linked_txns:
                for linked_txn in linked_txns:
                    txn_type = linked_txn.get('TxnType', '')
                    txn_id = linked_txn.get('TxnId', '')
                    if txn_id:
                        line_items.append({
                            'txn_type': txn_type,
                            'txn_id': str(txn_id),
                            'amount': amount,
                            'description': description,
                        })
            else:
                # Direct deposit line (no linked transaction) — e.g., cash, fee
                line_items.append({
                    'txn_type': 'DirectLine',
                    'txn_id': None,
                    'amount': amount,
                    'description': description or 'Direct deposit line',
                })
        return line_items

    def _insert_accounting_canonical_fact(
        self,
        cursor: sqlite3.Cursor,
        canonical_id: str,
        raw_event_id: str,
        event_type: str,
        source_object_type: str,
        amount: Decimal,
        currency: str,
        txn_date: str,
        normalized_payload: Dict[str, Any],
        observed_at: datetime,
        source_updated_at: Optional[datetime],
        created_at: datetime,
        customer_name: Optional[str] = None,
        create_time: Optional[datetime] = None,
    ) -> None:
        """
        Insert an accounting canonical fact into the canonical_facts table.

        This is the ONLY write operation performed by the canonicalizer.
        """
        # Use MetaData.CreateTime (has actual time) over TxnDate (date-only).
        # This ensures timestamps in Charles match what QBO shows in its UI.
        if create_time:
            processor_timestamp = create_time
        else:
            try:
                if txn_date:
                    processor_timestamp = datetime.fromisoformat(txn_date).replace(tzinfo=None)
                else:
                    processor_timestamp = observed_at
            except ValueError:
                processor_timestamp = observed_at

        cursor.execute("""
            INSERT INTO canonical_facts (
                id,
                workspace_id,
                raw_event_id,
                processor,
                event_type,
                source_object_type,
                amount_gross,
                amount_net,
                fees,
                currency,
                processor_payment_id,
                processor_payout_id,
                processor_customer_id,
                processor_invoice_id,
                payment_method,
                card_brand,
                card_last4,
                processor_timestamp,
                settlement_date,
                observed_at,
                source_updated_at,
                description,
                customer_email,
                customer_name,
                metadata,
                normalization_version,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            canonical_id,
            self.workspace_id,
            raw_event_id,
            self.SOURCE_SYSTEM,  # 'qbo'
            event_type,
            source_object_type,
            str(amount),
            str(amount),  # Net = gross for observations
            '0',  # No fees computed for observations
            currency,
            normalized_payload.get('qbo_id'),  # Use qbo_id as processor_payment_id for lookups
            None,  # No processor_payout_id for accounting facts
            None,  # No processor_customer_id
            None,  # No processor_invoice_id
            None,  # No payment_method
            None,  # No card_brand
            None,  # No card_last4
            processor_timestamp.isoformat(),
            txn_date or processor_timestamp.date().isoformat(),
            observed_at.isoformat(),
            source_updated_at.isoformat() if source_updated_at else None,
            f"{source_object_type}: {normalized_payload.get('qbo_id', 'unknown')}",
            None,  # No customer_email
            customer_name or None,
            json.dumps(normalized_payload),  # normalized_payload as metadata
            self.normalization_version,
            created_at.isoformat(),
        ))

    def _log_creation(self, result: AccountingCanonicalCreationResult) -> None:
        """
        Log accounting canonical fact creation for audit trail.

        Per Phase 3 contract, logs must allow reconstruction
        of "why this fact exists".
        """
        logger.info(
            f"[CANONICAL-QBO] Created {result.event_type}: "
            f"id={result.canonical_fact_id[:16]}... "
            f"source={result.source_system}:{result.source_object_type} "
            f"object_id={result.source_object_id} "
            f"amount={result.amount} "
            f"observed_at={result.observed_at.isoformat() if result.observed_at else 'N/A'} "
            f"version={result.normalization_version}"
        )


# =============================================================================
# QUERY FUNCTIONS (Read-Only)
# =============================================================================

def get_accounting_canonical_facts_by_source(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    source_system: str = 'qbo',
) -> List[Dict[str, Any]]:
    """
    Retrieve all accounting canonical facts for a workspace and source system.

    Returns ONLY accounting observation facts (payment_observed, etc.).

    Args:
        cursor: Database cursor
        workspace_id: Workspace identifier
        source_system: Source system name (default 'qbo')

    Returns:
        List of canonical fact dictionaries
    """
    cursor.execute("""
        SELECT * FROM canonical_facts
        WHERE workspace_id = ?
          AND processor = ?
          AND event_type IN ('payment_observed', 'refund_observed', 'deposit_observed')
        ORDER BY observed_at DESC, created_at DESC
    """, (workspace_id, source_system))

    return [dict(row) for row in cursor.fetchall()]


def get_accounting_canonical_fact_by_source_id(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    source_system: str,
    source_object_type: str,
    source_object_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Retrieve a specific accounting canonical fact by its source identifiers.

    Args:
        cursor: Database cursor
        workspace_id: Workspace identifier
        source_system: Source system name ('qbo', 'xero', etc.)
        source_object_type: Object type ('Payment', 'Deposit', etc.)
        source_object_id: Object ID from source system

    Returns:
        Canonical fact dictionary or None if not found
    """
    # Generate the deterministic ID
    canonical_id = generate_accounting_canonical_id(
        workspace_id=workspace_id,
        source_system=source_system,
        source_object_type=source_object_type,
        source_object_id=source_object_id,
        observation_type='observed',
    )

    cursor.execute("SELECT * FROM canonical_facts WHERE id = ?", (canonical_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


# =============================================================================
# CONTRACT COMPLIANCE VERIFICATION
# =============================================================================

def verify_accounting_canonical_compliance(cursor: sqlite3.Cursor) -> Dict[str, bool]:
    """
    Verify that the canonical_facts table complies with Phase 3B contract.

    Checks:
    - New event types are supported
    - source_object_type column exists
    - observed_at column exists
    - No matching columns exist

    Args:
        cursor: Database cursor

    Returns:
        Dict mapping check names to pass/fail booleans
    """
    cursor.execute("PRAGMA table_info(canonical_facts)")
    columns = {row[1] for row in cursor.fetchall()}

    # Required Phase 3B columns
    required_columns = {'source_object_type', 'observed_at', 'source_updated_at'}

    # Prohibited columns (must NOT exist)
    prohibited_columns = {'match_type', 'match_confidence', 'match_score_breakdown',
                         'human_confirmed_at', 'confirmed_by', 'reconciliation_status'}

    results = {
        'has_source_object_type': 'source_object_type' in columns,
        'has_observed_at': 'observed_at' in columns,
        'has_source_updated_at': 'source_updated_at' in columns,
        'no_match_columns': len(prohibited_columns & columns) == 0,
        'has_processor_column': 'processor' in columns,
        'has_metadata': 'metadata' in columns,
    }

    return results


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'QBOAccountingCanonicalizer',
    'AccountingCanonicalEventType',
    'AccountingCanonicalCreationResult',
    'QBOObjectType',
    'ACCOUNTING_NORMALIZATION_VERSION',
    'generate_accounting_canonical_id',
    'get_accounting_canonical_facts_by_source',
    'get_accounting_canonical_fact_by_source_id',
    'verify_accounting_canonical_compliance',
]
