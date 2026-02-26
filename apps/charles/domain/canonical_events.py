"""
Canonical Events Module

Phase 3: Canonical Events Layer

This module provides the PURE, ISOLATED canonicalization layer for transforming
processor transactions into canonical economic events.

Per PHASE3_CANONICAL_EVENTS_CONTRACT.md:
- CE-01 (Immutability): Canonical Events MUST be immutable once created
- CE-02 (Processor-Agnostic): Canonical Events normalize processor-specific data
- CE-03 (QBO Isolation): Canonical Events MUST NOT reference QBO objects
- CE-04 (Match-Free): Canonical Events MUST NOT contain matching metadata
- CE-05 (Append-Only): New information creates new events, never mutates existing ones
- CE-06 (Deterministic): Same input MUST produce same canonical output
- CE-07 (Idempotent): Re-canonicalization MUST NOT create duplicates

HARD CONSTRAINTS (NON-NEGOTIABLE):
During canonical event creation, this module MUST NOT:
- Read from QBO
- Write to QBO
- Perform matching
- Infer deposits or payouts
- Set readiness / status / confidence flags
- Write to matching tables
- Group events
- Depend on existing matching outcomes

This module writes ONLY to the canonical_facts table.
The canonical_facts table IS the Phase 3 Canonical Events store.
"""

# =============================================================================
# LEGACY STATUS — CANONICAL_EVENTS TABLE IS FROZEN
# =============================================================================
# Per PHASE3_CANONICAL_EVENTS_CONTRACT.md (CERTIFIED & FROZEN, 2026-02-05):
#
# The canonical_events table is a LEGACY artifact. The authoritative store
# is canonical_facts. This module writes ONLY to canonical_facts.
#
# PROHIBITED OPERATIONS on canonical_events:
#   - INSERT with qbo_* columns (CE-03, §5.2)
#   - UPDATE of qbo_* or match_* columns (CE-03, CE-04, §6.2)
#   - Using canonical_events for new matching or accounting intent
#
# DB triggers enforce this at the database layer (see api/app.py).
# Matching outcomes belong in the matching_outcomes table (§6.2).
# =============================================================================
CANONICAL_EVENTS_TABLE_STATUS = 'FROZEN_LEGACY'

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal
from enum import Enum

# Current normalization version - increment when normalization rules change
NORMALIZATION_VERSION = '3.0.0'

logger = logging.getLogger(__name__)


# =============================================================================
# CANONICAL EVENT TYPES (Per Contract Section 4)
# =============================================================================

class CanonicalEventType(Enum):
    """
    Normative set of canonical event types per PHASE3_CANONICAL_EVENTS_CONTRACT.md Section 4.
    This list is extensible ONLY via contract amendment.
    """
    PAYMENT = 'payment'
    REFUND = 'refund'
    RETURN = 'return'  # ACH return / reversal
    FEE = 'fee'
    ADJUSTMENT = 'adjustment'
    PAYOUT = 'payout'
    FAILED_PAYMENT = 'failed_payment'
    CHARGEBACK = 'chargeback'


class ProcessorType(Enum):
    """Supported payment processors."""
    STRIPE = 'stripe'
    AUTHORIZE_NET = 'authorize_net'
    TEST = 'test'


# =============================================================================
# CANONICAL EVENT DATA CLASSES
# =============================================================================

@dataclass
class CanonicalEvent:
    """
    Represents an immutable Canonical Event.

    This is a processor-derived economic fact that:
    - Has NO QBO references (CE-03)
    - Has NO match metadata (CE-04)
    - Is immutable once created (CE-01)
    - Is processor-agnostic (CE-02)
    """
    id: str
    workspace_id: str
    raw_event_id: str
    processor: str
    event_type: str
    amount_gross: Decimal
    amount_net: Decimal
    fees: Decimal
    currency: str
    processor_transaction_id: Optional[str]
    processor_payout_id: Optional[str]
    processor_customer_id: Optional[str]
    occurred_at: datetime
    settlement_date: Optional[date]
    description: str
    customer_email: Optional[str]
    customer_name: Optional[str]
    payment_method: Optional[str]
    card_brand: Optional[str]
    card_last4: Optional[str]
    metadata: Optional[Dict[str, Any]]
    normalization_version: str
    created_at: datetime


@dataclass
class CanonicalEventCreationResult:
    """
    Result of canonical event creation.

    Contains both the canonical_event_id and audit information
    for logging and debugging purposes.
    """
    canonical_event_id: str
    raw_event_id: str
    event_type: str
    processor: str
    processor_transaction_id: Optional[str]
    amount: Decimal
    occurred_at: datetime
    normalization_version: str
    created_at: datetime
    was_duplicate: bool = False

    def to_audit_dict(self) -> Dict[str, Any]:
        """Return audit-friendly dictionary for logging."""
        return {
            'canonical_event_id': self.canonical_event_id,
            'raw_event_id': self.raw_event_id,
            'event_type': self.event_type,
            'processor': self.processor,
            'processor_transaction_id': self.processor_transaction_id,
            'amount': str(self.amount),
            'occurred_at': self.occurred_at.isoformat() if self.occurred_at else None,
            'normalization_version': self.normalization_version,
            'created_at': self.created_at.isoformat(),
            'was_duplicate': self.was_duplicate,
        }


# =============================================================================
# IDEMPOTENCY KEY GENERATION
# =============================================================================

def generate_canonical_idempotency_key(
    workspace_id: str,
    processor: str,
    processor_transaction_id: str,
    event_type: str,
) -> str:
    """
    Generate a deterministic idempotency key for canonical event creation.

    Per CE-07 (Idempotent): Re-canonicalization MUST NOT create duplicates.

    The idempotency key is derived from:
    - workspace_id (data isolation)
    - processor (source identification)
    - processor_transaction_id (unique transaction reference)
    - event_type (allows same transaction ID for different event types, e.g., payment + refund)

    Args:
        workspace_id: Workspace identifier
        processor: Processor name (stripe, authorize_net, etc.)
        processor_transaction_id: Processor's unique transaction identifier
        event_type: Canonical event type

    Returns:
        SHA256 hash as idempotency key
    """
    key_parts = [
        'canonical_event',
        'v3',
        workspace_id,
        processor,
        processor_transaction_id,
        event_type,
    ]
    key_string = '|'.join(str(p) for p in key_parts)
    return hashlib.sha256(key_string.encode()).hexdigest()


# =============================================================================
# CANONICALIZER CLASS
# =============================================================================

class Canonicalizer:
    """
    Pure canonicalization engine for creating canonical events.

    This class provides isolated, deterministic canonicalization
    with NO QBO or matching side effects.

    Per PHASE3_CANONICAL_EVENTS_CONTRACT.md:
    - Reads ONLY from processor transactions
    - Writes ONLY to canonical_facts table
    - Has NO dependency on QBO state
    - Has NO dependency on matching state
    - Is deterministic and idempotent
    """

    def __init__(self, workspace_id: str, processor: str):
        """
        Initialize the canonicalizer.

        Args:
            workspace_id: Workspace identifier for data isolation
            processor: Processor name (stripe, authorize_net, test)
        """
        self.workspace_id = workspace_id
        self.processor = processor
        self.normalization_version = NORMALIZATION_VERSION

    def _generate_deterministic_id(
        self,
        processor_transaction_id: str,
        event_type: str,
    ) -> str:
        """
        Generate a deterministic canonical event ID.

        Per PHASE3_CANONICAL_EVENTS_CONTRACT.md CE-06:
        - Same input MUST produce same canonical output
        - ID is derived EXACTLY as: SHA256(workspace_id|processor|processor_transaction_id|event_type)

        This enables:
        - Idempotent replay (same transaction = same ID = INSERT fails gracefully)
        - Deterministic audit trail
        - DB-level duplicate prevention via PRIMARY KEY constraint

        Args:
            processor_transaction_id: Processor's unique transaction identifier
            event_type: Canonical event type (payment, refund, etc.)

        Returns:
            SHA256 hash as canonical event ID
        """
        key_string = f"{self.workspace_id}|{self.processor}|{processor_transaction_id}|{event_type}"
        return hashlib.sha256(key_string.encode()).hexdigest()

    def canonicalize_payment(
        self,
        cursor: sqlite3.Cursor,
        raw_event_id: str,
        processor_payment_id: str,
        amount_gross: Decimal,
        amount_net: Decimal,
        fees: Decimal,
        currency: str,
        processor_payout_id: str,
        processor_timestamp: datetime,
        processor_customer_id: Optional[str] = None,
        customer_email: Optional[str] = None,
        customer_name: Optional[str] = None,
        payment_method: Optional[str] = None,
        card_brand: Optional[str] = None,
        card_last4: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CanonicalEventCreationResult:
        """
        Create a canonical payment event from processor data.

        This method:
        1. Applies deterministic normalization rules
        2. Checks for idempotency (no duplicate creation)
        3. Writes to canonical_facts table
        4. Logs creation for audit trail

        DOES NOT:
        - Read QBO state
        - Perform matching
        - Set readiness flags
        - Group events

        Args:
            cursor: Database cursor (transaction managed by caller)
            raw_event_id: Reference to raw_events record
            processor_payment_id: Processor's payment identifier
            amount_gross: Gross payment amount
            amount_net: Net amount after fees
            fees: Processing fees
            currency: Currency code (e.g., 'USD')
            processor_payout_id: Parent payout identifier
            processor_timestamp: When payment occurred (economic time)
            processor_customer_id: Processor's customer identifier
            customer_email: Customer email from processor
            customer_name: Customer name from processor
            payment_method: Payment method (card, ach, etc.)
            card_brand: Card brand if applicable
            card_last4: Last 4 digits if applicable
            description: Payment description
            metadata: Additional processor-reported data

        Returns:
            CanonicalEventCreationResult with event details and audit info
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        event_type = CanonicalEventType.PAYMENT.value

        # Generate idempotency key for duplicate detection
        idempotency_key = generate_canonical_idempotency_key(
            self.workspace_id,
            self.processor,
            processor_payment_id,
            event_type,
        )

        # CE-06: Generate DETERMINISTIC canonical event ID
        # This replaces uuid.uuid4() per B2 remediation
        # ID is SHA256(workspace_id|processor|processor_transaction_id|event_type)
        canonical_event_id = self._generate_deterministic_id(processor_payment_id, event_type)

        # CE-07 (Idempotent): Check if already exists using deterministic ID
        # This is B4 remediation - DB-level idempotency via PRIMARY KEY
        cursor.execute("SELECT * FROM canonical_facts WHERE id = ?", (canonical_event_id,))
        existing = cursor.fetchone()
        if existing:
            existing_dict = dict(existing)
            logger.info(
                f"[CANONICAL] Idempotent skip: payment {processor_payment_id} already canonicalized "
                f"as {canonical_event_id[:16]}..."
            )
            return CanonicalEventCreationResult(
                canonical_event_id=existing_dict['id'],
                raw_event_id=existing_dict['raw_event_id'],
                event_type=event_type,
                processor=self.processor,
                processor_transaction_id=processor_payment_id,
                amount=Decimal(existing_dict['amount_gross']),
                occurred_at=datetime.fromisoformat(existing_dict['processor_timestamp']),
                normalization_version=existing_dict.get('normalization_version', self.normalization_version),
                created_at=datetime.fromisoformat(existing_dict['created_at']),
                was_duplicate=True,
            )

        # Build clean metadata (filter out any accidental QBO/match keys)
        clean_metadata = self._clean_metadata(metadata)
        clean_metadata['idempotency_key'] = idempotency_key
        clean_metadata['original_processor_payment_id'] = processor_payment_id

        # Determine settlement date (use processor timestamp date)
        occurred_at = processor_timestamp or now
        settlement_date = occurred_at.date().isoformat()

        # Build description
        effective_description = description or f'Payment {processor_payment_id}'

        # Insert canonical event
        self._insert_canonical_event(
            cursor=cursor,
            canonical_event_id=canonical_event_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            amount_gross=amount_gross,
            amount_net=amount_net,
            fees=fees,
            currency=currency,
            processor_transaction_id=processor_payment_id,
            processor_payout_id=processor_payout_id,
            processor_customer_id=processor_customer_id,
            processor_timestamp=occurred_at,
            settlement_date=settlement_date,
            description=effective_description,
            customer_email=customer_email,
            customer_name=customer_name,
            payment_method=payment_method,
            card_brand=card_brand,
            card_last4=card_last4,
            metadata=clean_metadata,
            created_at=now,
        )

        result = CanonicalEventCreationResult(
            canonical_event_id=canonical_event_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            processor=self.processor,
            processor_transaction_id=processor_payment_id,
            amount=amount_gross,
            occurred_at=occurred_at,
            normalization_version=self.normalization_version,
            created_at=now,
            was_duplicate=False,
        )

        # Log creation for audit trail
        self._log_creation(result)

        return result

    def canonicalize_refund(
        self,
        cursor: sqlite3.Cursor,
        raw_event_id: str,
        processor_refund_id: str,
        original_payment_id: str,
        amount: Decimal,
        currency: str,
        processor_payout_id: str,
        refund_timestamp: datetime,
        customer_email: Optional[str] = None,
        customer_name: Optional[str] = None,
        payment_method: Optional[str] = None,
        card_brand: Optional[str] = None,
        card_last4: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CanonicalEventCreationResult:
        """
        Create a canonical refund event from processor data.

        Per AUTHORIZE_NET_INTEGRATION_PLAN:
        - INV-ANET-13: Amounts stored as positive, sign applied during Deposit construction

        Args:
            cursor: Database cursor (transaction managed by caller)
            raw_event_id: Reference to raw_events record
            processor_refund_id: Processor's refund identifier
            original_payment_id: Original payment this refund applies to
            amount: Refund amount (stored positive)
            currency: Currency code
            processor_payout_id: Parent payout identifier
            refund_timestamp: When refund occurred
            customer_email: Customer email from processor
            customer_name: Customer name from processor
            payment_method: Payment method
            card_brand: Card brand if applicable
            card_last4: Last 4 digits if applicable
            metadata: Additional processor-reported data

        Returns:
            CanonicalEventCreationResult with event details and audit info
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        event_type = CanonicalEventType.REFUND.value

        # CE-06: Generate DETERMINISTIC canonical event ID
        canonical_event_id = self._generate_deterministic_id(processor_refund_id, event_type)

        # CE-07 (Idempotent): Check if already exists using deterministic ID
        cursor.execute("SELECT * FROM canonical_facts WHERE id = ?", (canonical_event_id,))
        existing = cursor.fetchone()
        if existing:
            existing_dict = dict(existing)
            logger.info(
                f"[CANONICAL] Idempotent skip: refund {processor_refund_id} already canonicalized "
                f"as {canonical_event_id[:16]}..."
            )
            return CanonicalEventCreationResult(
                canonical_event_id=existing_dict['id'],
                raw_event_id=existing_dict['raw_event_id'],
                event_type=event_type,
                processor=self.processor,
                processor_transaction_id=processor_refund_id,
                amount=Decimal(existing_dict['amount_gross']),
                occurred_at=datetime.fromisoformat(existing_dict['processor_timestamp']),
                normalization_version=existing_dict.get('normalization_version', self.normalization_version),
                created_at=datetime.fromisoformat(existing_dict['created_at']),
                was_duplicate=True,
            )

        # Generate idempotency key for metadata (retained for audit trail)
        idempotency_key = generate_canonical_idempotency_key(
            self.workspace_id,
            self.processor,
            processor_refund_id,
            event_type,
        )

        # Build clean metadata
        clean_metadata = self._clean_metadata(metadata)
        clean_metadata['idempotency_key'] = idempotency_key
        clean_metadata['original_payment_id'] = original_payment_id
        clean_metadata['refund_type'] = 'linked' if original_payment_id else 'full'

        occurred_at = refund_timestamp or now
        settlement_date = occurred_at.date().isoformat()

        # Insert canonical event
        # Note: Refunds have amount_gross = amount_net (no fees on refund)
        self._insert_canonical_event(
            cursor=cursor,
            canonical_event_id=canonical_event_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            amount_gross=amount,
            amount_net=amount,  # Net = gross for refunds
            fees=Decimal('0'),  # No fees on refund
            currency=currency,
            processor_transaction_id=processor_refund_id,
            processor_payout_id=processor_payout_id,
            processor_customer_id=None,  # Refunds don't have customer ID
            processor_timestamp=occurred_at,
            settlement_date=settlement_date,
            description=f'Refund for payment {original_payment_id}',
            customer_email=customer_email,
            customer_name=customer_name,
            payment_method=payment_method,
            card_brand=card_brand,
            card_last4=card_last4,
            metadata=clean_metadata,
            created_at=now,
        )

        result = CanonicalEventCreationResult(
            canonical_event_id=canonical_event_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            processor=self.processor,
            processor_transaction_id=processor_refund_id,
            amount=amount,
            occurred_at=occurred_at,
            normalization_version=self.normalization_version,
            created_at=now,
            was_duplicate=False,
        )

        self._log_creation(result)

        return result

    def canonicalize_fee(
        self,
        cursor: sqlite3.Cursor,
        raw_event_id: str,
        processor_fee_id: str,
        amount: Decimal,
        fee_type: str,
        currency: str,
        processor_payout_id: str,
        fee_timestamp: datetime,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CanonicalEventCreationResult:
        """
        Create a canonical fee event from processor data.

        Args:
            cursor: Database cursor
            raw_event_id: Reference to raw_events record
            processor_fee_id: Processor's fee identifier
            amount: Fee amount (positive or negative as reported)
            fee_type: Type of fee (processing, dispute, etc.)
            currency: Currency code
            processor_payout_id: Parent payout identifier
            fee_timestamp: When fee was assessed
            metadata: Additional processor-reported data

        Returns:
            CanonicalEventCreationResult with event details and audit info
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        event_type = CanonicalEventType.FEE.value

        # CE-06: Generate DETERMINISTIC canonical event ID
        canonical_event_id = self._generate_deterministic_id(processor_fee_id, event_type)

        # CE-07 (Idempotent): Check if already exists using deterministic ID
        cursor.execute("SELECT * FROM canonical_facts WHERE id = ?", (canonical_event_id,))
        existing = cursor.fetchone()
        if existing:
            existing_dict = dict(existing)
            logger.info(
                f"[CANONICAL] Idempotent skip: fee {processor_fee_id} already canonicalized "
                f"as {canonical_event_id[:16]}..."
            )
            return CanonicalEventCreationResult(
                canonical_event_id=existing_dict['id'],
                raw_event_id=existing_dict['raw_event_id'],
                event_type=event_type,
                processor=self.processor,
                processor_transaction_id=processor_fee_id,
                amount=Decimal(existing_dict['amount_gross']),
                occurred_at=datetime.fromisoformat(existing_dict['processor_timestamp']),
                normalization_version=existing_dict.get('normalization_version', self.normalization_version),
                created_at=datetime.fromisoformat(existing_dict['created_at']),
                was_duplicate=True,
            )

        # Generate idempotency key for metadata (retained for audit trail)
        idempotency_key = generate_canonical_idempotency_key(
            self.workspace_id,
            self.processor,
            processor_fee_id,
            event_type,
        )

        clean_metadata = self._clean_metadata(metadata)
        clean_metadata['idempotency_key'] = idempotency_key
        clean_metadata['fee_type'] = fee_type

        occurred_at = fee_timestamp or now
        settlement_date = occurred_at.date().isoformat()

        self._insert_canonical_event(
            cursor=cursor,
            canonical_event_id=canonical_event_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            amount_gross=amount,
            amount_net=amount,
            fees=Decimal('0'),
            currency=currency,
            processor_transaction_id=processor_fee_id,
            processor_payout_id=processor_payout_id,
            processor_customer_id=None,
            processor_timestamp=occurred_at,
            settlement_date=settlement_date,
            description=f'{fee_type} fee',
            customer_email=None,
            customer_name=None,
            payment_method=None,
            card_brand=None,
            card_last4=None,
            metadata=clean_metadata,
            created_at=now,
        )

        result = CanonicalEventCreationResult(
            canonical_event_id=canonical_event_id,
            raw_event_id=raw_event_id,
            event_type=event_type,
            processor=self.processor,
            processor_transaction_id=processor_fee_id,
            amount=amount,
            occurred_at=occurred_at,
            normalization_version=self.normalization_version,
            created_at=now,
            was_duplicate=False,
        )

        self._log_creation(result)

        return result

    def _check_existing_event(
        self,
        cursor: sqlite3.Cursor,
        idempotency_key: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Check if a canonical event with this idempotency key already exists.

        Uses the metadata JSON field to check for the idempotency_key.

        Args:
            cursor: Database cursor
            idempotency_key: The idempotency key to check

        Returns:
            Existing event dict if found, None otherwise
        """
        cursor.execute("""
            SELECT * FROM canonical_facts
            WHERE workspace_id = ?
              AND processor = ?
              AND json_extract(metadata, '$.idempotency_key') = ?
        """, (self.workspace_id, self.processor, idempotency_key))

        row = cursor.fetchone()
        return dict(row) if row else None

    def _insert_canonical_event(
        self,
        cursor: sqlite3.Cursor,
        canonical_event_id: str,
        raw_event_id: str,
        event_type: str,
        amount_gross: Decimal,
        amount_net: Decimal,
        fees: Decimal,
        currency: str,
        processor_transaction_id: Optional[str],
        processor_payout_id: Optional[str],
        processor_customer_id: Optional[str],
        processor_timestamp: datetime,
        settlement_date: str,
        description: str,
        customer_email: Optional[str],
        customer_name: Optional[str],
        payment_method: Optional[str],
        card_brand: Optional[str],
        card_last4: Optional[str],
        metadata: Optional[Dict[str, Any]],
        created_at: datetime,
    ) -> None:
        """
        Insert a canonical event into the canonical_facts table.

        This is the ONLY write operation performed by the canonicalizer.

        Args:
            All fields required for canonical_facts table insertion.
        """
        cursor.execute("""
            INSERT INTO canonical_facts (
                id,
                workspace_id,
                raw_event_id,
                processor,
                event_type,
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
                description,
                customer_email,
                customer_name,
                metadata,
                normalization_version,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            canonical_event_id,
            self.workspace_id,
            raw_event_id,
            self.processor,
            event_type,
            str(amount_gross),
            str(amount_net),
            str(fees),
            currency,
            processor_transaction_id,  # processor_payment_id column
            processor_payout_id,
            processor_customer_id,
            None,  # processor_invoice_id
            payment_method,
            card_brand,
            card_last4,
            processor_timestamp.isoformat(),
            settlement_date,
            description,
            customer_email,
            customer_name,
            json.dumps(metadata) if metadata else None,
            self.normalization_version,
            created_at.isoformat(),
        ))

    def _clean_metadata(self, metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Clean metadata by removing any QBO or match-related keys.

        Per CE-03 and CE-04: Canonical Events MUST NOT contain QBO or match data.

        Args:
            metadata: Original metadata dict

        Returns:
            Cleaned metadata dict with prohibited keys removed
        """
        if not metadata:
            return {}

        clean = {}
        for key, value in metadata.items():
            # Filter out QBO-related keys
            if key.startswith('qbo_'):
                continue
            # Filter out match-related keys
            if key.startswith('match_'):
                continue
            # Filter out status/readiness keys
            if key in ('status', 'ready', 'readiness', 'confidence', 'needs_review'):
                continue
            clean[key] = value

        return clean

    def _log_creation(self, result: CanonicalEventCreationResult) -> None:
        """
        Log canonical event creation for audit trail.

        Per PHASE3_CANONICAL_EVENTS_CONTRACT.md Section 8:
        Logs must allow reconstruction of "why this fact exists"

        Args:
            result: The creation result to log
        """
        logger.info(
            f"[CANONICAL] Created {result.event_type} event: "
            f"id={result.canonical_event_id} "
            f"processor={result.processor} "
            f"txn_id={result.processor_transaction_id} "
            f"amount={result.amount} "
            f"occurred_at={result.occurred_at.isoformat() if result.occurred_at else 'N/A'} "
            f"version={result.normalization_version}"
        )


# =============================================================================
# QUERY FUNCTIONS (Read-Only)
# =============================================================================

def get_canonical_events_for_payout(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    processor_payout_id: str,
) -> List[Dict[str, Any]]:
    """
    Retrieve canonical events for a payout.

    Returns ONLY pure processor-derived facts.
    NO QBO or match data is included.

    Args:
        cursor: Database cursor
        workspace_id: Workspace identifier
        processor_payout_id: Payout identifier

    Returns:
        List of canonical event dictionaries
    """
    cursor.execute("""
        SELECT * FROM canonical_facts
        WHERE workspace_id = ? AND processor_payout_id = ?
        ORDER BY processor_timestamp ASC
    """, (workspace_id, processor_payout_id))

    return [dict(row) for row in cursor.fetchall()]


def get_canonical_event_by_id(
    cursor: sqlite3.Cursor,
    canonical_event_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Retrieve a single canonical event by ID.

    Args:
        cursor: Database cursor
        canonical_event_id: The canonical event identifier

    Returns:
        Canonical event dictionary or None if not found
    """
    cursor.execute("""
        SELECT * FROM canonical_facts
        WHERE id = ?
    """, (canonical_event_id,))

    row = cursor.fetchone()
    return dict(row) if row else None


def get_canonical_events_by_processor_transaction(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    processor: str,
    processor_transaction_id: str,
) -> List[Dict[str, Any]]:
    """
    Retrieve canonical events for a processor transaction.

    May return multiple events if the same transaction has different event types
    (e.g., payment + refund).

    Args:
        cursor: Database cursor
        workspace_id: Workspace identifier
        processor: Processor name
        processor_transaction_id: Processor's transaction identifier

    Returns:
        List of canonical event dictionaries
    """
    cursor.execute("""
        SELECT * FROM canonical_facts
        WHERE workspace_id = ?
          AND processor = ?
          AND processor_payment_id = ?
        ORDER BY created_at ASC
    """, (workspace_id, processor, processor_transaction_id))

    return [dict(row) for row in cursor.fetchall()]


# =============================================================================
# CONTRACT COMPLIANCE VERIFICATION
# =============================================================================

def verify_schema_compliance(cursor: sqlite3.Cursor) -> Dict[str, bool]:
    """
    Verify that the canonical_facts table schema complies with the contract.

    Checks:
    - No QBO columns exist
    - No match columns exist
    - Required columns exist

    Args:
        cursor: Database cursor

    Returns:
        Dict mapping check names to pass/fail booleans
    """
    cursor.execute("PRAGMA table_info(canonical_facts)")
    columns = {row[1] for row in cursor.fetchall()}

    # Prohibited columns (must NOT exist)
    qbo_columns = {'qbo_customer_id', 'qbo_invoice_id', 'qbo_payment_id'}
    match_columns = {'match_type', 'match_confidence', 'match_score_breakdown',
                     'human_confirmed_at', 'confirmed_by'}
    status_columns = {'status'}  # Workflow status has no place in canonical facts

    results = {
        'no_qbo_columns': len(qbo_columns & columns) == 0,
        'no_match_columns': len(match_columns & columns) == 0,
        'no_status_column': 'status' not in columns,
        'has_processor': 'processor' in columns,
        'has_normalization_version': 'normalization_version' in columns,
        'has_immutability_trigger': _check_immutability_trigger(cursor),
    }

    return results


def _check_immutability_trigger(cursor: sqlite3.Cursor) -> bool:
    """Check if the immutability trigger exists."""
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type = 'trigger'
          AND name = 'canonical_facts_immutable'
    """)
    return cursor.fetchone() is not None


def verify_event_immutability(cursor: sqlite3.Cursor, canonical_event_id: str) -> bool:
    """
    Verify that a canonical event cannot be modified.

    This is a test helper that attempts to update a record
    and verifies the immutability trigger prevents it.

    Args:
        cursor: Database cursor
        canonical_event_id: The canonical event to test

    Returns:
        True if immutability is enforced (UPDATE fails), False otherwise
    """
    try:
        cursor.execute("""
            UPDATE canonical_facts
            SET description = 'MUTATION ATTEMPT - SHOULD FAIL'
            WHERE id = ?
        """, (canonical_event_id,))
        # If we get here, immutability is NOT enforced
        return False
    except sqlite3.IntegrityError:
        # Trigger fired - immutability is enforced
        return True


# =============================================================================
# BACKWARD COMPATIBILITY EXPORTS
# =============================================================================
# These maintain compatibility with existing code that imports from canonical_facts

# Import the old functions for backward compatibility during transition
from charles.domain.canonical_facts import (
    create_canonical_fact_from_payment,
    create_canonical_fact_from_refund,
    get_canonical_facts_for_payout as _get_canonical_facts_for_payout,
    get_canonical_fact_by_id as _get_canonical_fact_by_id,
    verify_canonical_fact_immutability,
    assert_no_qbo_fields,
    assert_no_match_fields,
    CanonicalFact,
)

# Re-export for compatibility
__all__ = [
    # Phase 3 classes and functions
    'Canonicalizer',
    'CanonicalEvent',
    'CanonicalEventType',
    'CanonicalEventCreationResult',
    'ProcessorType',
    'NORMALIZATION_VERSION',
    'generate_canonical_idempotency_key',
    'get_canonical_events_for_payout',
    'get_canonical_event_by_id',
    'get_canonical_events_by_processor_transaction',
    'verify_schema_compliance',
    'verify_event_immutability',
    # Backward compatibility exports
    'create_canonical_fact_from_payment',
    'create_canonical_fact_from_refund',
    'verify_canonical_fact_immutability',
    'assert_no_qbo_fields',
    'assert_no_match_fields',
    'CanonicalFact',
]
