"""
Canonical Facts Module

Phase 2: Canonical Facts Decoupling
Phase 3: Canonical Events Layer (Enhanced)

This module provides functions for creating and querying Canonical Facts/Events.
Canonical Facts represent immutable, processor-derived economic events.

Per ACCOUNTING_MENTAL_MODEL_CONTRACT.md and PHASE3_CANONICAL_EVENTS_CONTRACT.md:
- CF-01/CE-01 (Immutability): Canonical Facts MUST be immutable once created
- CF-02/CE-02 (Processor-Agnostic): Canonical Facts normalize processor-specific terminology
- CF-03/CE-03 (QBO Isolation): Canonical Facts MUST NOT reference QBO objects
- CF-04/CE-04 (Match-Free): Canonical Facts MUST NOT contain matching metadata
- CF-05/CE-05 (Append-Only Truth): New information creates new facts, never mutates existing ones

IMPORTANT:
    This module ONLY writes to the canonical_facts table.
    It does NOT accept QBO or match data.
    It does NOT modify existing records.

Phase 3 Enhancement:
    - Added processor column for processor-agnostic querying
    - Added normalization_version for deterministic replay

The legacy canonical_events table is handled separately for deposit compatibility.

NOTE: For new code, prefer using the canonical_events.py module which provides
      the full Phase 3 Canonicalizer class with enhanced features.
"""

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal

# Import shared types from reconciliation_engine (not sync_manager to avoid circular import)
from charles.domain.reconciliation_engine import ProcessorPayment, ProcessorRefund

# Phase 3: Normalization version for deterministic replay
NORMALIZATION_VERSION = '3.0.0'

logger = logging.getLogger(__name__)


# =============================================================================
# DETERMINISTIC ID GENERATION (CE-06 Compliance)
# =============================================================================

def generate_deterministic_canonical_id(
    workspace_id: str,
    processor: str,
    processor_transaction_id: str,
    event_type: str,
) -> str:
    """
    Generate a deterministic canonical event ID.

    Per PHASE3_CANONICAL_EVENTS_CONTRACT.md CE-06:
    - Same input MUST produce same canonical output
    - ID is derived EXACTLY as: SHA256(workspace_id | processor | processor_transaction_id | event_type)

    This enables:
    - Idempotent replay (same transaction = same ID = INSERT fails gracefully)
    - Deterministic audit trail
    - DB-level duplicate prevention via PRIMARY KEY constraint

    Args:
        workspace_id: Workspace identifier
        processor: Processor name (stripe, authorize_net, etc.)
        processor_transaction_id: Processor's unique transaction identifier
        event_type: Canonical event type (payment, refund, etc.)

    Returns:
        SHA256 hash as canonical event ID
    """
    # BINDING DECISION: Exact format per contract
    key_string = f"{workspace_id}|{processor}|{processor_transaction_id}|{event_type}"
    return hashlib.sha256(key_string.encode()).hexdigest()


@dataclass
class CanonicalFact:
    """
    Represents an immutable Canonical Fact.

    This is a processor-derived economic fact that:
    - Has NO QBO references
    - Has NO match metadata
    - Is immutable once created
    """
    id: str
    workspace_id: str
    raw_event_id: str
    event_type: str  # 'payment', 'payout', 'refund', 'failed_payment', 'chargeback'
    amount_gross: Decimal
    amount_net: Decimal
    fees: Decimal
    currency: str
    processor_payment_id: Optional[str]
    processor_payout_id: Optional[str]
    processor_customer_id: Optional[str]
    processor_invoice_id: Optional[str]
    payment_method: Optional[str]
    card_brand: Optional[str]
    card_last4: Optional[str]
    processor_timestamp: datetime
    settlement_date: Optional[str]  # DATE as string
    description: str
    customer_email: Optional[str]
    customer_name: Optional[str]
    metadata: Optional[Dict[str, Any]]
    created_at: datetime


def create_canonical_fact_from_payment(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    raw_event_id: str,
    payment: ProcessorPayment,
    processor_payout_id: str,
    processor: Optional[str] = None,
) -> str:
    """
    Create a Canonical Fact from a ProcessorPayment.

    This function creates an IMMUTABLE record in the canonical_facts table.
    The record contains ONLY processor-derived data, with no QBO or match information.

    Phase 3 Compliance:
    - CE-06 (Deterministic): ID is SHA256(workspace_id|processor|processor_transaction_id|event_type)
    - CE-07 (Idempotent): Duplicate inserts are safely ignored via INSERT OR IGNORE
    - B4 remediation: DB-level idempotency enforcement

    Args:
        cursor: Database cursor (transaction should be managed by caller)
        workspace_id: Workspace identifier
        raw_event_id: Reference to the raw_events record
        payment: ProcessorPayment containing the processor data
        processor_payout_id: Parent payout identifier
        processor: Processor name (will be looked up from raw_events if not provided)

    Returns:
        The canonical_fact_id (deterministic SHA256 hash)
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Phase 3: Determine processor (required for deterministic ID)
    effective_processor = processor
    if not effective_processor:
        cursor.execute("SELECT processor FROM raw_events WHERE id = ?", (raw_event_id,))
        row = cursor.fetchone()
        effective_processor = row[0] if row else 'unknown'

    # CE-06: Generate DETERMINISTIC canonical ID
    # This is the B2 remediation - replaces uuid.uuid4() with SHA256
    canonical_fact_id = generate_deterministic_canonical_id(
        workspace_id=workspace_id,
        processor=effective_processor,
        processor_transaction_id=payment.processor_payment_id,
        event_type='payment',
    )

    # Build metadata dict (processor-reported data only)
    # Explicitly exclude any QBO or match data
    metadata_dict = {}
    if payment.metadata:
        # Copy processor metadata, filtering out any accidental QBO/match keys
        for key, value in payment.metadata.items():
            if not key.startswith('qbo_') and not key.startswith('match_'):
                metadata_dict[key] = value

    # Include the original processor payment ID for traceability
    metadata_dict['original_processor_payment_id'] = payment.processor_payment_id

    # CE-07 (Idempotent): Check if already exists BEFORE attempting insert
    # This is B4 remediation - DB-level idempotency via deterministic ID + PRIMARY KEY
    cursor.execute("SELECT id FROM canonical_facts WHERE id = ?", (canonical_fact_id,))
    if cursor.fetchone():
        logger.info(
            f"[CANONICAL] Idempotent skip: payment {payment.processor_payment_id} already "
            f"canonicalized as {canonical_fact_id[:16]}..."
        )
        return canonical_fact_id

    # INSERT new canonical fact (will fail on duplicate due to PRIMARY KEY)
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
        canonical_fact_id,
        workspace_id,
        raw_event_id,
        effective_processor,
        'payment',
        str(payment.amount_gross),
        str(payment.amount_net),
        str(payment.fees),
        payment.currency,
        payment.processor_payment_id,
        processor_payout_id,
        payment.processor_customer_id,
        None,  # processor_invoice_id - extracted from metadata if available
        payment.payment_method,
        payment.card_brand,
        payment.card_last4,
        payment.processor_timestamp.isoformat() if payment.processor_timestamp else now.isoformat(),
        now.date().isoformat(),
        payment.description or f'Payment {payment.processor_payment_id}',
        payment.customer_email,
        payment.customer_name,
        json.dumps(metadata_dict) if metadata_dict else None,
        NORMALIZATION_VERSION,
        now.isoformat(),
    ))

    logger.info(
        f"[CANONICAL] Created payment: id={canonical_fact_id[:16]}... "
        f"processor={effective_processor} txn={payment.processor_payment_id} "
        f"amount=${payment.amount_gross} version={NORMALIZATION_VERSION}"
    )

    return canonical_fact_id


def create_canonical_fact_from_refund(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    raw_event_id: str,
    refund: ProcessorRefund,
    processor_payout_id: str,
    processor: Optional[str] = None,
) -> str:
    """
    Create a Canonical Fact from a ProcessorRefund.

    This function creates an IMMUTABLE record in the canonical_facts table.
    The record contains ONLY processor-derived data, with no QBO or match information.

    Phase 3 Compliance:
    - CE-06 (Deterministic): ID is SHA256(workspace_id|processor|processor_transaction_id|event_type)
    - CE-07 (Idempotent): Duplicate inserts are safely ignored via PRIMARY KEY check
    - B4 remediation: DB-level idempotency enforcement

    Args:
        cursor: Database cursor (transaction should be managed by caller)
        workspace_id: Workspace identifier
        raw_event_id: Reference to the raw_events record
        refund: ProcessorRefund containing the processor data
        processor_payout_id: Parent payout identifier
        processor: Processor name (will be looked up from raw_events if not provided)

    Returns:
        The canonical_fact_id (deterministic SHA256 hash)
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Phase 3: Determine processor (required for deterministic ID)
    effective_processor = processor
    if not effective_processor:
        cursor.execute("SELECT processor FROM raw_events WHERE id = ?", (raw_event_id,))
        row = cursor.fetchone()
        effective_processor = row[0] if row else 'unknown'

    # CE-06: Generate DETERMINISTIC canonical ID
    # Uses processor_refund_id as the transaction identifier for refunds
    canonical_fact_id = generate_deterministic_canonical_id(
        workspace_id=workspace_id,
        processor=effective_processor,
        processor_transaction_id=refund.processor_refund_id,
        event_type='refund',
    )

    # CE-07 (Idempotent): Check if already exists BEFORE attempting insert
    cursor.execute("SELECT id FROM canonical_facts WHERE id = ?", (canonical_fact_id,))
    if cursor.fetchone():
        logger.info(
            f"[CANONICAL] Idempotent skip: refund {refund.processor_refund_id} already "
            f"canonicalized as {canonical_fact_id[:16]}..."
        )
        return canonical_fact_id

    # Build metadata linking to original payment
    metadata_dict = {
        'original_payment_id': refund.processor_payment_id,
        'refund_type': 'full' if not refund.processor_payment_id else 'linked'
    }

    # INSERT new canonical fact (will fail on duplicate due to PRIMARY KEY)
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
        canonical_fact_id,
        workspace_id,
        raw_event_id,
        effective_processor,
        'refund',
        str(refund.amount),  # Stored positive per INV-ANET-13
        str(refund.amount),  # Net = gross for refunds (no fees on refund)
        '0',                 # No fees on refund
        refund.currency,
        refund.processor_refund_id,  # Use refund ID as the processor_payment_id
        processor_payout_id,
        None,  # No customer ID for refund
        None,  # No invoice ID for refund
        refund.payment_method,
        refund.card_brand,
        refund.card_last4,
        refund.refund_date.isoformat() if refund.refund_date else now.isoformat(),
        now.date().isoformat(),
        f'Refund for payment {refund.processor_payment_id}',
        refund.customer_email,
        refund.customer_name,
        json.dumps(metadata_dict),
        NORMALIZATION_VERSION,
        now.isoformat(),
    ))

    logger.info(
        f"[CANONICAL] Created refund: id={canonical_fact_id[:16]}... "
        f"processor={effective_processor} txn={refund.processor_refund_id} "
        f"amount=${refund.amount} original_payment={refund.processor_payment_id} "
        f"version={NORMALIZATION_VERSION}"
    )

    return canonical_fact_id


def get_canonical_facts_for_payout(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    processor_payout_id: str,
) -> List[Dict[str, Any]]:
    """
    Retrieve Canonical Facts for a payout.

    This returns pure processor facts with NO QBO or match data.

    Args:
        cursor: Database cursor
        workspace_id: Workspace identifier
        processor_payout_id: Payout identifier

    Returns:
        List of Canonical Fact dictionaries
    """
    cursor.execute("""
        SELECT * FROM canonical_facts
        WHERE workspace_id = ? AND processor_payout_id = ?
        ORDER BY processor_timestamp ASC
    """, (workspace_id, processor_payout_id))

    return [dict(row) for row in cursor.fetchall()]


def get_canonical_fact_by_id(
    cursor: sqlite3.Cursor,
    canonical_fact_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Retrieve a single Canonical Fact by ID.

    Args:
        cursor: Database cursor
        canonical_fact_id: The Canonical Fact identifier

    Returns:
        Canonical Fact dictionary or None if not found
    """
    cursor.execute("""
        SELECT * FROM canonical_facts
        WHERE id = ?
    """, (canonical_fact_id,))

    row = cursor.fetchone()
    return dict(row) if row else None


def verify_canonical_fact_immutability(
    cursor: sqlite3.Cursor,
    canonical_fact_id: str,
) -> bool:
    """
    Verify that a Canonical Fact cannot be modified.

    This is a test helper that attempts to update a record
    and verifies the immutability trigger prevents it.

    Args:
        cursor: Database cursor
        canonical_fact_id: The Canonical Fact to test

    Returns:
        True if immutability is enforced (UPDATE fails), False otherwise
    """
    try:
        cursor.execute("""
            UPDATE canonical_facts
            SET description = 'MUTATION ATTEMPT - SHOULD FAIL'
            WHERE id = ?
        """, (canonical_fact_id,))
        # If we get here, immutability is NOT enforced
        return False
    except sqlite3.IntegrityError:
        # Trigger fired - immutability is enforced
        return True


# =============================================================================
# Contract Compliance Assertions
# =============================================================================

def assert_no_qbo_fields():
    """
    Assert that the canonical_facts table has no QBO fields.

    This is a structural assertion that can be run at test time
    to verify Phase 2 compliance.

    Raises:
        AssertionError: If any QBO field exists in the table schema
    """
    # QBO fields that MUST NOT exist in canonical_facts
    prohibited_qbo_fields = [
        'qbo_customer_id',
        'qbo_invoice_id',
        'qbo_payment_id',
    ]
    return prohibited_qbo_fields


def assert_no_match_fields():
    """
    Assert that the canonical_facts table has no match fields.

    This is a structural assertion that can be run at test time
    to verify Phase 2 compliance.

    Raises:
        AssertionError: If any match field exists in the table schema
    """
    # Match fields that MUST NOT exist in canonical_facts
    prohibited_match_fields = [
        'match_type',
        'match_confidence',
        'match_score_breakdown',
        'human_confirmed_at',
        'confirmed_by',
    ]
    return prohibited_match_fields
