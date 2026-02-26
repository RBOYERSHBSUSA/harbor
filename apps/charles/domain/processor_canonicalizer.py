"""
Processor Transaction Canonicalizer

Transforms processor_transactions rows into canonical_facts.

Same contract principles as qbo_accounting_canonicalizer.py:
- Deterministic: Same input MUST produce same canonical output
- Idempotent: Re-canonicalization MUST NOT create duplicates
- Append-only: New observations create new facts, never mutate
- Pure: No matching, no inference, no side effects beyond canonical_facts

This bridges the gap between the processor sync layer (processor_transactions)
and the canonical layer (canonical_facts) so the matcher can operate on
one unified table.
"""

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)

PROCESSOR_NORMALIZATION_VERSION = '3.1.0'

# Transaction types that map to canonical_facts event_type CHECK constraint
SUPPORTED_EVENT_TYPES = {'payment', 'payout', 'refund', 'failed_payment', 'chargeback', 'fee', 'ach_return'}


@dataclass
class ProcessorCanonicalResult:
    """Result of canonicalizing a single processor transaction."""
    canonical_fact_id: str
    processor_transaction_id: str
    event_type: str
    was_duplicate: bool


def generate_processor_canonical_id(
    workspace_id: str,
    processor: str,
    transaction_type: str,
    processor_transaction_id: str,
) -> str:
    """
    Generate a deterministic canonical ID for processor transactions.

    canonical_id = SHA256(workspace_id | processor | transaction_type | processor_transaction_id)
    """
    key_parts = [
        workspace_id,
        processor,
        transaction_type.lower(),
        processor_transaction_id,
    ]
    key_string = '|'.join(str(p) for p in key_parts)
    return hashlib.sha256(key_string.encode()).hexdigest()


class ProcessorCanonicalizer:
    """
    Pure canonicalization engine for processor transactions.

    Reads processor_transactions rows (passed as input, not fetched).
    Writes ONLY to canonical_facts table.
    """

    def __init__(self, workspace_id: str):
        self.workspace_id = workspace_id
        self.normalization_version = PROCESSOR_NORMALIZATION_VERSION

    def canonicalize_transaction(
        self,
        cursor: sqlite3.Cursor,
        txn: Dict,
    ) -> ProcessorCanonicalResult:
        """
        Canonicalize a single processor_transactions row into canonical_facts.

        Args:
            cursor: Database cursor
            txn: Dict from processor_transactions row

        Returns:
            ProcessorCanonicalResult with canonical_fact_id and duplicate status
        """
        processor = txn.get('processor', 'unknown')
        transaction_type = txn.get('transaction_type', 'payment')
        processor_transaction_id = txn.get('processor_transaction_id', '')

        # Skip unsupported event types (e.g. declined, voided)
        if transaction_type not in SUPPORTED_EVENT_TYPES:
            return ProcessorCanonicalResult(
                canonical_fact_id='',
                processor_transaction_id=processor_transaction_id,
                event_type=transaction_type,
                was_duplicate=True,  # Signal to caller: no new fact created
            )

        # Generate deterministic canonical ID
        canonical_id = generate_processor_canonical_id(
            self.workspace_id, processor, transaction_type, processor_transaction_id,
        )

        # Check for duplicate (idempotency)
        cursor.execute("SELECT id FROM canonical_facts WHERE id = ?", (canonical_id,))
        if cursor.fetchone():
            return ProcessorCanonicalResult(
                canonical_fact_id=canonical_id,
                processor_transaction_id=processor_transaction_id,
                event_type=transaction_type,
                was_duplicate=True,
            )

        # Map event_type: processor_transactions uses same values as canonical_facts
        # (payment, refund, payout, chargeback, etc.)
        event_type = transaction_type

        # Synthetic raw_event_id (no actual raw_events row exists)
        raw_event_id = f"{processor}:{transaction_type}:{processor_transaction_id}"

        # Parse metadata from processor_transactions
        metadata_raw = txn.get('metadata')
        source_metadata = {}
        if metadata_raw:
            try:
                source_metadata = json.loads(metadata_raw) if isinstance(metadata_raw, str) else metadata_raw
            except (json.JSONDecodeError, TypeError):
                pass

        # Build canonical metadata
        canonical_metadata = {
            'processor_transaction_table_id': txn.get('id'),
            'processor': processor,
            'processor_transaction_id': processor_transaction_id,
            'payment_method': txn.get('payment_method'),
            'card_brand': txn.get('card_brand'),
            'card_last4': txn.get('card_last4'),
            'settlement_date': source_metadata.get('settlement_date'),
            'transaction_status': source_metadata.get('transaction_status'),
        }

        # Extract settlement_date
        settlement_date = source_metadata.get('settlement_date')

        now = datetime.now(timezone.utc)

        cursor.execute("""
            INSERT INTO canonical_facts (
                id, workspace_id, raw_event_id, processor,
                event_type, source_object_type,
                amount_gross, amount_net, fees, currency,
                processor_payment_id, processor_payout_id,
                processor_customer_id, processor_invoice_id,
                payment_method, card_brand, card_last4,
                processor_timestamp, settlement_date,
                observed_at, source_updated_at,
                description, customer_email, customer_name,
                metadata, normalization_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            canonical_id,
            self.workspace_id,
            raw_event_id,
            processor,
            event_type,
            None,  # source_object_type: not applicable for processor transactions
            txn.get('amount', '0'),       # amount_gross
            txn.get('amount', '0'),       # amount_net (no fee split available)
            '0',                          # fees
            txn.get('currency', 'USD'),
            processor_transaction_id,     # processor_payment_id = the transaction ID
            txn.get('processor_payout_id'),
            None,  # processor_customer_id
            None,  # processor_invoice_id
            txn.get('payment_method'),
            txn.get('card_brand'),
            txn.get('card_last4'),
            txn.get('processor_timestamp'),
            settlement_date,
            now.isoformat(),              # observed_at = when we canonicalized
            txn.get('ingested_at'),       # source_updated_at = when processor_transactions row was created
            txn.get('description', f"Transaction {processor_transaction_id}"),
            txn.get('customer_email'),
            txn.get('customer_name'),
            json.dumps(canonical_metadata),
            self.normalization_version,
            now.isoformat(),
        ))

        logger.info(
            f"[CANONICAL-PROC] Created {event_type}: "
            f"id={canonical_id[:16]}... "
            f"processor={processor} "
            f"txn_id={processor_transaction_id}"
        )

        return ProcessorCanonicalResult(
            canonical_fact_id=canonical_id,
            processor_transaction_id=processor_transaction_id,
            event_type=event_type,
            was_duplicate=False,
        )
