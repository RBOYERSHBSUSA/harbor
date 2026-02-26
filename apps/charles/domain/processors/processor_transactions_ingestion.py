"""
Processor Transactions Ingestion Module

================================================================================
LAYER 1 BOUNDARY — QUERYABLE TRANSACTION DATA
================================================================================

This module populates the processor_transactions table from raw_events.
It provides a queryable view of Layer 1 data for the frontend transaction browser.

ARCHITECTURAL CONSTRAINTS (per PROCESSOR_TRANSACTIONS_CONTRACT.md):
- This is Layer 1 data - preserved processor-native reality
- Read-only for users - no mutations that affect accounting
- No imports from canonicalization, replay, or explainability
- Idempotent operations - safe to re-run

FORBIDDEN imports:
- src/module4_reconciliation/* (canonicalization)
- src/replay/* (replay orchestration)
- src/explainability/* (explainability service)

================================================================================
"""

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple


# =============================================================================
# Data Models (Layer 1 only - no canonical models)
# =============================================================================

@dataclass
class NormalizedTransaction:
    """
    Normalized transaction data extracted from raw_events payload.

    This is a Layer 1 model - not to be confused with canonical events.
    """
    processor_transaction_id: str
    transaction_type: str  # payment, refund, payout, fee, adjustment, chargeback
    amount: Decimal  # Always positive, in dollars
    currency: str
    processor_timestamp: datetime
    customer_email: Optional[str]
    customer_name: Optional[str]
    description: Optional[str]
    payment_method: Optional[str]
    card_brand: Optional[str]
    card_last4: Optional[str]
    processor_payout_id: Optional[str]
    metadata: Optional[Dict]


# =============================================================================
# Normalizer Classes
# =============================================================================

class ProcessorTransactionNormalizer:
    """
    Normalizes raw_events payloads into NormalizedTransaction objects.

    Each processor has its own normalization logic to handle
    processor-specific payload structures.
    """

    @staticmethod
    def normalize_stripe(
        raw_payload: Dict,
        event_category: str,
        received_at: Optional[str] = None
    ) -> Optional[NormalizedTransaction]:
        """
        Normalize Stripe raw_events payload.

        Args:
            raw_payload: JSON payload from raw_events
            event_category: Category from raw_events (payment, refund)
            received_at: Fallback timestamp from raw_events.received_at

        Returns:
            NormalizedTransaction or None if unable to normalize
        """
        try:
            if event_category == 'payment':
                return ProcessorTransactionNormalizer._normalize_stripe_payment(
                    raw_payload, received_at
                )
            elif event_category == 'refund':
                return ProcessorTransactionNormalizer._normalize_stripe_refund(
                    raw_payload, received_at
                )
            else:
                return None
        except Exception:
            return None

    @staticmethod
    def _normalize_stripe_payment(
        payload: Dict,
        received_at: Optional[str]
    ) -> NormalizedTransaction:
        """Normalize Stripe payment event."""
        # Extract timestamp from charge_data if available
        timestamp = None
        metadata = payload.get('metadata', {})
        charge_data = metadata.get('charge_data', {})

        if charge_data.get('created'):
            timestamp = datetime.fromtimestamp(
                charge_data['created'], tz=timezone.utc
            )
        elif received_at:
            timestamp = datetime.fromisoformat(
                received_at.replace('Z', '+00:00')
            )
        else:
            timestamp = datetime.now(timezone.utc)

        # Convert cents to dollars
        amount_cents = payload.get('amount_gross_cents', 0)
        amount = Decimal(str(amount_cents)) / Decimal('100')

        return NormalizedTransaction(
            processor_transaction_id=payload.get('processor_payment_id', ''),
            transaction_type='payment',
            amount=amount,
            currency=payload.get('currency', 'USD'),
            processor_timestamp=timestamp,
            customer_email=payload.get('customer_email'),
            customer_name=payload.get('customer_name'),
            description=payload.get('description'),
            payment_method=payload.get('payment_method'),
            card_brand=payload.get('card_brand'),
            card_last4=payload.get('card_last4'),
            processor_payout_id=None,  # Would come from balance transaction
            metadata=metadata
        )

    @staticmethod
    def _normalize_stripe_refund(
        payload: Dict,
        received_at: Optional[str]
    ) -> NormalizedTransaction:
        """Normalize Stripe refund event."""
        # Refund date handling
        refund_date = payload.get('refund_date')
        if refund_date:
            timestamp = datetime.fromisoformat(
                refund_date.replace('Z', '+00:00')
            )
        elif received_at:
            timestamp = datetime.fromisoformat(
                received_at.replace('Z', '+00:00')
            )
        else:
            timestamp = datetime.now(timezone.utc)

        # Convert cents to dollars
        amount_cents = payload.get('amount_cents', 0)
        amount = Decimal(str(amount_cents)) / Decimal('100')

        return NormalizedTransaction(
            processor_transaction_id=payload.get('processor_refund_id', ''),
            transaction_type='refund',
            amount=amount,
            currency=payload.get('currency', 'USD'),
            processor_timestamp=timestamp,
            customer_email=None,
            customer_name=None,
            description=f"Refund for {payload.get('processor_payment_id', 'unknown')}",
            payment_method=None,
            card_brand=None,
            card_last4=None,
            processor_payout_id=None,
            metadata={'original_payment_id': payload.get('processor_payment_id')}
        )

    @staticmethod
    def normalize_authorize_net(
        raw_payload: Dict,
        event_category: str,
        received_at: Optional[str] = None
    ) -> Optional[NormalizedTransaction]:
        """
        Normalize Authorize.net raw_events payload.

        Args:
            raw_payload: JSON payload from raw_events
            event_category: Category from raw_events (payment, refund)
            received_at: Fallback timestamp from raw_events.received_at

        Returns:
            NormalizedTransaction or None if unable to normalize
        """
        try:
            if event_category == 'payment':
                return ProcessorTransactionNormalizer._normalize_authnet_payment(
                    raw_payload, received_at
                )
            elif event_category == 'refund':
                return ProcessorTransactionNormalizer._normalize_authnet_refund(
                    raw_payload, received_at
                )
            else:
                return None
        except Exception:
            return None

    @staticmethod
    def _normalize_authnet_payment(
        payload: Dict,
        received_at: Optional[str]
    ) -> NormalizedTransaction:
        """Normalize Authorize.net payment event."""
        # Timestamp handling
        if received_at:
            timestamp = datetime.fromisoformat(
                received_at.replace('Z', '+00:00')
            )
        else:
            timestamp = datetime.now(timezone.utc)

        # Convert cents to dollars
        amount_cents = payload.get('amount_gross_cents', 0)
        amount = Decimal(str(amount_cents)) / Decimal('100')

        return NormalizedTransaction(
            processor_transaction_id=payload.get('processor_payment_id', ''),
            transaction_type='payment',
            amount=amount,
            currency=payload.get('currency', 'USD'),
            processor_timestamp=timestamp,
            customer_email=payload.get('customer_email'),
            customer_name=payload.get('customer_name'),
            description=payload.get('description'),
            payment_method=payload.get('payment_method'),
            card_brand=payload.get('card_brand'),
            card_last4=payload.get('card_last4'),
            processor_payout_id=None,
            metadata=payload.get('metadata')
        )

    @staticmethod
    def _normalize_authnet_refund(
        payload: Dict,
        received_at: Optional[str]
    ) -> NormalizedTransaction:
        """Normalize Authorize.net refund event."""
        # Refund date handling
        refund_date = payload.get('refund_date')
        if refund_date:
            timestamp = datetime.fromisoformat(
                refund_date.replace('Z', '+00:00')
            )
        elif received_at:
            timestamp = datetime.fromisoformat(
                received_at.replace('Z', '+00:00')
            )
        else:
            timestamp = datetime.now(timezone.utc)

        # Convert cents to dollars
        amount_cents = payload.get('amount_cents', 0)
        amount = Decimal(str(amount_cents)) / Decimal('100')

        return NormalizedTransaction(
            processor_transaction_id=payload.get('processor_refund_id', ''),
            transaction_type='refund',
            amount=amount,
            currency=payload.get('currency', 'USD'),
            processor_timestamp=timestamp,
            customer_email=None,
            customer_name=None,
            description=f"Refund for {payload.get('processor_payment_id', 'unknown')}",
            payment_method=None,
            card_brand=None,
            card_last4=None,
            processor_payout_id=None,
            metadata={'original_payment_id': payload.get('processor_payment_id')}
        )


# =============================================================================
# Backfill Function
# =============================================================================

@dataclass
class BackfillResult:
    """Result of a backfill operation."""
    inserted: int
    skipped: int  # Duplicates
    errors: int
    processor: str
    workspace_id: str


def backfill_processor_transactions(
    workspace_id: str,
    processor: str,
    conn: sqlite3.Connection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> BackfillResult:
    """
    Backfill processor_transactions from raw_events.

    This function reads raw_events and populates the processor_transactions
    table with normalized data. It is idempotent - safe to re-run.

    Args:
        workspace_id: Workspace to backfill
        processor: Processor name ('stripe' or 'authorize_net')
        conn: Database connection
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)

    Returns:
        BackfillResult with counts
    """
    cursor = conn.cursor()

    # Build query for raw_events
    query = """
        SELECT id, processor_event_id, event_category, payload, received_at
        FROM raw_events
        WHERE workspace_id = ?
          AND processor = ?
    """
    params: List = [workspace_id, processor]

    if start_date:
        query += " AND DATE(received_at) >= ?"
        params.append(start_date)

    if end_date:
        query += " AND DATE(received_at) <= ?"
        params.append(end_date)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    inserted = 0
    skipped = 0
    errors = 0

    for row in rows:
        raw_event_id = row[0]
        processor_event_id = row[1]
        event_category = row[2]
        payload_str = row[3]
        received_at = row[4]

        try:
            payload = json.loads(payload_str)

            # Normalize based on processor
            if processor == 'stripe':
                normalized = ProcessorTransactionNormalizer.normalize_stripe(
                    payload, event_category, received_at
                )
            elif processor == 'authorize_net':
                normalized = ProcessorTransactionNormalizer.normalize_authorize_net(
                    payload, event_category, received_at
                )
            else:
                normalized = None

            if not normalized:
                errors += 1
                continue

            # Insert with IGNORE for idempotency
            result = _insert_processor_transaction(
                cursor=cursor,
                workspace_id=workspace_id,
                processor=processor,
                normalized=normalized,
                raw_event_id=raw_event_id
            )

            if result:
                inserted += 1
            else:
                skipped += 1

        except Exception:
            errors += 1

    conn.commit()

    return BackfillResult(
        inserted=inserted,
        skipped=skipped,
        errors=errors,
        processor=processor,
        workspace_id=workspace_id
    )


# =============================================================================
# Incremental Ingestion
# =============================================================================

def ingest_processor_transaction(
    workspace_id: str,
    raw_event_id: str,
    conn: sqlite3.Connection,
) -> bool:
    """
    Ingest a single raw event into processor_transactions.

    Called after raw event persistence during sync operations.
    Idempotent via INSERT OR IGNORE.

    Args:
        workspace_id: Workspace ID
        raw_event_id: ID of the raw_event to process
        conn: Database connection

    Returns:
        True if inserted, False if duplicate or error
    """
    cursor = conn.cursor()

    # Fetch the raw event
    cursor.execute("""
        SELECT processor, processor_event_id, event_category, payload, received_at
        FROM raw_events
        WHERE id = ? AND workspace_id = ?
    """, (raw_event_id, workspace_id))

    row = cursor.fetchone()
    if not row:
        return False

    processor = row[0]
    event_category = row[2]
    payload_str = row[3]
    received_at = row[4]

    try:
        payload = json.loads(payload_str)

        # Normalize based on processor
        if processor == 'stripe':
            normalized = ProcessorTransactionNormalizer.normalize_stripe(
                payload, event_category, received_at
            )
        elif processor == 'authorize_net':
            normalized = ProcessorTransactionNormalizer.normalize_authorize_net(
                payload, event_category, received_at
            )
        else:
            return False

        if not normalized:
            return False

        result = _insert_processor_transaction(
            cursor=cursor,
            workspace_id=workspace_id,
            processor=processor,
            normalized=normalized,
            raw_event_id=raw_event_id
        )

        if result:
            conn.commit()

        return result

    except Exception:
        return False


# =============================================================================
# Helper Functions
# =============================================================================

def _insert_processor_transaction(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    processor: str,
    normalized: NormalizedTransaction,
    raw_event_id: str,
) -> bool:
    """
    Insert a normalized transaction into processor_transactions.

    Uses INSERT OR IGNORE for idempotency.

    Returns:
        True if inserted, False if duplicate
    """
    txn_id = f"pt_{uuid.uuid4().hex[:16]}"

    try:
        cursor.execute("""
            INSERT OR IGNORE INTO processor_transactions (
                id,
                workspace_id,
                processor,
                processor_transaction_id,
                transaction_type,
                amount,
                currency,
                processor_timestamp,
                customer_email,
                customer_name,
                description,
                payment_method,
                card_brand,
                card_last4,
                processor_payout_id,
                raw_event_id,
                metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            txn_id,
            workspace_id,
            processor,
            normalized.processor_transaction_id,
            normalized.transaction_type,
            str(normalized.amount),
            normalized.currency,
            normalized.processor_timestamp.isoformat(),
            normalized.customer_email,
            normalized.customer_name,
            normalized.description,
            normalized.payment_method,
            normalized.card_brand,
            normalized.card_last4,
            normalized.processor_payout_id,
            raw_event_id,
            json.dumps(normalized.metadata) if normalized.metadata else None
        ))

        return cursor.rowcount > 0

    except sqlite3.IntegrityError:
        # Duplicate - this is expected for idempotency
        return False


def backfill_all_processors(
    workspace_id: str,
    conn: sqlite3.Connection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, BackfillResult]:
    """
    Backfill processor_transactions for all processors.

    Args:
        workspace_id: Workspace to backfill
        conn: Database connection
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        Dict mapping processor name to BackfillResult
    """
    results = {}

    for processor in ['stripe', 'authorize_net']:
        results[processor] = backfill_processor_transactions(
            workspace_id=workspace_id,
            processor=processor,
            conn=conn,
            start_date=start_date,
            end_date=end_date
        )

    return results
