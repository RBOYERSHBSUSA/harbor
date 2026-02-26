"""
Processor Transactions Sync Module

================================================================================
LAYER 1 BOUNDARY — PROCESSOR API SYNC FOR TRANSACTIONS
================================================================================

This module syncs transaction data from processor APIs (Stripe, Authorize.net)
into the processor_transactions table for the Transactions page.

Per PROCESSOR_TRANSACTIONS_CONTRACT.md:
- Initial bootstrap: ~63 days of history (beginning of month 2 months prior)
- Incremental sync: cursor-based, append-only, idempotent
- No imports from canonicalization/replay/explainability

FORBIDDEN imports:
- src/module4_reconciliation/* (canonicalization)
- src/replay/* (replay orchestration)
- src/explainability/* (explainability service)

================================================================================
"""

import json
import os
import sqlite3
import uuid
import requests
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET


# =============================================================================
# Constants
# =============================================================================

# Default sync window: 62 days (guarantees 2 full calendar months, 2 Authorize.net API calls)
DEFAULT_BOOTSTRAP_DAYS = 62

# Authorize.net API
ANET_PRODUCTION_URL = 'https://api.authorize.net/xml/v1/request.api'
ANET_NAMESPACE = 'AnetApi/xml/v1/schema/AnetApiSchema.xsd'

# Valid settlement states to ingest
ANET_VALID_STATES = {'settledSuccessfully', 'refundSettledSuccessfully'}

# Authorize.net transactionStatus mapping to our schema types
# Schema allows: payment, refund, payout, fee, adjustment, chargeback
# Status values come from getTransactionListResponse
# Goal: Mirror Authorize.net completely so bookkeepers don't need to log in there
ANET_STATUS_MAPPING = {
    # Successful payments
    'settledSuccessfully': 'payment',
    'capturedPendingSettlement': 'payment',
    'authorizedPendingCapture': 'payment',
    # Refunds (money back to customer)
    'refundSettledSuccessfully': 'refund',
    'refundPendingSettlement': 'refund',
    # ACH returns (failed/reversed ACH payments)
    'returnedItem': 'ach_return',
    # Chargebacks
    'chargeback': 'chargeback',
    'chargebackPending': 'chargeback',
    # Voided transactions (cancelled before settlement)
    'voided': 'voided',
    # Declined transactions (failed attempts)
    'declined': 'declined',
    'expired': 'declined',
    'failedReview': 'declined',
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class SyncResult:
    """Result of a sync operation."""
    processor: str
    workspace_id: str
    transactions_synced: int
    transactions_skipped: int
    errors: int
    is_bootstrap: bool
    sync_cursor: Optional[str]
    error_message: Optional[str] = None


# =============================================================================
# Stripe Sync
# =============================================================================

def sync_stripe_transactions(
    workspace_id: str,
    api_key: str,
    conn: sqlite3.Connection,
    is_bootstrap: bool = False,
    last_cursor: Optional[str] = None,
) -> SyncResult:
    """
    Sync ALL transactions from Stripe API into processor_transactions.

    Fetches charges and refunds directly to get all available transactions.

    Args:
        workspace_id: Workspace to sync
        api_key: Stripe secret key
        conn: Database connection
        is_bootstrap: True for initial 63-day sync
        last_cursor: Last sync cursor for incremental

    Returns:
        SyncResult with counts
    """
    cursor = conn.cursor()
    synced = 0
    skipped = 0
    errors = 0
    new_cursor = last_cursor
    latest_timestamp = int(last_cursor) if last_cursor else 0

    try:
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        # Determine date range
        if is_bootstrap:
            # Bootstrap: 63 days back
            start_date = datetime.now(timezone.utc) - timedelta(days=DEFAULT_BOOTSTRAP_DAYS)
            created_gte = int(start_date.timestamp())
        else:
            # Incremental: use cursor (timestamp) or last 7 days as safety
            if last_cursor:
                created_gte = int(last_cursor)
            else:
                created_gte = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())

        # =========================================
        # Fetch ALL charges directly
        # =========================================
        has_more = True
        starting_after = None

        while has_more:
            params = {
                'limit': 100,
                'created[gte]': created_gte,
            }
            if starting_after:
                params['starting_after'] = starting_after

            response = requests.get(
                'https://api.stripe.com/v1/charges',
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code != 200:
                errors += 1
                break

            data = response.json()
            charges = data.get('data', [])
            has_more = data.get('has_more', False)

            for charge in charges:
                charge_created = charge.get('created', 0)
                if charge_created > latest_timestamp:
                    latest_timestamp = charge_created

                result = _insert_stripe_charge(cursor, workspace_id, charge)
                if result:
                    synced += 1
                else:
                    skipped += 1

                starting_after = charge.get('id')

        # =========================================
        # Fetch ALL refunds directly
        # =========================================
        has_more = True
        starting_after = None

        while has_more:
            params = {
                'limit': 100,
                'created[gte]': created_gte,
            }
            if starting_after:
                params['starting_after'] = starting_after

            response = requests.get(
                'https://api.stripe.com/v1/refunds',
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code != 200:
                errors += 1
                break

            data = response.json()
            refunds = data.get('data', [])
            has_more = data.get('has_more', False)

            for refund in refunds:
                refund_created = refund.get('created', 0)
                if refund_created > latest_timestamp:
                    latest_timestamp = refund_created

                result = _insert_stripe_refund_direct(cursor, workspace_id, refund)
                if result:
                    synced += 1
                else:
                    skipped += 1

                starting_after = refund.get('id')

        # Update cursor to latest timestamp
        if latest_timestamp > 0:
            new_cursor = str(latest_timestamp)

        conn.commit()

        return SyncResult(
            processor='stripe',
            workspace_id=workspace_id,
            transactions_synced=synced,
            transactions_skipped=skipped,
            errors=errors,
            is_bootstrap=is_bootstrap,
            sync_cursor=new_cursor
        )

    except Exception as e:
        return SyncResult(
            processor='stripe',
            workspace_id=workspace_id,
            transactions_synced=synced,
            transactions_skipped=skipped,
            errors=errors + 1,
            is_bootstrap=is_bootstrap,
            sync_cursor=last_cursor,
            error_message=str(e)
        )


def _insert_stripe_charge(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    charge: Dict,
) -> bool:
    """Insert a Stripe charge as processor_transaction."""
    txn_id = f"pt_{uuid.uuid4().hex[:16]}"
    charge_id = charge.get('id', '')

    # Get amount in dollars
    amount_cents = charge.get('amount', 0)
    amount = Decimal(str(amount_cents)) / Decimal('100')

    # Get timestamp
    created = charge.get('created', 0)
    timestamp = datetime.fromtimestamp(created, tz=timezone.utc).isoformat()

    # Get customer info
    billing = charge.get('billing_details', {})
    customer_email = billing.get('email') or charge.get('receipt_email')
    customer_name = billing.get('name')

    # Get payment method details
    pm_details = charge.get('payment_method_details', {})
    card_details = pm_details.get('card', {})
    payment_method = pm_details.get('type', 'card')
    card_brand = card_details.get('brand')
    card_last4 = card_details.get('last4')

    # Get balance transaction ID if available
    balance_txn_id = charge.get('balance_transaction')

    try:
        cursor.execute("""
            INSERT OR IGNORE INTO processor_transactions (
                id, workspace_id, processor, processor_transaction_id,
                transaction_type, amount, currency, processor_timestamp,
                customer_email, customer_name, description, payment_method,
                card_brand, card_last4, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            txn_id,
            workspace_id,
            'stripe',
            charge_id,
            'payment',
            str(amount),
            charge.get('currency', 'usd').upper(),
            timestamp,
            customer_email,
            customer_name,
            charge.get('description'),
            payment_method,
            card_brand,
            card_last4,
            json.dumps({
                'balance_transaction_id': balance_txn_id,
                'status': charge.get('status'),
                'paid': charge.get('paid'),
            })
        ))
        return cursor.rowcount > 0
    except sqlite3.IntegrityError:
        return False


def _insert_stripe_refund_direct(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    refund: Dict,
) -> bool:
    """Insert a Stripe refund as processor_transaction."""
    txn_id = f"pt_{uuid.uuid4().hex[:16]}"
    refund_id = refund.get('id', '')

    # Get amount in dollars (always positive)
    amount_cents = refund.get('amount', 0)
    amount = Decimal(str(amount_cents)) / Decimal('100')

    # Get timestamp
    created = refund.get('created', 0)
    timestamp = datetime.fromtimestamp(created, tz=timezone.utc).isoformat()

    # Get balance transaction ID if available
    balance_txn_id = refund.get('balance_transaction')

    try:
        cursor.execute("""
            INSERT OR IGNORE INTO processor_transactions (
                id, workspace_id, processor, processor_transaction_id,
                transaction_type, amount, currency, processor_timestamp,
                description, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            txn_id,
            workspace_id,
            'stripe',
            refund_id,
            'refund',
            str(amount),
            refund.get('currency', 'usd').upper(),
            timestamp,
            f"Refund for {refund.get('charge', 'unknown')}",
            json.dumps({
                'charge_id': refund.get('charge'),
                'balance_transaction_id': balance_txn_id,
                'status': refund.get('status'),
            })
        ))
        return cursor.rowcount > 0
    except sqlite3.IntegrityError:
        return False


# =============================================================================
# Authorize.net Sync
# =============================================================================

def sync_authnet_transactions(
    workspace_id: str,
    api_login_id: str,
    transaction_key: str,
    conn: sqlite3.Connection,
    is_bootstrap: bool = False,
    last_cursor: Optional[str] = None,
) -> SyncResult:
    """
    Sync transactions from Authorize.net API into processor_transactions.

    For bootstrap: fetches last 63 days of settled batches.
    For incremental: fetches from last cursor (date).

    Args:
        workspace_id: Workspace to sync
        api_login_id: Authorize.net API Login ID
        transaction_key: Authorize.net Transaction Key
        conn: Database connection
        is_bootstrap: True for initial 63-day sync
        last_cursor: Last sync cursor (ISO date string)

    Returns:
        SyncResult with counts
    """
    cursor = conn.cursor()
    synced = 0
    skipped = 0
    errors = 0
    new_cursor = last_cursor

    try:
        # Determine date range
        if is_bootstrap:
            start_date = date.today() - timedelta(days=DEFAULT_BOOTSTRAP_DAYS)
        else:
            if last_cursor:
                start_date = date.fromisoformat(last_cursor)
            else:
                start_date = date.today() - timedelta(days=7)

        end_date = date.today()

        # Fetch settled batches
        batches = _fetch_authnet_batches(api_login_id, transaction_key, start_date, end_date)

        for batch in batches:
            batch_id = batch.get('batchId')
            settlement_state = batch.get('settlementState')
            settlement_date = batch.get('settlementDate')

            # Update cursor
            if settlement_date and (not new_cursor or settlement_date > new_cursor):
                new_cursor = settlement_date

            # Skip invalid states
            if settlement_state not in ANET_VALID_STATES:
                continue

            # Fetch transactions in this batch
            transactions = _fetch_authnet_batch_transactions(
                api_login_id, transaction_key, batch_id
            )

            # Calculate batch total for payout record
            # Only count SETTLED transactions: payments add, refunds/returns subtract
            # Declined/voided transactions do NOT affect the settlement amount
            batch_total = Decimal('0')
            for txn in transactions:
                try:
                    amt = Decimal(txn.get('settleAmount', '0'))
                    txn_status = txn.get('transactionStatus', '')

                    # Classify via status mapping only (no inference)
                    txn_type = ANET_STATUS_MAPPING.get(txn_status, 'payment')

                    # Only settled transactions count toward batch total
                    if txn_type == 'payment':
                        batch_total += amt
                    elif txn_type in ('refund', 'ach_return'):
                        batch_total -= amt
                    # declined, voided, chargeback don't affect settlement
                except Exception:
                    pass

                result = _insert_authnet_transaction(
                    cursor, workspace_id, txn, batch_id, settlement_date
                )
                if result:
                    synced += 1
                else:
                    skipped += 1

            # Insert batch/payout record
            if batch_total > 0:
                payout_result = _insert_authnet_batch_payout(
                    cursor, workspace_id, batch, batch_total
                )
                if payout_result:
                    synced += 1

        conn.commit()

        return SyncResult(
            processor='authorize_net',
            workspace_id=workspace_id,
            transactions_synced=synced,
            transactions_skipped=skipped,
            errors=errors,
            is_bootstrap=is_bootstrap,
            sync_cursor=new_cursor
        )

    except Exception as e:
        return SyncResult(
            processor='authorize_net',
            workspace_id=workspace_id,
            transactions_synced=synced,
            transactions_skipped=skipped,
            errors=errors + 1,
            is_bootstrap=is_bootstrap,
            sync_cursor=last_cursor,
            error_message=str(e)
        )


def _fetch_authnet_batches(
    api_login_id: str,
    transaction_key: str,
    start_date: date,
    end_date: date,
) -> List[Dict]:
    """
    Fetch settled batches from Authorize.net.

    Note: Authorize.net API limits date range to 31 days max.
    This function automatically chunks requests if needed.
    """
    all_batches = []

    # Authorize.net API limits to 31 days per request
    MAX_DAYS = 31
    current_start = start_date

    while current_start < end_date:
        # Calculate chunk end date (max 31 days from start)
        chunk_end = min(current_start + timedelta(days=MAX_DAYS - 1), end_date)

        xml_request = f'''<?xml version="1.0" encoding="utf-8"?>
<getSettledBatchListRequest xmlns="{ANET_NAMESPACE}">
    <merchantAuthentication>
        <name>{api_login_id}</name>
        <transactionKey>{transaction_key}</transactionKey>
    </merchantAuthentication>
    <includeStatistics>true</includeStatistics>
    <firstSettlementDate>{current_start.isoformat()}T00:00:00Z</firstSettlementDate>
    <lastSettlementDate>{chunk_end.isoformat()}T23:59:59Z</lastSettlementDate>
</getSettledBatchListRequest>'''

        response = requests.post(
            ANET_PRODUCTION_URL,
            data=xml_request,
            headers={'Content-Type': 'application/xml'},
            timeout=30
        )

        if response.status_code == 200:
            # Parse XML
            response_text = response.text.replace(f'xmlns="{ANET_NAMESPACE}"', '')
            root = ET.fromstring(response_text)

            for batch in root.findall('.//batch'):
                batch_id = _get_xml_text(batch, 'batchId')
                settlement_state = _get_xml_text(batch, 'settlementState')
                settlement_time = _get_xml_text(batch, 'settlementTimeUTC')
                payment_method = _get_xml_text(batch, 'paymentMethod')  # creditCard or eCheck

                if batch_id:
                    settlement_date = None
                    settlement_timestamp = None
                    if settlement_time:
                        try:
                            dt = datetime.fromisoformat(settlement_time.replace('Z', '+00:00'))
                            settlement_date = dt.date().isoformat()
                            settlement_timestamp = dt.isoformat()
                        except ValueError:
                            pass

                    all_batches.append({
                        'batchId': batch_id,
                        'settlementState': settlement_state,
                        'settlementDate': settlement_date,
                        'settlementTimestamp': settlement_timestamp,
                        'paymentMethod': payment_method,
                    })

        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)

    return all_batches


def _fetch_authnet_batch_transactions(
    api_login_id: str,
    transaction_key: str,
    batch_id: str,
) -> List[Dict]:
    """Fetch transactions in a batch from Authorize.net."""
    xml_request = f'''<?xml version="1.0" encoding="utf-8"?>
<getTransactionListRequest xmlns="{ANET_NAMESPACE}">
    <merchantAuthentication>
        <name>{api_login_id}</name>
        <transactionKey>{transaction_key}</transactionKey>
    </merchantAuthentication>
    <batchId>{batch_id}</batchId>
</getTransactionListRequest>'''

    response = requests.post(
        ANET_PRODUCTION_URL,
        data=xml_request,
        headers={'Content-Type': 'application/xml'},
        timeout=30
    )

    if response.status_code != 200:
        return []

    response_text = response.text.replace(f'xmlns="{ANET_NAMESPACE}"', '')
    root = ET.fromstring(response_text)

    transactions = []
    for txn in root.findall('.//transaction'):
        trans_id = _get_xml_text(txn, 'transId')
        txn_status = _get_xml_text(txn, 'transactionStatus')  # e.g., settledSuccessfully, refundSettledSuccessfully
        submit_time = _get_xml_text(txn, 'submitTimeUTC')
        settle_amount = _get_xml_text(txn, 'settleAmount')
        account_type = _get_xml_text(txn, 'accountType')  # e.g., Visa, eCheck
        account_number = _get_xml_text(txn, 'accountNumber')
        has_returned = _get_xml_text(txn, 'hasReturnedItems')  # for ACH returns

        # Get customer info
        first_name = _get_xml_text(txn, 'firstName')
        last_name = _get_xml_text(txn, 'lastName')
        customer_name = f"{first_name or ''} {last_name or ''}".strip() or None

        if trans_id:
            transactions.append({
                'transId': trans_id,
                'transactionStatus': txn_status,
                'submitTimeUTC': submit_time,
                'settleAmount': settle_amount,
                'accountType': account_type,
                'accountNumber': account_number,
                'customerName': customer_name,
                'hasReturnedItems': has_returned,
            })

    return transactions


def _insert_authnet_transaction(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    txn: Dict,
    batch_id: str,
    settlement_date: Optional[str],
) -> bool:
    """Insert an Authorize.net transaction as processor_transaction."""
    txn_id = f"pt_{uuid.uuid4().hex[:16]}"
    trans_id = txn.get('transId', '')
    txn_status = txn.get('transactionStatus', '')

    # Classify via status mapping only (no inference from hasReturnedItems flag)
    transaction_type = ANET_STATUS_MAPPING.get(txn_status, 'payment')

    # Get amount
    settle_amount = txn.get('settleAmount', '0')
    try:
        amount = Decimal(settle_amount)
    except Exception:
        amount = Decimal('0')

    # Get timestamp
    submit_time = txn.get('submitTimeUTC')
    if submit_time:
        try:
            timestamp = datetime.fromisoformat(
                submit_time.replace('Z', '+00:00')
            ).isoformat()
        except ValueError:
            timestamp = datetime.now(timezone.utc).isoformat()
    else:
        timestamp = datetime.now(timezone.utc).isoformat()

    # Determine payment method
    account_type = txn.get('accountType', '').lower()
    if account_type in ('echeck', 'checking', 'savings'):
        payment_method = 'bank_account'
        card_brand = None
    else:
        payment_method = 'card'
        card_brand = account_type if account_type else None

    # Get last 4 digits
    account_number = txn.get('accountNumber', '')
    card_last4 = account_number[-4:] if account_number else None

    try:
        cursor.execute("""
            INSERT OR IGNORE INTO processor_transactions (
                id, workspace_id, processor, processor_transaction_id,
                transaction_type, amount, currency, processor_timestamp,
                customer_name, description, payment_method, card_brand,
                card_last4, processor_payout_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            txn_id,
            workspace_id,
            'authorize_net',
            trans_id,
            transaction_type,
            str(amount),
            'USD',
            timestamp,
            txn.get('customerName'),
            f"Transaction {trans_id}",
            payment_method,
            card_brand,
            card_last4,
            batch_id,
            json.dumps({
                'settlement_date': settlement_date,
                'transaction_status': txn_status,
                'has_returned_items': txn.get('hasReturnedItems'),
            })
        ))
        return cursor.rowcount > 0
    except sqlite3.IntegrityError:
        return False


def _get_xml_text(element: ET.Element, tag: str) -> Optional[str]:
    """Get text content of XML child element."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def _insert_authnet_batch_payout(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    batch: Dict,
    batch_total: Decimal,
) -> bool:
    """Insert an Authorize.net settlement batch as a payout record."""
    txn_id = f"pt_{uuid.uuid4().hex[:16]}"
    batch_id = batch.get('batchId', '')
    payment_method = batch.get('paymentMethod', '')
    settlement_timestamp = batch.get('settlementTimestamp')
    settlement_date = batch.get('settlementDate')

    if not settlement_timestamp:
        settlement_timestamp = datetime.now(timezone.utc).isoformat()

    # Determine if this is card or ACH payout
    if payment_method and 'echeck' in payment_method.lower():
        method = 'bank_account'
        description = f"ACH Settlement Batch {batch_id}"
    else:
        method = 'card'
        description = f"Card Settlement Batch {batch_id}"

    try:
        cursor.execute("""
            INSERT OR IGNORE INTO processor_transactions (
                id, workspace_id, processor, processor_transaction_id,
                transaction_type, amount, currency, processor_timestamp,
                description, payment_method, processor_payout_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            txn_id,
            workspace_id,
            'authorize_net',
            f"batch_{batch_id}",  # Use batch_ prefix to distinguish from transactions
            'payout',
            str(batch_total),
            'USD',
            settlement_timestamp,
            description,
            method,
            batch_id,
            json.dumps({
                'settlement_date': settlement_date,
                'settlement_state': batch.get('settlementState'),
                'payment_method': payment_method,
            })
        ))
        return cursor.rowcount > 0
    except sqlite3.IntegrityError:
        return False


# =============================================================================
# Main Sync Function
# =============================================================================

def sync_processor_transactions(
    workspace_id: str,
    conn: sqlite3.Connection,
    processor: Optional[str] = None,
) -> Dict[str, SyncResult]:
    """
    Sync processor transactions for a workspace.

    Checks sync state to determine bootstrap vs incremental.
    Syncs all configured processors or a specific one.

    Args:
        workspace_id: Workspace to sync
        conn: Database connection
        processor: Specific processor to sync, or None for all

    Returns:
        Dict mapping processor name to SyncResult
    """
    cursor = conn.cursor()
    results = {}

    # Get configured processors for this workspace
    cursor.execute("""
        SELECT processor_type, secret_key, secret_key_encrypted
        FROM processor_config
        WHERE workspace_id = ?
    """, (workspace_id,))

    configs = cursor.fetchall()

    for config in configs:
        proc_type = config[0]
        secret_key = config[1] or config[2]  # Use encrypted if available

        if processor and proc_type != processor:
            continue

        if not secret_key:
            continue

        # Check sync state
        cursor.execute("""
            SELECT first_sync_at, last_sync_cursor
            FROM processor_transactions_sync
            WHERE workspace_id = ? AND processor = ?
        """, (workspace_id, proc_type))

        sync_state = cursor.fetchone()
        is_bootstrap = sync_state is None or sync_state[0] is None
        last_cursor = sync_state[1] if sync_state else None

        if proc_type == 'stripe':
            result = sync_stripe_transactions(
                workspace_id=workspace_id,
                api_key=secret_key,
                conn=conn,
                is_bootstrap=is_bootstrap,
                last_cursor=last_cursor
            )
        elif proc_type == 'authorize_net':
            # For Authorize.net, we need both API login ID and transaction key
            # The secret_key field stores them as JSON: {"api_login_id": "...", "transaction_key": "..."}
            api_login_id = None
            transaction_key = None

            try:
                # Try JSON format first
                creds = json.loads(secret_key)
                api_login_id = creds.get('api_login_id')
                transaction_key = creds.get('transaction_key')
            except (json.JSONDecodeError, TypeError):
                # Fallback to colon-separated format
                if ':' in secret_key:
                    api_login_id, transaction_key = secret_key.split(':', 1)

            if not api_login_id or not transaction_key:
                continue

            result = sync_authnet_transactions(
                workspace_id=workspace_id,
                api_login_id=api_login_id,
                transaction_key=transaction_key,
                conn=conn,
                is_bootstrap=is_bootstrap,
                last_cursor=last_cursor
            )
        else:
            continue

        # Update sync state
        _update_sync_state(cursor, workspace_id, proc_type, result)
        conn.commit()

        results[proc_type] = result

    return results


def _update_sync_state(
    cursor: sqlite3.Cursor,
    workspace_id: str,
    processor: str,
    result: SyncResult,
) -> None:
    """Update sync tracking state after a sync."""
    now = datetime.now(timezone.utc).isoformat()

    # Check if record exists
    cursor.execute("""
        SELECT id FROM processor_transactions_sync
        WHERE workspace_id = ? AND processor = ?
    """, (workspace_id, processor))

    existing = cursor.fetchone()

    if existing:
        cursor.execute("""
            UPDATE processor_transactions_sync
            SET last_sync_at = ?,
                last_sync_cursor = ?,
                transactions_synced = transactions_synced + ?,
                sync_status = ?,
                last_error = ?,
                updated_at = ?
            WHERE workspace_id = ? AND processor = ?
        """, (
            now,
            result.sync_cursor,
            result.transactions_synced,
            'completed' if not result.error_message else 'failed',
            result.error_message,
            now,
            workspace_id,
            processor
        ))
    else:
        sync_id = f"pts_{uuid.uuid4().hex[:16]}"
        cursor.execute("""
            INSERT INTO processor_transactions_sync (
                id, workspace_id, processor, first_sync_at, last_sync_at,
                last_sync_cursor, transactions_synced, sync_status, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sync_id,
            workspace_id,
            processor,
            now if result.is_bootstrap else None,
            now,
            result.sync_cursor,
            result.transactions_synced,
            'completed' if not result.error_message else 'failed',
            result.error_message
        ))
