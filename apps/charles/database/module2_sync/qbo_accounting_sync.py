"""
Phase 3B: QBO Accounting Sync Wiring (Lifecycle-Governed)

Fetches accounting objects from QuickBooks Online and canonicalizes them
into canonical_facts using the certified QBOAccountingCanonicalizer.

Per PHASE3B-ACCOUNTING_CANONICAL_FACTS_CONTRACT.md:
- FACT -> INTENT -> ACTION ordering is binding
- Phase 3B = Accounting-System Canonical Facts (this module)
- This module writes ONLY to canonical_facts
- This module reads ONLY from QBO (never processor canonical facts)

GOVERNANCE REQUIREMENTS (SYNC_RUN_LIFECYCLE_CONTRACT):
- Every sync run acquires a sync_run_id via SyncRunManager
- Single-sync-per-workspace locking is respected
- All logs include sync_run_id and workspace_id
- Sync runs cleanly mark completion or failure

HARD CONSTRAINTS (NON-NEGOTIABLE):
This module MUST NOT:
- Read processor canonical facts
- Perform matching
- Infer settlement groupings
- Compute fees
- Write to matching tables
- Write to QBO
- Update canonical facts (append-only)
- Introduce workflow state into canonical_facts
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from module3_canonicalization.qbo_accounting_canonicalizer import (
    QBOAccountingCanonicalizer,
)
from shared.config import get_config, CharlesConfig
from shared.structured_logging import get_logger, EventType
from shared.sync_lifecycle import (
    SyncRunManager,
    SyncState,
    SyncRunStats,
    get_sync_manager,
)


# =============================================================================
# QBO QUERY LAYER (Read-Only, Paginated)
# =============================================================================

# QBO Query API maximum page size
_QBO_MAX_PAGE_SIZE = 1000


def _get_qbo_base_url(config: CharlesConfig) -> str:
    """
    Get the QBO API base URL from validated configuration.

    Uses CharlesConfig per CONFIGURATION_MANAGEMENT_CONTRACT.
    """
    if config.qbo_environment == 'sandbox':
        return 'https://sandbox-quickbooks.api.intuit.com'
    return 'https://quickbooks.api.intuit.com'


def _query_qbo_page(
    qbo_client: Any,
    config: CharlesConfig,
    entity_type: str,
    since_updated_at: Optional[datetime] = None,
    start_position: int = 1,
    max_results: int = _QBO_MAX_PAGE_SIZE,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Fetch a single page of QBO objects via the QBO Query API.

    Uses direct HTTP requests (same pattern as qbo_adapter.py).

    Args:
        qbo_client: Authenticated QuickBooks client (python-quickbooks)
        config: Validated CharlesConfig instance
        entity_type: QBO entity name ('Payment', 'RefundReceipt', etc.)
        since_updated_at: Optional filter for incremental sync
        start_position: 1-based offset for pagination
        max_results: Page size (max 1000 per QBO API)

    Returns:
        Tuple of (list of QBO objects, count returned on this page)
    """
    base_url = _get_qbo_base_url(config)
    realm_id = qbo_client.company_id

    # Build QBO query with pagination
    where_clause = ""
    if since_updated_at:
        ts = since_updated_at.strftime('%Y-%m-%dT%H:%M:%S%z')
        where_clause = f" WHERE MetaData.LastUpdatedTime >= '{ts}'"

    query = (
        f"SELECT * FROM {entity_type}"
        f"{where_clause}"
        f" STARTPOSITION {start_position}"
        f" MAXRESULTS {max_results}"
    )

    url = f'{base_url}/v3/company/{realm_id}/query'
    headers = {
        'Authorization': f'Bearer {qbo_client.auth_client.access_token}',
        'Accept': 'application/json',
        'Content-Type': 'application/text',
    }
    params = {'query': query, 'minorversion': '65'}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    query_response = data.get('QueryResponse', {})
    objects = query_response.get(entity_type, [])
    return objects, len(objects)


def _fetch_qbo_account_map(
    qbo_client: Any,
    config: Optional['CharlesConfig'] = None,
) -> Dict[str, str]:
    """
    Fetch all QBO accounts and return a map of account ID -> account name.

    Called once per sync to resolve account IDs that QBO returns without names
    (e.g. DepositToAccountRef on Payments only includes the ID, not the name).
    """
    if config is None:
        config = get_config()

    accounts = _query_qbo_objects(qbo_client, 'Account', config=config)
    return {str(a.get('Id', '')): a.get('Name', '') for a in accounts if a.get('Id')}


def _enrich_deposit_account_names(
    objects: List[Dict[str, Any]],
    account_map: Dict[str, str],
) -> None:
    """
    Inject account name into DepositToAccountRef where QBO omitted it.

    Mutates objects in place. Only fills in 'name' if it's missing.
    """
    for obj in objects:
        ref = obj.get('DepositToAccountRef')
        if isinstance(ref, dict) and ref.get('value') and not ref.get('name'):
            name = account_map.get(str(ref['value']))
            if name:
                ref['name'] = name


def _query_qbo_objects(
    qbo_client: Any,
    entity_type: str,
    since_updated_at: Optional[datetime] = None,
    config: Optional[CharlesConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch ALL QBO objects of a given entity type with full pagination.

    Iterates through pages until exhaustion to prevent silent data truncation.
    Per governance requirements: silent truncation is a CRITICAL FAILURE.

    Args:
        qbo_client: Authenticated QuickBooks client (python-quickbooks)
        entity_type: QBO entity name ('Payment', 'RefundReceipt', 'CreditMemo', 'Deposit')
        since_updated_at: Optional filter for incremental sync
        config: CharlesConfig instance (loaded from get_config() if not provided)

    Returns:
        Complete list of ALL QBO objects as dictionaries
    """
    if config is None:
        config = get_config()

    all_objects: List[Dict[str, Any]] = []
    start_position = 1

    while True:
        page, page_count = _query_qbo_page(
            qbo_client, config, entity_type, since_updated_at,
            start_position=start_position,
            max_results=_QBO_MAX_PAGE_SIZE,
        )
        all_objects.extend(page)

        if page_count < _QBO_MAX_PAGE_SIZE:
            break

        start_position += _QBO_MAX_PAGE_SIZE

    return all_objects


def _fetch_qbo_object_by_id(
    qbo_client: Any,
    entity_type: str,
    qbo_id: str,
    config: Optional[CharlesConfig] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch a single QBO object by entity type and ID.

    Uses the QBO Query API with a WHERE Id clause (same HTTP pattern as
    _query_qbo_page). Returns the object dict, or None if not found.

    Args:
        qbo_client: Authenticated QuickBooks client (python-quickbooks)
        entity_type: QBO entity name ('RefundReceipt', 'CreditMemo', etc.)
        qbo_id: QBO object ID
        config: CharlesConfig instance (loaded from get_config() if not provided)
    """
    if config is None:
        config = get_config()

    base_url = _get_qbo_base_url(config)
    realm_id = qbo_client.company_id

    query = f"SELECT * FROM {entity_type} WHERE Id = '{qbo_id}'"
    url = f'{base_url}/v3/company/{realm_id}/query'
    headers = {
        'Authorization': f'Bearer {qbo_client.auth_client.access_token}',
        'Accept': 'application/json',
        'Content-Type': 'application/text',
    }
    params = {'query': query, 'minorversion': '65'}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    objects = data.get('QueryResponse', {}).get(entity_type, [])
    return objects[0] if objects else None


def _backfill_deposit_referenced_refunds(
    cursor: sqlite3.Cursor,
    qbo_client: Any,
    canonicalizer: 'QBOAccountingCanonicalizer',
    account_map: Dict[str, str],
    workspace_id: str,
    observed_at: datetime,
    config: Optional[CharlesConfig] = None,
) -> int:
    """
    Scan ALL deposit_observed facts for RefundReceipt/CreditMemo line items
    that don't have standalone refund_observed facts. Fetch from QBO and
    canonicalize them.

    This is self-healing: it fixes gaps from prior syncs where a deposit
    referenced a refund that fell outside the sync window, and prevents
    future gaps for any newly canonicalized deposits.

    Idempotent — uses deterministic canonical ID checks before fetching.

    Returns:
        Count of newly backfilled refund facts.
    """
    logger = logging.getLogger(__name__)

    # Get all deposit_observed facts for this workspace
    cursor.execute("""
        SELECT metadata FROM canonical_facts
        WHERE workspace_id = ? AND event_type = 'deposit_observed'
    """, (workspace_id,))
    deposit_rows = cursor.fetchall()

    backfill_types = ('RefundReceipt', 'CreditMemo')
    backfilled = 0

    for row in deposit_rows:
        try:
            metadata = json.loads(row['metadata']) if row['metadata'] else {}
        except (json.JSONDecodeError, TypeError):
            continue

        for item in metadata.get('deposit_line_items', []):
            txn_type = item.get('txn_type', '')
            txn_id = item.get('txn_id')
            if txn_type not in backfill_types or not txn_id:
                continue

            # Check if standalone fact already exists
            canonical_id = canonicalizer._generate_canonical_id(txn_type, str(txn_id))
            cursor.execute("SELECT id FROM canonical_facts WHERE id = ?", (canonical_id,))
            if cursor.fetchone():
                continue

            # Fetch full object from QBO
            logger.info(
                f"[BACKFILL] Fetching missing {txn_type} {txn_id} referenced by deposit"
            )
            try:
                qbo_obj = _fetch_qbo_object_by_id(
                    qbo_client, txn_type, str(txn_id), config=config
                )
            except Exception as e:
                logger.warning(
                    f"[BACKFILL] Failed to fetch {txn_type} {txn_id} from QBO: {e}"
                )
                continue

            if not qbo_obj:
                logger.warning(
                    f"[BACKFILL] {txn_type} {txn_id} not found in QBO"
                )
                continue

            # Enrich account names and canonicalize
            _enrich_deposit_account_names([qbo_obj], account_map)
            result = canonicalizer.canonicalize_refund_observed(
                cursor, qbo_obj, txn_type, observed_at
            )
            if not result.was_duplicate:
                backfilled += 1
                logger.info(
                    f"[BACKFILL] Created refund_observed fact for {txn_type} {txn_id}"
                )

    return backfilled


# =============================================================================
# SYNC ENTRY POINT (Lifecycle-Governed)
# =============================================================================

def run_qbo_accounting_sync(
    workspace_id: str,
    qbo_client: Any,
    db_conn: sqlite3.Connection,
    since_updated_at: Optional[datetime] = None,
    initiation_source: str = "manual",
) -> Dict[str, Any]:
    """
    Run a lifecycle-governed Phase 3B QBO accounting sync.

    Fetches accounting objects from QBO, canonicalizes each one, and commits
    canonical facts in a single DB transaction. Idempotent — re-running
    creates no duplicate facts.

    Every sync run:
    - Acquires a sync_run_id via SyncRunManager
    - Respects single-sync-per-workspace locking
    - Logs with sync_run_id and workspace_id
    - Cleanly marks completion or failure

    Args:
        workspace_id: Workspace identifier for data isolation
        qbo_client: Authenticated QuickBooks client (from module2_oauth)
        db_conn: SQLite connection (transaction managed here)
        since_updated_at: Optional filter for incremental sync
        initiation_source: How sync was initiated (manual, scheduled, api)

    Returns:
        Dict with sync_run_id and counts by object type:
            sync_run_id, sync_state,
            payments_fetched, payments_created, payments_skipped,
            refund_receipts_fetched, refund_receipts_created, refund_receipts_skipped,
            credit_memos_fetched, credit_memos_created, credit_memos_skipped,
            deposits_fetched, deposits_created, deposits_skipped

    Raises:
        RuntimeError: If another sync is already in progress
        Exception: Any error during fetch or canonicalization aborts the
            transaction, marks the sync as failed, and propagates.
    """
    config = get_config()
    company_id = qbo_client.company_id

    # --- Lifecycle: Acquire sync_run_id and lock ---
    sync_manager = get_sync_manager(db_conn, company_id)
    sync_run = sync_manager.start_sync(initiation_source=initiation_source)
    sync_run_id = sync_run.sync_run_id

    # --- Governed logging ---
    slog = get_logger(
        service="qbo_accounting_sync",
        workspace_id=workspace_id,
        company_id=company_id,
        sync_run_id=sync_run_id,
    )

    observed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    slog.info(
        EventType.SYNC_STARTED,
        f"Phase 3B accounting sync started: "
        f"workspace_id={workspace_id} "
        f"observed_at={observed_at.isoformat()} "
        f"since_updated_at={since_updated_at.isoformat() if since_updated_at else 'full'}",
        initiation_source=initiation_source,
    )

    counts = {
        'sync_run_id': sync_run_id,
        'sync_state': None,
        'payments_fetched': 0,
        'payments_created': 0,
        'payments_skipped': 0,
        'refund_receipts_fetched': 0,
        'refund_receipts_created': 0,
        'refund_receipts_skipped': 0,
        'credit_memos_fetched': 0,
        'credit_memos_created': 0,
        'credit_memos_skipped': 0,
        'deposits_fetched': 0,
        'deposits_created': 0,
        'deposits_skipped': 0,
        'refund_receipts_backfilled': 0,
    }

    try:
        # --- Lifecycle: Transition to DISCOVERING ---
        sync_manager.transition_to(sync_run_id, SyncState.DISCOVERING)

        canonicalizer = QBOAccountingCanonicalizer(workspace_id)
        cursor = db_conn.cursor()

        # Fetch QBO account map once to resolve account IDs to names
        account_map = _fetch_qbo_account_map(qbo_client, config=config)

        # --- Payments ---
        payments = _query_qbo_objects(
            qbo_client, 'Payment', since_updated_at, config=config,
        )
        _enrich_deposit_account_names(payments, account_map)
        counts['payments_fetched'] = len(payments)
        slog.info(
            EventType.SYNC_DISCOVERY_COMPLETED,
            f"Fetched {len(payments)} Payment(s)",
            entity_type="Payment",
            count=len(payments),
        )

        for payment in payments:
            result = canonicalizer.canonicalize_payment_observed(
                cursor, payment, observed_at
            )
            if result.was_duplicate:
                counts['payments_skipped'] += 1
            else:
                counts['payments_created'] += 1

        # --- RefundReceipts ---
        refund_receipts = _query_qbo_objects(
            qbo_client, 'RefundReceipt', since_updated_at, config=config,
        )
        _enrich_deposit_account_names(refund_receipts, account_map)
        counts['refund_receipts_fetched'] = len(refund_receipts)
        slog.info(
            EventType.SYNC_DISCOVERY_COMPLETED,
            f"Fetched {len(refund_receipts)} RefundReceipt(s)",
            entity_type="RefundReceipt",
            count=len(refund_receipts),
        )

        for refund in refund_receipts:
            result = canonicalizer.canonicalize_refund_observed(
                cursor, refund, 'RefundReceipt', observed_at
            )
            if result.was_duplicate:
                counts['refund_receipts_skipped'] += 1
            else:
                counts['refund_receipts_created'] += 1

        # --- CreditMemos ---
        credit_memos = _query_qbo_objects(
            qbo_client, 'CreditMemo', since_updated_at, config=config,
        )
        _enrich_deposit_account_names(credit_memos, account_map)
        counts['credit_memos_fetched'] = len(credit_memos)
        slog.info(
            EventType.SYNC_DISCOVERY_COMPLETED,
            f"Fetched {len(credit_memos)} CreditMemo(s)",
            entity_type="CreditMemo",
            count=len(credit_memos),
        )

        for memo in credit_memos:
            result = canonicalizer.canonicalize_refund_observed(
                cursor, memo, 'CreditMemo', observed_at
            )
            if result.was_duplicate:
                counts['credit_memos_skipped'] += 1
            else:
                counts['credit_memos_created'] += 1

        # --- Deposits ---
        deposits = _query_qbo_objects(
            qbo_client, 'Deposit', since_updated_at, config=config,
        )
        _enrich_deposit_account_names(deposits, account_map)
        counts['deposits_fetched'] = len(deposits)
        slog.info(
            EventType.SYNC_DISCOVERY_COMPLETED,
            f"Fetched {len(deposits)} Deposit(s)",
            entity_type="Deposit",
            count=len(deposits),
        )

        for deposit in deposits:
            result = canonicalizer.canonicalize_deposit_observed(
                cursor, deposit, observed_at
            )
            if result.was_duplicate:
                counts['deposits_skipped'] += 1
            else:
                counts['deposits_created'] += 1

        # --- Backfill missing refunds/credit memos referenced in deposits ---
        backfilled = _backfill_deposit_referenced_refunds(
            cursor, qbo_client, canonicalizer, account_map,
            workspace_id, observed_at, config,
        )
        counts['refund_receipts_backfilled'] = backfilled
        if backfilled:
            slog.info(
                EventType.SYNC_DISCOVERY_COMPLETED,
                f"Backfilled {backfilled} refund(s) referenced by deposits",
                entity_type="RefundBackfill",
                count=backfilled,
            )

        # Commit all canonical facts atomically
        db_conn.commit()

        total_created = (
            counts['payments_created']
            + counts['refund_receipts_created']
            + counts['credit_memos_created']
            + counts['deposits_created']
        )
        total_skipped = (
            counts['payments_skipped']
            + counts['refund_receipts_skipped']
            + counts['credit_memos_skipped']
            + counts['deposits_skipped']
        )
        total_fetched = (
            counts['payments_fetched']
            + counts['refund_receipts_fetched']
            + counts['credit_memos_fetched']
            + counts['deposits_fetched']
        )

        # --- Lifecycle: Complete sync (DISCOVERING → COMPLETED) ---
        stats = SyncRunStats(
            payouts_total=total_fetched,
            payouts_processed=total_created,
            payouts_skipped=total_skipped,
        )
        completed_run = sync_manager.complete_sync(sync_run_id, stats)
        counts['sync_state'] = completed_run.state.value

        slog.info(
            EventType.SYNC_COMPLETED,
            f"Phase 3B accounting sync complete: "
            f"workspace_id={workspace_id} "
            f"created={total_created} skipped={total_skipped} "
            f"payments={counts['payments_fetched']} "
            f"refund_receipts={counts['refund_receipts_fetched']} "
            f"credit_memos={counts['credit_memos_fetched']} "
            f"deposits={counts['deposits_fetched']}",
            total_fetched=total_fetched,
            total_created=total_created,
            total_skipped=total_skipped,
        )

        return counts

    except Exception as e:
        db_conn.rollback()

        # --- Lifecycle: Mark sync as failed ---
        try:
            sync_manager.fail_sync(sync_run_id, str(e))
            counts['sync_state'] = SyncState.FAILED.value
        except Exception:
            pass  # Don't mask the original error

        slog.error(
            EventType.SYNC_FAILED,
            f"Phase 3B accounting sync FAILED: workspace_id={workspace_id} error={e}",
            failure_reason=str(e),
        )

        raise
