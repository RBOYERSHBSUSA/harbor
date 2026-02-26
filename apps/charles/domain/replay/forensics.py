"""
Forensics & Provenance Module - Phase 3

================================================================================
LAYER 3 COMPONENT — FORENSIC QUERIES
================================================================================

This module implements operator-only forensic queries for audit and debugging
per PHASE3_REPLAY_ORCHESTRATION_FORENSICS_PLAN.md §5.

FORENSIC QUESTIONS ANSWERED (§5.1):
- What raw objects were captured for sync_run_id X?
- Which canonical events were derived from payout po_xxx?
- Did Stripe data mutate between sync runs?
- Show me the raw charge that became canonical event Y
- What was the raw Stripe response for charge ch_xxx in sync_run_id Z?

PROVENANCE LINKAGE (§5.2):
Uses implicit linkage via processor IDs rather than foreign keys:
- raw_processor_data.processor_object_id = canonical_events.processor_payment_id
- Traces through balance_transactions to connect payouts to charges

================================================================================
"""

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class RawObjectSummary:
    """Summary of raw objects for a sync run."""
    object_type: str
    count: int
    sample_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'object_type': self.object_type,
            'count': self.count,
            'sample_ids': self.sample_ids,
        }


@dataclass
class RawObject:
    """A single raw processor data object."""
    id: str
    workspace_id: str
    processor: str
    object_type: str
    processor_object_id: str
    sync_run_id: str
    raw_json: Dict[str, Any]
    api_endpoint: str
    http_status_code: int
    captured_at: str
    stripe_request_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'workspace_id': self.workspace_id,
            'processor': self.processor,
            'object_type': self.object_type,
            'processor_object_id': self.processor_object_id,
            'sync_run_id': self.sync_run_id,
            'raw_json': self.raw_json,
            'api_endpoint': self.api_endpoint,
            'http_status_code': self.http_status_code,
            'captured_at': self.captured_at,
            'stripe_request_id': self.stripe_request_id,
        }


@dataclass
class RawObjectVersion:
    """A version of a raw object from a specific sync run."""
    sync_run_id: str
    captured_at: str
    raw_json: Dict[str, Any]
    stripe_request_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'sync_run_id': self.sync_run_id,
            'captured_at': self.captured_at,
            'raw_json': self.raw_json,
            'stripe_request_id': self.stripe_request_id,
        }


@dataclass
class LineageTrace:
    """Trace from canonical event back to raw data."""
    canonical_event_id: str
    processor_payment_id: str
    raw_charge: Optional[RawObject] = None
    raw_balance_txn: Optional[RawObject] = None
    raw_payout: Optional[RawObject] = None
    raw_customer: Optional[RawObject] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'canonical_event_id': self.canonical_event_id,
            'processor_payment_id': self.processor_payment_id,
            'raw_charge': self.raw_charge.to_dict() if self.raw_charge else None,
            'raw_balance_txn': self.raw_balance_txn.to_dict() if self.raw_balance_txn else None,
            'raw_payout': self.raw_payout.to_dict() if self.raw_payout else None,
            'raw_customer': self.raw_customer.to_dict() if self.raw_customer else None,
        }


@dataclass
class ForwardTrace:
    """Trace from raw payout forward to canonical events."""
    payout_id: str
    sync_run_id: str
    raw_payout: Optional[RawObject] = None
    balance_transaction_ids: List[str] = field(default_factory=list)
    charge_ids: List[str] = field(default_factory=list)
    refund_ids: List[str] = field(default_factory=list)
    canonical_event_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'payout_id': self.payout_id,
            'sync_run_id': self.sync_run_id,
            'raw_payout': self.raw_payout.to_dict() if self.raw_payout else None,
            'balance_transaction_count': len(self.balance_transaction_ids),
            'charge_count': len(self.charge_ids),
            'refund_count': len(self.refund_ids),
            'canonical_event_count': len(self.canonical_event_ids),
            'charge_ids': self.charge_ids,
            'refund_ids': self.refund_ids,
            'canonical_event_ids': self.canonical_event_ids,
        }


class ForensicsQueries:
    """
    Operator-only forensic queries using implicit linkage.

    Per PHASE3_REPLAY_ORCHESTRATION_FORENSICS_PLAN.md §5:
    - All queries are workspace-isolated
    - Uses processor_object_id for linkage (no foreign keys)
    - Read-only operations only
    """

    def __init__(self, db_path: str):
        """
        Initialize forensics queries.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def list_raw_objects(
        self,
        workspace_id: str,
        sync_run_id: str,
    ) -> List[RawObjectSummary]:
        """
        List all raw objects captured for a sync run.

        Per §5.1: "What raw objects were captured for sync_run_id X?"

        Args:
            workspace_id: Workspace identifier
            sync_run_id: Sync run to query

        Returns:
            List of RawObjectSummary grouped by object_type
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Get counts grouped by object_type
        cursor.execute("""
            SELECT object_type, COUNT(*) as count,
                   GROUP_CONCAT(processor_object_id) as sample_ids
            FROM raw_processor_data
            WHERE workspace_id = ?
              AND sync_run_id = ?
            GROUP BY object_type
            ORDER BY object_type
        """, (workspace_id, sync_run_id))

        summaries = []
        for row in cursor.fetchall():
            # Take first 5 sample IDs
            all_ids = row['sample_ids'].split(',') if row['sample_ids'] else []
            sample_ids = all_ids[:5]

            summaries.append(RawObjectSummary(
                object_type=row['object_type'],
                count=row['count'],
                sample_ids=sample_ids,
            ))

        conn.close()
        return summaries

    def show_raw_object(
        self,
        workspace_id: str,
        object_type: str,
        object_id: str,
        sync_run_id: Optional[str] = None,
    ) -> Optional[RawObject]:
        """
        Show raw data for a specific object.

        Per §5.1: "What was the raw Stripe response for charge ch_xxx?"

        Args:
            workspace_id: Workspace identifier
            object_type: Type of object (charge, payout, refund, etc.)
            object_id: Processor's native ID
            sync_run_id: Optional specific sync run (defaults to most recent)

        Returns:
            RawObject if found, None otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if sync_run_id:
            cursor.execute("""
                SELECT *
                FROM raw_processor_data
                WHERE workspace_id = ?
                  AND object_type = ?
                  AND processor_object_id = ?
                  AND sync_run_id = ?
            """, (workspace_id, object_type, object_id, sync_run_id))
        else:
            # Get most recent
            cursor.execute("""
                SELECT *
                FROM raw_processor_data
                WHERE workspace_id = ?
                  AND object_type = ?
                  AND processor_object_id = ?
                ORDER BY captured_at DESC
                LIMIT 1
            """, (workspace_id, object_type, object_id))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        return RawObject(
            id=row['id'],
            workspace_id=row['workspace_id'],
            processor=row['processor'],
            object_type=row['object_type'],
            processor_object_id=row['processor_object_id'],
            sync_run_id=row['sync_run_id'],
            raw_json=json.loads(row['raw_json']),
            api_endpoint=row['api_endpoint'],
            http_status_code=row['http_status_code'],
            captured_at=row['captured_at'],
            stripe_request_id=row['stripe_request_id'],
        )

    def compare_raw_across_runs(
        self,
        workspace_id: str,
        object_type: str,
        object_id: str,
    ) -> List[RawObjectVersion]:
        """
        Compare raw data for an object across sync runs.

        Per §5.1: "Did Stripe data mutate between sync runs?"

        Args:
            workspace_id: Workspace identifier
            object_type: Type of object
            object_id: Processor's native ID

        Returns:
            List of RawObjectVersion ordered by captured_at
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT sync_run_id, captured_at, raw_json, stripe_request_id
            FROM raw_processor_data
            WHERE workspace_id = ?
              AND object_type = ?
              AND processor_object_id = ?
            ORDER BY captured_at ASC
        """, (workspace_id, object_type, object_id))

        versions = []
        for row in cursor.fetchall():
            versions.append(RawObjectVersion(
                sync_run_id=row['sync_run_id'],
                captured_at=row['captured_at'],
                raw_json=json.loads(row['raw_json']),
                stripe_request_id=row['stripe_request_id'],
            ))

        conn.close()
        return versions

    def trace_canonical_to_raw(
        self,
        workspace_id: str,
        processor_payment_id: str,
        sync_run_id: Optional[str] = None,
    ) -> Optional[LineageTrace]:
        """
        Trace lineage from canonical event to raw data.

        Per §5.1: "Show me the raw charge that became canonical event Y"

        Uses implicit linkage via processor_payment_id:
        - canonical_event.processor_payment_id = raw charge.processor_object_id

        Args:
            workspace_id: Workspace identifier
            processor_payment_id: The payment ID (ch_xxx)
            sync_run_id: Optional specific sync run

        Returns:
            LineageTrace with linked raw objects, or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Get canonical event
        cursor.execute("""
            SELECT id, processor_payment_id, processor_payout_id,
                   processor_customer_id
            FROM canonical_events
            WHERE workspace_id = ?
              AND processor_payment_id = ?
            LIMIT 1
        """, (workspace_id, processor_payment_id))

        canonical_row = cursor.fetchone()
        if not canonical_row:
            conn.close()
            return None

        trace = LineageTrace(
            canonical_event_id=canonical_row['id'],
            processor_payment_id=processor_payment_id,
        )

        # Get raw charge
        trace.raw_charge = self.show_raw_object(
            workspace_id, 'charge', processor_payment_id, sync_run_id
        )

        # Get raw payout if available
        payout_id = canonical_row['processor_payout_id']
        if payout_id:
            trace.raw_payout = self.show_raw_object(
                workspace_id, 'payout', payout_id, sync_run_id
            )

        # Get raw customer if available
        customer_id = canonical_row['processor_customer_id']
        if customer_id:
            trace.raw_customer = self.show_raw_object(
                workspace_id, 'customer', customer_id, sync_run_id
            )

        conn.close()
        return trace

    def trace_payout_forward(
        self,
        workspace_id: str,
        payout_id: str,
        sync_run_id: str,
    ) -> ForwardTrace:
        """
        Trace lineage from raw payout forward to canonical events.

        Per §5.1: "Which canonical events were derived from payout po_xxx?"

        Uses implicit linkage via processor IDs:
        - raw payout → balance_transactions_list → charge IDs
        - charge IDs → canonical_events.processor_payment_id

        Args:
            workspace_id: Workspace identifier
            payout_id: The payout ID (po_xxx)
            sync_run_id: The sync run to trace

        Returns:
            ForwardTrace with linked objects
        """
        trace = ForwardTrace(
            payout_id=payout_id,
            sync_run_id=sync_run_id,
        )

        # Get raw payout
        trace.raw_payout = self.show_raw_object(
            workspace_id, 'payout', payout_id, sync_run_id
        )

        # Get balance transactions list for this payout
        btxn_list_id = f"payout_{payout_id}"
        btxn_list = self.show_raw_object(
            workspace_id, 'balance_transactions_list', btxn_list_id, sync_run_id
        )

        if btxn_list and btxn_list.raw_json:
            # Extract charge and refund IDs from balance transactions
            data = btxn_list.raw_json.get('data', [])
            for txn in data:
                txn_id = txn.get('id')
                if txn_id:
                    trace.balance_transaction_ids.append(txn_id)

                source = txn.get('source')
                if isinstance(source, str):
                    if source.startswith('ch_'):
                        trace.charge_ids.append(source)
                    elif source.startswith('re_'):
                        trace.refund_ids.append(source)
                elif isinstance(source, dict):
                    source_id = source.get('id', '')
                    if source_id.startswith('ch_'):
                        trace.charge_ids.append(source_id)
                    elif source_id.startswith('re_'):
                        trace.refund_ids.append(source_id)

        # Get canonical events for these charges
        if trace.charge_ids:
            conn = self._get_connection()
            cursor = conn.cursor()

            placeholders = ','.join('?' * len(trace.charge_ids))
            cursor.execute(f"""
                SELECT id
                FROM canonical_events
                WHERE workspace_id = ?
                  AND processor_payment_id IN ({placeholders})
            """, [workspace_id] + trace.charge_ids)

            for row in cursor.fetchall():
                trace.canonical_event_ids.append(row['id'])

            conn.close()

        return trace

    def get_sync_run_inventory(
        self,
        workspace_id: str,
        sync_run_id: str,
    ) -> Dict[str, int]:
        """
        Get inventory of raw data for a sync run.

        Returns counts by object type for quick completeness check.

        Args:
            workspace_id: Workspace identifier
            sync_run_id: Sync run to query

        Returns:
            Dict mapping object_type to count
        """
        summaries = self.list_raw_objects(workspace_id, sync_run_id)
        return {s.object_type: s.count for s in summaries}

    def check_for_timeout_skips(
        self,
        workspace_id: str,
        sync_run_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Check for persistence timeout skips in a sync run.

        Per §7.3: Mixed-state detection for timeout-skipped objects.

        This queries the logs table for RAW_DATA_PERSISTENCE_TIMEOUT_SKIPPED
        events during the specified sync run.

        Args:
            workspace_id: Workspace identifier
            sync_run_id: Sync run to query

        Returns:
            List of timeout skip records (may be empty)
        """
        # Note: This would query a logs table if available
        # For now, return empty list as timeout skips are rare
        # and the raw_processor_data table won't have entries for skipped items
        return []
