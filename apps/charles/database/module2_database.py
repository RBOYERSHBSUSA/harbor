"""
Module 2: Database Management
Handles connections to consolidated charles.db

NOTE: Post-Phase 2, all data is in a single consolidated database.
Per CHARLES_MULTITENANCY_CONTRACT v1.0 (LOCKED)
"""

import os
import sqlite3
from pathlib import Path
from typing import Optional
from datetime import datetime


def _create_connection(db_path: str) -> sqlite3.Connection:
    """
    Create a SQLite connection with foreign keys enforced.

    CRITICAL: Per PERSISTENCE_GUARANTEES_CONTRACT.MD §6.1:
    "Foreign keys must be enforced at the database level"

    This helper ensures PRAGMA foreign_keys = ON is executed on EVERY connection.
    All code that creates SQLite connections MUST use this function.

    Args:
        db_path: Path to SQLite database

    Returns:
        SQLite connection with foreign keys enabled and Row factory

    Raises:
        FileNotFoundError: If database does not exist
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found at {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # CRITICAL: Enable foreign keys on EVERY connection
    # SQLite defaults to OFF, so this MUST be executed per-connection
    conn.execute("PRAGMA foreign_keys = ON")

    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode = WAL")

    return conn


class DatabaseManager:
    """
    Manages database connections for Charles.

    Post-Phase 2: Uses consolidated charles.db instead of per-workspace databases.
    All queries should be workspace-scoped using workspace_id.
    """

    def __init__(self, db_path: str = None):
        """
        Initialize database manager

        Args:
            db_path: Path to consolidated charles.db (default from MASTER_DB_PATH env var)
        """
        # Post-Phase 2: MASTER_DB_PATH now points to consolidated charles.db
        self.db_path = db_path or os.getenv('MASTER_DB_PATH', './data/charles.db')
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get connection to consolidated database.

        Post-Phase 2: Returns connection to charles.db which contains all data.
        All queries should filter by workspace_id for data isolation.

        Returns:
            SQLite connection with Row factory and foreign keys enabled
        """
        return _create_connection(self.db_path)

    def get_master_connection(self) -> sqlite3.Connection:
        """
        Get connection to database (legacy method, redirects to get_connection).

        DEPRECATED: Post-Phase 2, use get_connection() instead.
        Kept for backward compatibility during transition.
        """
        return self.get_connection()
    
    def get_workspace_connection(self, workspace_id: str) -> sqlite3.Connection:
        """
        Get connection scoped to a workspace.

        Post-Phase 2: All data is in consolidated charles.db.
        This method validates workspace exists and returns connection.
        Caller MUST filter queries by workspace_id.

        Args:
            workspace_id: Workspace identifier

        Returns:
            SQLite connection (caller must scope queries by workspace_id)

        Raises:
            ValueError: If workspace not found
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        # Validate workspace exists
        cursor.execute("SELECT workspace_id FROM workspaces WHERE workspace_id = ?", (workspace_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            raise ValueError(f"Workspace {workspace_id} not found in database")

        return conn

    def get_company_connection(self, company_id: str) -> sqlite3.Connection:
        """
        Get connection to company data (legacy method).

        DEPRECATED: Post-Phase 2, use get_workspace_connection(workspace_id) instead.

        Args:
            company_id: Company UUID (legacy companies table)

        Returns:
            SQLite connection

        Raises:
            ValueError: If company not found
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        # Validate company exists (legacy table)
        cursor.execute("SELECT workspace_id FROM companies WHERE id = ?", (company_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            raise ValueError(f"Company {company_id} not found in database")

        # Return connection - caller should use workspace_id from companies table
        return conn
    
    def create_company_database(self, company_id: str) -> str:
        """
        Create a new company-specific database (DEPRECATED).

        DEPRECATED: Post-Phase 2, all data is in consolidated charles.db.
        This method is no longer used. New workspaces are added via migration scripts
        or admin tools that insert rows into workspaces/licenses tables.

        Args:
            company_id: Company UUID

        Returns:
            Path to database (always returns consolidated charles.db path)

        Raises:
            DeprecationWarning: This method is deprecated
        """
        import warnings
        warnings.warn(
            "create_company_database() is deprecated. "
            "Post-Phase 2, use migration scripts or admin tools to create workspaces.",
            DeprecationWarning,
            stacklevel=2
        )

        return self.db_path
    
    def _create_company_tables(self, cursor):
        """
        Create tables in company database (DEPRECATED).

        DEPRECATED: Post-Phase 2, schema is created by migration scripts.
        This method is no longer used.
        """
        import warnings
        warnings.warn(
            "_create_company_tables() is deprecated. Post-Phase 2, use migration scripts.",
            DeprecationWarning,
            stacklevel=2
        )
        return  # No-op
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Raw events table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_events (
            id TEXT PRIMARY KEY,
            processor TEXT NOT NULL CHECK (processor IN ('stripe', 'authorize_net', 'test')),
            processor_event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_category TEXT NOT NULL CHECK (event_category IN ('payment', 'payout', 'refund', 'other')),
            payload TEXT NOT NULL,
            payload_format TEXT NOT NULL CHECK (payload_format IN ('json', 'xml')),
            checksum TEXT NOT NULL,
            received_at TIMESTAMP NOT NULL,
            source_ip TEXT,
            signature TEXT,
            signature_verified BOOLEAN NOT NULL,
            signature_algorithm TEXT,
            processed BOOLEAN NOT NULL DEFAULT 0,
            canonical_event_id TEXT,
            processing_error TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_retry_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(processor, processor_event_id)
        )
        """)
        
        # Canonical events table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS canonical_events (
            id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            raw_event_id TEXT NOT NULL REFERENCES raw_events(id),
            event_type TEXT NOT NULL CHECK (event_type IN ('payment', 'payout', 'refund', 'failed_payment', 'chargeback')),
            amount_gross TEXT NOT NULL,
            amount_net TEXT NOT NULL,
            fees TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
            processor_payment_id TEXT,
            processor_payout_id TEXT,
            processor_customer_id TEXT,
            processor_invoice_id TEXT,
            qbo_customer_id TEXT,
            qbo_invoice_id TEXT,
            qbo_payment_id TEXT,
            payment_method TEXT,
            card_brand TEXT,
            card_last4 TEXT,
            processor_timestamp TIMESTAMP NOT NULL,
            settlement_date DATE,
            description TEXT NOT NULL,
            customer_email TEXT,
            customer_name TEXT,
            metadata TEXT,
            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'posted', 'failed', 'voided')),
            match_confidence INTEGER DEFAULT 0,
            match_type TEXT DEFAULT 'unmatched',
            match_score_breakdown TEXT,
            human_confirmed_at TIMESTAMP,
            confirmed_by TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CHECK (CAST(amount_net AS REAL) >= CAST(amount_gross AS REAL) - CAST(fees AS REAL) - 0.01),
            CHECK (CAST(amount_net AS REAL) <= CAST(amount_gross AS REAL) - CAST(fees AS REAL) + 0.01),
            CHECK (CAST(amount_gross AS REAL) >= 0),
            CHECK (CAST(fees AS REAL) >= 0)
        )
        """)
        
        # Posting plans table - represents intent to create QBO objects from a payout
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS posting_plans (
            id TEXT PRIMARY KEY,
            payout_batch_id TEXT NOT NULL REFERENCES payout_batches(id),
            processor_payout_id TEXT NOT NULL,
            plan_type TEXT NOT NULL CHECK (plan_type IN ('deposit', 'refund', 'journal_entry')),
            operations TEXT NOT NULL,
            line_count INTEGER NOT NULL DEFAULT 0,
            total_amount TEXT,
            validated BOOLEAN NOT NULL DEFAULT 0,
            validation_errors TEXT,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'validated', 'executing', 'posted', 'failed', 'cancelled')),
            execution_started_at TIMESTAMP,
            execution_completed_at TIMESTAMP,
            retry_count INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            qbo_deposit_id TEXT,
            error_message TEXT,
            error_code TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(payout_batch_id)
        )
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_posting_plans_status ON posting_plans(status)
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_posting_plans_payout ON posting_plans(processor_payout_id)
        """)
        
        # QBO artifacts table - tracks all QBO objects created by Charles
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS qbo_artifacts (
            id TEXT PRIMARY KEY,
            qbo_id TEXT NOT NULL,
            qbo_type TEXT NOT NULL CHECK (qbo_type IN ('Payment', 'Deposit', 'JournalEntry', 'CreditMemo')),
            posting_plan_id TEXT REFERENCES posting_plans(id),
            payout_batch_id TEXT REFERENCES payout_batches(id),
            processor_payout_id TEXT,
            amount TEXT NOT NULL,
            account_ref TEXT NOT NULL,
            txn_date DATE NOT NULL,
            qbo_txn_number TEXT,
            qbo_doc_number TEXT,
            memo TEXT,
            line_items TEXT,
            status TEXT NOT NULL DEFAULT 'created' CHECK (status IN ('created', 'synced', 'voided', 'deleted')),
            voided_at TIMESTAMP,
            void_reason TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_verified_at TIMESTAMP,
            qbo_response TEXT,
            UNIQUE(qbo_id, qbo_type)
        )
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_qbo_artifacts_payout ON qbo_artifacts(processor_payout_id)
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_qbo_artifacts_type ON qbo_artifacts(qbo_type)
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_qbo_artifacts_status ON qbo_artifacts(status)
        """)
        
        # Payout batches table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS payout_batches (
            id TEXT PRIMARY KEY,
            processor TEXT NOT NULL CHECK (processor IN ('stripe', 'authorize_net', 'test')),
            processor_payout_id TEXT NOT NULL,
            gross_amount TEXT NOT NULL,
            fees TEXT NOT NULL,
            net_amount TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            settlement_date DATE NOT NULL,
            arrival_date DATE,
            payment_count INTEGER NOT NULL,
            payment_ids TEXT,
            qbo_deposit_id TEXT,
            posting_plan_id TEXT REFERENCES posting_plans(id),
            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'complete', 'posted', 'failed')),
            description TEXT,
            metadata TEXT,
            deposit_stamp TEXT,  -- CHARLES deposit stamp for QBO memo (idempotent)
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(processor, processor_payout_id),
            CHECK (CAST(net_amount AS REAL) >= CAST(gross_amount AS REAL) - CAST(fees AS REAL) - 0.01),
            CHECK (CAST(net_amount AS REAL) <= CAST(gross_amount AS REAL) - CAST(fees AS REAL) + 0.01)
        )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_canonical_events_payout ON canonical_events(processor_payout_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posting_plans_status ON posting_plans(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payout_batches_status ON payout_batches(status)")
        
        # Create view for pending posting plans
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_pending_posting_plans AS
        SELECT
            pp.*,
            pb.net_amount as payout_net_amount,
            pb.gross_amount as payout_gross_amount,
            pb.fees as payout_fees,
            pb.payment_count
        FROM posting_plans pp
        JOIN payout_batches pb ON pb.id = pp.payout_batch_id
        WHERE pp.status IN ('pending', 'validated')
          AND pb.status = 'complete'
          AND pp.retry_count < pp.max_retries
        ORDER BY pp.created_at ASC
        """)
        
        # Create view for payouts ready for deposit
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_payouts_ready_for_deposit AS
        SELECT 
            pb.*,
            COUNT(ce.id) as actual_payment_count
        FROM payout_batches pb
        LEFT JOIN canonical_events ce ON ce.processor_payout_id = pb.processor_payout_id
        WHERE pb.status = 'complete'
          AND pb.posting_plan_id IS NULL
        GROUP BY pb.id
        HAVING actual_payment_count = pb.payment_count
        """)
        
        # Schema version
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
        """)
        
        cursor.execute("""
        INSERT OR IGNORE INTO schema_version (version, description)
        VALUES (1, 'Initial company database schema')
        """)

        # Run v2 migration for payment matching
        self._migrate_to_v2_matching(cursor)

    def _migrate_to_v2_matching(self, cursor):
        """
        Migration v2: Add payment matching columns and tables.

        Implements schema for Enhanced Payment Matching feature:
        - Adds matching columns to canonical_events
        - Creates match_reviews table for manual review workflow

        This migration is idempotent - safe to run multiple times.
        """

        # Check if migration already applied
        cursor.execute("SELECT version FROM schema_version WHERE version = 2")
        if cursor.fetchone():
            return  # Already migrated

        # Add matching columns to canonical_events
        # SQLite doesn't have ADD COLUMN IF NOT EXISTS, so we check pragmas
        cursor.execute("PRAGMA table_info(canonical_events)")
        existing_columns = {row[1] for row in cursor.fetchall()}

        if 'qbo_payment_id' not in existing_columns:
            cursor.execute("""
            ALTER TABLE canonical_events ADD COLUMN qbo_payment_id TEXT
            """)

        if 'match_confidence' not in existing_columns:
            cursor.execute("""
            ALTER TABLE canonical_events ADD COLUMN match_confidence INTEGER
                CHECK (match_confidence IS NULL OR (match_confidence >= 0 AND match_confidence <= 100))
            """)

        if 'match_type' not in existing_columns:
            cursor.execute("""
            ALTER TABLE canonical_events ADD COLUMN match_type TEXT
                CHECK (match_type IS NULL OR match_type IN ('auto', 'manual', 'unmatched', 'pending'))
            """)

        if 'match_score_breakdown' not in existing_columns:
            cursor.execute("""
            ALTER TABLE canonical_events ADD COLUMN match_score_breakdown TEXT
            """)

        # Create match_reviews table for manual review workflow
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS match_reviews (
            id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            canonical_event_id TEXT NOT NULL REFERENCES canonical_events(id),
            processor_payment_id TEXT NOT NULL,
            amount TEXT NOT NULL,
            charge_date DATE NOT NULL,
            customer_email TEXT,
            card_last4 TEXT,
            match_candidates TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'resolved', 'skipped')),
            selected_qbo_payment_id TEXT,
            resolved_at TIMESTAMP,
            resolved_by TEXT,
            idempotency_key TEXT UNIQUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Create indexes for match_reviews
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_match_reviews_status
        ON match_reviews(status)
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_match_reviews_canonical
        ON match_reviews(canonical_event_id)
        """)

        # Create index for qbo_payment_id lookups
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_canonical_events_qbo_payment
        ON canonical_events(qbo_payment_id)
        """)

        # Create index for match_type lookups
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_canonical_events_match_type
        ON canonical_events(match_type)
        """)

        # Confirmed match pairs table for prior match continuity
        # Records (processor_customer, qbo_customer) pairs learned from confirmed matches
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS confirmed_match_pairs (
            id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            processor_type TEXT NOT NULL CHECK (processor_type IN ('stripe', 'authorize_net', 'test')),
            processor_customer_id TEXT NOT NULL,
            qbo_customer_id TEXT NOT NULL,
            first_confirmed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_confirmed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            match_source TEXT NOT NULL CHECK (match_source IN ('auto', 'manual')),
            confirmation_count INTEGER NOT NULL DEFAULT 1,
            first_canonical_event_id TEXT,
            last_canonical_event_id TEXT,
            UNIQUE(workspace_id, processor_type, processor_customer_id, qbo_customer_id)
        )
        """)

        # Create indexes for confirmed_match_pairs
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_confirmed_pairs_lookup
        ON confirmed_match_pairs(workspace_id, processor_type, processor_customer_id)
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_confirmed_pairs_qbo_customer
        ON confirmed_match_pairs(workspace_id, qbo_customer_id)
        """)

        # Record schema version
        cursor.execute("""
        INSERT OR IGNORE INTO schema_version (version, description)
        VALUES (2, 'Add payment matching columns, match_reviews table, and confirmed_match_pairs')
        """)


    def _migrate_to_v3_deposit_views(self, cursor) -> None:
        """
        Migration v3: Update deposit views for multi-factor matching.

        Updates views to only allow deposit creation for payouts where
        ALL payments have high-confidence (auto) matches.

        Creates a new view for payouts needing manual review.

        This migration is idempotent - safe to run multiple times.
        """

        # Check if migration already applied
        cursor.execute("""
        SELECT 1 FROM schema_version WHERE version = 3
        """)
        if cursor.fetchone():
            return  # Already migrated

        # Drop and recreate v_payouts_ready_for_deposit to require all payments
        # to have qbo_payment_id set (HIGH confidence matches only)
        cursor.execute("DROP VIEW IF EXISTS v_payouts_ready_for_deposit")
        cursor.execute("""
        CREATE VIEW v_payouts_ready_for_deposit AS
        SELECT
            pb.*,
            COUNT(ce.id) as actual_payment_count,
            SUM(CASE WHEN ce.qbo_payment_id IS NOT NULL THEN 1 ELSE 0 END) as matched_payment_count
        FROM payout_batches pb
        LEFT JOIN canonical_events ce ON ce.processor_payout_id = pb.processor_payout_id
        WHERE pb.status = 'complete'
          AND pb.posting_plan_id IS NULL
        GROUP BY pb.id
        HAVING actual_payment_count = pb.payment_count
           AND matched_payment_count = actual_payment_count
        """)

        # Create view for payouts needing review (have at least one unmatched payment)
        cursor.execute("DROP VIEW IF EXISTS v_payouts_needing_review")
        cursor.execute("""
        CREATE VIEW v_payouts_needing_review AS
        SELECT
            pb.*,
            COUNT(ce.id) as actual_payment_count,
            SUM(CASE WHEN ce.qbo_payment_id IS NOT NULL THEN 1 ELSE 0 END) as matched_payment_count,
            SUM(CASE WHEN ce.match_type = 'pending' THEN 1 ELSE 0 END) as pending_review_count,
            SUM(CASE WHEN ce.match_type = 'unmatched' THEN 1 ELSE 0 END) as unmatched_count
        FROM payout_batches pb
        LEFT JOIN canonical_events ce ON ce.processor_payout_id = pb.processor_payout_id
        WHERE pb.status = 'complete'
          AND pb.posting_plan_id IS NULL
        GROUP BY pb.id
        HAVING actual_payment_count = pb.payment_count
           AND (pending_review_count > 0 OR unmatched_count > 0)
        """)

        # Record schema version
        cursor.execute("""
        INSERT OR IGNORE INTO schema_version (version, description)
        VALUES (3, 'Update deposit views for multi-factor matching')
        """)


    def apply_migrations(self, company_id: str = None, workspace_id: str = None) -> None:
        """
        Apply pending migrations (DEPRECATED for per-workspace migrations).

        DEPRECATED: Post-Phase 2, migrations are applied via migration scripts.
        The consolidated database schema is version 20 and managed by migration scripts.

        This method is kept for backward compatibility but does nothing.

        Args:
            company_id: Company UUID (deprecated)
            workspace_id: Workspace ID (deprecated)
        """
        import warnings
        warnings.warn(
            "apply_migrations() is deprecated. Post-Phase 2, use migration scripts.",
            DeprecationWarning,
            stacklevel=2
        )
        return  # No-op


# Singleton instance
_db_manager = None

def get_db_manager() -> DatabaseManager:
    """Get singleton database manager instance"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def verify_foreign_keys_enabled(db_path: Optional[str] = None) -> None:
    """
    Verify that foreign keys are enabled (startup assertion).

    CRITICAL: Per PERSISTENCE_GUARANTEES_CONTRACT.MD §6.1:
    "Foreign keys must be enforced at the database level"

    This function MUST be called at application startup to ensure FK enforcement.
    If foreign keys are not enabled, it raises a fatal error.

    Args:
        db_path: Path to database (defaults to env var)

    Raises:
        RuntimeError: If foreign keys are disabled (FAIL-LOUD)
    """
    import sys
    from shared.structured_logging import get_logger, EventType

    if db_path is None:
        db_path = os.getenv('MASTER_DB_PATH', './data/charles.db')

    logger = get_logger(service="database_startup", workspace_id="system")

    try:
        # Create connection using our helper (which enables FKs)
        conn = _create_connection(db_path)
        cursor = conn.cursor()

        # Verify FK pragma is actually ON
        cursor.execute("PRAGMA foreign_keys")
        result = cursor.fetchone()
        fk_enabled = result[0] if result else 0

        conn.close()

        if fk_enabled == 0:
            # CRITICAL: Foreign keys are OFF - this violates contract
            logger.error(
                EventType.ERROR_DETECTED,
                "FATAL: Foreign key enforcement is DISABLED. This violates PERSISTENCE_GUARANTEES_CONTRACT.MD §6.1",
                db_path=db_path,
                fk_pragma_result=fk_enabled,
                error_code="FK_ENFORCEMENT_DISABLED"
            )

            raise RuntimeError(
                "FATAL STARTUP ERROR: Foreign key enforcement is DISABLED.\n"
                f"Database: {db_path}\n"
                "PRAGMA foreign_keys returned: 0 (expected: 1)\n"
                "This violates PERSISTENCE_GUARANTEES_CONTRACT.MD §6.1.\n"
                "Application cannot start with foreign keys disabled."
            )

        # FK enforcement verified - log success
        logger.info(
            EventType.WARNING_DETECTED,  # Using existing event type
            "Foreign key enforcement verified: ENABLED",
            db_path=db_path,
            fk_pragma_result=fk_enabled
        )

    except FileNotFoundError:
        # Database doesn't exist yet - this is OK during initial setup
        logger.warn(
            EventType.WARNING_DETECTED,
            "Database not found during FK verification - skipping (initial setup)",
            db_path=db_path
        )
    except Exception as e:
        # Any other error is fatal
        logger.error(
            EventType.ERROR_DETECTED,
            "FATAL: Failed to verify foreign key enforcement",
            db_path=db_path,
            error=str(e),
            error_type=type(e).__name__
        )
        raise
