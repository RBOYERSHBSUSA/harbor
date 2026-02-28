#!/usr/bin/env python3
"""
WorkspaceContext - Workspace-scoped configuration resolver

Per WORKSPACE_CONTEXT_CONTRACT (LOCKED):
- MUST validate license before construction
- MUST NOT construct with invalid/expired license
- Provides typed access to workspace-scoped configuration
- All operations scoped to a single workspace

Per CHARLES_CLAUDE_DOMAIN_INSTRUCTIONS:
- Workspace = exactly one QBO company file
- License = grants operation rights to exactly one Workspace (1:1)
- Never share credentials across Workspaces
"""

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from cryptography.fernet import Fernet

from module2_database import _create_connection


@dataclass
class WorkspaceMetadata:
    """Workspace identity and metadata."""
    workspace_id: str
    customer_id: str
    workspace_name: str
    qbo_realm_id: str  # IMMUTABLE
    status: str
    created_at: datetime
    updated_at: datetime
    last_sync_at: Optional[datetime]
    metadata: Optional[Dict[str, Any]]


@dataclass
class LicenseInfo:
    """License information for workspace."""
    license_id: str
    workspace_id: str
    license_type: str
    status: str
    expires_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    metadata: Optional[Dict[str, Any]]

    def is_valid(self) -> bool:
        """Check if license is currently valid."""
        if self.status != 'active':
            return False

        if self.expires_at is not None:
            now = datetime.now(timezone.utc)
            if self.expires_at < now:
                return False

        return True


@dataclass
class QBOCredentials:
    """QBO OAuth credentials for workspace."""
    qbo_credentials_id: str
    workspace_id: str
    qbo_app_id: str
    access_token_encrypted: str
    refresh_token_encrypted: str
    token_type: str
    expires_at: datetime
    refresh_token_expires_at: datetime
    scope: str
    encryption_key_id: str
    status: str
    consecutive_refresh_failures: int
    last_refreshed_at: Optional[datetime]


@dataclass
class ProcessorConnection:
    """Payment processor connection for workspace."""
    processor_connection_id: str
    workspace_id: str
    processor_type: str
    processor_account_id: str
    api_key_encrypted: str
    api_secret_encrypted: Optional[str]
    deposit_account_id: Optional[str]
    fee_expense_account_id: Optional[str]
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass
class CompanyConfiguration:
    """Company configuration for workspace."""
    id: str
    workspace_id: str
    company_id: str
    qbo_undeposited_funds_account_id: str
    qbo_bank_account_id: str
    qbo_fee_expense_account_id: str
    qbo_refund_liability_account_id: Optional[str]
    qbo_chargeback_expense_account_id: Optional[str]
    auto_create_deposits: bool
    auto_create_payments: bool
    require_manual_approval: bool
    deposit_memo_template: str
    payment_memo_template: str
    email_on_deposit_created: bool
    email_on_error: bool
    notification_email: Optional[str]
    timezone: str
    date_format: str
    matching_algorithm: str
    auto_match_enabled: bool
    daily_sync_enabled: bool
    version: int
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str]
    updated_by: Optional[str]


class WorkspaceContextError(Exception):
    """Raised when WorkspaceContext cannot be constructed."""
    pass


class WorkspaceContext:
    """
    Workspace-scoped configuration resolver.

    CRITICAL CONTRACT (LOCKED):
    - MUST validate license before construction
    - MUST NOT construct with invalid/expired license
    - All operations scoped to workspace_id

    Usage:
        ctx = WorkspaceContext.from_workspace_id(workspace_id, db_path)
        qbo_creds = ctx.get_qbo_credentials()
        processor = ctx.get_processor_connection('stripe')
    """

    def __init__(
        self,
        workspace: WorkspaceMetadata,
        license: LicenseInfo,
        db_path: str
    ):
        """
        Private constructor. Use WorkspaceContext.from_workspace_id() instead.

        Args:
            workspace: Workspace metadata
            license: License information (MUST be valid)
            db_path: Path to consolidated charles.db

        Raises:
            WorkspaceContextError: If license is invalid
        """
        # CRITICAL: Validate license before allowing construction
        if not license.is_valid():
            raise WorkspaceContextError(
                f"Cannot construct WorkspaceContext: License {license.license_id} "
                f"is invalid (status={license.status}, expires_at={license.expires_at})"
            )

        self._workspace = workspace
        self._license = license
        self._db_path = db_path
        self._encryption_key: Optional[Fernet] = None

    @classmethod
    def from_workspace_id(cls, workspace_id: str, db_path: Optional[str] = None) -> 'WorkspaceContext':
        """
        Construct WorkspaceContext from workspace_id.

        Args:
            workspace_id: Workspace identifier
            db_path: Path to consolidated charles.db (defaults to env var)

        Returns:
            WorkspaceContext instance

        Raises:
            WorkspaceContextError: If workspace not found or license invalid
        """
        if db_path is None:
            db_path = os.getenv('MASTER_DB_PATH', './data/charles.db')

        conn = _create_connection(db_path)
        cursor = conn.cursor()

        try:
            # Load workspace
            cursor.execute(
                "SELECT * FROM workspaces WHERE workspace_id = ?",
                (workspace_id,)
            )
            workspace_row = cursor.fetchone()
            if not workspace_row:
                raise WorkspaceContextError(f"Workspace not found: {workspace_id}")

            workspace = WorkspaceMetadata(
                workspace_id=workspace_row['workspace_id'],
                customer_id=workspace_row['customer_id'],
                workspace_name=workspace_row['workspace_name'],
                qbo_realm_id=workspace_row['qbo_realm_id'],
                status=workspace_row['status'],
                created_at=datetime.fromisoformat(workspace_row['created_at']),
                updated_at=datetime.fromisoformat(workspace_row['updated_at']),
                last_sync_at=datetime.fromisoformat(workspace_row['last_sync_at']) if workspace_row['last_sync_at'] else None,
                metadata=None  # TODO: Parse JSON if needed
            )

            # Load license (1:1 relationship with workspace)
            cursor.execute(
                "SELECT * FROM licenses WHERE workspace_id = ?",
                (workspace_id,)
            )
            license_row = cursor.fetchone()
            if not license_row:
                raise WorkspaceContextError(f"No license found for workspace: {workspace_id}")

            license = LicenseInfo(
                license_id=license_row['license_id'],
                workspace_id=license_row['workspace_id'],
                license_type=license_row['license_type'],
                status=license_row['status'],
                expires_at=datetime.fromisoformat(license_row['expires_at']) if license_row['expires_at'] else None,
                created_at=datetime.fromisoformat(license_row['created_at']),
                updated_at=datetime.fromisoformat(license_row['updated_at']),
                metadata=None  # TODO: Parse JSON if needed
            )

            # Construct (this will validate license)
            return cls(workspace, license, db_path)

        finally:
            conn.close()

    @property
    def workspace_id(self) -> str:
        """Get workspace identifier."""
        return self._workspace.workspace_id

    @property
    def customer_id(self) -> str:
        """Get customer identifier."""
        return self._workspace.customer_id

    @property
    def qbo_realm_id(self) -> str:
        """Get QBO realm ID (IMMUTABLE)."""
        return self._workspace.qbo_realm_id

    @property
    def workspace_name(self) -> str:
        """Get workspace name."""
        return self._workspace.workspace_name

    @property
    def license_type(self) -> str:
        """Get license type."""
        return self._license.license_type

    @property
    def workspace(self) -> WorkspaceMetadata:
        """Get workspace metadata."""
        return self._workspace

    @property
    def license(self) -> LicenseInfo:
        """Get license information."""
        return self._license

    def get_qbo_credentials(self, qbo_app_id: Optional[str] = None) -> Optional[QBOCredentials]:
        """
        Get QBO OAuth credentials for this workspace.

        NON-PRODUCTION: Phase 4D (INV-4D-5) — In production, QBO tokens
        are retrieved by Shell and injected via WorkspaceExecutionContext.
        Charles must not read token storage directly in production paths.
        This method is retained for development and testing tooling only.

        Args:
            qbo_app_id: Specific QBO app ID (optional, uses active credential if None)

        Returns:
            QBOCredentials or None if not found
        """
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            if qbo_app_id:
                cursor.execute(
                    "SELECT * FROM qbo_credentials WHERE workspace_id = ? AND qbo_app_id = ? AND status = 'active'",
                    (self.workspace_id, qbo_app_id)
                )
            else:
                cursor.execute(
                    "SELECT * FROM qbo_credentials WHERE workspace_id = ? AND status = 'active' LIMIT 1",
                    (self.workspace_id,)
                )

            row = cursor.fetchone()
            if not row:
                return None

            return QBOCredentials(
                qbo_credentials_id=row['qbo_credentials_id'],
                workspace_id=row['workspace_id'],
                qbo_app_id=row['qbo_app_id'],
                access_token_encrypted=row['access_token_encrypted'],
                refresh_token_encrypted=row['refresh_token_encrypted'],
                token_type=row['token_type'],
                expires_at=datetime.fromisoformat(row['expires_at']),
                refresh_token_expires_at=datetime.fromisoformat(row['refresh_token_expires_at']),
                scope=row['scope'],
                encryption_key_id=row['encryption_key_id'],
                status=row['status'],
                consecutive_refresh_failures=row['consecutive_refresh_failures'],
                last_refreshed_at=datetime.fromisoformat(row['last_refreshed_at']) if row['last_refreshed_at'] else None
            )

        finally:
            conn.close()

    def get_processor_connection(self, processor_type: str, processor_account_id: Optional[str] = None) -> Optional[ProcessorConnection]:
        """
        Get payment processor connection for this workspace.

        Args:
            processor_type: 'stripe', 'authorize_net', 'square', or 'paypal'
            processor_account_id: Specific processor account (optional)

        Returns:
            ProcessorConnection or None if not found
        """
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            if processor_account_id:
                cursor.execute(
                    "SELECT * FROM processor_connections WHERE workspace_id = ? AND processor_type = ? AND processor_account_id = ? AND status = 'active'",
                    (self.workspace_id, processor_type, processor_account_id)
                )
            else:
                cursor.execute(
                    "SELECT * FROM processor_connections WHERE workspace_id = ? AND processor_type = ? AND status = 'active' LIMIT 1",
                    (self.workspace_id, processor_type)
                )

            row = cursor.fetchone()
            if not row:
                return None

            return ProcessorConnection(
                processor_connection_id=row['processor_connection_id'],
                workspace_id=row['workspace_id'],
                processor_type=row['processor_type'],
                processor_account_id=row['processor_account_id'],
                api_key_encrypted=row['api_key_encrypted'],
                api_secret_encrypted=row['api_secret_encrypted'],
                deposit_account_id=row['deposit_account_id'],
                fee_expense_account_id=row['fee_expense_account_id'],
                status=row['status'],
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at'])
            )

        finally:
            conn.close()

    def get_processor_connections(self) -> List[ProcessorConnection]:
        """
        Get all active processor connections for this workspace.

        Returns:
            List of ProcessorConnection objects
        """
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute(
                "SELECT * FROM processor_connections WHERE workspace_id = ? AND status = 'active'",
                (self.workspace_id,)
            )

            return [
                ProcessorConnection(
                    processor_connection_id=row['processor_connection_id'],
                    workspace_id=row['workspace_id'],
                    processor_type=row['processor_type'],
                    processor_account_id=row['processor_account_id'],
                    api_key_encrypted=row['api_key_encrypted'],
                    api_secret_encrypted=row['api_secret_encrypted'],
                    deposit_account_id=row['deposit_account_id'],
                    fee_expense_account_id=row['fee_expense_account_id'],
                    status=row['status'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                )
                for row in cursor.fetchall()
            ]

        finally:
            conn.close()

    def get_configuration(self) -> Optional[CompanyConfiguration]:
        """
        Get company configuration for this workspace.

        Returns:
            CompanyConfiguration or None if not configured
        """
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute(
                "SELECT * FROM company_configuration WHERE workspace_id = ?",
                (self.workspace_id,)
            )

            row = cursor.fetchone()
            if not row:
                return None

            return CompanyConfiguration(
                id=row['id'],
                workspace_id=row['workspace_id'],
                company_id=row['company_id'],
                qbo_undeposited_funds_account_id=row['qbo_undeposited_funds_account_id'],
                qbo_bank_account_id=row['qbo_bank_account_id'],
                qbo_fee_expense_account_id=row['qbo_fee_expense_account_id'],
                qbo_refund_liability_account_id=row['qbo_refund_liability_account_id'],
                qbo_chargeback_expense_account_id=row['qbo_chargeback_expense_account_id'],
                auto_create_deposits=bool(row['auto_create_deposits']),
                auto_create_payments=bool(row['auto_create_payments']),
                require_manual_approval=bool(row['require_manual_approval']),
                deposit_memo_template=row['deposit_memo_template'],
                payment_memo_template=row['payment_memo_template'],
                email_on_deposit_created=bool(row['email_on_deposit_created']),
                email_on_error=bool(row['email_on_error']),
                notification_email=row['notification_email'],
                timezone=row['timezone'],
                date_format=row['date_format'],
                matching_algorithm=row['matching_algorithm'],
                auto_match_enabled=bool(row['auto_match_enabled']),
                daily_sync_enabled=bool(row['daily_sync_enabled']),
                version=row['version'],
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at']),
                created_by=row['created_by'],
                updated_by=row['updated_by']
            )

        finally:
            conn.close()

    def _get_encryption_key(self) -> Fernet:
        """
        Get Fernet encryption key for decrypting credentials.

        Returns:
            Fernet instance

        Raises:
            WorkspaceContextError: If encryption key not found
        """
        if self._encryption_key is not None:
            return self._encryption_key

        # Get encryption key from environment
        encryption_key_str = os.getenv('ENCRYPTION_KEY')
        if not encryption_key_str:
            raise WorkspaceContextError("ENCRYPTION_KEY not found in environment")

        self._encryption_key = Fernet(encryption_key_str.encode())
        return self._encryption_key

    def decrypt_api_key(self, encrypted_value: str) -> str:
        """
        Decrypt an encrypted API key.

        Args:
            encrypted_value: Encrypted API key

        Returns:
            Decrypted API key
        """
        fernet = self._get_encryption_key()
        return fernet.decrypt(encrypted_value.encode()).decode()

    def __repr__(self) -> str:
        return (
            f"WorkspaceContext(workspace_id={self.workspace_id}, "
            f"workspace_name={self.workspace_name}, "
            f"qbo_realm_id={self.qbo_realm_id}, "
            f"license_type={self.license_type})"
        )
