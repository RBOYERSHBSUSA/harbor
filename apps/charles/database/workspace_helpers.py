#!/usr/bin/env python3
"""
Workspace Helper Functions

Bridge functions to help transition from company_id-based code to workspace_id-based code.
These helpers allow legacy code to continue working during the migration to Phase 2 architecture.

Per CHARLES_MULTITENANCY_CONTRACT v1.0:
- Post-Phase 2, all operations must be workspace-scoped
- company_id is legacy; workspace_id is the canonical identifier
"""

import os
import sqlite3
from typing import Optional, Dict, Any

from shared.structured_logging import get_logger, EventType
from module2_database import _create_connection


def resolve_workspace_id(
    workspace_id: Optional[str],
    company_id: Optional[str],
    caller: str
) -> str:
    """
    Centralized workspace resolution with deprecation warnings.

    Per PHASE 3 REFINEMENT R1: All workspace_id resolution must use this function.
    This ensures single-path resolution and consistent deprecation warnings.

    Args:
        workspace_id: Direct workspace ID (preferred)
        company_id: Legacy company ID (deprecated)
        caller: Name of calling function for logging

    Returns:
        Resolved workspace_id

    Raises:
        ValueError: If both parameters are None

    Example:
        workspace_id = resolve_workspace_id(
            workspace_id=None,
            company_id="company_abc",
            caller="QBOAuthManager.get_tokens"
        )
    """
    if workspace_id is None and company_id is None:
        raise ValueError(f"{caller}: Must provide either workspace_id or company_id")

    if workspace_id is None:
        # Resolve via existing bridge function
        workspace_id = get_workspace_id_from_company_id(company_id)

        if workspace_id is None:
            raise ValueError(f"{caller}: Could not resolve workspace_id from company_id={company_id}")

        # Emit deprecation warning (Per R1) - AFTER workspace_id resolution
        logger = get_logger(service="workspace_helpers", workspace_id=workspace_id)
        logger.warn(
            EventType.WARNING_DETECTED,
            f"{caller} called with company_id - use workspace_id instead",
            company_id=company_id,
            caller=caller,
            warning_type="deprecation"
        )

    return workspace_id


def get_workspace_id_from_company_id(company_id: str, db_path: Optional[str] = None) -> Optional[str]:
    """
    Get workspace_id for a given company_id (legacy bridge function).

    Post-Phase 2: companies table has workspace_id FK.
    This function allows legacy code using company_id to get the workspace_id.

    Args:
        company_id: Company UUID (legacy identifier)
        db_path: Path to consolidated charles.db (defaults to env var)

    Returns:
        workspace_id or None if company not found
    """
    if db_path is None:
        db_path = os.getenv('MASTER_DB_PATH', './data/charles.db')

    conn = _create_connection(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT workspace_id FROM companies WHERE id = ?",
            (company_id,)
        )
        row = cursor.fetchone()
        return row['workspace_id'] if row else None
    finally:
        conn.close()


def get_company_info_with_workspace(company_id: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get company info including workspace_id (legacy bridge function).

    Returns company details with workspace_id for legacy code that needs both.

    Args:
        company_id: Company UUID
        db_path: Path to consolidated charles.db

    Returns:
        Dict with company_id, company_name, qbo_realm_id, workspace_id or None
    """
    if db_path is None:
        db_path = os.getenv('MASTER_DB_PATH', './data/charles.db')

    conn = _create_connection(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT id, company_name, qbo_realm_id, workspace_id FROM companies WHERE id = ?",
            (company_id,)
        )
        row = cursor.fetchone()

        if row:
            return {
                'id': row['id'],
                'company_id': row['id'],  # Alias for legacy code
                'company_name': row['company_name'],
                'qbo_realm_id': row['qbo_realm_id'],
                'workspace_id': row['workspace_id']
            }
        return None
    finally:
        conn.close()


def get_active_workspace_id(db_path: Optional[str] = None) -> Optional[str]:
    """
    Get the active workspace_id (for single-workspace systems).

    Post-Phase 2: Returns workspace_id of the first active workspace.
    In multi-workspace systems (Phase 3+), this should not be used.

    Args:
        db_path: Path to consolidated charles.db

    Returns:
        workspace_id or None
    """
    if db_path is None:
        db_path = os.getenv('MASTER_DB_PATH', './data/charles.db')

    conn = _create_connection(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT workspace_id FROM workspaces WHERE status = 'active' ORDER BY created_at ASC LIMIT 1"
        )
        row = cursor.fetchone()
        return row['workspace_id'] if row else None
    finally:
        conn.close()


def get_workspace_info(workspace_id: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get workspace information.

    Args:
        workspace_id: Workspace identifier
        db_path: Path to consolidated charles.db

    Returns:
        Dict with workspace details or None
    """
    if db_path is None:
        db_path = os.getenv('MASTER_DB_PATH', './data/charles.db')

    conn = _create_connection(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT
                w.workspace_id,
                w.workspace_name,
                w.customer_id,
                w.qbo_realm_id,
                w.status,
                l.license_type,
                l.license_id
            FROM workspaces w
            JOIN licenses l ON l.workspace_id = w.workspace_id
            WHERE w.workspace_id = ?
            """,
            (workspace_id,)
        )
        row = cursor.fetchone()

        if row:
            return {
                'workspace_id': row['workspace_id'],
                'workspace_name': row['workspace_name'],
                'customer_id': row['customer_id'],
                'qbo_realm_id': row['qbo_realm_id'],
                'status': row['status'],
                'license_type': row['license_type'],
                'license_id': row['license_id']
            }
        return None
    finally:
        conn.close()
