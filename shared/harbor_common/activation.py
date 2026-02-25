"""Harbor Phase 3 — Activation Bootstrap Orchestration.

Implements deterministic activation readiness and completion.

Activation readiness is DERIVED (not stored):
    activation_ready(workspace_id) :=
        workspace_has_qbo_entitlement(workspace_id)
        AND qbo_status(workspace_id) == CONNECTED

Activation is recorded by presence of a row in workspace_activations.
No status column, no soft deletes, no flags.

Concurrency: pg_advisory_xact_lock(workspace_id_hash(workspace_id))
    — same lock namespace as Phase 2 QBO connection flows.
Idempotency: ON CONFLICT (workspace_id) DO NOTHING
"""

from uuid import UUID

from harbor_common.db import get_db
from harbor_common.errors import HarborError
from harbor_common.licensing import (
    require_qbo_entitlement,
    workspace_has_qbo_entitlement,
)
from harbor_common.locks import workspace_id_hash


def _get_qbo_connection_status(workspace_id: UUID) -> str | None:
    """Read-only fetch of QBO connection status for a workspace.

    Returns the status string, or None if no connection row exists.
    Does NOT acquire row-level lock — use _get_qbo_connection_status_locked
    when inside a write transaction.
    """
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "SELECT status FROM qbo_connections WHERE workspace_id = %s",
        (str(workspace_id),),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return row["status"]


def _is_activated(workspace_id: UUID) -> bool:
    """Read-only check: does an activation row exist for this workspace?"""
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "SELECT 1 FROM workspace_activations WHERE workspace_id = %s",
        (str(workspace_id),),
    )
    return cur.fetchone() is not None


def get_activation_status(workspace_id: UUID) -> dict:
    """Compute full activation status for a workspace.

    All values are derived at query time — nothing is cached.
    Read-only — no advisory lock, no row locks.

    Returns:
        {
            "entitlement_valid": bool,
            "qbo_status": str | None,
            "activation_ready": bool,
            "activation_completed": bool,
        }
    """
    entitlement_valid = workspace_has_qbo_entitlement(workspace_id)
    qbo_status = _get_qbo_connection_status(workspace_id)
    activation_completed = _is_activated(workspace_id)

    activation_ready = entitlement_valid and qbo_status == "CONNECTED"

    return {
        "entitlement_valid": entitlement_valid,
        "qbo_status": qbo_status,
        "activation_ready": activation_ready,
        "activation_completed": activation_completed,
    }


def complete_activation(workspace_id: UUID) -> dict:
    """Activate a workspace. Transaction-safe, idempotent, advisory-locked.

    Preconditions (enforced inside advisory lock):
        1. Workspace must have a valid QBO entitlement.
        2. QBO connection status must be CONNECTED.

    Transaction discipline:
        - Acquire pg_advisory_xact_lock(workspace_id_hash(workspace_id))
          using the SAME lock namespace as Phase 2 QBO connection flows.
        - Re-evaluate entitlement (raising mode).
        - Re-fetch QBO connection status with FOR UPDATE row lock.
        - INSERT activation row (ON CONFLICT DO NOTHING for idempotency).
        - Commit on success.
        - On error: raise HarborError without rollback. The advisory lock
          remains held until Flask teardown closes the connection (implicit
          rollback). This matches Phase 2 domain function error handling.

    Returns:
        {"activation_completed": True, "already_completed": bool}
    """
    db = get_db()
    cur = db.cursor()

    # Acquire workspace-scoped advisory lock — unified namespace with Phase 2.
    # Serializes with: OAuth callback, connect-start, disconnect, revoke,
    # token refresh, and concurrent activation attempts.
    cur.execute(
        "SELECT pg_advisory_xact_lock(%s)",
        (workspace_id_hash(workspace_id),),
    )

    # Re-evaluate entitlement under lock (raising mode).
    # No rollback on failure — advisory lock stays held until connection close.
    try:
        require_qbo_entitlement(workspace_id)
    except HarborError:
        raise HarborError(
            code="ACTIVATION_NOT_READY",
            message="Workspace does not have a valid QBO entitlement.",
            metadata={"workspace_id": str(workspace_id)},
            status_code=409,
        )

    # Re-fetch QBO connection status under lock with FOR UPDATE row lock
    # to prevent TOCTOU even if another code path attempts concurrent writes.
    cur.execute(
        "SELECT status FROM qbo_connections WHERE workspace_id = %s FOR UPDATE",
        (str(workspace_id),),
    )
    qbo_row = cur.fetchone()
    qbo_status = qbo_row["status"] if qbo_row is not None else None

    if qbo_status != "CONNECTED":
        raise HarborError(
            code="ACTIVATION_NOT_READY",
            message="QBO connection is not in CONNECTED state.",
            metadata={
                "workspace_id": str(workspace_id),
                "qbo_status": qbo_status,
            },
            status_code=409,
        )

    # Insert activation row — idempotent via ON CONFLICT DO NOTHING.
    cur.execute(
        """
        INSERT INTO workspace_activations (workspace_id, activated_at)
        VALUES (%s, NOW())
        ON CONFLICT (workspace_id) DO NOTHING
        RETURNING id
        """,
        (str(workspace_id),),
    )
    inserted = cur.fetchone()
    already_completed = inserted is None

    db.commit()

    return {
        "activation_completed": True,
        "already_completed": already_completed,
    }
