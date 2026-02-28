"""Charles Activation Gate — Shell-side precondition validation.

Phase 4D (INV-4D-3): All preconditions MUST be validated before any
Charles logic executes.  This gate delegates to existing shell services;
it does NOT duplicate SQL or licensing logic.

Flow:
    1. Verify workspace exists
    2. Verify QBO entitlement (license valid)
    3. Verify workspace is activated
    4. Verify QBO connection is CONNECTED
    5. Retrieve and decrypt token (shell-mediated)
    6. Build WEC via build_wec()
"""

from datetime import datetime, timezone
from uuid import UUID

from harbor_common.activation import get_activation_status
from harbor_common.db import get_db
from harbor_common.errors import HarborError
from harbor_common.qbo_connection import get_qbo_tokens
from shell.core.wec import WorkspaceExecutionContext, build_wec


def gate_charles_execution(
    workspace_id: UUID,
    request_id: str | None = None,
) -> WorkspaceExecutionContext:
    """Validate all preconditions and return a shell-issued WEC.

    Raises HarborError with classified error code on any failure.
    On success, returns a fully-populated WorkspaceExecutionContext
    that Charles may consume but never construct.

    Args:
        workspace_id: Target workspace UUID.
        request_id:   Optional correlation ID for tracing.

    Returns:
        WorkspaceExecutionContext ready for Charles invocation.

    Raises:
        HarborError NOT_FOUND (404):      Workspace does not exist.
        HarborError FORBIDDEN (403):      License or activation invalid.
        HarborError OAUTH_REQUIRED (403): Token missing or QBO not connected.
    """
    wid = str(workspace_id)

    # ------------------------------------------------------------------
    # 1. Workspace existence
    # ------------------------------------------------------------------
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "SELECT id FROM workspaces WHERE id = %s AND deleted_at IS NULL",
        (wid,),
    )
    if cur.fetchone() is None:
        raise HarborError(
            code="NOT_FOUND",
            message="Workspace not found.",
            metadata={"workspace_id": wid},
            status_code=404,
        )

    # ------------------------------------------------------------------
    # 2-4. Entitlement, activation, QBO connection status
    #      Delegates to harbor_common.activation.get_activation_status()
    # ------------------------------------------------------------------
    status = get_activation_status(workspace_id)

    if not status["entitlement_valid"]:
        raise HarborError(
            code="FORBIDDEN",
            message="Workspace does not have a valid QBO-entitled license.",
            metadata={"workspace_id": wid},
            status_code=403,
        )

    if not status["activation_completed"]:
        raise HarborError(
            code="FORBIDDEN",
            message="Workspace activation has not been completed.",
            metadata={"workspace_id": wid},
            status_code=403,
        )

    if status["qbo_status"] != "CONNECTED":
        raise HarborError(
            code="OAUTH_REQUIRED",
            message="QBO connection is not in CONNECTED state.",
            metadata={
                "workspace_id": wid,
                "qbo_status": status["qbo_status"],
            },
            status_code=403,
        )

    # ------------------------------------------------------------------
    # 5. Token retrieval (shell-mediated, decrypt via existing service)
    # ------------------------------------------------------------------
    try:
        tokens = get_qbo_tokens(workspace_id)
    except HarborError:
        raise HarborError(
            code="OAUTH_REQUIRED",
            message="QBO tokens could not be retrieved for this workspace.",
            metadata={"workspace_id": wid},
            status_code=403,
        )

    access_token = tokens.get("access_token")
    if not access_token:
        raise HarborError(
            code="OAUTH_REQUIRED",
            message="QBO access token is missing.",
            metadata={"workspace_id": wid},
            status_code=403,
        )

    qbo_realm_id = tokens.get("qbo_realm_id", "")

    # ------------------------------------------------------------------
    # 6. Build WEC (shell-only construction)
    # ------------------------------------------------------------------
    return build_wec(
        workspace_id=wid,
        app_id="charles",
        qbo_realm_id=qbo_realm_id,
        qbo_access_token=access_token,
        issued_at=datetime.now(timezone.utc),
        request_id=request_id,
    )
