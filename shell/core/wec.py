"""WorkspaceExecutionContext — Shell → App boundary contract.

This module defines the formal execution context that Shell issues to apps.
It is the sole authority for WEC construction (via build_wec).

Boundary rules:
  - Apps may import WorkspaceExecutionContext for type checking only.
  - Apps must NEVER import build_wec().
  - Apps must NEVER instantiate WorkspaceExecutionContext directly.

Dependency constraint (Phase 4D — INV-4D-2):
  - This module imports ONLY Python stdlib.
  - No Flask, no HTTP, no web-layer references.
  - Must be extractable to a shared boundary package without modification.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class WorkspaceExecutionContext:
    """Shell-issued execution context required for any app invocation.

    Only Shell may construct instances via build_wec().
    Apps may type-check and read fields, but never instantiate.

    Fields:
        workspace_id:     UUID of the target workspace.
        app_id:           Constant identifier for the app (e.g. "charles").
        qbo_realm_id:     QBO company realm ID bound to the workspace.
        qbo_access_token: Decrypted OAuth access token, shell-mediated.
        issued_at:        Timestamp when this context was issued.
        request_id:       Optional correlation ID for tracing.
    """
    workspace_id: str
    app_id: str
    qbo_realm_id: str
    qbo_access_token: str
    issued_at: datetime
    request_id: str | None = None


def build_wec(
    *,
    workspace_id: str,
    app_id: str,
    qbo_realm_id: str,
    qbo_access_token: str,
    issued_at: datetime,
    request_id: str | None = None,
) -> WorkspaceExecutionContext:
    """Construct a WorkspaceExecutionContext.

    Shell-only — apps must never call this function.

    All parameters are keyword-only to prevent positional mistakes
    at the sovereignty boundary.
    """
    return WorkspaceExecutionContext(
        workspace_id=workspace_id,
        app_id=app_id,
        qbo_realm_id=qbo_realm_id,
        qbo_access_token=qbo_access_token,
        issued_at=issued_at,
        request_id=request_id,
    )
