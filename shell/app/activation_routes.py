"""Harbor Phase 3 — Activation endpoints.

Routes:
    GET  /v1/workspaces/<workspace_id>/activation/status
    POST /v1/workspaces/<workspace_id>/activation/complete

Authentication & authorization:
    These endpoints follow the same auth posture as all other Harbor endpoints.
    No Harbor endpoint performs application-layer authentication — identity
    verification is delegated to the infrastructure layer (reverse proxy / API
    gateway) per the existing architecture. All queries are workspace-scoped
    by the URL path parameter, maintaining multi-tenant data isolation.
"""

from flask import Blueprint, jsonify

from harbor_common.activation import complete_activation, get_activation_status

activation_bp = Blueprint("activation", __name__)


# ------------------------------------------------------------------
# GET /v1/workspaces/<workspace_id>/activation/status
# ------------------------------------------------------------------

@activation_bp.route(
    "/v1/workspaces/<uuid:workspace_id>/activation/status",
    methods=["GET"],
)
def activation_status(workspace_id):
    """Return derived activation readiness and completion status.

    Read-only — no advisory lock required.
    """
    status = get_activation_status(workspace_id)
    return jsonify(status), 200


# ------------------------------------------------------------------
# POST /v1/workspaces/<workspace_id>/activation/complete
# ------------------------------------------------------------------

@activation_bp.route(
    "/v1/workspaces/<uuid:workspace_id>/activation/complete",
    methods=["POST"],
)
def activation_complete(workspace_id):
    """Complete workspace activation.

    Requires valid QBO entitlement and CONNECTED QBO state.
    Transaction-safe, idempotent, advisory-locked.
    """
    result = complete_activation(workspace_id)
    return jsonify(result), 200
