"""Shell-owned routes for Harbor Charles.

Phase 4D (INV-4D-1): All externally reachable Charles capabilities
MUST be invoked through Harbor Shell routing.  Charles does not
register independent production routes.

Routes:
    POST /v1/workspaces/<workspace_id>/charles/sync
    POST /v1/workspaces/<workspace_id>/charles/deposit
    GET  /v1/workspaces/<workspace_id>/charles/status
"""

import os

from flask import Blueprint, jsonify, request

from harbor_common.errors import HarborError
from shell.app.charles_gate import gate_charles_execution

charles_bp = Blueprint("charles", __name__)


def _get_charles_db_path() -> str:
    """Resolve the Charles database path from shell configuration.

    Shell owns this configuration — Charles never resolves its own
    storage path in production.
    """
    path = os.environ.get("CHARLES_DB_PATH")
    if not path:
        raise HarborError(
            code="CONFIGURATION_ERROR",
            message="CHARLES_DB_PATH is not configured.",
            status_code=500,
        )
    return path


def _get_charles_module(db_path: str):
    """Instantiate CharlesModule with shell-provided db_path."""
    from charles.module_entry import CharlesModule
    return CharlesModule(db_path)


# ------------------------------------------------------------------
# POST /v1/workspaces/<workspace_id>/charles/sync
# ------------------------------------------------------------------

@charles_bp.route(
    "/v1/workspaces/<uuid:workspace_id>/charles/sync",
    methods=["POST"],
)
def charles_sync(workspace_id):
    """Trigger a sync for a workspace.

    Shell gates, builds WEC, invokes CharlesModule.run_sync(wec).
    """
    request_id = request.headers.get("X-Request-ID")
    wec = gate_charles_execution(workspace_id, request_id=request_id)

    db_path = _get_charles_db_path()
    module = _get_charles_module(db_path)
    result = module.run_sync(wec)

    return jsonify({"status": "ok", "result": result}), 200


# ------------------------------------------------------------------
# POST /v1/workspaces/<workspace_id>/charles/deposit
# ------------------------------------------------------------------

@charles_bp.route(
    "/v1/workspaces/<uuid:workspace_id>/charles/deposit",
    methods=["POST"],
)
def charles_deposit(workspace_id):
    """Build a deposit for a specific payout.

    Shell gates, builds WEC, invokes CharlesModule.build_deposit(wec, payout_id).
    """
    data = request.get_json(silent=True)
    if data is None or not data.get("payout_id"):
        raise HarborError(
            code="BAD_REQUEST",
            message="Request body must include payout_id.",
            status_code=400,
        )

    payout_id = data["payout_id"]
    request_id = request.headers.get("X-Request-ID")
    wec = gate_charles_execution(workspace_id, request_id=request_id)

    db_path = _get_charles_db_path()
    module = _get_charles_module(db_path)
    result = module.build_deposit(wec, payout_id)

    return jsonify({"status": "ok", "result": result}), 200


# ------------------------------------------------------------------
# GET /v1/workspaces/<workspace_id>/charles/status
# ------------------------------------------------------------------

@charles_bp.route(
    "/v1/workspaces/<uuid:workspace_id>/charles/status",
    methods=["GET"],
)
def charles_status(workspace_id):
    """Return Charles sync status for a workspace.

    Requires activation gate (ensures workspace is entitled and connected).
    """
    request_id = request.headers.get("X-Request-ID")
    wec = gate_charles_execution(workspace_id, request_id=request_id)

    return jsonify({
        "workspace_id": wec.workspace_id,
        "app_id": wec.app_id,
        "qbo_realm_id": wec.qbo_realm_id,
        "status": "ready",
    }), 200
