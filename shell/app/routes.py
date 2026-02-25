import json
from datetime import datetime
from uuid import UUID

import psycopg2.errors
from flask import Blueprint, jsonify, request
from psycopg2.extras import Json

from harbor_common.db import get_db
from harbor_common.errors import HarborError
from harbor_common.licensing import require_qbo_entitlement, workspace_has_qbo_entitlement
from harbor_common.qbo_connection import handle_oauth_callback, start_connect

bp = Blueprint("shell", __name__)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _serialize(row):
    """Convert a psycopg2 RealDictRow to a JSON-safe dict."""
    out = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, UUID):
            out[k] = str(v)
        else:
            out[k] = v
    return out


def _require_json(*keys):
    data = request.get_json(silent=True)
    if data is None:
        raise HarborError("INVALID_REQUEST", "Request body must be JSON.")
    missing = [k for k in keys if not data.get(k)]
    if missing:
        raise HarborError(
            "VALIDATION_ERROR",
            f"Missing required fields: {', '.join(missing)}",
            status_code=422,
        )
    return data


# ------------------------------------------------------------------
# POST /v1/workspaces
# ------------------------------------------------------------------

@bp.route("/v1/workspaces", methods=["POST"])
def create_workspace():
    data = _require_json("customer_id", "name", "user_id")

    customer_id = data["customer_id"]
    name = data["name"]
    user_id = data["user_id"]

    db = get_db()
    cur = db.cursor()

    # --- idempotency check ---
    idempotency_key = request.headers.get("Idempotency-Key")
    if idempotency_key:
        cur.execute(
            "SELECT response_payload FROM workspace_creation_requests "
            "WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        existing = cur.fetchone()
        if existing:
            return jsonify(existing["response_payload"]), 200

    # --- create workspace + owner membership atomically ---
    try:
        cur.execute(
            """
            INSERT INTO workspaces (customer_id, name, status)
            VALUES (%s, %s, 'active')
            RETURNING id, customer_id, name, status, created_at
            """,
            (customer_id, name),
        )
        workspace = _serialize(cur.fetchone())

        cur.execute(
            """
            INSERT INTO memberships (workspace_id, user_id, role)
            VALUES (%s, %s, 'owner')
            RETURNING id, workspace_id, user_id, role, created_at
            """,
            (workspace["id"], user_id),
        )
        membership = _serialize(cur.fetchone())

        payload = {"workspace": workspace, "membership": membership}

        if idempotency_key:
            cur.execute(
                "INSERT INTO workspace_creation_requests "
                "(idempotency_key, workspace_id, response_payload) "
                "VALUES (%s, %s, %s)",
                (idempotency_key, workspace["id"], Json(payload)),
            )

        db.commit()
        return jsonify(payload), 201

    except psycopg2.errors.UniqueViolation:
        db.rollback()
        # Could be a race on idempotency_key — check for stored response
        if idempotency_key:
            cur = db.cursor()
            cur.execute(
                "SELECT response_payload FROM workspace_creation_requests "
                "WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            existing = cur.fetchone()
            if existing:
                return jsonify(existing["response_payload"]), 200
        raise HarborError(
            "CONFLICT", "Workspace creation conflicted with an existing record.",
            status_code=409,
        )
    except psycopg2.errors.ForeignKeyViolation:
        db.rollback()
        raise HarborError(
            "INVALID_REFERENCE",
            "customer_id or user_id does not reference an existing record.",
            status_code=422,
        )


# ------------------------------------------------------------------
# POST /v1/workspaces/<workspace_id>/licenses
# ------------------------------------------------------------------

@bp.route("/v1/workspaces/<uuid:workspace_id>/licenses", methods=["POST"])
def attach_license(workspace_id):
    data = _require_json("app_key", "purchase_id", "status", "starts_at")

    db = get_db()
    cur = db.cursor()

    # Look up workspace to derive customer_id
    cur.execute(
        "SELECT customer_id FROM workspaces WHERE id = %s AND deleted_at IS NULL",
        (str(workspace_id),),
    )
    ws = cur.fetchone()
    if ws is None:
        raise HarborError(
            "NOT_FOUND", "Workspace not found.",
            metadata={"workspace_id": str(workspace_id)},
            status_code=404,
        )

    app_key = data["app_key"]
    purchase_id = data["purchase_id"]

    try:
        # --- Step 1: Check for existing purchase_id (row-locked) ---
        cur.execute(
            """
            SELECT workspace_id, app_key
              FROM workspace_app_licenses
             WHERE purchase_id = %s
               FOR UPDATE
            """,
            (purchase_id,),
        )
        existing = cur.fetchone()

        if existing is not None:
            # --- Step 2a: Ownership check — block cross-tenant mutation ---
            if (str(existing["workspace_id"]) != str(workspace_id)
                    or existing["app_key"] != app_key):
                db.rollback()
                raise HarborError(
                    "PURCHASE_ID_OWNERSHIP_VIOLATION",
                    "purchase_id is already bound to a different workspace or app.",
                    metadata={
                        "purchase_id": purchase_id,
                        "workspace_id": str(workspace_id),
                        "app_key": app_key,
                    },
                    status_code=409,
                )

            # --- Step 2b: Identity matches — idempotent update ---
            cur.execute(
                """
                UPDATE workspace_app_licenses
                   SET status        = %s,
                       starts_at     = %s,
                       ends_at       = %s,
                       trial_ends_at = %s,
                       updated_at    = now()
                 WHERE purchase_id = %s
                RETURNING *
                """,
                (
                    data["status"],
                    data["starts_at"],
                    data.get("ends_at"),
                    data.get("trial_ends_at"),
                    purchase_id,
                ),
            )
            license_row = _serialize(cur.fetchone())
            db.commit()
            return jsonify({"license": license_row}), 200

        # --- Step 3: No existing row — INSERT ---
        cur.execute(
            """
            INSERT INTO workspace_app_licenses
                (workspace_id, customer_id, app_key, purchase_id,
                 status, starts_at, ends_at, trial_ends_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                str(workspace_id),
                str(ws["customer_id"]),
                app_key,
                purchase_id,
                data["status"],
                data["starts_at"],
                data.get("ends_at"),
                data.get("trial_ends_at"),
            ),
        )
        license_row = _serialize(cur.fetchone())
        db.commit()
        return jsonify({"license": license_row}), 201

    except psycopg2.errors.UniqueViolation:
        db.rollback()
        raise HarborError(
            "LICENSE_CONFLICT",
            "A license for this app already exists on this workspace with a different purchase.",
            metadata={"workspace_id": str(workspace_id), "app_key": app_key},
            status_code=409,
        )
    except psycopg2.errors.ForeignKeyViolation:
        db.rollback()
        raise HarborError(
            "INVALID_APP_KEY",
            f"app_key '{app_key}' is not a registered application.",
            status_code=422,
        )


# ------------------------------------------------------------------
# GET /v1/workspaces/<workspace_id>/licenses
# ------------------------------------------------------------------

@bp.route("/v1/workspaces/<uuid:workspace_id>/licenses", methods=["GET"])
def get_licenses(workspace_id):
    db = get_db()
    cur = db.cursor()

    cur.execute(
        """
        SELECT * FROM workspace_app_licenses
         WHERE workspace_id = %s AND deleted_at IS NULL
        """,
        (str(workspace_id),),
    )
    licenses = [_serialize(row) for row in cur.fetchall()]

    qbo_entitled = workspace_has_qbo_entitlement(workspace_id)

    return jsonify({"licenses": licenses, "qbo_entitled": qbo_entitled}), 200


# ------------------------------------------------------------------
# POST /v1/workspaces/<workspace_id>/qbo/connect
# ------------------------------------------------------------------

@bp.route("/v1/workspaces/<uuid:workspace_id>/qbo/connect", methods=["POST"])
def qbo_connect_start(workspace_id):
    """Q1 — Initiate QBO OAuth connect flow.

    Requires valid QBO entitlement. Returns authorize URL.
    """
    require_qbo_entitlement(workspace_id)
    result = start_connect(workspace_id)
    return jsonify(result), 200


# ------------------------------------------------------------------
# GET /v1/qbo/callback
# ------------------------------------------------------------------

@bp.route("/v1/qbo/callback", methods=["GET"])
def qbo_oauth_callback():
    """Q2 — Handle Intuit OAuth callback.

    Validates state, exchanges code, binds realm, persists connection.
    """
    code = request.args.get("code")
    realm_id = request.args.get("realmId")
    state = request.args.get("state")

    if not code or not realm_id or not state:
        raise HarborError(
            code="VALIDATION_ERROR",
            message="Missing required query parameters: code, realmId, state.",
            status_code=400,
        )

    result = handle_oauth_callback(code=code, realm_id=realm_id, state_token=state)
    return jsonify(result), 200
