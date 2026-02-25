"""Harbor Phase 2 — QBO Connection lifecycle.

Implements:
- State machine transition validation (contract: activation_qbo_state_machine.md)
- OAuth state generation and validation
- Token exchange with Intuit
- Connection persistence with realm binding enforcement
- Advisory locking per contract Part V
"""

import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from uuid import UUID

import requests as http_requests

from harbor_common.crypto import encrypt_str, decrypt_str, is_fernet_token
from harbor_common.db import get_db
from harbor_common.errors import HarborError
from harbor_common.licensing import require_qbo_entitlement
from harbor_common.locks import workspace_id_hash

# ---------------------------------------------------------------------------
# QBO Connection States (contract: activation_qbo_state_machine.md Part III)
# ---------------------------------------------------------------------------

QBO_STATES = frozenset({
    "NOT_CONNECTED",
    "OAUTH_PENDING",
    "CONNECTED",
    "TOKEN_REFRESH_FAILED",
    "REVOKED",
    "ERROR",
    "DISCONNECTED",
})

# ---------------------------------------------------------------------------
# Allowed transitions (contract: activation_qbo_state_machine.md Part IV)
# ---------------------------------------------------------------------------
# Key = (from_status, to_status).  All unlisted pairs are invalid.

ALLOWED_TRANSITIONS = frozenset({
    # Q1 — Start Connect
    ("NOT_CONNECTED", "OAUTH_PENDING"),
    ("ERROR", "OAUTH_PENDING"),
    ("DISCONNECTED", "OAUTH_PENDING"),
    ("TOKEN_REFRESH_FAILED", "OAUTH_PENDING"),
    # Q2 — OAuth Success
    ("OAUTH_PENDING", "CONNECTED"),
    # Q3 — OAuth Failure
    ("OAUTH_PENDING", "ERROR"),
    # Q4 — Refresh Success
    ("CONNECTED", "CONNECTED"),
    ("TOKEN_REFRESH_FAILED", "CONNECTED"),
    # Q5 — Refresh Failure (transient)
    ("CONNECTED", "TOKEN_REFRESH_FAILED"),
    # Q6 — Revoked
    ("CONNECTED", "REVOKED"),
    ("TOKEN_REFRESH_FAILED", "REVOKED"),
    # Q7 — Disconnect (Any → DISCONNECTED)
    ("NOT_CONNECTED", "DISCONNECTED"),
    ("OAUTH_PENDING", "DISCONNECTED"),
    ("CONNECTED", "DISCONNECTED"),
    ("TOKEN_REFRESH_FAILED", "DISCONNECTED"),
    ("REVOKED", "DISCONNECTED"),
    ("ERROR", "DISCONNECTED"),
    ("DISCONNECTED", "DISCONNECTED"),
})


def validate_qbo_transition(from_status: str, to_status: str) -> None:
    """Enforce state machine transitions per contract.

    Raises HarborError INVALID_STATE_TRANSITION on illegal transition.
    """
    if from_status not in QBO_STATES:
        raise HarborError(
            code="INVALID_STATE_TRANSITION",
            message=f"Unknown current state: {from_status}",
            metadata={"from_status": from_status, "to_status": to_status},
        )
    if to_status not in QBO_STATES:
        raise HarborError(
            code="INVALID_STATE_TRANSITION",
            message=f"Unknown target state: {to_status}",
            metadata={"from_status": from_status, "to_status": to_status},
        )
    if (from_status, to_status) not in ALLOWED_TRANSITIONS:
        raise HarborError(
            code="INVALID_STATE_TRANSITION",
            message=f"Transition from {from_status} to {to_status} is not allowed.",
            metadata={"from_status": from_status, "to_status": to_status},
        )


# ---------------------------------------------------------------------------
# OAuth state helpers
# ---------------------------------------------------------------------------

OAUTH_STATE_TTL_MINUTES = 10


def _hash_state(state_token: str) -> str:
    """SHA-256 hash of the state token for DB storage."""
    return hashlib.sha256(state_token.encode()).hexdigest()


def create_oauth_state(workspace_id: UUID) -> str:
    """Generate a cryptographically strong state token, persist it, return raw token."""
    db = get_db()
    cur = db.cursor()
    state_token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=OAUTH_STATE_TTL_MINUTES)

    cur.execute(
        """
        INSERT INTO qbo_oauth_states (workspace_id, state, created_at, expires_at)
        VALUES (%s, %s, now(), %s)
        """,
        (str(workspace_id), state_token, expires_at),
    )
    # Not committed here — caller controls transaction boundary.
    return state_token


def consume_oauth_state(state_token: str):
    """Validate and consume an OAuth state token. Returns workspace_id.

    Must be called inside an open transaction — consume is atomic with
    subsequent connection writes.
    """
    db = get_db()
    cur = db.cursor()
    now = datetime.now(timezone.utc)

    cur.execute(
        """
        SELECT id, workspace_id, expires_at, consumed_at
          FROM qbo_oauth_states
         WHERE state = %s
           FOR UPDATE
        """,
        (state_token,),
    )
    row = cur.fetchone()

    if row is None:
        raise HarborError(
            code="INVALID_OAUTH_STATE",
            message="OAuth state token is invalid or does not exist.",
            status_code=400,
        )

    if row["consumed_at"] is not None:
        raise HarborError(
            code="INVALID_OAUTH_STATE",
            message="OAuth state token has already been consumed.",
            status_code=400,
        )

    if row["expires_at"] <= now:
        raise HarborError(
            code="INVALID_OAUTH_STATE",
            message="OAuth state token has expired.",
            status_code=400,
        )

    # Mark consumed
    cur.execute(
        "UPDATE qbo_oauth_states SET consumed_at = %s WHERE id = %s",
        (now, str(row["id"])),
    )

    return row["workspace_id"]


# ---------------------------------------------------------------------------
# Intuit token exchange
# ---------------------------------------------------------------------------

INTUIT_TOKEN_URL = os.environ.get(
    "INTUIT_TOKEN_URL",
    "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
)
INTUIT_CLIENT_ID = os.environ.get("INTUIT_CLIENT_ID", "")
INTUIT_CLIENT_SECRET = os.environ.get("INTUIT_CLIENT_SECRET", "")
INTUIT_REDIRECT_URI = os.environ.get("INTUIT_REDIRECT_URI", "")
INTUIT_AUTH_BASE_URL = os.environ.get(
    "INTUIT_AUTH_BASE_URL",
    "https://appcenter.intuit.com/connect/oauth2",
)


def exchange_code_for_tokens(code: str, redirect_uri: str | None = None):
    """Exchange an authorization code for access + refresh tokens.

    Returns dict with keys: access_token, refresh_token, expires_in.
    Raises HarborError on failure.
    """
    uri = redirect_uri or INTUIT_REDIRECT_URI
    if not INTUIT_CLIENT_ID or not INTUIT_CLIENT_SECRET:
        raise HarborError(
            code="QBO_CONFIG_ERROR",
            message="Intuit OAuth client credentials are not configured.",
            status_code=500,
        )

    resp = http_requests.post(
        INTUIT_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": uri,
        },
        auth=(INTUIT_CLIENT_ID, INTUIT_CLIENT_SECRET),
        headers={"Accept": "application/json"},
        timeout=30,
    )

    if resp.status_code != 200:
        raise HarborError(
            code="QBO_TOKEN_EXCHANGE_FAILED",
            message="Failed to exchange authorization code for tokens.",
            metadata={"intuit_status": resp.status_code},
            status_code=502,
        )

    data = resp.json()
    required_keys = ("access_token", "refresh_token", "expires_in")
    for key in required_keys:
        if key not in data:
            raise HarborError(
                code="QBO_TOKEN_EXCHANGE_FAILED",
                message=f"Intuit token response missing required field: {key}",
                status_code=502,
            )

    return data


def build_authorize_url(state: str, redirect_uri: str | None = None) -> str:
    """Build the Intuit OAuth2 authorization URL."""
    uri = redirect_uri or INTUIT_REDIRECT_URI
    params = (
        f"?client_id={INTUIT_CLIENT_ID}"
        f"&response_type=code"
        f"&scope=com.intuit.quickbooks.accounting"
        f"&redirect_uri={uri}"
        f"&state={state}"
    )
    return INTUIT_AUTH_BASE_URL + params


# ---------------------------------------------------------------------------
# Connection persistence
# ---------------------------------------------------------------------------

def start_connect(workspace_id: UUID) -> dict:
    """Q1 — Start OAuth connect flow.

    Upserts a qbo_connections row to OAUTH_PENDING.
    Returns dict with authorize_url.
    """
    db = get_db()
    cur = db.cursor()

    # Verify workspace exists
    cur.execute(
        "SELECT id FROM workspaces WHERE id = %s AND deleted_at IS NULL",
        (str(workspace_id),),
    )
    if cur.fetchone() is None:
        raise HarborError(
            code="NOT_FOUND",
            message="Workspace not found.",
            metadata={"workspace_id": str(workspace_id)},
            status_code=404,
        )

    # Advisory lock per contract Part V
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (workspace_id_hash(workspace_id),))

    # Check current connection state (if any)
    cur.execute(
        """
        SELECT id, status FROM qbo_connections
         WHERE workspace_id = %s
           FOR UPDATE
        """,
        (str(workspace_id),),
    )
    existing = cur.fetchone()

    if existing is None:
        # No connection row yet — initial state is NOT_CONNECTED, transition to OAUTH_PENDING
        validate_qbo_transition("NOT_CONNECTED", "OAUTH_PENDING")
        state_token = create_oauth_state(workspace_id)
        state_hash = _hash_state(state_token)
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=OAUTH_STATE_TTL_MINUTES)

        cur.execute(
            """
            INSERT INTO qbo_connections
                (workspace_id, status, oauth_state_hash, oauth_state_expires_at)
            VALUES (%s, 'OAUTH_PENDING', %s, %s)
            """,
            (str(workspace_id), state_hash, expires_at),
        )
    else:
        # Validate transition from current state
        validate_qbo_transition(existing["status"], "OAUTH_PENDING")
        state_token = create_oauth_state(workspace_id)
        state_hash = _hash_state(state_token)
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=OAUTH_STATE_TTL_MINUTES)

        cur.execute(
            """
            UPDATE qbo_connections
               SET status = 'OAUTH_PENDING',
                   oauth_state_hash = %s,
                   oauth_state_expires_at = %s,
                   last_error_code = NULL,
                   last_error_message = NULL,
                   last_error_at = NULL,
                   updated_at = now()
             WHERE workspace_id = %s
            """,
            (state_hash, expires_at, str(workspace_id)),
        )

    db.commit()

    authorize_url = build_authorize_url(state_token)
    return {
        "authorize_url": authorize_url,
        "state": state_token,
        "expires_in_seconds": OAUTH_STATE_TTL_MINUTES * 60,
    }


def handle_oauth_callback(code: str, realm_id: str, state_token: str) -> dict:
    """Q2 — OAuth callback handler.

    Validates state, exchanges code for tokens, binds realm, persists connection.
    All writes happen in a single transaction.
    """
    db = get_db()
    cur = db.cursor()

    # 1. Validate and consume state token (inside transaction)
    workspace_id = consume_oauth_state(state_token)

    # Guard: entitlement must be valid at callback time (contract compliance)
    require_qbo_entitlement(workspace_id)

    # 2. Advisory lock per contract Part V
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (workspace_id_hash(workspace_id),))

    # 3. Exchange code for tokens (external HTTP call — happens inside our txn,
    #    but before any connection writes; if this fails, nothing is written)
    token_data = exchange_code_for_tokens(code)
    now = datetime.now(timezone.utc)
    access_token_plain = token_data["access_token"]
    refresh_token_plain = token_data["refresh_token"]
    expires_in = token_data["expires_in"]
    token_expires_at = now + timedelta(seconds=expires_in)

    # 4. Encrypt tokens at application layer before storage
    access_token_enc = encrypt_str(access_token_plain)
    refresh_token_enc = encrypt_str(refresh_token_plain)

    # 5. Lock and read current connection row
    cur.execute(
        """
        SELECT id, workspace_id, status, qbo_realm_id
          FROM qbo_connections
         WHERE workspace_id = %s
           FOR UPDATE
        """,
        (str(workspace_id),),
    )
    conn_row = cur.fetchone()

    if conn_row is None:
        raise HarborError(
            code="INVALID_STATE_TRANSITION",
            message="No connection record found for this workspace.",
            metadata={"workspace_id": str(workspace_id)},
        )

    # 6. Validate state transition: must be OAUTH_PENDING → CONNECTED
    validate_qbo_transition(conn_row["status"], "CONNECTED")

    # 7. Check realm binding — realm_id must not be bound to another workspace
    cur.execute(
        """
        SELECT workspace_id FROM qbo_connections
         WHERE qbo_realm_id = %s AND workspace_id != %s
        """,
        (realm_id, str(workspace_id)),
    )
    conflict = cur.fetchone()
    if conflict is not None:
        # Per contract Section 4: set status = ERROR, set last_error_code
        # Q3 transition: OAUTH_PENDING → ERROR
        validate_qbo_transition(conn_row["status"], "ERROR")
        cur.execute(
            """
            UPDATE qbo_connections
               SET status = 'ERROR',
                   last_error_code = 'REALM_ALREADY_BOUND',
                   last_error_message = %s,
                   last_error_at = %s,
                   updated_at = now()
             WHERE workspace_id = %s
            """,
            (
                f"Realm {realm_id} is already bound to workspace {conflict['workspace_id']}.",
                now,
                str(workspace_id),
            ),
        )
        db.commit()
        raise HarborError(
            code="QBO_REALM_ALREADY_BOUND",
            message="This QBO company is already connected to another workspace.",
            metadata={
                "realm_id": realm_id,
                "workspace_id": str(workspace_id),
            },
            status_code=409,
        )

    # 8. Update connection row — bind realm, store tokens (encrypted), set CONNECTED
    cur.execute(
        """
        UPDATE qbo_connections
           SET status = 'CONNECTED',
               qbo_realm_id = %s,
               access_token = %s,
               refresh_token = %s,
               expires_at = %s,
               connected_at = %s,
               oauth_state_hash = NULL,
               oauth_state_expires_at = NULL,
               last_error_code = NULL,
               last_error_message = NULL,
               last_error_at = NULL,
               updated_at = now()
         WHERE workspace_id = %s
        """,
        (
            realm_id,
            access_token_enc,
            refresh_token_enc,
            token_expires_at,
            now,
            str(workspace_id),
        ),
    )

    db.commit()

    return {
        "workspace_id": str(workspace_id),
        "realm_id": realm_id,
        "status": "CONNECTED",
        "connected_at": now.isoformat(),
    }


def get_qbo_tokens(workspace_id: UUID) -> dict:
    """Retrieve and decrypt QBO tokens for a workspace.
    
    Implements backward compatibility: if token does not appear to be
    encrypted (via is_fernet_token check), it is returned as plaintext.
    
    Returns:
        dict: {access_token, refresh_token, qbo_realm_id, status}
    """
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        SELECT access_token, refresh_token, qbo_realm_id, status
          FROM qbo_connections
         WHERE workspace_id = %s
        """,
        (str(workspace_id),),
    )
    row = cur.fetchone()
    if not row:
        raise HarborError(
            code="NOT_FOUND",
            message="No QBO connection found for this workspace.",
            status_code=404,
        )
    
    def _decrypt_if_needed(val: str | None) -> str | None:
        if val is None:
            return None
        if is_fernet_token(val):
            return decrypt_str(val)
        return val

    return {
        "access_token": _decrypt_if_needed(row["access_token"]),
        "refresh_token": _decrypt_if_needed(row["refresh_token"]),
        "qbo_realm_id": row["qbo_realm_id"],
        "status": row["status"],
    }
