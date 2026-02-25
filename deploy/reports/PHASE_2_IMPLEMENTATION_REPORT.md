# HARBOR — PHASE 2 IMPLEMENTATION REPORT

Status: Post-Implementation
Phase: 2 — QBO Connection Lifecycle + OAuth Flow
Mode: Exhaustive Disclosure

---

# 1. SCHEMA — NEW TABLES AND MIGRATIONS

## 1.1 Migration File

**File:** `deploy/migrations/003_qbo_connections_phase2.sql`

Full contents:

```sql
/* Harbor Phase 2 — QBO Connection lifecycle + OAuth state storage
   Aligns harbor.qbo_connections with qbo_connection_contract.md and
   activation_qbo_state_machine.md.
   Creates harbor.qbo_oauth_states for server-side OAuth state persistence.

   Run after 002_workspace_licenses.sql:
     psql -h localhost -U harbor_app -d harbor -v ON_ERROR_STOP=1 -f 003_qbo_connections_phase2.sql
*/

BEGIN;

SET search_path = harbor;

-- -------------------------------------------------------------------
-- qbo_connections — add contract-required columns
-- -------------------------------------------------------------------

-- 1. Add status column with CHECK constraint matching contract states
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'NOT_CONNECTED'
    CHECK (status IN (
      'NOT_CONNECTED',
      'OAUTH_PENDING',
      'CONNECTED',
      'TOKEN_REFRESH_FAILED',
      'REVOKED',
      'ERROR',
      'DISCONNECTED'
    ));

-- 2. Add realm_id column (contract field: qbo_realm_id, UNIQUE across all workspaces)
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS qbo_realm_id TEXT NULL;

ALTER TABLE harbor.qbo_connections
  ADD CONSTRAINT uq_qbo_conn_realm_id UNIQUE (qbo_realm_id);

ALTER TABLE harbor.qbo_connections
  ADD CONSTRAINT qbo_conn_realm_id_nonempty
    CHECK (qbo_realm_id IS NULL OR length(trim(qbo_realm_id)) > 0);

-- 3. Add OAuth state hash and expiry (for state validation on callback)
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS oauth_state_hash TEXT NULL;

ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS oauth_state_expires_at TIMESTAMPTZ NULL;

-- 4. Add error tracking columns
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS last_error_code TEXT NULL;

ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS last_error_message TEXT NULL;

ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS last_error_at TIMESTAMPTZ NULL;

-- 5. Add connected_at timestamp
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS connected_at TIMESTAMPTZ NULL;

-- 6. Add refresh_token_expires_at (nullable per contract)
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS refresh_token_expires_at TIMESTAMPTZ NULL;

-- -------------------------------------------------------------------
-- qbo_oauth_states — server-side OAuth state persistence
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.qbo_oauth_states (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id    UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  state           TEXT NOT NULL UNIQUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at      TIMESTAMPTZ NOT NULL,
  consumed_at     TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_qbo_oauth_states_workspace
  ON harbor.qbo_oauth_states(workspace_id);

CREATE INDEX IF NOT EXISTS idx_qbo_oauth_states_expires
  ON harbor.qbo_oauth_states(expires_at);

COMMIT;
```

## 1.2 Resulting qbo_connections Table (After 001 + 003)

```sql
CREATE TABLE harbor.qbo_connections (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id            UUID NOT NULL UNIQUE REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  qbo_app_key             TEXT NOT NULL DEFAULT 'harbor'
                            CHECK (qbo_app_key IN ('harbor')),
  access_token            TEXT NULL,
  refresh_token           TEXT NULL,
  token_type              TEXT NULL,
  expires_at              TIMESTAMPTZ NULL,
  connected_at            TIMESTAMPTZ NULL,
  last_refresh_at         TIMESTAMPTZ NULL,
  revoked_at              TIMESTAMPTZ NULL,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Phase 2 additions:
  status                  TEXT NOT NULL DEFAULT 'NOT_CONNECTED'
                            CHECK (status IN (
                              'NOT_CONNECTED','OAUTH_PENDING','CONNECTED',
                              'TOKEN_REFRESH_FAILED','REVOKED','ERROR','DISCONNECTED'
                            )),
  qbo_realm_id            TEXT NULL,
  oauth_state_hash        TEXT NULL,
  oauth_state_expires_at  TIMESTAMPTZ NULL,
  last_error_code         TEXT NULL,
  last_error_message      TEXT NULL,
  last_error_at           TIMESTAMPTZ NULL,
  refresh_token_expires_at TIMESTAMPTZ NULL
);

-- Constraints:
--   UNIQUE(workspace_id) — from 001
--   UNIQUE(qbo_realm_id) — uq_qbo_conn_realm_id from 003
--   CHECK qbo_conn_realm_id_nonempty — from 003

-- Indexes:
--   idx_qbo_connections_expires ON (expires_at) — from 001
```

## 1.3 qbo_oauth_states Table

```sql
CREATE TABLE harbor.qbo_oauth_states (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id    UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  state           TEXT NOT NULL UNIQUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at      TIMESTAMPTZ NOT NULL,
  consumed_at     TIMESTAMPTZ NULL
);

CREATE INDEX idx_qbo_oauth_states_workspace ON harbor.qbo_oauth_states(workspace_id);
CREATE INDEX idx_qbo_oauth_states_expires ON harbor.qbo_oauth_states(expires_at);
```

---

# 2. STATE MACHINE ENFORCEMENT

## 2.1 States (contract: activation_qbo_state_machine.md Part III)

```
QC0 — NOT_CONNECTED
QC1 — OAUTH_PENDING
QC2 — CONNECTED
QC3 — TOKEN_REFRESH_FAILED
QC4 — REVOKED
QC5 — ERROR
QC6 — DISCONNECTED
```

## 2.2 Allowed Transition Map

```python
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
```

## 2.3 Transition Validator

```python
def validate_qbo_transition(from_status: str, to_status: str) -> None:
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
```

All status changes in `start_connect` and `handle_oauth_callback` go through this validator. No silent upgrades. No implicit transitions.

---

# 3. ROUTE IMPLEMENTATIONS

## 3.1 POST /v1/workspaces/{workspace_id}/qbo/connect

File: `shell/app/routes.py`

```python
@bp.route("/v1/workspaces/<uuid:workspace_id>/qbo/connect", methods=["POST"])
def qbo_connect_start(workspace_id):
    """Q1 — Initiate QBO OAuth connect flow.

    Requires valid QBO entitlement. Returns authorize URL.
    """
    require_qbo_entitlement(workspace_id)
    result = start_connect(workspace_id)
    return jsonify(result), 200
```

Flow:
1. `require_qbo_entitlement(workspace_id)` — Phase 1 guard. Raises `QBO_ENTITLEMENT_REQUIRED` (403) if no valid QBO-dependent license exists.
2. `start_connect(workspace_id)` — see Section 4.1 below.
3. Returns JSON: `{"authorize_url": "...", "state": "...", "expires_in_seconds": 600}`

## 3.2 GET /v1/qbo/callback

File: `shell/app/routes.py`

```python
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
```

Flow:
1. Extract `code`, `realmId`, `state` from query parameters.
2. Validate all three are present.
3. `handle_oauth_callback(...)` — see Section 4.2 below.
4. Returns JSON: `{"workspace_id": "...", "realm_id": "...", "status": "CONNECTED", "connected_at": "..."}`

---

# 4. CONNECTION LOGIC (shared/harbor_common/qbo_connection.py)

## 4.1 start_connect(workspace_id)

```python
def start_connect(workspace_id: UUID) -> dict:
    db = get_db()
    cur = db.cursor()

    # Verify workspace exists
    cur.execute(
        "SELECT id FROM workspaces WHERE id = %s AND deleted_at IS NULL",
        (str(workspace_id),),
    )
    if cur.fetchone() is None:
        raise HarborError(code="NOT_FOUND", ...)

    # Advisory lock per contract Part V
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (_workspace_id_hash(workspace_id),))

    # Check current connection state (if any)
    cur.execute(
        "SELECT id, status FROM qbo_connections WHERE workspace_id = %s FOR UPDATE",
        (str(workspace_id),),
    )
    existing = cur.fetchone()

    if existing is None:
        # No row — implicit NOT_CONNECTED → OAUTH_PENDING
        validate_qbo_transition("NOT_CONNECTED", "OAUTH_PENDING")
        state_token = create_oauth_state(workspace_id)
        state_hash = _hash_state(state_token)
        cur.execute(
            "INSERT INTO qbo_connections (workspace_id, status, oauth_state_hash, oauth_state_expires_at) "
            "VALUES (%s, 'OAUTH_PENDING', %s, %s)",
            (str(workspace_id), state_hash, expires_at),
        )
    else:
        validate_qbo_transition(existing["status"], "OAUTH_PENDING")
        state_token = create_oauth_state(workspace_id)
        state_hash = _hash_state(state_token)
        cur.execute(
            "UPDATE qbo_connections SET status = 'OAUTH_PENDING', "
            "oauth_state_hash = %s, oauth_state_expires_at = %s, "
            "last_error_code = NULL, last_error_message = NULL, last_error_at = NULL, "
            "updated_at = now() WHERE workspace_id = %s",
            (state_hash, expires_at, str(workspace_id)),
        )

    db.commit()
    authorize_url = build_authorize_url(state_token)
    return {"authorize_url": authorize_url, "state": state_token, "expires_in_seconds": 600}
```

## 4.2 handle_oauth_callback(code, realm_id, state_token)

```python
def handle_oauth_callback(code: str, realm_id: str, state_token: str) -> dict:
    db = get_db()
    cur = db.cursor()

    # 1. Validate and consume state token (inside transaction)
    workspace_id = consume_oauth_state(state_token)

    # 2. Advisory lock per contract Part V
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (_workspace_id_hash(workspace_id),))

    # 3. Exchange code for tokens (external HTTP call)
    token_data = exchange_code_for_tokens(code)
    now = datetime.now(timezone.utc)
    access_token = token_data["access_token"]
    refresh_token = token_data["refresh_token"]
    expires_in = token_data["expires_in"]
    token_expires_at = now + timedelta(seconds=expires_in)

    # 4. Lock and read current connection row
    cur.execute(
        "SELECT id, workspace_id, status, qbo_realm_id FROM qbo_connections "
        "WHERE workspace_id = %s FOR UPDATE",
        (str(workspace_id),),
    )
    conn_row = cur.fetchone()

    if conn_row is None:
        raise HarborError(code="INVALID_STATE_TRANSITION", ...)

    # 5. Validate state transition: OAUTH_PENDING → CONNECTED
    validate_qbo_transition(conn_row["status"], "CONNECTED")

    # 6. Check realm binding
    cur.execute(
        "SELECT workspace_id FROM qbo_connections "
        "WHERE qbo_realm_id = %s AND workspace_id != %s",
        (realm_id, str(workspace_id)),
    )
    conflict = cur.fetchone()
    if conflict is not None:
        # Q3: OAUTH_PENDING → ERROR
        validate_qbo_transition(conn_row["status"], "ERROR")
        cur.execute(
            "UPDATE qbo_connections SET status = 'ERROR', "
            "last_error_code = 'REALM_ALREADY_BOUND', "
            "last_error_message = %s, last_error_at = %s, "
            "updated_at = now() WHERE workspace_id = %s",
            (...),
        )
        db.commit()
        raise HarborError(code="QBO_REALM_ALREADY_BOUND", ..., status_code=409)

    # 7. Update connection row — bind realm, store tokens, set CONNECTED
    cur.execute(
        "UPDATE qbo_connections SET status = 'CONNECTED', "
        "qbo_realm_id = %s, access_token = %s, refresh_token = %s, "
        "expires_at = %s, connected_at = %s, "
        "oauth_state_hash = NULL, oauth_state_expires_at = NULL, "
        "last_error_code = NULL, last_error_message = NULL, last_error_at = NULL, "
        "updated_at = now() WHERE workspace_id = %s",
        (realm_id, access_token, refresh_token, token_expires_at, now, str(workspace_id)),
    )

    db.commit()
    return {"workspace_id": str(workspace_id), "realm_id": realm_id,
            "status": "CONNECTED", "connected_at": now.isoformat()}
```

---

# 5. TOKEN EXCHANGE LOGIC

## 5.1 Intuit OAuth Token Endpoint

```python
INTUIT_TOKEN_URL = os.environ.get(
    "INTUIT_TOKEN_URL",
    "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
)
```

## 5.2 exchange_code_for_tokens(code, redirect_uri)

```python
def exchange_code_for_tokens(code: str, redirect_uri: str | None = None):
    uri = redirect_uri or INTUIT_REDIRECT_URI
    if not INTUIT_CLIENT_ID or not INTUIT_CLIENT_SECRET:
        raise HarborError(code="QBO_CONFIG_ERROR", ..., status_code=500)

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
        raise HarborError(code="QBO_TOKEN_EXCHANGE_FAILED", ..., status_code=502)

    data = resp.json()
    # Validates: access_token, refresh_token, expires_in all present
    return data
```

Parameters used:
- `grant_type`: `"authorization_code"`
- `code`: The authorization code from Intuit callback
- `redirect_uri`: From `INTUIT_REDIRECT_URI` env var
- Auth: HTTP Basic with `INTUIT_CLIENT_ID`:`INTUIT_CLIENT_SECRET`

## 5.3 build_authorize_url(state)

```python
def build_authorize_url(state: str, redirect_uri: str | None = None) -> str:
    uri = redirect_uri or INTUIT_REDIRECT_URI
    params = (
        f"?client_id={INTUIT_CLIENT_ID}"
        f"&response_type=code"
        f"&scope=com.intuit.quickbooks.accounting"
        f"&redirect_uri={uri}"
        f"&state={state}"
    )
    return INTUIT_AUTH_BASE_URL + params
```

---

# 6. TOKEN STORAGE POSTURE

Tokens (`access_token`, `refresh_token`) are stored as **plaintext** in the `qbo_connections` table.

Harbor does not currently have an application-level encryption helper in `shared/`. No encryption helper was found in any existing module.

**Current protection:** Encrypted at disk layer only (Postgres data directory on encrypted volume, if configured at the infrastructure level).

**Phase 4 hardening steps (recommended):**
1. Add an application-level encryption module to `shared/harbor_common/` using Fernet (symmetric) or envelope encryption with a KMS.
2. Encrypt `access_token` and `refresh_token` before INSERT/UPDATE.
3. Decrypt on read for QBO API calls.
4. Rename columns to `access_token_encrypted` and `refresh_token_encrypted` per contract naming.
5. Rotate encryption keys periodically.

---

# 7. OAUTH STATE MANAGEMENT

## 7.1 State Generation

- `secrets.token_urlsafe(32)` — 256 bits of cryptographic randomness, URL-safe base64 encoded
- Stored in `qbo_oauth_states.state` column (raw token value)
- SHA-256 hash of the token stored in `qbo_connections.oauth_state_hash` for connection-level tracking
- TTL: 10 minutes (`OAUTH_STATE_TTL_MINUTES = 10`)

## 7.2 State Consumption

```python
def consume_oauth_state(state_token: str):
    # SELECT ... FOR UPDATE locks the row
    # Validates: exists, not consumed, not expired
    # Marks consumed_at = now()
    # Returns workspace_id
```

Consumption happens inside the same transaction as the connection write. If the transaction rolls back, the state remains unconsumed.

---

# 8. IDEMPOTENCY EXPLANATION

## 8.1 Duplicate Callback Protection

**Scenario: User refreshes browser on callback URL (same state/code).**

1. First callback: `consume_oauth_state` finds state row, marks `consumed_at = now()`, proceeds with token exchange and connection write. Transaction commits.
2. Second callback: `consume_oauth_state` finds state row with `consumed_at IS NOT NULL`. Raises `INVALID_OAUTH_STATE` ("already consumed"). No token exchange occurs. No connection mutation.

**Scenario: Concurrent callback execution (two requests hit callback simultaneously with same state).**

1. Request A: `SELECT ... FOR UPDATE` on `qbo_oauth_states` row — acquires row lock.
2. Request B: `SELECT ... FOR UPDATE` on same row — blocks waiting for A's lock.
3. Request A: Marks consumed, exchanges tokens, writes connection, commits. Row lock released.
4. Request B: Unblocks, re-reads row. `consumed_at IS NOT NULL`. Raises `INVALID_OAUTH_STATE`. No mutation.

## 8.2 Realm Binding Safety

**Scenario: Two workspaces attempt to bind the same realm_id simultaneously.**

1. Both must go through the callback path.
2. Each acquires `pg_advisory_xact_lock` on their own workspace_id — these are different locks and don't block each other.
3. However, the realm binding check (`SELECT workspace_id FROM qbo_connections WHERE qbo_realm_id = %s AND workspace_id != %s`) runs inside the transaction.
4. The first to commit wins: its `qbo_realm_id` is set.
5. The second sees the committed row in the realm check (or hits the `UNIQUE(qbo_realm_id)` constraint). Raises `QBO_REALM_ALREADY_BOUND` and sets its own connection to ERROR status.
6. Additionally, `UNIQUE(qbo_realm_id)` at the DB level is a backstop: even if the application-level check were bypassed, the constraint prevents duplicate binding.

## 8.3 Connect Start Idempotency

**Scenario: User clicks "Connect to QBO" twice rapidly.**

1. First request: Acquires advisory lock, either inserts or updates connection to `OAUTH_PENDING`, creates state token, commits.
2. Second request: Acquires advisory lock (serialized), reads connection row (status is now `OAUTH_PENDING`). However, `OAUTH_PENDING` is not in the allowed Q1 "from" states (`NOT_CONNECTED`, `ERROR`, `DISCONNECTED`, `TOKEN_REFRESH_FAILED`). Raises `INVALID_STATE_TRANSITION`.

This is by design: once OAuth is pending, the user should complete or abandon the flow rather than start a new one. If the state expires, the next attempt will find `OAUTH_PENDING` — this requires a reconnect from a non-pending state. A future enhancement could auto-expire stale OAUTH_PENDING states, but that is outside Phase 2 scope.

**Alternative: If the connection is in a re-connectable state** (ERROR, DISCONNECTED, TOKEN_REFRESH_FAILED), the transition is valid and proceeds normally. A new state token is generated, and the old state token (if any) remains in `qbo_oauth_states` but will expire naturally or be rejected as consumed if the old callback arrives.

---

# 9. CONCURRENCY / LOCKING EXPLANATION

## 9.1 Advisory Lock

Per contract Part V, all OAuth callback writes and token operations use:

```python
cur.execute("SELECT pg_advisory_xact_lock(%s)", (_workspace_id_hash(workspace_id),))
```

`_workspace_id_hash` produces a deterministic int64 from the workspace UUID via SHA-256, ensuring consistent lock identity. The lock is transaction-scoped (`pg_advisory_xact_lock`) and automatically released on commit or rollback.

## 9.2 Row-Level Locks

- `qbo_connections` row locked with `SELECT ... FOR UPDATE` before any status read or write.
- `qbo_oauth_states` row locked with `SELECT ... FOR UPDATE` before consumption check.

## 9.3 Lock Ordering

Within a single callback transaction:
1. `qbo_oauth_states` row lock (consume state)
2. `pg_advisory_xact_lock(workspace_id)` (serialize per-workspace operations)
3. `qbo_connections` row lock (read + write connection)

This ordering is consistent across all callback invocations, preventing deadlocks.

---

# 10. ERROR CODES

| Error Code | HTTP | Condition |
|---|---|---|
| `QBO_ENTITLEMENT_REQUIRED` | 403 | Workspace lacks valid QBO-dependent license (Phase 1) |
| `QBO_REALM_ALREADY_BOUND` | 409 | realm_id is already connected to a different workspace |
| `INVALID_OAUTH_STATE` | 400 | State token invalid, expired, or already consumed |
| `INVALID_STATE_TRANSITION` | 400 | Attempted transition not allowed by state machine |
| `QBO_TOKEN_EXCHANGE_FAILED` | 502 | Intuit token endpoint returned non-200 or missing fields |
| `QBO_CONFIG_ERROR` | 500 | Intuit OAuth client credentials not configured |
| `NOT_FOUND` | 404 | Workspace does not exist |
| `VALIDATION_ERROR` | 400 | Missing required query parameters on callback |

All errors are returned as structured JSON via the Phase 1 HarborError model:

```json
{
  "error": "<ERROR_CODE>",
  "message": "<human-readable message>",
  ...metadata fields
}
```

---

# 11. FILES CHANGED / ADDED

**New files:**
- `deploy/migrations/003_qbo_connections_phase2.sql` — Migration: adds columns to qbo_connections, creates qbo_oauth_states
- `shared/harbor_common/qbo_connection.py` — State machine, OAuth state management, token exchange, connection persistence
- `deploy/reports/PHASE_2_IMPLEMENTATION_REPORT.md` — This report

**Modified files:**
- `shell/app/routes.py` — Added two endpoints: `POST /v1/workspaces/{workspace_id}/qbo/connect` and `GET /v1/qbo/callback`
- `shell/requirements.txt` — Added `requests>=2.31`
- `shared/setup.py` — Added `requests>=2.31` to install_requires

**Unmodified Phase 1 files:**
- `shared/harbor_common/errors.py` — No changes
- `shared/harbor_common/licensing.py` — No changes
- `shared/harbor_common/db.py` — No changes
- `shared/harbor_common/__init__.py` — No changes
- `deploy/migrations/001_harbor_core.sql` — No changes
- `deploy/migrations/002_workspace_licenses.sql` — No changes
- `contracts/*` — No changes

---

# 12. MANUAL TEST PROCEDURE

No automated test harness exists in the repository. The following manual test procedure validates the Phase 2 implementation:

**Prerequisites:**
- Postgres 16 running with harbor schema (migrations 001-003 applied)
- Environment variables set: `INTUIT_CLIENT_ID`, `INTUIT_CLIENT_SECRET`, `INTUIT_REDIRECT_URI`
- Flask app running via gunicorn

**Test 1 — Entitlement Gate:**
```bash
# Create workspace (Phase 1)
curl -X POST http://localhost:8100/v1/workspaces \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"<uuid>","name":"Test WS","user_id":"<uuid>"}'

# Attempt connect without license — expect 403 QBO_ENTITLEMENT_REQUIRED
curl -X POST http://localhost:8100/v1/workspaces/<ws_id>/qbo/connect
```

**Test 2 — Connect Start (with valid license):**
```bash
# Attach license first
curl -X POST http://localhost:8100/v1/workspaces/<ws_id>/licenses \
  -H 'Content-Type: application/json' \
  -d '{"app_key":"charles","purchase_id":"test-1","status":"active","starts_at":"2026-01-01T00:00:00Z"}'

# Start connect — expect 200 with authorize_url
curl -X POST http://localhost:8100/v1/workspaces/<ws_id>/qbo/connect
```

**Test 3 — Callback with Invalid State:**
```bash
curl "http://localhost:8100/v1/qbo/callback?code=test&realmId=123&state=invalid"
# Expect 400 INVALID_OAUTH_STATE
```

**Test 4 — Double Connect Attempt:**
```bash
# After Test 2, try starting connect again — expect INVALID_STATE_TRANSITION
curl -X POST http://localhost:8100/v1/workspaces/<ws_id>/qbo/connect
```

---

# 13. PHASE BOUNDARY CERTIFICATION

Phase 2 introduced no licensing mutation, no entitlement caching, and no Phase 3 orchestration.

Specifically:

- **No licensing changes:** `workspace_app_licenses` table not modified. `licensing.py` not modified. No new license types, statuses, or purchase_id logic.
- **No entitlement caching:** QBO entitlement is checked live via `require_qbo_entitlement()` from Phase 1 on every connect-start request. No flags stored in DB or memory.
- **No Phase 3 orchestration:** No activation wizard, no multi-step bootstrap flows, no license+connection coordination beyond the entitlement gate.
- **No contract modifications:** All three contract files remain unmodified.
- **No Phase 1 table mutations:** migrations 001 and 002 are untouched. The 003 migration only ALTERs `qbo_connections` (adding columns) and creates a new table (`qbo_oauth_states`).
- **No background workers:** All operations are synchronous, request-scoped.
- **No Redis, no queues, no cron.**
- **No UI pages.**
- **No token auto-refresh jobs.**

---

End of Phase 2 Implementation Report
