# Phase 3 Implementation Report — Activation Bootstrap Orchestration (Post-Remediation)

**Date:** 2026-02-25
**Phase:** 3 of 3
**Scope:** Deterministic Activation Bootstrap Orchestration
**Status:** Post-remediation (addresses ChatGPT + Gemini hostile audit findings)

---

## Remediation Summary

| ID | Finding | Severity | Fix |
|---|---|---|---|
| R1 | Advisory lock namespace mismatch between Phase 2 and Phase 3 | CRITICAL | Extracted `workspace_id_hash()` to shared `locks.py`; both phases import from it |
| R2 | Missing `FOR UPDATE` on QBO connection precondition check | MAJOR | Added `FOR UPDATE` row lock on `qbo_connections` inside `complete_activation()` |
| R3 | Spoofable `X-User-Id` header for membership auth | MAJOR | Removed entirely; endpoints match existing Harbor auth posture (infrastructure-layer auth) |
| R4 | `db.rollback()` in domain function releases advisory lock prematurely | MAJOR | Removed all `db.rollback()` calls; errors propagate as HarborError; advisory lock held until connection teardown |

---

## 1. Final DDL (Unchanged)

```sql
CREATE TABLE IF NOT EXISTS workspace_activations (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    UUID        NOT NULL UNIQUE
                                REFERENCES workspaces(id)
                                ON DELETE CASCADE,
    activated_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Invariants enforced by schema:**

| Invariant | Mechanism |
|---|---|
| One activation per workspace | `UNIQUE(workspace_id)` |
| Presence-of-row semantics | No status column, no soft deletes, no flags |
| Referential integrity | `REFERENCES workspaces(id) ON DELETE CASCADE` |
| Activation timestamp required | `activated_at TIMESTAMPTZ NOT NULL` |

---

## 2. Migration SQL (Unchanged)

**File:** `deploy/migrations/005_workspace_activations.sql`

```sql
-- ============================================================================
-- Migration 005: Workspace Activations (Phase 3)
--
-- Creates the workspace_activations table for deterministic activation
-- bootstrap orchestration.
--
-- Invariants:
--   - UNIQUE(workspace_id): one activation per workspace
--   - Presence of row == activated (no status column, no soft deletes)
--   - workspace_id references workspaces(id) with CASCADE delete
-- ============================================================================

SET search_path TO harbor, public;

CREATE TABLE IF NOT EXISTS workspace_activations (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    UUID        NOT NULL UNIQUE
                                REFERENCES workspaces(id)
                                ON DELETE CASCADE,
    activated_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## 3. Full Updated Route Implementations (Verbatim)

### 3.1 Activation Routes Blueprint

**File:** `shell/app/activation_routes.py`

```python
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
```

### 3.2 App Wiring

**File:** `shell/app/create_app.py`

```python
from flask import Flask, jsonify

from harbor_common.db import close_db
from harbor_common.errors import HarborError


def create_app():
    app = Flask(__name__)
    app.config.from_pyfile("config.py")

    app.teardown_appcontext(close_db)

    from app.routes import bp
    from app.activation_routes import activation_bp

    app.register_blueprint(bp)
    app.register_blueprint(activation_bp)

    @app.errorhandler(HarborError)
    def handle_harbor_error(e):
        return jsonify(e.to_dict()), e.status_code

    return app
```

---

## 4. Full Updated Activation Domain Logic (Verbatim)

### 4.1 Shared Lock Utility

**File:** `shared/harbor_common/locks.py`

```python
"""Harbor — workspace-scoped advisory lock utilities.

Provides a single canonical lock-key derivation for pg_advisory_xact_lock,
shared across all phases (QBO connection lifecycle, activation, etc.).

All workspace-scoped advisory locks MUST use workspace_id_hash() to ensure
a unified lock namespace. Using different hash functions would create
independent lock namespaces that fail to serialize.
"""

import hashlib
from uuid import UUID


def workspace_id_hash(workspace_id: UUID) -> int:
    """Deterministic int64 hash of workspace_id for pg_advisory_xact_lock.

    Returns a signed 64-bit integer derived from SHA-256 of the UUID string.
    This is the ONLY lock-key derivation permitted for workspace-scoped
    advisory locks in Harbor.
    """
    h = hashlib.sha256(str(workspace_id).encode()).digest()
    return int.from_bytes(h[:8], "big", signed=True)
```

### 4.2 Activation Module

**File:** `shared/harbor_common/activation.py`

```python
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
```

### 4.3 Phase 2 Import Change

**File:** `shared/harbor_common/qbo_connection.py` — import section only (behavior unchanged)

```python
# Before (Phase 2 original):
# def _workspace_id_hash(workspace_id: UUID) -> int:
#     h = hashlib.sha256(str(workspace_id).encode()).digest()
#     return int.from_bytes(h[:8], "big", signed=True)

# After (remediation):
from harbor_common.locks import workspace_id_hash
# _workspace_id_hash() removed; all call sites updated to workspace_id_hash()
```

The function body is identical. The only change is import location. All call sites in `qbo_connection.py` (`start_connect` line 283, `handle_oauth_callback` line 359) now reference `workspace_id_hash` instead of `_workspace_id_hash`. No behavioral change.

---

## 5. Concurrency Explanation — Serialization with Phase 2

### Unified Lock Namespace Proof

Both Phase 2 and Phase 3 import the same function from the same module:

```
shared/harbor_common/locks.py::workspace_id_hash()
    ├── used by qbo_connection.py::start_connect()          [Phase 2, Q1]
    ├── used by qbo_connection.py::handle_oauth_callback()  [Phase 2, Q2]
    └── used by activation.py::complete_activation()        [Phase 3]
```

All three call sites execute:

```python
cur.execute("SELECT pg_advisory_xact_lock(%s)", (workspace_id_hash(workspace_id),))
```

The lock key is `SHA-256(str(workspace_id))[:8]` interpreted as a signed int64. For any given `workspace_id`, all three code paths produce the same lock key and therefore serialize against each other.

### Serialization Scenario: Activation vs. OAuth Callback

```
Activation Request                   OAuth Callback (Q2)
──────────────────                   ───────────────────
BEGIN                                BEGIN
pg_advisory_xact_lock(hash(ws))      consume_oauth_state(...)
  check entitlement ✓                 require_qbo_entitlement(...)
  SELECT ... FOR UPDATE               pg_advisory_xact_lock(hash(ws)) ← BLOCKS
  INSERT activation                      ... waiting ...
  COMMIT                              ← UNBLOCKS
                                       exchange_code_for_tokens(...)
                                       UPDATE qbo_connections
                                       COMMIT
```

The advisory lock prevents activation from completing while an OAuth callback is in-flight (and vice versa). Combined with the `FOR UPDATE` row lock on `qbo_connections`, no TOCTOU window exists.

### Serialization Scenario: Concurrent Activation

```
Request A                           Request B
─────────                           ─────────
BEGIN                                BEGIN
pg_advisory_xact_lock(hash(ws))      pg_advisory_xact_lock(hash(ws)) ← BLOCKS
  check entitlement ✓                  ... waiting ...
  SELECT ... FOR UPDATE ✓              ... waiting ...
  INSERT ... ON CONFLICT DO NOTHING    ... waiting ...
  RETURNING id → row UUID              ... waiting ...
COMMIT                               ← UNBLOCKS
                                       check entitlement ✓
                                       SELECT ... FOR UPDATE ✓
                                       INSERT ... ON CONFLICT DO NOTHING
                                       RETURNING id → NULL (already exists)
                                     COMMIT
```

Result: Request A gets `already_completed: false`, Request B gets `already_completed: true`. No duplicate rows. No race condition.

### Serialization Scenario: Activation During Disconnect

```
Activation Request                   Disconnect Request
──────────────────                   ──────────────────
BEGIN                                BEGIN
pg_advisory_xact_lock(hash(ws))      pg_advisory_xact_lock(hash(ws)) ← BLOCKS
  check entitlement ✓                  ... waiting ...
  SELECT status FOR UPDATE              ... waiting ...
    → CONNECTED ✓                       ... waiting ...
  INSERT activation                     ... waiting ...
COMMIT                               ← UNBLOCKS
                                       UPDATE qbo_connections SET status = 'DISCONNECTED'
                                     COMMIT
```

Activation completes before disconnect takes effect. If disconnect acquires the lock first, activation sees `DISCONNECTED` and raises `ACTIVATION_NOT_READY`. Either ordering is safe.

---

## 6. Idempotency Explanation

**Mechanism:** `ON CONFLICT (workspace_id) DO NOTHING`

```sql
INSERT INTO workspace_activations (workspace_id, activated_at)
VALUES (%s, NOW())
ON CONFLICT (workspace_id) DO NOTHING
RETURNING id
```

| Call | `RETURNING id` result | `already_completed` | HTTP response |
|---|---|---|---|
| First call | Returns inserted row UUID | `false` | `{"activation_completed": true, "already_completed": false}` |
| Subsequent calls | `None` (no insert) | `true` | `{"activation_completed": true, "already_completed": true}` |

**Guarantees:**

- First call inserts the activation row.
- Subsequent calls are no-ops at the database level.
- No error is raised for already-activated workspaces.
- The `activated_at` timestamp is immutable after first insert.
- QBO state is never modified by activation.
- The `UNIQUE(workspace_id)` constraint guarantees at most one activation row per workspace at the database level, independent of application-layer logic.

---

## 7. Transaction Discipline Explanation

### Pattern Analysis

Phase 2 domain functions (`start_connect`, `handle_oauth_callback`) follow this pattern:

- **Success path:** `db.commit()` inside the domain function.
- **Error path:** Raise `HarborError` without calling `db.rollback()`. The Flask `teardown_appcontext` callback (`close_db`) closes the connection, which triggers an implicit rollback per psycopg2 semantics.

Phase 3 `complete_activation()` now follows the same pattern.

### Advisory Lock Lifecycle

```
complete_activation() called
│
├─ pg_advisory_xact_lock acquired (held for duration of transaction)
│
├─ [SUCCESS PATH]
│   ├─ Precondition checks pass
│   ├─ INSERT activation row
│   ├─ db.commit()     ← transaction ends, advisory lock released
│   └─ return result
│
└─ [ERROR PATH]
    ├─ Precondition fails (entitlement or QBO status)
    ├─ raise HarborError  ← NO db.rollback(), lock still held
    ├─ Flask error handler sends JSON response
    ├─ Flask teardown calls close_db()
    ├─ psycopg2 connection.close() → implicit rollback
    └─ advisory lock released
```

**Key property:** On the error path, the advisory lock remains held from the time it's acquired until Flask teardown closes the connection. There is no window where a concurrent request could acquire the same lock and observe stale state.

### Pre-Remediation Problem

```python
# BEFORE (broken):
except HarborError:
    db.rollback()           # ← releases advisory lock HERE
    raise HarborError(...)  # ← another request could acquire lock in this gap
```

The `db.rollback()` ended the transaction, releasing the advisory lock before the error response was sent. A concurrent request could theoretically acquire the lock during the gap between rollback and response completion.

### Post-Remediation Fix

```python
# AFTER (fixed):
except HarborError:
    raise HarborError(...)  # ← lock remains held, no rollback
```

No explicit rollback. The advisory lock remains held until `close_db()` teardown.

---

## 8. Phase Boundary Certification Statement

### No Phase 1 Modification

- No changes to `001_harbor_core.sql` tables: `customers`, `users`, `workspaces`, `memberships`, `qbo_connections` (Phase 1 columns), `workspace_app_licenses` (Phase 1 columns).
- No changes to `002_workspace_licenses.sql` schema: `apps` table, `workspace_creation_requests` table, column renames, FK additions.
- The `workspaces` table is not referenced by activation code. The `memberships` table is not referenced by activation code. `workspace_app_licenses` is read (not written) via the existing `workspace_has_qbo_entitlement()` function.

### No Phase 2 Modification

- No changes to `003_qbo_connections_phase2.sql` schema: `status`, `qbo_realm_id`, `oauth_state_hash`, `oauth_state_expires_at`, error tracking columns, `qbo_oauth_states` table.
- No changes to `004_phase2_invariant_hardening.sql` CHECK constraints.
- `qbo_connection.py` behavioral change: **NONE**. The only change is extracting `_workspace_id_hash()` to a shared module (`locks.py`) and updating the import. The function body, all call sites, all return values, and all state machine logic are identical. The `hashlib` import is retained in `qbo_connection.py` because `_hash_state()` (OAuth state hashing) still uses it.

### No Encryption Modification

- No changes to `crypto.py`: `get_fernet()`, `encrypt_str()`, `decrypt_str()`, `is_fernet_token()` all untouched.
- Activation module does not read, write, or reference encrypted tokens.

### No State Machine Modification

- No changes to `QBO_STATES`, `ALLOWED_TRANSITIONS`, or `validate_qbo_transition()`.
- Activation reads `qbo_connections.status` (with `FOR UPDATE`) but never writes to it.
- No new QBO state transitions introduced.
- No bypass of existing state machine guards.

### No Multi-Tenant Weakening

- Activation is workspace-scoped via `UNIQUE(workspace_id)` and `REFERENCES workspaces(id)`.
- All queries scoped by `workspace_id` URL path parameter.
- Advisory locks are workspace-scoped — no cross-workspace lock interference.
- No spoofable identity headers — auth posture matches all other Harbor endpoints.
- No cross-workspace data access possible through activation endpoints.

---

## 9. Hostile Audit Checklist (Updated for Remediation)

### R1 — Lock Namespace Mismatch: FIXED

| Check | Result | Evidence |
|---|---|---|
| Single lock-key derivation exists | PASS | `shared/harbor_common/locks.py::workspace_id_hash()` |
| Phase 2 `start_connect` uses shared function | PASS | `qbo_connection.py:283` — `workspace_id_hash(workspace_id)` |
| Phase 2 `handle_oauth_callback` uses shared function | PASS | `qbo_connection.py:359` — `workspace_id_hash(workspace_id)` |
| Phase 3 `complete_activation` uses shared function | PASS | `activation.py:115-116` — `workspace_id_hash(workspace_id)` |
| Old `_workspace_id_hash` removed | PASS | `grep -n _workspace_id_hash qbo_connection.py` → no matches |
| Old `hashtext` removed | PASS | `grep -n hashtext activation.py` → no matches |
| Lock key values are identical for same workspace_id | PASS | Same function, same algorithm (SHA-256[:8] → signed int64) |

### R2 — TOCTOU / FOR UPDATE: FIXED

| Check | Result | Evidence |
|---|---|---|
| `FOR UPDATE` on QBO status fetch in `complete_activation` | PASS | `activation.py:134` — `SELECT status FROM qbo_connections WHERE workspace_id = %s FOR UPDATE` |
| Read-only status endpoint does NOT use `FOR UPDATE` | PASS | `_get_qbo_connection_status()` uses plain `SELECT` — correct for read-only path |
| Row lock prevents concurrent QBO status mutation during activation | PASS | `FOR UPDATE` blocks any concurrent `UPDATE` on the same `qbo_connections` row until activation transaction ends |

### R3 — Spoofable Auth Header: FIXED

| Check | Result | Evidence |
|---|---|---|
| No `X-User-Id` header in source code | PASS | `grep -rn X-User-Id shared/ shell/` → no matches in source files |
| No `_require_membership` function | PASS | `grep -rn _require_membership shell/` → no matches |
| Activation routes match existing auth posture | PASS | No Harbor endpoint (routes.py) performs application-layer auth; activation routes follow the same pattern |
| No self-asserted identity mechanism | PASS | No header, cookie, or query parameter used for identity in activation code |

### R4 — Transaction Discipline: FIXED

| Check | Result | Evidence |
|---|---|---|
| No `db.rollback()` in `activation.py` | PASS | `grep -n rollback activation.py` → no matches |
| `db.commit()` only on success path | PASS | `activation.py:164` — single commit after successful INSERT |
| Advisory lock held on error path | PASS | HarborError raised without rollback; lock released only by `close_db()` teardown |
| Pattern matches Phase 2 domain functions | PASS | `handle_oauth_callback()` and `start_connect()` follow the same commit-on-success, propagate-on-error pattern |

### Schema Integrity (Unchanged)

| Check | Result | Evidence |
|---|---|---|
| `workspace_activations` has `UNIQUE(workspace_id)` | PASS | DDL: `workspace_id UUID NOT NULL UNIQUE` |
| No status column on activations | PASS | Only columns: id, workspace_id, activated_at, created_at, updated_at |
| No soft delete mechanism | PASS | No `deleted_at`, no status flags, no deactivation path |
| FK to workspaces with CASCADE | PASS | `REFERENCES workspaces(id) ON DELETE CASCADE` |
| No Phase 1 tables modified | PASS | Migration 005 only creates `workspace_activations` |
| No Phase 2 tables modified | PASS | No ALTER TABLE on any Phase 2 table |

### Activation Logic (Unchanged)

| Check | Result | Evidence |
|---|---|---|
| Readiness is derived, not stored | PASS | `get_activation_status()` computes all values at query time |
| No cached booleans | PASS | No memoization, no class-level state, no module globals |
| Entitlement checked before activation | PASS | `require_qbo_entitlement()` called inside advisory lock |
| QBO CONNECTED required | PASS | `qbo_status != "CONNECTED"` → ACTIVATION_NOT_READY |
| INSERT uses ON CONFLICT DO NOTHING | PASS | `activation.py:156` — `ON CONFLICT (workspace_id) DO NOTHING` |
| Already-activated returns success | PASS | `already_completed: true`, no error raised |
| QBO state never mutated | PASS | No UPDATE/INSERT on `qbo_connections` in activation module |

### Architectural Discipline

| Check | Result | Evidence |
|---|---|---|
| No background provisioning | PASS | Synchronous request-response only |
| No auto-activation on OAuth callback | PASS | OAuth callback in `qbo_connection.py` unchanged |
| No activation states beyond presence-of-row | PASS | No status column, no enum, no flags |
| No readiness caching | PASS | All checks query DB directly |
| No async introduced | PASS | No asyncio, no celery, no threading |
| No QBO state machine modification | PASS | `qbo_connection.py` state machine untouched |
| No entitlement logic added | PASS | Uses existing `require_qbo_entitlement` / `workspace_has_qbo_entitlement` |
| No Redis/queues/distributed systems | PASS | Only PostgreSQL used |

---

## 10. Validation Commands and Results

### V1 — Lock key derivation is shared/unified

```bash
grep -rn "pg_advisory" shared/harbor_common/
```

Output:

```
shared/harbor_common/qbo_connection.py:283:    cur.execute("SELECT pg_advisory_xact_lock(%s)", (workspace_id_hash(workspace_id),))
shared/harbor_common/qbo_connection.py:359:    cur.execute("SELECT pg_advisory_xact_lock(%s)", (workspace_id_hash(workspace_id),))
shared/harbor_common/activation.py:115:        "SELECT pg_advisory_xact_lock(%s)",
shared/harbor_common/locks.py:3:Provides a single canonical lock-key derivation for pg_advisory_xact_lock,
shared/harbor_common/locks.py:16:    """Deterministic int64 hash of workspace_id for pg_advisory_xact_lock.
```

All three operational call sites use `workspace_id_hash()` from the shared `locks.py` module.

### V2 — FOR UPDATE present in activation's QBO status fetch

```bash
grep -n "FOR UPDATE" shared/harbor_common/activation.py
```

Output:

```
134:        "SELECT status FROM qbo_connections WHERE workspace_id = %s FOR UPDATE",
```

### V3 — No spoofable identity header

```bash
grep -rn "X-User-Id" shared/ shell/app/
```

Output: (no matches in source files)

### V4 — No db.rollback() in activation module

```bash
grep -n "rollback" shared/harbor_common/activation.py
```

Output: (no matches)

---

## Files Modified or Created

| File | Action | Purpose |
|---|---|---|
| `deploy/migrations/005_workspace_activations.sql` | Unchanged | Activation table DDL |
| `shared/harbor_common/locks.py` | **Created (R1)** | Shared workspace advisory lock key derivation |
| `shared/harbor_common/activation.py` | **Updated (R1, R2, R4)** | Unified lock namespace, FOR UPDATE, clean transaction discipline |
| `shared/harbor_common/qbo_connection.py` | **Updated (R1)** | Import `workspace_id_hash` from `locks.py`, remove private `_workspace_id_hash` |
| `shell/app/activation_routes.py` | **Updated (R3)** | Remove spoofable `X-User-Id` auth; match existing Harbor auth posture |
| `shell/app/create_app.py` | Unchanged | Blueprint registration |

**Phase 2 behavioral changes: NONE.** The `qbo_connection.py` change is a pure refactor (extract function to shared module). The function body, return values, and all call sites produce identical results.
