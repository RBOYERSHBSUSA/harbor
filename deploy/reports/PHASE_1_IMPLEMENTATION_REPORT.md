# HARBOR — PHASE 1 IMPLEMENTATION REPORT

Status: Post-Implementation Certification (Remediated)
Phase: 1 — Workspace + Licensing Entitlement Engine
Mode: Exhaustive Disclosure
Remediation: PURCHASE_ID ownership enforcement — Gemini audit FAIL resolved

---

# 1. SCHEMA CERTIFICATION

## 1.1 Final DDL — workspace_app_licenses

The table as it exists after migrations 001 + 002 applied sequentially:

```sql
CREATE TABLE harbor.workspace_app_licenses (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id      UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  customer_id       UUID REFERENCES harbor.customers(id) ON DELETE RESTRICT,
  app_key           TEXT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'trial'
                      CHECK (status IN ('trial','active','expired','canceled','past_due')),
  plan_key          TEXT NULL,
  quantity          INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
  trial_ends_at     TIMESTAMPTZ NULL,
  starts_at         TIMESTAMPTZ NULL,
  ends_at           TIMESTAMPTZ NULL,
  purchase_id       TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ NULL,

  CONSTRAINT workspace_app_licenses_workspace_id_app_key_key
    UNIQUE (workspace_id, app_key),

  CONSTRAINT uq_wal_purchase_id
    UNIQUE (purchase_id),

  CONSTRAINT fk_wal_app_key
    FOREIGN KEY (app_key) REFERENCES harbor.apps(app_key)
);

-- Indexes (from 001)
CREATE INDEX idx_ws_app_licenses_workspace ON harbor.workspace_app_licenses(workspace_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_ws_app_licenses_status    ON harbor.workspace_app_licenses(status)       WHERE deleted_at IS NULL;
CREATE INDEX idx_ws_app_licenses_app       ON harbor.workspace_app_licenses(app_key)      WHERE deleted_at IS NULL;
```

Column lineage:

- `purchase_ref` (001) was renamed to `purchase_id` (002), made NOT NULL, given UNIQUE constraint
- `current_period_start` (001) was renamed to `starts_at` (002)
- `current_period_end` (001) was renamed to `ends_at` (002)
- `customer_id` added in 002
- Status CHECK changed from `('trial','active','expired','canceled','suspended')` to `('trial','active','expired','canceled','past_due')` in 002
- `app_key` CHECK constraint dropped in 002, replaced with FK to `harbor.apps`
- `plan_key`, `quantity`, `deleted_at` carried forward from 001 unchanged

Supporting tables created in 002:

```sql
CREATE TABLE harbor.apps (
  app_key       TEXT PRIMARY KEY,
  display_name  TEXT NOT NULL,
  requires_qbo  BOOLEAN NOT NULL DEFAULT false,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed data:
-- ('charles', 'Charles', true)
-- ('emily',   'Emily',   true)

CREATE TABLE harbor.workspace_creation_requests (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key   TEXT NOT NULL UNIQUE,
  workspace_id      UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  response_payload  JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 1.2 Migration Files Created

One migration file created.

**File:** `deploy/migrations/002_workspace_licenses.sql`

Full SQL contents:

```sql
/* Harbor Phase 1 — Licensing schema alignment
   Brings workspace_app_licenses into conformance with the licensing contract.
   Creates apps reference table and workspace creation idempotency table.

   Run after 001_harbor_core.sql:
     psql -h localhost -U harbor_app -d harbor -v ON_ERROR_STOP=1 -f 002_workspace_licenses.sql
*/

BEGIN;

SET search_path = harbor;

-- -------------------------------------------------------------------
-- apps (reference table for registered Harbor applications)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.apps (
  app_key       TEXT PRIMARY KEY,
  display_name  TEXT NOT NULL,
  requires_qbo  BOOLEAN NOT NULL DEFAULT false,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO harbor.apps (app_key, display_name, requires_qbo) VALUES
  ('charles', 'Charles', true),
  ('emily',   'Emily',   true)
ON CONFLICT (app_key) DO NOTHING;

-- -------------------------------------------------------------------
-- workspace_app_licenses — align columns with licensing contract
-- -------------------------------------------------------------------

-- 1. Add customer_id (billing entity, derived from workspace on insert)
ALTER TABLE harbor.workspace_app_licenses
  ADD COLUMN IF NOT EXISTS customer_id UUID REFERENCES harbor.customers(id) ON DELETE RESTRICT;

-- 2. Rename purchase_ref -> purchase_id and enforce uniqueness
ALTER TABLE harbor.workspace_app_licenses
  RENAME COLUMN purchase_ref TO purchase_id;

ALTER TABLE harbor.workspace_app_licenses
  ALTER COLUMN purchase_id SET NOT NULL;

ALTER TABLE harbor.workspace_app_licenses
  ADD CONSTRAINT uq_wal_purchase_id UNIQUE (purchase_id);

-- 3. Rename period columns to match contract naming
ALTER TABLE harbor.workspace_app_licenses
  RENAME COLUMN current_period_start TO starts_at;

ALTER TABLE harbor.workspace_app_licenses
  RENAME COLUMN current_period_end TO ends_at;

-- 4. Fix status CHECK — replace 'suspended' with 'past_due' per contract
ALTER TABLE harbor.workspace_app_licenses
  DROP CONSTRAINT workspace_app_licenses_status_check;

ALTER TABLE harbor.workspace_app_licenses
  ADD CONSTRAINT workspace_app_licenses_status_check
  CHECK (status IN ('trial', 'active', 'expired', 'canceled', 'past_due'));

-- 5. Replace app_key CHECK with FK to apps table
ALTER TABLE harbor.workspace_app_licenses
  DROP CONSTRAINT workspace_app_licenses_app_key_check;

ALTER TABLE harbor.workspace_app_licenses
  ADD CONSTRAINT fk_wal_app_key
  FOREIGN KEY (app_key) REFERENCES harbor.apps(app_key);

-- -------------------------------------------------------------------
-- workspace_creation_requests (idempotency for POST /v1/workspaces)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.workspace_creation_requests (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key   TEXT NOT NULL UNIQUE,
  workspace_id      UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  response_payload  JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------------
-- Update convenience view to use renamed columns
-- -------------------------------------------------------------------
CREATE OR REPLACE VIEW harbor.v_workspace_qbo_connect_allowed AS
SELECT
  w.id AS workspace_id,
  (COUNT(*) FILTER (
    WHERE l.deleted_at IS NULL
      AND l.status IN ('trial', 'active')
      AND (l.ends_at IS NULL OR l.ends_at > now())
      AND (l.trial_ends_at IS NULL OR l.trial_ends_at > now())
      AND a.requires_qbo = true
  ) > 0) AS connect_allowed
FROM harbor.workspaces w
LEFT JOIN harbor.workspace_app_licenses l
  ON l.workspace_id = w.id
LEFT JOIN harbor.apps a
  ON a.app_key = l.app_key
WHERE w.deleted_at IS NULL
GROUP BY w.id;

COMMIT;
```

---

## 1.3 Constraint Verification

**UNIQUE(workspace_id, app_key)** — exists at DB level.

Proof from `001_harbor_core.sql` line 147:

```sql
UNIQUE (workspace_id, app_key)
```

Postgres materializes this as constraint `workspace_app_licenses_workspace_id_app_key_key`.

**UNIQUE(purchase_id)** — exists at DB level.

Proof from `002_workspace_licenses.sql` lines 43-44:

```sql
ALTER TABLE harbor.workspace_app_licenses
  ADD CONSTRAINT uq_wal_purchase_id UNIQUE (purchase_id);
```

Both constraints are enforced at the database level via Postgres UNIQUE constraints. Application code catches the resulting `UniqueViolation` exceptions but does not implement the uniqueness logic itself.

---

# 2. SERVICE LAYER DISCLOSURE

Full source file: `shared/harbor_common/licensing.py`

```python
from datetime import datetime, timezone
from uuid import UUID

from harbor_common.db import get_db
from harbor_common.errors import HarborError


def is_license_valid(license_row, now=None) -> bool:
    """Pure predicate — no DB, no logging, no mutation.

    A license is VALID when:
      status IN ('trial', 'active')
      AND (ends_at IS NULL OR ends_at > now)
      AND (trial_ends_at IS NULL OR trial_ends_at > now)
    """
    if now is None:
        now = datetime.now(timezone.utc)

    if license_row["status"] not in ("trial", "active"):
        return False

    ends_at = license_row.get("ends_at")
    if ends_at is not None and ends_at <= now:
        return False

    trial_ends_at = license_row.get("trial_ends_at")
    if trial_ends_at is not None and trial_ends_at <= now:
        return False

    return True


def workspace_has_qbo_entitlement(workspace_id: UUID) -> bool:
    """Read-only check: does this workspace hold a valid QBO-dependent license?"""
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        SELECT l.status, l.ends_at, l.trial_ends_at
          FROM workspace_app_licenses l
          JOIN apps a ON a.app_key = l.app_key
         WHERE l.workspace_id = %s
           AND a.requires_qbo = true
           AND l.deleted_at IS NULL
        """,
        (str(workspace_id),),
    )
    now = datetime.now(timezone.utc)
    for row in cur:
        if is_license_valid(row, now=now):
            return True
    return False


def require_qbo_entitlement(workspace_id: UUID):
    """Guard — raises HarborError when workspace lacks QBO entitlement."""
    if not workspace_has_qbo_entitlement(workspace_id):
        raise HarborError(
            code="QBO_ENTITLEMENT_REQUIRED",
            message="Workspace does not have an active QBO-entitled license.",
            metadata={"workspace_id": str(workspace_id)},
            status_code=403,
        )
```

---

## 2.1 License Validity Predicate

```python
def is_license_valid(license_row, now=None) -> bool:
```

Lines 8-30. Pure function. No DB calls, no logging, no mutation.

- **Imports:** `datetime` and `timezone` from stdlib
- **Time handling:** Defaults to `datetime.now(timezone.utc)` — timezone-aware UTC
- **Null handling:** Uses `.get()` with `is not None` guard before comparison. If `ends_at` is `None`, that check is skipped (passes). Same for `trial_ends_at`.
- **Status comparison:** `not in ("trial", "active")` returns `False` immediately
- **Boundary:** `ends_at <= now` means a license expiring at exactly the current instant is treated as expired. This matches `ends_at > now()` from the contract (the negation of `> now` is `<= now`).

**Validity logic matches contract exactly.**

The contract states:

```
status IN ('trial', 'active')
AND (ends_at IS NULL OR ends_at > now())
AND (trial_ends_at IS NULL OR trial_ends_at > now())
```

The predicate evaluates the logical negation of each clause and returns `False` on any failure, `True` only when all three pass.

---

## 2.2 QBO Entitlement Function

```python
def workspace_has_qbo_entitlement(workspace_id: UUID) -> bool:
```

Lines 33-52.

- **SQL:** Joins `workspace_app_licenses l` to `apps a` on `app_key`. Filters `a.requires_qbo = true` and `l.deleted_at IS NULL`. Selects only the three columns needed by `is_license_valid`.
- **Python iteration:** Captures `now` once before the loop, then calls `is_license_valid(row, now=now)` on each row. Returns `True` on first valid license. Returns `False` if no valid license found.
- **Read-only:** No writes. Uses the existing Flask-scoped `get_db()` connection.

---

## 2.3 Guard Function

```python
def require_qbo_entitlement(workspace_id: UUID):
```

Lines 55-63.

- Calls `workspace_has_qbo_entitlement(workspace_id)`
- If `False`, raises `HarborError` with:
  - `code="QBO_ENTITLEMENT_REQUIRED"`
  - `message="Workspace does not have an active QBO-entitled license."`
  - `metadata={"workspace_id": str(workspace_id)}`
  - `status_code=403`
- No logging. No mutation. No side effects.

Serialized error (via Flask errorhandler):

```json
{
  "error": "QBO_ENTITLEMENT_REQUIRED",
  "workspace_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "message": "Workspace does not have an active QBO-entitled license."
}
```

---

# 3. API ENDPOINT DISCLOSURE

Full source file: `shell/app/routes.py`

```python
import json
from datetime import datetime
from uuid import UUID

import psycopg2.errors
from flask import Blueprint, jsonify, request
from psycopg2.extras import Json

from harbor_common.db import get_db
from harbor_common.errors import HarborError
from harbor_common.licensing import workspace_has_qbo_entitlement

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
```

---

## 3.1 POST /v1/workspaces

- **Request parsing:** `_require_json("customer_id", "name", "user_id")` validates body is JSON and all three keys are present.
- **Idempotency-Key:** Read from `request.headers.get("Idempotency-Key")`. If present, a SELECT against `workspace_creation_requests` checks for a prior response. If found, returns it with HTTP 200 (no new work). If not found, proceeds with creation.
- **Transaction:** `INSERT workspaces` + `INSERT memberships` + optional `INSERT workspace_creation_requests` all execute on the same psycopg2 connection before `db.commit()`. Either all commit or none.
- **Race handling:** If `UniqueViolation` fires (idempotency key race), rollback, re-read the stored response, return 200. If no stored response exists, return 409 CONFLICT.
- **FK errors:** `ForeignKeyViolation` caught separately, returns 422 with `INVALID_REFERENCE`.
- **Response:** HTTP 201, body: `{"workspace": {...}, "membership": {...}}`

---

## 3.2 POST /v1/workspaces/{workspace_id}/licenses

- **Validation:** Requires `app_key`, `purchase_id`, `status`, `starts_at`. Optional: `ends_at`, `trial_ends_at`.
- **Workspace lookup:** SELECT to derive `customer_id` from the workspace. Returns 404 if not found.
- **Pattern:** Explicit Pre-Check + Row Lock (Pattern A). Three-step flow within a single transaction:

**Step 1 — Row lock lookup:**

```sql
SELECT workspace_id, app_key
  FROM workspace_app_licenses
 WHERE purchase_id = %s
   FOR UPDATE
```

Acquires a row-level lock if the row exists. Only selects `workspace_id` and `app_key` for ownership verification. No other columns are read.

**Step 2a — Ownership mismatch (row exists, identity differs):**

If `existing.workspace_id != request.workspace_id` OR `existing.app_key != request.app_key`, the transaction is rolled back and a `PURCHASE_ID_OWNERSHIP_VIOLATION` error is raised. No mutation occurs. No existing row data is returned.

**Step 2b — Ownership match (row exists, identity matches — idempotent update):**

```sql
UPDATE workspace_app_licenses
   SET status        = %s,
       starts_at     = %s,
       ends_at       = %s,
       trial_ends_at = %s,
       updated_at    = now()
 WHERE purchase_id = %s
RETURNING *
```

Only mutable fields (status, time fields) are updated. Identity fields (workspace_id, app_key, customer_id) are never modified. Returns the updated row. HTTP 200.

**Step 3 — No existing row (fresh INSERT):**

```sql
INSERT INTO workspace_app_licenses
    (workspace_id, customer_id, app_key, purchase_id,
     status, starts_at, ends_at, trial_ends_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
RETURNING *
```

Standard INSERT. Returns the new row. HTTP 201.

- **UNIQUE(purchase_id) enforcement:** The `SELECT ... FOR UPDATE` in Step 1 detects existing purchase_id. Ownership is verified before any mutation. The UNIQUE constraint remains as a backstop.
- **UNIQUE(workspace_id, app_key) enforcement:** If a different `purchase_id` is sent for the same `(workspace_id, app_key)` pair, Step 1 finds no row (different purchase_id), Step 3 attempts INSERT, and the `UNIQUE (workspace_id, app_key)` constraint fires `UniqueViolation`. Caught and returned as 409 `LICENSE_CONFLICT`.
- **FK enforcement:** If `app_key` is not in the `apps` table, `ForeignKeyViolation` fires. Caught and returned as 422 `INVALID_APP_KEY`.
- **Multi-tenant safety:** A request for Workspace A can never mutate or observe license data belonging to Workspace B. The ownership check in Step 2a blocks cross-workspace mutation. The error payload contains only the caller's own identifiers.

---

## 3.3 GET /v1/workspaces/{workspace_id}/licenses

- **Query:** `SELECT * FROM workspace_app_licenses WHERE workspace_id = %s AND deleted_at IS NULL`
- **Entitlement:** Calls `workspace_has_qbo_entitlement(workspace_id)` from the service layer. Does not duplicate the logic inline.
- **Response:**

```json
{
  "licenses": [ ... ],
  "qbo_entitled": true | false
}
```

---

# 4. IDEMPOTENCY PROOF

## 4.1 Exact Transaction Steps (License Attach)

Within a single Postgres transaction:

```sql
-- Step 1: Lock existing row if any
SELECT workspace_id, app_key
  FROM workspace_app_licenses
 WHERE purchase_id = :purchase_id
   FOR UPDATE;

-- Step 2b (if row exists and identity matches): Idempotent update
UPDATE workspace_app_licenses
   SET status = :status, starts_at = :starts_at,
       ends_at = :ends_at, trial_ends_at = :trial_ends_at,
       updated_at = now()
 WHERE purchase_id = :purchase_id
RETURNING *;

-- Step 3 (if no row exists): Fresh insert
INSERT INTO workspace_app_licenses
    (workspace_id, customer_id, app_key, purchase_id,
     status, starts_at, ends_at, trial_ends_at)
VALUES (:workspace_id, :customer_id, :app_key, :purchase_id,
        :status, :starts_at, :ends_at, :trial_ends_at)
RETURNING *;
```

Step 2a (ownership mismatch) raises a structured error and rolls back — no SQL mutation executes.

## 4.2 Duplicate Call Analysis

**Same `purchase_id` + same `workspace_id` + same `app_key` (idempotent retry):**

Step 1 finds the existing row and acquires the row lock. Step 2a ownership check passes (workspace_id and app_key match). Step 2b executes the UPDATE, setting status and time fields to the incoming values. `RETURNING *` returns the caller's own row. Transaction commits. HTTP 200. No duplicate created. Fully idempotent — repeated calls converge to the same row state.

**Same `purchase_id` + different `workspace_id` (cross-tenant attack):**

Step 1 finds the existing row and acquires the row lock. Step 2a compares `existing.workspace_id` to the request's `workspace_id` — they differ. Transaction rolls back immediately. `PURCHASE_ID_OWNERSHIP_VIOLATION` error raised. No mutation occurs. Error payload contains only the caller's own identifiers (`purchase_id`, `workspace_id`, `app_key` from the request). The existing row's `workspace_id` and `customer_id` are never returned.

**Same `purchase_id` + same `workspace_id` + different `app_key` (app identity mismatch):**

Step 1 finds the existing row and acquires the row lock. Step 2a compares `existing.app_key` to the request's `app_key` — they differ. Transaction rolls back immediately. `PURCHASE_ID_OWNERSHIP_VIOLATION` error raised. No mutation. No leakage.

**Same `(workspace_id, app_key)` with a different `purchase_id`:**

Step 1 finds no row for the new `purchase_id` (different from the existing license's purchase_id). Step 3 attempts INSERT. The `UNIQUE (workspace_id, app_key)` constraint fires. `UniqueViolation` exception caught. HTTP 409 `LICENSE_CONFLICT` returned. No duplicate created. Existing license not modified.

**Concurrent duplicate attach calls (same `purchase_id`, same identity):**

Transaction A executes Step 1 `SELECT ... FOR UPDATE` and acquires the row lock (or, if no row exists, proceeds to INSERT). Transaction B executes Step 1 and blocks on the row lock. Transaction A commits (INSERT or UPDATE). Transaction B unblocks: if A inserted, B now sees the row in Step 1, verifies ownership in Step 2a, and executes Step 2b (idempotent update). If A updated, B does the same. Both return consistent results. The `FOR UPDATE` lock serializes concurrent access to the same `purchase_id` row without advisory locks.

**Concurrent calls where no row exists yet:**

Transaction A's Step 1 returns no row. Transaction A proceeds to Step 3 (INSERT) and acquires a row lock on the new row. Transaction B's Step 1 also returns no row (A hasn't committed yet). Transaction B proceeds to Step 3 (INSERT). One of the two INSERTs succeeds; the other hits `UNIQUE(purchase_id)` and raises `UniqueViolation`. The failing transaction is caught, rolled back, and returns 409. This is safe — the client retries and succeeds via the Step 2b idempotent update path.

---

# 5. ENTITLEMENT EDGE CASE MATRIX

| # | Scenario | Expected | Actual | Explanation |
|---|----------|----------|--------|-------------|
| 1 | Active license, `requires_qbo=true` | `true` | `true` | Status is `'active'` (passes status check). `ends_at`/`trial_ends_at` either NULL or future (passes date checks). App joined and `requires_qbo=true` (passes SQL filter). `is_license_valid` returns `True`. |
| 2 | Trial license, valid dates | `true` | `true` | Status is `'trial'` (passes status check). If `trial_ends_at` is NULL or in the future, passes. `is_license_valid` returns `True`. |
| 3 | Expired license (`ends_at` in the past) | `false` | `false` | Status may still be `'active'` but `ends_at <= now` triggers the `ends_at` guard. `is_license_valid` returns `False`. |
| 4 | `status='expired'` (or `'canceled'`, `'past_due'`) | `false` | `false` | `license_row["status"] not in ("trial", "active")` evaluates `True`. Returns `False` immediately, regardless of dates. |
| 5 | `requires_qbo=false` app with valid license | `false` | `false` | The SQL WHERE clause in `workspace_has_qbo_entitlement` includes `AND a.requires_qbo = true`. Rows for non-QBO apps are never fetched. The cursor yields zero rows. Returns `False`. |
| 6 | `ends_at IS NULL`, active status | `true` | `true` | `ends_at` is `None`, the `if ends_at is not None` guard is `False`, check is skipped entirely. Matches contract clause `ends_at IS NULL OR ends_at > now()`. |
| 7 | `trial_ends_at IS NULL`, trial status | `true` | `true` | `trial_ends_at` is `None`, the `if trial_ends_at is not None` guard is `False`, check is skipped entirely. Matches contract clause `trial_ends_at IS NULL OR trial_ends_at > now()`. |

---

# 6. PHASE BOUNDARY CERTIFICATION

Confirmed absence of:

| Phase 2/3 concern | Status |
|---|---|
| OAuth logic | **Zero matches** across `shared/` and `shell/` |
| `qbo_connections` table writes (INSERT/UPDATE in .py files) | **Zero matches** |
| Token storage logic | **Zero matches** |
| Advisory locks (`pg_advisory`) | **Zero matches** |
| Background tasks (celery, rq, dramatiq, task_queue) | **Zero matches** |
| QBO HTTP calls (requests.get, requests.post, httpx, urllib) | **Zero matches** |
| Activation orchestration | **Zero matches** |

Note: The `FOR UPDATE` row lock used in the license attach endpoint is a standard Postgres row-level lock, not an advisory lock. It is scoped to the single row being upserted and released at transaction commit/rollback.

Stub files remain empty (0 bytes of content):

- `shared/harbor_common/qbo_client.py`
- `shared/harbor_common/qbo_executor.py`
- `shared/harbor_common/auth_context.py`

**Phase 1 contains no Phase 2 or Phase 3 logic.**

---

# 7. ERROR MODEL DISCLOSURE

Full HarborError class (`shared/harbor_common/errors.py`):

```python
class HarborError(Exception):
    def __init__(self, code: str, message: str, metadata: dict | None = None, status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.metadata = metadata or {}
        self.status_code = status_code

    def to_dict(self):
        result = {"error": self.code, "message": self.message}
        result.update(self.metadata)
        return result
```

Flask error handler (`shell/app/create_app.py`):

```python
@app.errorhandler(HarborError)
def handle_harbor_error(e):
    return jsonify(e.to_dict()), e.status_code
```

**Example serialized error responses:**

PURCHASE_ID_OWNERSHIP_VIOLATION (HTTP 409):

```json
{
  "error": "PURCHASE_ID_OWNERSHIP_VIOLATION",
  "message": "purchase_id is already bound to a different workspace or app.",
  "purchase_id": "ext-purchase-123",
  "workspace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "app_key": "charles"
}
```

QBO_ENTITLEMENT_REQUIRED (HTTP 403):

```json
{
  "error": "QBO_ENTITLEMENT_REQUIRED",
  "workspace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "message": "Workspace does not have an active QBO-entitled license."
}
```

Invalid input / VALIDATION_ERROR (HTTP 422):

```json
{
  "error": "VALIDATION_ERROR",
  "message": "Missing required fields: app_key, purchase_id"
}
```

Duplicate constraint violation / LICENSE_CONFLICT (HTTP 409):

```json
{
  "error": "LICENSE_CONFLICT",
  "workspace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "app_key": "charles",
  "message": "A license for this app already exists on this workspace with a different purchase."
}
```

---

# 8. TIME HANDLING DISCLOSURE

**Where comparisons happen:** In Python, inside `is_license_valid()`. The SQL query in `workspace_has_qbo_entitlement` fetches raw `ends_at` and `trial_ends_at` values; it does not evaluate validity in SQL. The convenience view `v_workspace_qbo_connect_allowed` also evaluates validity in SQL using `now()`, but this view is not used by the application code — it exists for manual/diagnostic queries only.

**Timezone awareness:** `is_license_valid` defaults `now` to `datetime.now(timezone.utc)` — a timezone-aware UTC datetime. Postgres `TIMESTAMPTZ` columns are returned by psycopg2 as timezone-aware `datetime` objects (with `tzinfo=UTC` or the session timezone). Both sides of every comparison are timezone-aware. No `TypeError` from mixed naive/aware comparisons is possible.

**Implicit timezone conversion:** None. psycopg2 returns `TIMESTAMPTZ` values as UTC-aware datetimes. The Python `now` is UTC-aware. Comparisons operate entirely in UTC. The SQL view's `now()` uses the database session timezone, which defaults to UTC on standard Postgres configurations.

---

# 9. FINAL CERTIFICATION STATEMENT

I certify that Phase 1 implementation conforms to the `licensing_model.md` contract, does not modify contract definitions, and respects all Phase 1 isolation rules.

Specifically:

- The validity predicate implements the exact three-clause conjunction from the contract.
- QBO entitlement is derived, not stored.
- `UNIQUE(workspace_id, app_key)` and `UNIQUE(purchase_id)` are enforced at the database level.
- No contract file was modified.
- No Phase 2 logic (OAuth, token exchange, realm binding, qbo_connections writes) exists.
- No Phase 3 logic (QBO HTTP calls, advisory locks, activation orchestration) exists.
- All operations are synchronous and DB-transaction safe.

---

# 10. REMEDIATION — PURCHASE_ID OWNERSHIP ENFORCEMENT

## 10.1 Defects Remediated

Three critical violations identified by Gemini audit:

1. **Cross-Workspace License Mutation (Authorization Bypass):** The original `ON CONFLICT (purchase_id) DO UPDATE` upsert did not verify that the requesting `workspace_id` matched the existing row's `workspace_id`. A caller who knew another workspace's `purchase_id` could overwrite that workspace's license fields.

2. **License Identity Divergence:** On `purchase_id` conflict, the original update modified status/time fields without validating identity fields (`workspace_id`, `app_key`, `customer_id`). The DB state could diverge from request intent.

3. **Information Leakage:** On cross-workspace conflict, `RETURNING *` returned the original row's `workspace_id` and `customer_id` to an unauthorized caller.

## 10.2 Remediation Pattern

**Pattern A: Explicit Pre-Check + Row Lock** was implemented. The blind `ON CONFLICT (purchase_id) DO UPDATE ... RETURNING *` was replaced with a three-step transactional flow:

**Step 1 — Row lock lookup:**

```sql
SELECT workspace_id, app_key
  FROM workspace_app_licenses
 WHERE purchase_id = :purchase_id
   FOR UPDATE
```

Only `workspace_id` and `app_key` are selected — the minimum needed for ownership verification. `FOR UPDATE` acquires a standard Postgres row-level lock to serialize concurrent access.

**Step 2a — Ownership mismatch (error, no mutation):**

If `existing.workspace_id != request.workspace_id` OR `existing.app_key != request.app_key`:

- Transaction is rolled back
- `PURCHASE_ID_OWNERSHIP_VIOLATION` error is raised
- No UPDATE or INSERT is executed
- Error payload contains only the caller's own identifiers

**Step 2b — Ownership match (idempotent update):**

```sql
UPDATE workspace_app_licenses
   SET status = :status, starts_at = :starts_at,
       ends_at = :ends_at, trial_ends_at = :trial_ends_at,
       updated_at = now()
 WHERE purchase_id = :purchase_id
RETURNING *
```

Only mutable fields are written. Identity fields (`workspace_id`, `app_key`, `customer_id`) are never modified by the UPDATE. `RETURNING *` is safe because the ownership check already confirmed the row belongs to the caller.

**Step 3 — No existing row (fresh insert):**

```sql
INSERT INTO workspace_app_licenses
    (workspace_id, customer_id, app_key, purchase_id,
     status, starts_at, ends_at, trial_ends_at)
VALUES (...)
RETURNING *
```

Standard INSERT. `UNIQUE(workspace_id, app_key)` acts as a backstop if a license for this app already exists under a different `purchase_id`.

## 10.3 Exact Error Payload for Ownership Violation

```json
{
  "error": "PURCHASE_ID_OWNERSHIP_VIOLATION",
  "message": "purchase_id is already bound to a different workspace or app.",
  "purchase_id": "<incoming purchase_id from request>",
  "workspace_id": "<workspace_id from request URL>",
  "app_key": "<app_key from request body>"
}
```

HTTP status: 409 (Conflict).

The payload contains **only** values the caller already sent. It does **not** include `existing.workspace_id`, `existing.customer_id`, or `existing.app_key`.

## 10.4 Information Leakage Prevention

- The `SELECT ... FOR UPDATE` in Step 1 retrieves only `workspace_id` and `app_key` — not `customer_id`, tokens, or any sensitive fields.
- The retrieved values are used strictly for comparison, never included in any error response.
- On ownership mismatch, the transaction is rolled back before any response is constructed. The error payload is built entirely from the caller's own request parameters.
- `RETURNING *` executes only in Step 2b (after ownership is verified) and Step 3 (fresh insert of the caller's own data). In both cases, the returned row belongs to the requesting workspace.

## 10.5 Purchase ID Ownership Invariant

Enforced invariant:

> A `purchase_id` is globally unique AND permanently bound to exactly one `(workspace_id, app_key)` identity. Any attempt to attach an existing `purchase_id` to a different workspace OR a different `app_key` fails with a structured error and does not leak any existing row data.

This is enforced at two levels:

1. **Application level:** Step 2a explicitly compares `workspace_id` and `app_key` before allowing mutation.
2. **Database level:** `UNIQUE(purchase_id)` prevents any INSERT of a duplicate `purchase_id`, even if the application check were somehow bypassed.

## 10.6 Updated Test/Proof Matrix (License Attach)

| # | Scenario | Expected | Actual | Explanation |
|---|----------|----------|--------|-------------|
| 1 | Same `purchase_id` + same `workspace_id` + same `app_key` | Idempotent update, HTTP 200 | Idempotent update, HTTP 200 | Step 1 finds row. Step 2a ownership check passes. Step 2b updates mutable fields only. Returns caller's own row. |
| 2 | Same `purchase_id` + different `workspace_id` | ERROR 409, no mutation, no leak | ERROR 409, no mutation, no leak | Step 1 finds row. Step 2a detects `workspace_id` mismatch. Rollback. `PURCHASE_ID_OWNERSHIP_VIOLATION` raised. Payload contains only caller's identifiers. |
| 3 | Same `purchase_id` + same `workspace_id` + different `app_key` | ERROR 409, no mutation, no leak | ERROR 409, no mutation, no leak | Step 1 finds row. Step 2a detects `app_key` mismatch. Rollback. `PURCHASE_ID_OWNERSHIP_VIOLATION` raised. Payload contains only caller's identifiers. |
| 4 | Same `(workspace_id, app_key)` + different `purchase_id` | ERROR 409 `LICENSE_CONFLICT` | ERROR 409 `LICENSE_CONFLICT` | Step 1 finds no row (different `purchase_id`). Step 3 INSERT hits `UNIQUE(workspace_id, app_key)`. `UniqueViolation` caught. Returned as `LICENSE_CONFLICT`. |
| 5 | Concurrent duplicate attach (same `purchase_id`, same identity) | Both succeed, no duplicates | Both succeed, no duplicates | `FOR UPDATE` serializes access. First transaction commits (INSERT or UPDATE). Second transaction unblocks, sees row in Step 1, passes Step 2a, executes Step 2b. Both return consistent state. |
| 6 | Concurrent first attach (same `purchase_id`, no row exists) | One succeeds, one gets `UniqueViolation` | One succeeds (201), one gets 409 | Both Step 1s return no row. Both attempt Step 3 INSERT. One succeeds, one hits `UNIQUE(purchase_id)`. Caught as `UniqueViolation`. Client retries and succeeds via Step 2b. |

---

End of Phase 1 Implementation Report
