# HARBOR PHASE 2 HOSTILE AUDIT v3

**Verdict: FAIL**
**Date: 2026-02-25**
**Auditor: Independent Adversarial (Claude Code)**
**Scope: Phase 2 — QBO OAuth + Token Storage + Realm Binding + State Machine**
**Mode: Zero-Trust, Contract-Driven Verification**

---

## 1. Executive Verdict

**FAIL.**

Phase 2 fails certification due to one confirmed contract violation with a viable exploitation path:

**The OAuth callback endpoint (`GET /v1/qbo/callback`) and its handler (`handle_oauth_callback`) do not verify workspace QBO entitlement before completing the connection.** The `qbo_connection_contract.md` Section 8 and `activation_qbo_state_machine.md` Part IV Q2 Guard both require entitlement verification at callback time with no exceptions. This invariant is violated.

All other Phase 2 invariants are verified as correctly enforced. Remediation is targeted and small (one guard insertion).

---

## 2. Invariant Verification Matrix

| # | Invariant | Verified | Evidence | Risk Level |
|---|-----------|----------|----------|------------|
| 2.1 | Realm Isolation | PASS | UNIQUE(qbo_realm_id) constraint in 003 migration, verified in 004 migration. Application-level check at `qbo_connection.py:398-404`. DB constraint is backstop against race. | LOW |
| 2.2 | CONNECTED State Integrity | PASS | Five CHECK constraints in 004 migration enforce non-null realm, access_token, refresh_token, expires_at, connected_at when status='CONNECTED'. Pre-migration validation rejects invalid existing rows. Code path at `qbo_connection.py:437-463` populates all fields atomically. | NONE |
| 2.3 | OAUTH_PENDING Integrity | PASS | Two CHECK constraints in 004 migration enforce non-null oauth_state_hash and oauth_state_expires_at when status='OAUTH_PENDING'. Code paths at `qbo_connection.py:307-313` (insert) and `qbo_connection.py:322-335` (update) both set these fields. | NONE |
| 2.4 | Token Encryption Enforcement | PASS | Write path: `encrypt_str()` called at `qbo_connection.py:372-373` before DB write at line 457-458. Read path: `get_qbo_tokens()` at line 502-507 uses `is_fernet_token()` + `decrypt_str()`. No plaintext write path exists post-remediation. No token logging found (grep for `print\|log\|logger.*token` returned zero matches across all .py files). | LOW (see 3.4) |
| 2.5 | Encryption Key Discipline | PASS | `crypto.py:13-18`: missing key raises HARBOR_ENCRYPTION_KEY_REQUIRED (500). Lines 22-28: invalid key raises HARBOR_ENCRYPTION_KEY_INVALID (500). No auto-generation. No logging of key value. `encrypt_str()` calls `get_fernet()` on every invocation (no cached state that could persist a bad key). | NONE |
| 2.6 | OAuth Replay / Concurrency | PASS | State consumed atomically via `SELECT ... FOR UPDATE` at `qbo_connection.py:136-144`. consumed_at checked at line 154. Advisory lock at line 360. Duplicate callback returns INVALID_OAUTH_STATE. See Section 5 for full lock analysis. | LOW |
| 2.7 | State Machine Discipline | PASS | All four status-mutation sites call `validate_qbo_transition()`: lines 302, 317, 395, 409. No direct UPDATE bypasses found. Transition map exactly matches contract. See Section 4.3 for full trace. | NONE |
| 2.8 | Phase Boundary Discipline | PASS | Phase 1 migrations (001, 002) unmodified. `licensing.py`, `errors.py`, `db.py` contain no Phase 2 mutations. No background workers, no Redis, no queues. App placeholder files (charles, emily) are empty. | NONE |
| 2.9 | Migration Safety | PASS | 004 migration validates existing rows BEFORE adding CHECK constraints (lines 8-41). Raises EXCEPTION on invariant violation, aborting within BEGIN/COMMIT. Constraints use `status != X OR field IS NOT NULL` pattern (correct). No data dropped. | NONE |
| **2.10** | **Entitlement Gate at Callback** | **FAIL** | **Contract requires entitlement check at callback. `routes.py:299-317` does not call `require_qbo_entitlement()`. `handle_oauth_callback()` does not call it. See Section 3.1.** | **HIGH** |

---

## 3. Attack Scenario Analysis

### 3.1 ATTACK: Callback Without Entitlement (SUCCESSFUL EXPLOIT)

**Contract Requirement:**
- `qbo_connection_contract.md` Section 8: "QBO connect start and OAuth callback MUST verify workspace QBO entitlement. No exceptions."
- `activation_qbo_state_machine.md` Part IV, Q2 Guard: "Entitlement valid"

**Attack Path:**
1. Workspace W1 has a valid trial license for Charles (requires_qbo=true), trial_ends_at = T+5min.
2. At T+0: User calls `POST /v1/workspaces/W1/qbo/connect`. Entitlement check passes. OAuth state created. User receives authorize_url.
3. At T+6min: Trial expires. `is_license_valid()` now returns False for W1.
4. At T+7min: User completes authorization on Intuit's site. Intuit redirects to `GET /v1/qbo/callback?code=X&realmId=R1&state=S1`.
5. Callback handler: `consume_oauth_state(S1)` succeeds (state TTL is 10min, only 7min elapsed). Returns workspace_id=W1.
6. **No entitlement check occurs.**
7. `exchange_code_for_tokens(X)` succeeds. Tokens encrypted and stored.
8. Connection status set to CONNECTED. Realm R1 bound.
9. W1 is now CONNECTED with no valid license.

**Result:** Invariant violated. Workspace without entitlement has an active QBO connection.

**Evidence:**
- `shell/app/routes.py:299-317`: `qbo_oauth_callback()` calls `handle_oauth_callback()` directly. No `require_qbo_entitlement()` call.
- `shared/harbor_common/qbo_connection.py:347-472`: `handle_oauth_callback()` calls `consume_oauth_state()`, acquires lock, exchanges tokens, validates state transition, checks realm binding, writes connection. No entitlement call at any point.
- Confirmed via grep: `require_qbo_entitlement` appears only at `routes.py:290` (connect-start) and `routes.py:11` (import). Never in `qbo_connection.py`.

**Severity:** HIGH. Direct contract violation with viable exploitation.

**Risk Classification:** Exploitable in production. The 10-minute OAuth state TTL is the attack window. License expiration during this window is a realistic scenario for trial licenses.

---

### 3.2 ATTACK: Cross-Tenant Realm Binding via Concurrent Callbacks

**Attack Path:** Two workspaces (A and B) both complete OAuth with the same Intuit company (realm_id R1) simultaneously.

**Result:** DEFENDED.

**Evidence:**
1. Thread A: `consume_oauth_state` locks state row A, returns workspace A. Acquires `pg_advisory_xact_lock(hash(A))`.
2. Thread B: `consume_oauth_state` locks state row B, returns workspace B. Acquires `pg_advisory_xact_lock(hash(B))` (different lock, no blocking).
3. Thread A: Application-level realm check (`qbo_connection.py:398-404`) finds no existing binding. Executes `UPDATE ... SET qbo_realm_id = 'R1' WHERE workspace_id = A`.
4. Thread B: Application-level realm check also finds no existing binding (READ COMMITTED doesn't see A's uncommitted write). Attempts `UPDATE ... SET qbo_realm_id = 'R1' WHERE workspace_id = B`. **PostgreSQL blocks this UPDATE** because the UNIQUE index on qbo_realm_id detects a pending conflicting value from Thread A's uncommitted transaction.
5. Thread A commits. Thread B unblocks and receives `UniqueViolation`.
6. Thread B's transaction rolls back entirely (including state consumption).

**Defense layers:** UNIQUE(qbo_realm_id) constraint enforced by PostgreSQL at the index level during UPDATE, blocking the second writer until the first commits. Application-level check (`qbo_connection.py:398-404`) handles the non-concurrent case. Combined: invariant is maintained.

**Note:** The `UniqueViolation` exception from psycopg2 is NOT caught in `handle_oauth_callback()`. The client receives a raw 500 error instead of the structured `QBO_REALM_ALREADY_BOUND` error. This is a UX deficiency, not a security vulnerability. The state token consumption rolls back, but the Intuit auth code is consumed and cannot be reused.

**Risk Classification:** LOW (error handling gap only; data integrity maintained).

---

### 3.3 ATTACK: Direct SQL UPDATE to Bypass State Machine

**Attack Path:** Attacker with DB access attempts `UPDATE qbo_connections SET status = 'CONNECTED' WHERE workspace_id = X` without populating required fields.

**Result:** DEFENDED.

**Evidence:** 004 migration CHECK constraints:
- `qbo_connected_requires_realm`: `status != 'CONNECTED' OR qbo_realm_id IS NOT NULL`
- `qbo_connected_requires_access_token`: `status != 'CONNECTED' OR access_token IS NOT NULL`
- `qbo_connected_requires_refresh_token`: `status != 'CONNECTED' OR refresh_token IS NOT NULL`
- `qbo_connected_requires_expires_at`: `status != 'CONNECTED' OR expires_at IS NOT NULL`
- `qbo_connected_requires_connected_at`: `status != 'CONNECTED' OR connected_at IS NOT NULL`

Direct SQL setting status to CONNECTED without all five fields will be rejected by PostgreSQL.

**Risk Classification:** NONE (DB-level enforcement).

---

### 3.4 ATTACK: Plaintext Token Injection via DB Write

**Attack Path:** Attacker writes a plaintext (non-Fernet) token directly to `qbo_connections.access_token`.

**Result:** PARTIAL DEFENSE.

**Evidence:**
- DB column `access_token` is `TEXT NULL` with no format constraint. Plaintext can be written via direct SQL.
- `get_qbo_tokens()` (`qbo_connection.py:502-507`) uses `is_fernet_token()` to detect encryption. Non-Fernet values are returned as-is (plaintext passthrough).
- `is_fernet_token()` (`crypto.py:56-63`) checks `value.startswith("gAAAAA")`. This is a correct heuristic for Fernet tokens with modern timestamps (version byte 0x80 + 4 zero-bytes of timestamp high word = "gAAAAA" prefix in base64 for all timestamps < 2^32, i.e., before year 2106).
- The write path (`handle_oauth_callback`) always encrypts via `encrypt_str()`. No application-level plaintext write path exists.

**Risk Assessment:** If an attacker has direct DB write access, they have already breached the security perimeter. The plaintext passthrough in `get_qbo_tokens()` is a backward-compatibility accommodation for pre-encryption data, not a new vulnerability introduced by Phase 2.

**Risk Classification:** LOW (requires pre-existing DB-level compromise).

---

### 3.5 ATTACK: OAuth State Replay

**Attack Path:** Attacker replays a previously used state token in a second callback.

**Result:** DEFENDED.

**Evidence:**
- `consume_oauth_state()` (`qbo_connection.py:126-174`): `SELECT ... FOR UPDATE` locks the state row, checks `consumed_at IS NOT NULL` (line 154), rejects already-consumed tokens with `INVALID_OAUTH_STATE`.
- Concurrent replays: Second request blocks on `FOR UPDATE` until first completes. First marks `consumed_at`. Second sees non-null `consumed_at`, rejects.
- State token is unique (UNIQUE constraint on `qbo_oauth_states.state`).
- Expiration checked: `expires_at <= now` rejects expired tokens (line 161).

**Risk Classification:** NONE.

---

### 3.6 ATTACK: Empty Token Bypass of encrypt_str

**Attack Path:** Intuit returns `access_token: ""` which bypasses encryption (`encrypt_str("")` returns `""`).

**Result:** MITIGATED.

**Evidence:**
- `encrypt_str("")` returns `""` (`crypto.py:33-34`). Empty string would be stored unencrypted.
- However, `exchange_code_for_tokens()` validates `"access_token" in data` and `"refresh_token" in data` (`qbo_connection.py:229-236`). Present-but-empty would pass this check.
- DB CHECK constraint (`qbo_connected_requires_access_token`) checks `IS NOT NULL`. Empty string is NOT NULL, so it passes.
- Intuit's OAuth2 implementation would never return an empty access_token in practice.

**Risk Classification:** NEGLIGIBLE (theoretical only; no practical exploitation path via Intuit's API).

---

### 3.7 ATTACK: OAUTH_PENDING → CONNECTED Transition Without OAUTH_PENDING State

**Attack Path:** Connection is in NOT_CONNECTED; attacker tries to skip directly to CONNECTED.

**Result:** DEFENDED.

**Evidence:**
- `handle_oauth_callback` reads current state with `FOR UPDATE` at line 378 and validates transition at line 395: `validate_qbo_transition(conn_row["status"], "CONNECTED")`.
- ALLOWED_TRANSITIONS only permits `("OAUTH_PENDING", "CONNECTED")` for the Q2 transition. Any other from-state raises `INVALID_STATE_TRANSITION`.

**Risk Classification:** NONE.

---

## 4. State Machine Verification

### 4.1 Contract Transition Map vs. Code

| Transition | Contract | Code (ALLOWED_TRANSITIONS) | Match |
|------------|----------|---------------------------|-------|
| Q1: NC/ERR/DISC/TRF → OAUTH_PENDING | QC0,QC5,QC6,QC3 → QC1 | Lines 44-47 | EXACT |
| Q2: OAUTH_PENDING → CONNECTED | QC1 → QC2 | Line 49 | EXACT |
| Q3: OAUTH_PENDING → ERROR | QC1 → QC5 | Line 51 | EXACT |
| Q4: CONN/TRF → CONNECTED | QC2,QC3 → QC2 | Lines 53-54 | EXACT |
| Q5: CONNECTED → TRF | QC2 → QC3 | Line 56 | EXACT |
| Q6: CONN/TRF → REVOKED | QC2,QC3 → QC4 | Lines 58-59 | EXACT |
| Q7: Any → DISCONNECTED | Any → QC6 | Lines 61-67 (all 7 states) | EXACT |

### 4.2 Status Mutation Sites (Exhaustive)

| Location | SQL Operation | Status Value | validate_qbo_transition Called | Verified |
|----------|--------------|--------------|-------------------------------|----------|
| `qbo_connection.py:302` | Pre-INSERT check | NOT_CONNECTED → OAUTH_PENDING | Yes (line 302) | PASS |
| `qbo_connection.py:307-313` | INSERT | OAUTH_PENDING | Validated above | PASS |
| `qbo_connection.py:317` | Pre-UPDATE check | existing → OAUTH_PENDING | Yes (line 317) | PASS |
| `qbo_connection.py:322-335` | UPDATE | OAUTH_PENDING | Validated above | PASS |
| `qbo_connection.py:395` | Pre-UPDATE check | OAUTH_PENDING → CONNECTED | Yes (line 395) | PASS |
| `qbo_connection.py:409` | Pre-UPDATE check | OAUTH_PENDING → ERROR | Yes (line 409) | PASS |
| `qbo_connection.py:412-425` | UPDATE | ERROR | Validated above | PASS |
| `qbo_connection.py:440-463` | UPDATE | CONNECTED | Validated at 395 | PASS |

### 4.3 Bypass Search

Searched all `.py` files for `status =` and `UPDATE qbo_connections`:
- `routes.py:196`: `SET status = %s` — this is in `workspace_app_licenses` UPDATE (license attach), NOT qbo_connections. False positive.
- All `UPDATE qbo_connections` matches are within `qbo_connection.py` and are preceded by `validate_qbo_transition()` calls.
- `INSERT INTO qbo_connections` appears only at `qbo_connection.py:309` with hardcoded `'OAUTH_PENDING'`, preceded by validation at line 302.

**No bypass found.** All state mutations go through the validator.

---

## 5. Concurrency & Lock Analysis

### 5.1 Advisory Lock Usage

| Function | Lock Call | Lock ID | Scope |
|----------|----------|---------|-------|
| `start_connect` | `qbo_connection.py:287` | `_workspace_id_hash(workspace_id)` | Transaction |
| `handle_oauth_callback` | `qbo_connection.py:360` | `_workspace_id_hash(workspace_id)` | Transaction |

Lock ID derivation: `_workspace_id_hash()` (`qbo_connection.py:258-261`) uses SHA-256 of workspace UUID, takes first 8 bytes as signed int64. Deterministic and consistent.

`pg_advisory_xact_lock` is transaction-scoped, automatically released on COMMIT or ROLLBACK.

### 5.2 Row Lock Coverage

| Query | Location | Table | Purpose |
|-------|----------|-------|---------|
| `SELECT ... FOR UPDATE` | `qbo_connection.py:141` | qbo_oauth_states | Prevent concurrent state consumption |
| `SELECT ... FOR UPDATE` | `qbo_connection.py:291-296` | qbo_connections | Serialize connect-start per workspace |
| `SELECT ... FOR UPDATE` | `qbo_connection.py:376-384` | qbo_connections | Serialize callback write per workspace |
| `SELECT ... FOR UPDATE` | `routes.py:166-172` | workspace_app_licenses | License upsert (Phase 1) |

### 5.3 Lock Ordering in handle_oauth_callback

1. `qbo_oauth_states` row lock (via `consume_oauth_state`, line 357 → 141)
2. `pg_advisory_xact_lock(workspace_id)` (line 360)
3. `qbo_connections` row lock (line 376-384)

This ordering is consistent across all callback invocations. No deadlock cycle possible.

In `start_connect`, the ordering is:
1. `pg_advisory_xact_lock(workspace_id)` (line 287)
2. `qbo_connections` row lock (line 291-296)

No cross-ordering conflict with callback path because `start_connect` doesn't touch `qbo_oauth_states` with `FOR UPDATE` (it only INSERTs into it after acquiring the advisory lock).

### 5.4 Race Window Analysis

**Same-workspace concurrent operations:** Serialized by `pg_advisory_xact_lock(workspace_id)`. Second request blocks until first commits or rolls back.

**Cross-workspace concurrent realm binding:** UNIQUE(qbo_realm_id) constraint on the index blocks the second UPDATE until the first transaction resolves. If the first commits, the second receives `UniqueViolation`. Data integrity maintained. See Attack 3.2.

**Connect-start during callback:** If workspace starts a new connect while a callback is in progress, the advisory lock serializes them. The callback holds the lock; the connect-start blocks until callback completes.

**State token race:** `SELECT ... FOR UPDATE` on `qbo_oauth_states` serializes concurrent consumption attempts for the same state token.

**Long-running HTTP call concern:** `exchange_code_for_tokens()` (30s timeout) executes while advisory lock and state row lock are held (`qbo_connection.py:364`, after locks acquired at lines 357, 360). This holds locks for up to 30 seconds. Not a security vulnerability, but an operational concern for high-concurrency scenarios.

---

## 6. Schema Integrity Review

### 6.1 CHECK Constraints (post-004 migration)

| Constraint | Table | Condition | Verified |
|------------|-------|-----------|----------|
| Status enum | qbo_connections | `status IN (7 values)` | Yes (003) |
| qbo_conn_realm_id_nonempty | qbo_connections | `qbo_realm_id IS NULL OR length(trim(qbo_realm_id)) > 0` | Yes (003) |
| qbo_connected_requires_realm | qbo_connections | `status != 'CONNECTED' OR qbo_realm_id IS NOT NULL` | Yes (004) |
| qbo_connected_requires_access_token | qbo_connections | `status != 'CONNECTED' OR access_token IS NOT NULL` | Yes (004) |
| qbo_connected_requires_refresh_token | qbo_connections | `status != 'CONNECTED' OR refresh_token IS NOT NULL` | Yes (004) |
| qbo_connected_requires_expires_at | qbo_connections | `status != 'CONNECTED' OR expires_at IS NOT NULL` | Yes (004) |
| qbo_connected_requires_connected_at | qbo_connections | `status != 'CONNECTED' OR connected_at IS NOT NULL` | Yes (004) |
| qbo_oauth_pending_requires_state_hash | qbo_connections | `status != 'OAUTH_PENDING' OR oauth_state_hash IS NOT NULL` | Yes (004) |
| qbo_oauth_pending_requires_state_expiry | qbo_connections | `status != 'OAUTH_PENDING' OR oauth_state_expires_at IS NOT NULL` | Yes (004) |

### 6.2 UNIQUE Constraints

| Constraint | Table | Column(s) | Verified |
|------------|-------|-----------|----------|
| PK | qbo_connections | id | Yes (001) |
| workspace_id | qbo_connections | workspace_id | Yes (001, declared with UNIQUE) |
| uq_qbo_conn_realm_id | qbo_connections | qbo_realm_id | Yes (003, idempotently re-verified in 004) |
| state | qbo_oauth_states | state | Yes (003) |

### 6.3 NOT NULL Coverage

CONNECTED state: All five critical fields (qbo_realm_id, access_token, refresh_token, expires_at, connected_at) enforced non-null by CHECK constraints.

OAUTH_PENDING state: oauth_state_hash and oauth_state_expires_at enforced non-null by CHECK constraints.

### 6.4 Invariant Gap: No Encryption Format Constraint

The CHECK constraints enforce `access_token IS NOT NULL` when CONNECTED, but do not enforce that the value is encrypted (e.g., starts with the Fernet prefix). This means a direct SQL INSERT of a plaintext token value would satisfy the CHECK constraint. This is expected — format enforcement at the DB level for application-layer encryption is atypical and not required by the contract.

### 6.5 Column Naming Deviation

Contract (`qbo_connection_contract.md` Section 2) specifies `access_token_encrypted` and `refresh_token_encrypted`. Actual columns are `access_token` and `refresh_token` (from 001 migration). The tokens are now encrypted at the application layer despite the column names not reflecting this. Noted as a cosmetic deviation, not a functional or security gap.

---

## 7. Phase Boundary Verification

### 7.1 Phase 1 Tables — Confirmed Untouched

| Table | Modified by Phase 2? | Evidence |
|-------|----------------------|----------|
| customers | No | Not referenced in any Phase 2 file |
| users | No | Not referenced in any Phase 2 file |
| workspaces | No | Read-only SELECT in `qbo_connection.py:274` |
| memberships | No | Not referenced in any Phase 2 file |
| workspace_app_licenses | No | Read-only in `licensing.py:37-47`. Write paths only in `routes.py` license endpoint (Phase 1, unmodified) |
| apps | No | Read-only JOIN in `licensing.py:41` |
| workspace_creation_requests | No | Not referenced in any Phase 2 file |

### 7.2 Phase 1 Migrations — Confirmed Unmodified

- `001_harbor_core.sql`: Reviewed. Creates core tables. No Phase 2 changes.
- `002_workspace_licenses.sql`: Reviewed. Creates apps table, aligns licensing columns. No Phase 2 changes.

### 7.3 Phase 1 Code — Confirmed Unmodified

- `shared/harbor_common/errors.py`: 12 lines. HarborError class only. No Phase 2 changes.
- `shared/harbor_common/licensing.py`: 63 lines. `is_license_valid`, `workspace_has_qbo_entitlement`, `require_qbo_entitlement`. Read-only. No Phase 2 changes.
- `shared/harbor_common/db.py`: 25 lines. `get_db`, `close_db`. No Phase 2 changes.

### 7.4 Contracts — Confirmed Unmodified

All three contract files reviewed. No modifications detected by Phase 2.

### 7.5 Phase 3 Boundary — Confirmed Clean

- No background workers or schedulers.
- No Redis, message queues, or cron.
- No activation wizard or multi-step orchestration.
- No entitlement caching (checked live on every request).
- Placeholder files (`qbo_executor.py`, `qbo_client.py`, `auth_context.py`, `apps/charles/`, `apps/emily/`) are all empty.

---

## 8. FAIL Finding — Required Remediation

### 8.1 Vulnerability: Missing Entitlement Check on OAuth Callback

**Invariant Violated:** Contract Section 8 of `qbo_connection_contract.md`: "QBO connect start and OAuth callback MUST verify workspace QBO entitlement. No exceptions." Contract Q2 Guard of `activation_qbo_state_machine.md`: "Entitlement valid."

**Exact Location:** `shared/harbor_common/qbo_connection.py`, function `handle_oauth_callback()`, between line 357 (consume_oauth_state returns workspace_id) and line 360 (advisory lock acquisition).

**Minimal Reproduction Path:**
1. Create workspace W1 with a trial license for Charles (requires_qbo=true), trial_ends_at = now() + 5 minutes.
2. `POST /v1/workspaces/W1/qbo/connect` — succeeds, returns authorize_url.
3. Wait 6 minutes (trial expires).
4. Complete Intuit OAuth, callback arrives: `GET /v1/qbo/callback?code=X&realmId=R1&state=S1`
5. Connection completes. W1 is CONNECTED with expired license.

**Severity:** HIGH.

**Required Remediation:**
Insert `require_qbo_entitlement(workspace_id)` in `handle_oauth_callback()` after `workspace_id` is obtained from `consume_oauth_state()` and before any token exchange or connection write. Specifically, add between current lines 357 and 359:

```python
workspace_id = consume_oauth_state(state_token)

# Guard: entitlement must be valid at callback time (contract Section 8)
require_qbo_entitlement(workspace_id)

# Advisory lock per contract Part V
cur.execute("SELECT pg_advisory_xact_lock(%s)", (_workspace_id_hash(workspace_id),))
```

This requires adding the import: `from harbor_common.licensing import require_qbo_entitlement`

The entitlement check should occur before the advisory lock and token exchange to fail fast without holding locks or making external HTTP calls.

---

## 9. Secondary Observations (Not FAIL-worthy)

### 9.1 Unhandled UniqueViolation in Realm Race (LOW)

`handle_oauth_callback()` does not catch `psycopg2.errors.UniqueViolation` from the COMMIT at line 465. In the rare concurrent-realm-binding race, the client receives a raw 500 instead of a structured `QBO_REALM_ALREADY_BOUND` (409) error. Data integrity is maintained by the DB constraint; only the error response is degraded.

### 9.2 Pre-Encryption Plaintext Token Persistence (LOW)

`get_qbo_tokens()` silently returns pre-encryption plaintext tokens via the `is_fernet_token()` heuristic fallback. No re-encryption occurs on read. These tokens persist until the workspace re-connects. Documented in the remediation report as expected behavior. No new plaintext write path exists.

### 9.3 encrypt_str Empty String Bypass (NEGLIGIBLE)

`encrypt_str("")` returns `""` without encryption. Theoretical only — Intuit's API never returns empty tokens, and `exchange_code_for_tokens()` validates key presence.

---

End of Phase 2 Hostile Audit v3.
