# HARBOR PHASE 2 HOSTILE AUDIT v3 (POST-ENCRYPTION HARDENING)

**Verdict: PASS**
**Date: 2026-02-25**
**Auditor: Independent Adversarial (Claude Code — Clean Session)**
**Scope: Phase 2 — QBO OAuth + Token Storage + Realm Binding + State Machine**
**Mode: Zero-Trust, Contract-Driven Verification**
**Prior Audit: PHASE_2_AUDIT_v3_FAIL.md — FAIL (missing callback entitlement check)**

---

## 1. Executive Verdict

**PASS.**

Phase 2 is certified. The single FAIL finding from the prior v3 audit (missing `require_qbo_entitlement()` at OAuth callback time) has been confirmed remediated. All nine core invariants are enforced at both application and database levels. No exploitable vulnerability remains.

Three secondary observations are carried forward as LOW/NEGLIGIBLE risk items. None constitute invariant violations or viable exploitation paths.

---

## 2. Invariant Verification Matrix

| # | Invariant | Verified | Evidence | Risk Level |
|---|-----------|----------|----------|------------|
| 2.1 | Realm Isolation | PASS | `UNIQUE(qbo_realm_id)` constraint in 003 migration (`uq_qbo_conn_realm_id`), idempotently re-verified in 004 migration. Application-level check at `qbo_connection.py:401-407`. UNIQUE index blocks concurrent realm binding at PostgreSQL level. `qbo_conn_realm_id_nonempty` CHECK prevents empty-string realm IDs. | LOW |
| 2.2 | CONNECTED State Integrity | PASS | Five CHECK constraints in 004 migration enforce non-null `qbo_realm_id`, `access_token`, `refresh_token`, `expires_at`, `connected_at` when `status='CONNECTED'`. Pre-migration validation (004 lines 8-25) rejects invalid existing rows. Code path at `qbo_connection.py:441-466` populates all fields atomically with encrypted token values. | NONE |
| 2.3 | OAUTH_PENDING Integrity | PASS | Two CHECK constraints in 004 migration enforce non-null `oauth_state_hash` and `oauth_state_expires_at` when `status='OAUTH_PENDING'`. Code path INSERT (`qbo_connection.py:308-314`) and UPDATE (`qbo_connection.py:323-335`) both set these fields before status mutation. | NONE |
| 2.4 | Token Encryption Enforcement | PASS | Write path: `encrypt_str()` called at `qbo_connection.py:376-377` before DB write at lines 447-448/461-462. Read path: `get_qbo_tokens()` at lines 506-511 uses `is_fernet_token()` + `decrypt_str()`. No plaintext write path exists in application code. No token logging found (grep for `print\|log\|logger` across all `.py` files returned zero token-related matches). | LOW |
| 2.5 | Encryption Key Discipline | PASS | `crypto.py:13-18`: missing key raises `HARBOR_ENCRYPTION_KEY_REQUIRED` (500). Lines 22-29: invalid key raises `HARBOR_ENCRYPTION_KEY_INVALID` (500). No auto-generation. No key logging. `encrypt_str()` calls `get_fernet()` on every invocation — no cached key state. | NONE |
| 2.6 | OAuth Replay / Concurrency | PASS | State consumed atomically via `SELECT ... FOR UPDATE` at `qbo_connection.py:137-145`. `consumed_at` checked at line 155. `expires_at` checked at line 162. Advisory lock at line 364. Duplicate callback returns `INVALID_OAUTH_STATE`. Row lock serializes concurrent consumption of same state token. | LOW |
| 2.7 | State Machine Discipline | PASS | All four status-mutation sites call `validate_qbo_transition()`: lines 303, 318, 399, 413. No direct UPDATE/INSERT bypass found. Transition map (lines 43-69) exactly matches contract Parts III/IV. Exhaustive search of all `.py` files confirms no qbo_connections status writes outside `qbo_connection.py`. | NONE |
| 2.8 | Phase Boundary Discipline | PASS | Phase 1 migrations (001, 002) unmodified. `licensing.py`, `errors.py`, `db.py` contain no Phase 2 mutations. No background workers, no Redis, no queues, no cron. Placeholder files (`qbo_executor.py`, `qbo_client.py`, `auth_context.py`, `apps/charles/`, `apps/emily/`) are all empty. No entitlement caching. | NONE |
| 2.9 | Migration Safety | PASS | 004 migration validates existing rows BEFORE adding CHECK constraints (lines 8-41). Raises EXCEPTION on invariant violation, aborting within BEGIN/COMMIT. Constraints use correct `status != X OR field IS NOT NULL` pattern. No data dropped. UNIQUE constraint addition is idempotent (checks `pg_constraint` first). | NONE |
| 2.10 | Entitlement Gate at Callback | PASS | **Remediated.** `require_qbo_entitlement(workspace_id)` now called at `qbo_connection.py:361`, after state consumption (line 358) and before advisory lock (line 364). Import confirmed at `qbo_connection.py:22`. Connect-start entitlement check confirmed at `routes.py:290`. Both entry points now enforce contract Section 8. | NONE |

---

## 3. Attack Scenario Analysis

### 3.1 ATTACK: Callback Without Entitlement (Previously FAIL — Now REMEDIATED)

**Contract Requirement:**
- `qbo_connection_contract.md` Section 8: "QBO connect start and OAuth callback MUST verify workspace QBO entitlement. No exceptions."
- `activation_qbo_state_machine.md` Part IV, Q2 Guard: "Entitlement valid"

**Prior Attack Path:**
1. Workspace W1 has a trial license, trial_ends_at = T+5min.
2. At T+0: `POST /v1/workspaces/W1/qbo/connect` — entitlement check passes.
3. At T+6min: Trial expires.
4. At T+7min: Callback arrives — previously succeeded without entitlement check.

**Current Result: DEFENDED.**

**Evidence:**
- `qbo_connection.py:22`: `from harbor_common.licensing import require_qbo_entitlement`
- `qbo_connection.py:360-361`:
  ```python
  # Guard: entitlement must be valid at callback time (contract compliance)
  require_qbo_entitlement(workspace_id)
  ```
- Placement is correct: after `consume_oauth_state()` returns `workspace_id` (line 358), before advisory lock (line 364) and token exchange (line 368).
- If entitlement check fails, `HarborError` with code `QBO_ENTITLEMENT_REQUIRED` (403) is raised. Transaction rolls back (state consumption included), no token exchange occurs, no connection write occurs.
- Connect-start also checks: `routes.py:290` calls `require_qbo_entitlement(workspace_id)`.

**Grep verification:** `require_qbo_entitlement` appears at:
- `routes.py:290` (connect-start guard)
- `qbo_connection.py:361` (callback guard)
- `licensing.py:55` (definition)
- Nowhere else in application code.

**Risk Classification:** NONE. Contract fully satisfied.

---

### 3.2 ATTACK: Cross-Tenant Realm Binding via Concurrent Callbacks

**Attack Path:** Two workspaces (A and B) both complete OAuth with the same Intuit company (realm_id R1) simultaneously.

**Result: DEFENDED.**

**Evidence:**
1. Thread A: `consume_oauth_state` locks state row A (FOR UPDATE), returns workspace A. Acquires `pg_advisory_xact_lock(hash(A))`.
2. Thread B: `consume_oauth_state` locks state row B (FOR UPDATE), returns workspace B. Acquires `pg_advisory_xact_lock(hash(B))` (different lock — no blocking).
3. Thread A: Application-level realm check (`qbo_connection.py:401-407`) finds no existing binding. Executes `UPDATE ... SET qbo_realm_id = 'R1' WHERE workspace_id = A`.
4. Thread B: Application-level realm check also finds no existing binding (READ COMMITTED doesn't see A's uncommitted write). Attempts `UPDATE ... SET qbo_realm_id = 'R1' WHERE workspace_id = B`.
5. **PostgreSQL blocks Thread B's UPDATE** — the UNIQUE index on `qbo_realm_id` detects a pending conflicting value from Thread A's uncommitted transaction.
6. Thread A commits. Thread B unblocks and receives `UniqueViolation`.
7. Thread B's transaction rolls back entirely (including state consumption).

**Defense layers:** `UNIQUE(qbo_realm_id)` constraint enforced at the PostgreSQL index level during UPDATE, blocking the second writer. Application-level check handles the non-concurrent case. Combined: realm isolation invariant is maintained under all conditions.

**Note:** The `UniqueViolation` exception is not caught in `handle_oauth_callback()` — client receives 500 instead of structured `QBO_REALM_ALREADY_BOUND` (409). UX deficiency only; data integrity maintained.

**Risk Classification:** LOW (error handling gap; data integrity maintained by DB constraint).

---

### 3.3 ATTACK: Direct SQL UPDATE to Bypass State Machine

**Attack Path:** Attacker with DB access attempts `UPDATE qbo_connections SET status = 'CONNECTED' WHERE workspace_id = X` without populating required fields.

**Result: DEFENDED.**

**Evidence:** 004 migration CHECK constraints reject at the PostgreSQL level:
- `qbo_connected_requires_realm`: `status != 'CONNECTED' OR qbo_realm_id IS NOT NULL`
- `qbo_connected_requires_access_token`: `status != 'CONNECTED' OR access_token IS NOT NULL`
- `qbo_connected_requires_refresh_token`: `status != 'CONNECTED' OR refresh_token IS NOT NULL`
- `qbo_connected_requires_expires_at`: `status != 'CONNECTED' OR expires_at IS NOT NULL`
- `qbo_connected_requires_connected_at`: `status != 'CONNECTED' OR connected_at IS NOT NULL`

Direct SQL setting status to CONNECTED without all five fields is rejected by PostgreSQL regardless of application layer.

Similarly, `UPDATE ... SET status = 'OAUTH_PENDING'` without `oauth_state_hash` and `oauth_state_expires_at` is rejected.

**Risk Classification:** NONE (DB-level enforcement).

---

### 3.4 ATTACK: Plaintext Token Injection via Direct DB Write

**Attack Path:** Attacker writes a plaintext (non-Fernet) token directly to `qbo_connections.access_token`.

**Result: PARTIAL DEFENSE (acceptable).**

**Evidence:**
- DB column `access_token` is `TEXT NULL` with no format constraint. Plaintext can be written via direct SQL.
- `get_qbo_tokens()` (`qbo_connection.py:506-511`) uses `is_fernet_token()` to detect encryption. Non-Fernet values are returned as-is (plaintext passthrough).
- `is_fernet_token()` (`crypto.py:56-63`) checks `value.startswith("gAAAAA")` — correct heuristic for all Fernet tokens with timestamps before year 2106.
- All application-layer write paths encrypt via `encrypt_str()` before DB write (`qbo_connection.py:376-377`).
- A crafted plaintext starting with "gAAAAA" would be sent through `decrypt_str()`, which would fail with `TOKEN_DECRYPTION_FAILED` (500) — fails closed, not open.

**Risk Assessment:** Requires pre-existing DB-level compromise. No application-level plaintext write path exists. Plaintext passthrough in `get_qbo_tokens()` is a backward-compatibility accommodation for pre-encryption data, not a new vulnerability.

**Risk Classification:** LOW (requires DB-level compromise).

---

### 3.5 ATTACK: OAuth State Replay

**Attack Path:** Attacker replays a previously used state token in a second callback.

**Result: DEFENDED.**

**Evidence:**
- `consume_oauth_state()` (`qbo_connection.py:127-175`): `SELECT ... FOR UPDATE` locks the state row, checks `consumed_at IS NOT NULL` (line 155), rejects already-consumed tokens with `INVALID_OAUTH_STATE`.
- Concurrent replays: Second request blocks on `FOR UPDATE` until first completes. First marks `consumed_at`. Second sees non-null `consumed_at`, rejects.
- State token is unique (`UNIQUE` constraint on `qbo_oauth_states.state`).
- Expiration checked: `expires_at <= now` rejects expired tokens (line 162).
- TTL is 10 minutes (`OAUTH_STATE_TTL_MINUTES = 10`).

**Risk Classification:** NONE.

---

### 3.6 ATTACK: Empty Token Bypass of encrypt_str

**Attack Path:** Intuit returns `access_token: ""` which bypasses encryption (`encrypt_str("")` returns `""`).

**Result: MITIGATED.**

**Evidence:**
- `encrypt_str("")` returns `""` (`crypto.py:33-34`). Empty string would be stored unencrypted.
- `exchange_code_for_tokens()` validates key presence (`"access_token" in data`, lines 230-236) but not emptiness.
- DB CHECK constraint (`qbo_connected_requires_access_token`) checks `IS NOT NULL`. Empty string is NOT NULL, so it passes.
- Intuit's OAuth2 implementation never returns empty access tokens. This is a theoretical edge case with no practical exploitation path.

**Risk Classification:** NEGLIGIBLE.

---

### 3.7 ATTACK: OAUTH_PENDING to CONNECTED Skip

**Attack Path:** Connection is in NOT_CONNECTED; attacker crafts callback to skip directly to CONNECTED.

**Result: DEFENDED.**

**Evidence:**
- `handle_oauth_callback` reads current state with `FOR UPDATE` at line 381 and validates transition at line 399: `validate_qbo_transition(conn_row["status"], "CONNECTED")`.
- `ALLOWED_TRANSITIONS` only permits `("OAUTH_PENDING", "CONNECTED")` for the Q2 transition. Any other from-state raises `INVALID_STATE_TRANSITION`.
- Additionally, the callback requires a valid, unconsumed, non-expired state token, which can only exist if `start_connect` was called first (creating the OAUTH_PENDING state).

**Risk Classification:** NONE.

---

### 3.8 ATTACK: Encryption Key Absent or Invalid

**Attack Path:** Deploy without `HARBOR_ENCRYPTION_KEY` or with an invalid key.

**Result: DEFENDED.**

**Evidence:**
- `crypto.py:13-18`: Missing key (`os.environ.get("HARBOR_ENCRYPTION_KEY")` returns `None` or `""`) raises `HarborError(code="HARBOR_ENCRYPTION_KEY_REQUIRED", status_code=500)`.
- `crypto.py:22-29`: Invalid key (wrong format for Fernet) raises `HarborError(code="HARBOR_ENCRYPTION_KEY_INVALID", status_code=500)`.
- No auto-generation: the code reads from env only, never generates or persists a key.
- No key logging: exception message references key validity, not key value.
- `get_fernet()` is called fresh on every `encrypt_str()`/`decrypt_str()` invocation — no cached Fernet instance that could persist with a stale key.

**Risk Classification:** NONE.

---

### 3.9 ATTACK: ON CONFLICT Misuse for Realm Overwrite

**Attack Path:** Exploit an `ON CONFLICT` clause to silently overwrite realm binding.

**Result: NO ATTACK SURFACE.**

**Evidence:** No `ON CONFLICT` clause exists in any `qbo_connections` query. The connection write path uses:
- `INSERT INTO qbo_connections ... VALUES ...` (line 308-314) — no ON CONFLICT, will raise UniqueViolation on duplicate workspace_id.
- `UPDATE qbo_connections ... WHERE workspace_id = %s` (lines 323-335, 416-429, 444-466) — scoped to the workspace's own row.
- No `INSERT ... ON CONFLICT DO UPDATE` pattern that could be exploited.

**Risk Classification:** NONE.

---

## 4. Concurrency & Lock Analysis

### 4.1 Advisory Lock Usage

| Function | Lock Call Location | Lock ID | Scope |
|----------|-------------------|---------|-------|
| `start_connect` | `qbo_connection.py:288` | `_workspace_id_hash(workspace_id)` | Transaction (`pg_advisory_xact_lock`) |
| `handle_oauth_callback` | `qbo_connection.py:364` | `_workspace_id_hash(workspace_id)` | Transaction (`pg_advisory_xact_lock`) |

Lock ID derivation: `_workspace_id_hash()` (`qbo_connection.py:259-262`) uses SHA-256 of workspace UUID string, takes first 8 bytes as signed int64. Deterministic and consistent. Collision probability negligible for realistic workspace counts.

### 4.2 Row Lock Coverage

| Query | Location | Table | Purpose |
|-------|----------|-------|---------|
| `SELECT ... FOR UPDATE` | `qbo_connection.py:137-145` | `qbo_oauth_states` | Prevent concurrent state consumption |
| `SELECT ... FOR UPDATE` | `qbo_connection.py:291-298` | `qbo_connections` | Serialize connect-start per workspace |
| `SELECT ... FOR UPDATE` | `qbo_connection.py:380-388` | `qbo_connections` | Serialize callback write per workspace |

### 4.3 Lock Ordering in handle_oauth_callback

1. `qbo_oauth_states` row lock (via `consume_oauth_state`, line 358 -> 137)
2. `pg_advisory_xact_lock(workspace_id_hash)` (line 364)
3. `qbo_connections` row lock (lines 380-388)

This ordering is consistent across all callback invocations. No deadlock cycle is possible.

### 4.4 Lock Ordering in start_connect

1. `pg_advisory_xact_lock(workspace_id_hash)` (line 288)
2. `qbo_connections` row lock (lines 291-298)

No cross-ordering conflict with callback path: `start_connect` only INSERTs into `qbo_oauth_states` (no `FOR UPDATE`), and the INSERT occurs after the advisory lock is held.

### 4.5 Race Window Analysis

| Scenario | Defense | Result |
|----------|---------|--------|
| Same-workspace concurrent callbacks | Advisory lock serializes. Second request blocks until first commits/rolls back. | SAFE |
| Cross-workspace concurrent realm binding | `UNIQUE(qbo_realm_id)` index blocks second UPDATE until first transaction resolves. | SAFE |
| Connect-start during callback (same workspace) | Advisory lock serializes. Connect-start blocks until callback completes. | SAFE |
| Concurrent state token consumption | `SELECT ... FOR UPDATE` on `qbo_oauth_states` serializes. Second sees `consumed_at IS NOT NULL`. | SAFE |
| Entitlement expiry during OAuth redirect | `require_qbo_entitlement()` at callback (line 361) catches expired licenses. Transaction rolls back. State unconsumed. | SAFE |

### 4.6 Long-Running Lock Concern (Operational, Not Security)

`exchange_code_for_tokens()` (30s timeout) executes while advisory lock and state row lock are held (after lines 358, 364). This holds locks for up to 30 seconds per callback. Not a security vulnerability, but an operational concern for high-concurrency scenarios.

---

## 5. Schema Integrity Review

### 5.1 CHECK Constraints (post-004 migration)

| Constraint | Table | Condition | Source |
|------------|-------|-----------|--------|
| Status enum | qbo_connections | `status IN ('NOT_CONNECTED','OAUTH_PENDING','CONNECTED','TOKEN_REFRESH_FAILED','REVOKED','ERROR','DISCONNECTED')` | 003 |
| qbo_conn_realm_id_nonempty | qbo_connections | `qbo_realm_id IS NULL OR length(trim(qbo_realm_id)) > 0` | 003 |
| qbo_connected_requires_realm | qbo_connections | `status != 'CONNECTED' OR qbo_realm_id IS NOT NULL` | 004 |
| qbo_connected_requires_access_token | qbo_connections | `status != 'CONNECTED' OR access_token IS NOT NULL` | 004 |
| qbo_connected_requires_refresh_token | qbo_connections | `status != 'CONNECTED' OR refresh_token IS NOT NULL` | 004 |
| qbo_connected_requires_expires_at | qbo_connections | `status != 'CONNECTED' OR expires_at IS NOT NULL` | 004 |
| qbo_connected_requires_connected_at | qbo_connections | `status != 'CONNECTED' OR connected_at IS NOT NULL` | 004 |
| qbo_oauth_pending_requires_state_hash | qbo_connections | `status != 'OAUTH_PENDING' OR oauth_state_hash IS NOT NULL` | 004 |
| qbo_oauth_pending_requires_state_expiry | qbo_connections | `status != 'OAUTH_PENDING' OR oauth_state_expires_at IS NOT NULL` | 004 |

### 5.2 UNIQUE Constraints

| Constraint | Table | Column(s) | Source |
|------------|-------|-----------|--------|
| PK | qbo_connections | id | 001 |
| (implicit UNIQUE) | qbo_connections | workspace_id | 001 |
| uq_qbo_conn_realm_id | qbo_connections | qbo_realm_id | 003, re-verified 004 |
| (implicit UNIQUE) | qbo_oauth_states | state | 003 |

### 5.3 NOT NULL Coverage

- **CONNECTED state:** All five critical fields (`qbo_realm_id`, `access_token`, `refresh_token`, `expires_at`, `connected_at`) enforced non-null by CHECK constraints.
- **OAUTH_PENDING state:** Both `oauth_state_hash` and `oauth_state_expires_at` enforced non-null by CHECK constraints.

### 5.4 Noted Deviations (Non-FAIL)

**Column naming:** Contract (`qbo_connection_contract.md` Section 2) specifies `access_token_encrypted` and `refresh_token_encrypted`. Actual columns are `access_token` and `refresh_token` (from 001 migration). Tokens ARE encrypted at the application layer; column names do not reflect this. Cosmetic deviation, not a functional or security gap.

**No encryption format constraint:** CHECK constraints enforce `IS NOT NULL` for tokens when CONNECTED, but do not enforce Fernet format. A direct SQL write of plaintext satisfies the CHECK. This is expected and standard — format enforcement for application-layer encryption at the DB level is atypical and not required by the contract.

---

## 6. Phase Boundary Verification

### 6.1 Phase 1 Tables — Confirmed Untouched

| Table | Modified by Phase 2? | Evidence |
|-------|---------------------|----------|
| customers | No | Not referenced in any Phase 2 code file |
| users | No | Not referenced in any Phase 2 code file |
| workspaces | No | Read-only SELECT in `qbo_connection.py:275` |
| memberships | No | Not referenced in any Phase 2 code file |
| workspace_app_licenses | No | Read-only in `licensing.py:37-47`. Write paths only in `routes.py` license endpoint (Phase 1, unmodified) |
| apps | No | Read-only JOIN in `licensing.py:41` |
| workspace_creation_requests | No | Not referenced in any Phase 2 code file |

### 6.2 Phase 1 Migrations — Confirmed Unmodified

- `001_harbor_core.sql`: Creates core tables. No Phase 2 changes.
- `002_workspace_licenses.sql`: Creates apps table, aligns licensing columns. No Phase 2 changes.

### 6.3 Phase 1 Code — Confirmed Unmodified

- `shared/harbor_common/errors.py`: 12 lines. HarborError class only. No Phase 2 changes.
- `shared/harbor_common/licensing.py`: 63 lines. Pure read-only functions. No Phase 2 changes.
- `shared/harbor_common/db.py`: 25 lines. Connection management only. No Phase 2 changes.

### 6.4 Contracts — Confirmed Unmodified

- `contracts/licensing_model.md`: Reviewed. No modifications.
- `contracts/qbo_connection_contract.md`: Reviewed. No modifications.
- `contracts/activation_qbo_state_machine.md`: Reviewed. No modifications.

### 6.5 Phase 3 Boundary — Confirmed Clean

- No background workers or schedulers.
- No Redis, message queues, or cron.
- No activation wizard or multi-step orchestration.
- No entitlement caching (checked live on every request via `require_qbo_entitlement`).
- Placeholder files are all empty: `qbo_executor.py`, `qbo_client.py`, `auth_context.py`, `apps/charles/app/routes.py`, `apps/emily/app/routes.py`.
- No Phase 3 imports or references in any Phase 2 code.

---

## 7. Prior FAIL Remediation Verification

### 7.1 Finding: Missing Entitlement Check on OAuth Callback

**Prior Audit:** `PHASE_2_AUDIT_v3_FAIL.md`, Section 8.1.

**Required Remediation:** Insert `require_qbo_entitlement(workspace_id)` in `handle_oauth_callback()` after state consumption and before token exchange.

**Verification:**

1. **Import added:** `qbo_connection.py:22` — `from harbor_common.licensing import require_qbo_entitlement`
2. **Guard inserted:** `qbo_connection.py:360-361`:
   ```python
   # Guard: entitlement must be valid at callback time (contract compliance)
   require_qbo_entitlement(workspace_id)
   ```
3. **Placement correct:** After `consume_oauth_state()` (line 358), before advisory lock (line 364) and token exchange (line 368). Fails fast without holding locks or making external HTTP calls.
4. **Transaction semantics correct:** If entitlement check fails, `HarborError` propagates, transaction rolls back (state consumption reversed), no tokens exchanged, no connection written.

**Status: REMEDIATED. Verified.**

---

## 8. Secondary Observations (Not FAIL-worthy)

### 8.1 Unhandled UniqueViolation in Concurrent Realm Binding (LOW)

`handle_oauth_callback()` does not catch `psycopg2.errors.UniqueViolation` from the COMMIT. In the rare concurrent-realm-binding race, the client receives 500 instead of structured `QBO_REALM_ALREADY_BOUND` (409). Data integrity is maintained by the DB UNIQUE constraint; only the error response is degraded. State consumption rolls back, but the Intuit auth code is already consumed externally.

### 8.2 Pre-Encryption Plaintext Token Persistence (LOW)

`get_qbo_tokens()` silently returns pre-encryption plaintext tokens via the `is_fernet_token()` heuristic fallback. No re-encryption on read. These tokens persist until workspace re-connects. Documented in `PHASE_2_TOKEN_ENCRYPTION_REMEDIATION.md` Section 5 as expected behavior. No new plaintext write path exists.

### 8.3 encrypt_str Empty String Bypass (NEGLIGIBLE)

`encrypt_str("")` returns `""` without encryption. Theoretical only — Intuit's API never returns empty tokens, and `exchange_code_for_tokens()` validates key presence in the response.

---

## 9. Audit Methodology

### 9.1 Files Examined

**Contracts:**
- `contracts/licensing_model.md`
- `contracts/qbo_connection_contract.md`
- `contracts/activation_qbo_state_machine.md`

**Migrations:**
- `deploy/migrations/001_harbor_core.sql`
- `deploy/migrations/002_workspace_licenses.sql`
- `deploy/migrations/003_qbo_connections_phase2.sql`
- `deploy/migrations/004_phase2_invariant_hardening.sql`

**Implementation:**
- `shared/harbor_common/qbo_connection.py`
- `shared/harbor_common/crypto.py`
- `shared/harbor_common/errors.py`
- `shared/harbor_common/licensing.py`
- `shared/harbor_common/db.py`
- `shared/harbor_common/__init__.py`
- `shared/setup.py`
- `shell/app/routes.py`
- `shell/app/create_app.py`
- `shell/app/config.py`
- `shell/gunicorn.conf.py`
- `shell/requirements.txt`

**Phase 3 Boundary Verification (confirmed empty):**
- `shared/harbor_common/qbo_executor.py`
- `shared/harbor_common/qbo_client.py`
- `shared/harbor_common/auth_context.py`
- `apps/charles/app/routes.py`
- `apps/emily/app/routes.py`

**Prior Reports (not trusted, verified against code):**
- `deploy/reports/PHASE_2_IMPLEMENTATION_REPORT.md`
- `deploy/reports/PHASE_2_HARDENING_CERTIFICATION.md`
- `deploy/reports/PHASE_2_TOKEN_ENCRYPTION_REMEDIATION.md`
- `deploy/reports/PHASE_2_ENTITLEMENT_CALLBACK_FIX.md`
- `deploy/reports/PHASE_2_AUDIT_v3_FAIL.md`

### 9.2 Search Patterns Executed

| Pattern | Scope | Purpose | Findings |
|---------|-------|---------|----------|
| `access_token` | All files | Token write/read paths | All application writes use `encrypt_str()` |
| `refresh_token` | All files | Token write/read paths | All application writes use `encrypt_str()` |
| `UPDATE qbo_connections` | All files | Status mutation bypass | All 3 UPDATE sites in `qbo_connection.py`, all preceded by `validate_qbo_transition()` |
| `status\s*=` | `*.py` | Direct status assignment | Only in SQL strings within `qbo_connection.py` (validated) and `routes.py` license endpoint (Phase 1, not qbo_connections) |
| `realm` | `*.py` | Realm binding paths | All realm operations in `qbo_connection.py`, properly guarded |
| `oauth` (case-insensitive) | `*.py` | OAuth flow completeness | All OAuth operations in `qbo_connection.py` and `routes.py` |
| `require_qbo_entitlement` | All files | Entitlement guard placement | Present at `routes.py:290` (connect-start) and `qbo_connection.py:361` (callback) |
| `encrypt_str\|decrypt_str\|is_fernet_token` | `*.py` | Encryption usage | Correct usage in `qbo_connection.py`; definitions in `crypto.py` |
| `log\|print\|logger` | `*.py` | Token logging risk | No token-related logging found |

---

End of Phase 2 Hostile Audit v3 (Post-Encryption Hardening).
