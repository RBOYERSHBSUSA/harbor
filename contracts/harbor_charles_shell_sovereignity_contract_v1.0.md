# HARBOR — PHASE 4D SHELL SOVEREIGNTY CONTRACT (v1.0)

**Status:** DRAFT → FOR IMPLEMENTATION  
**Project:** Harbor  
**App:** Harbor Charles  
**Phase:** 4D — Shell Integration Layer  
**Date:** 2026-02-28  
**Preconditions:** Phase 4C Certified (DB-enforced workspace isolation) — tag `PHASE_4C_CERTIFIED` exists

---

## 0. Purpose (Binding)

This contract defines the **non-negotiable invariants** required to make **Harbor Shell the sole execution authority** for Harbor Charles.

Phase 4D is **control-plane integration** only. It must not rewrite or alter reconciliation logic.

**Core invariant:** Harbor Charles MUST NOT be executable without a **Harbor Shell–issued workspace execution context**.

---

## 1. Scope

### 1.1 In Scope (Allowed Changes)
- Shell → Charles integration routing and middleware
- Introduction of a minimal, explicit **Shell→App invocation interface**
- Removal/disablement of standalone execution paths inside Harbor Charles
- Token access refactor to be **shell-mediated**
- Activation gating enforced **before any Charles route/handler executes**
- Tests that prove the invariants

### 1.2 Out of Scope (Forbidden)
- Any reconciliation behavior change (importing, canonicalization, matching, reconciliation, posting semantics)
- Data model redesign beyond what’s required for shell mediation (no schema churn)
- Migration of pilot clients
- Switching DB engine (SQLite remains)
- Refactoring “for cleanliness” not required by this contract

---

## 2. Definitions (Normative)

### 2.1 Harbor Shell
The control plane responsible for:
- Workspace identification and boundary enforcement
- Licensing and activation gating
- OAuth token lifecycle governance
- App registry and routing

### 2.2 Harbor Charles
The business engine responsible for:
- Importing transactions
- Canonicalization
- Matching and reconciliation
- App-specific UI/API behaviors (as invoked under shell governance)

### 2.3 WorkspaceExecutionContext (WEC)
A structured, shell-issued context object required for any Charles execution. Minimum fields:
- `workspace_id` (UUID)
- `app_id` or constant identifier for Charles
- `qbo_realm_id` (string) — if required for the execution path
- `qbo_access_token` (string) — provided by shell mediation (see §5)
- `issued_at` (timestamp) — informational/trace
- Optional: `request_id` / correlation id

**Normative rule:** Charles MUST NOT construct WEC; only shell issues it.

---

## 3. Authority & Execution Invariants (MUST)

### INV-4D-1 — Shell Is the Only Entry Authority
All externally reachable Charles capabilities (routes/handlers) MUST be invoked through Harbor Shell routing.

- Charles MUST NOT mount/serve its own independent web application to the public.
- Charles MUST NOT expose direct “app root” routes that bypass shell middleware.

**Pass condition:** There exists no HTTP route path that executes Charles logic without first passing through shell gating and WEC injection.

---

### INV-4D-2 — WEC Is Mandatory for Any Charles Execution
All Charles request handlers that perform work MUST require WEC and MUST reject execution if WEC is absent.

- No inference of workspace from company_id
- No inference from headers, cookies, query params, or realm_id
- No global or thread-local fallback

**Pass condition:** Attempting to call any Charles handler without WEC fails deterministically with a classified error.

---

### INV-4D-3 — Activation Gate Happens Before Charles Logic
Before any Charles logic begins:
- Workspace MUST be validated
- App activation MUST be validated for workspace
- License/entitlement MUST be valid for workspace
- Realm binding (if applicable) MUST be consistent with workspace
- Token availability MUST be verified (or explicitly return OAuth-required error)

**Pass condition:** For a non-entitled workspace, Charles code is not entered (no DB writes, no sync, no business actions).

---

### INV-4D-4 — No Standalone Runtime
Harbor Charles MUST NOT be operable as a standalone system.

Forbidden:
- Charles starting its own server process for production usage
- Standalone CLI entrypoints that execute real work without shell gating
- Internal schedulers/cron loops inside Charles

**Pass condition:** There is no supported/maintained “run Charles alone” path that can execute business operations.

---

### INV-4D-5 — OAuth Token Sovereignty (Shell-Mediated)
Charles MUST NOT:
- Read tokens directly from persistent token storage
- Refresh tokens
- Manage token lifecycle

Instead:
- Shell retrieves/refreshes tokens (per existing Harbor OAuth governance)
- Shell injects `qbo_access_token` into WEC (or a shell-owned token provider interface)
- Charles consumes token only from WEC (or the provided interface)

**Pass condition:** Code search shows no Charles-owned token refresh/storage access paths used for runtime execution.

---

### INV-4D-6 — Background Execution Is Shell-Controlled
All background operations (sync, scheduled tasks) MUST be initiated by shell with WEC.

- No autonomous schedulers inside Charles
- No background jobs that can run with inferred workspace context

**Pass condition:** Any job runner requires explicit workspace_id via WEC.

---

### INV-4D-7 — Logging Must Preserve Tenant Traceability
All Charles operations MUST log:
- `workspace_id`
- `request_id` or equivalent correlation id (if available)
- operation name / handler
- result classification

**Pass condition:** For representative routes, logs contain workspace_id consistently.

---

## 4. Prohibited Behaviors (MUST NOT)

- MUST NOT add “shortcuts” that allow bypass of shell gating for dev convenience.
- MUST NOT read workspace_id from `company_id` or any legacy identifier to determine tenant boundary.
- MUST NOT implement a second licensing system or duplicate entitlement logic inside Charles.
- MUST NOT add cross-module imports that couple Charles directly to shell internals beyond the defined interface layer (§6).
- MUST NOT change reconciliation semantics (including ordering, matching criteria, idempotency meanings).

---

## 5. Token Interface Requirements (MUST)

One of the following MUST be implemented:

### Option A (Preferred): Token-In-WEC
Shell injects `qbo_access_token` into WEC and passes WEC to Charles.

### Option B: Shell-Owned Token Provider
Shell passes a `TokenProvider` in WEC:
- `get_access_token(workspace_id) -> str`
- Refresh behavior remains shell-owned
- Charles only calls provider; it never reads storage

**Normative constraint:** Under either option, token persistence/refresh resides only in shell.

---

## 6. Integration Architecture Shape (MUST)

A thin adapter layer MUST exist, conceptually:

`Shell Router/Middleware → WEC Builder → Charles Adapter → Charles Handlers`

Requirements:
- Shell owns HTTP routing and middleware
- Charles handler signatures accept WEC (directly or via a request-scoped object)
- Charles adapter is the only bridging surface and must remain minimal

---

## 7. Testing & Verification (MUST)

Phase 4D MUST include automated checks proving:

### TEST-4D-1 — No-Context Rejection
Calling Charles execution entrypoints without WEC fails deterministically.

### TEST-4D-2 — License/Activation Gate
Non-entitled workspace cannot enter Charles logic.

### TEST-4D-3 — Token Mediation
If token missing/expired and shell cannot provide token, request fails with OAuth-required classification; Charles does not attempt refresh.

### TEST-4D-4 — Route Exposure Audit
A route inventory test (or static check) proves that externally exposed routes are shell-owned and gated.

### TEST-4D-5 — No Standalone Server
Static grep/assertions or CI checks ensure no production entrypoint mounts Charles as an independent server.

---

## 8. Error Classification (MUST)

Failures must be classified consistently with Harbor contracts (errors vs ambiguity taxonomy, where applicable). At minimum, Phase 4D introduces/uses these outcome classes:

- `AUTH_REQUIRED` / `OAUTH_REQUIRED` (token missing)
- `FORBIDDEN` (activation/license invalid)
- `BAD_REQUEST` (missing WEC / malformed context)
- `INTERNAL_ERROR` (unexpected)

**Normative rule:** Missing WEC is a contract violation and MUST be treated as an immediate failure.

---

## 9. Acceptance Criteria (PASS/FAIL)

Phase 4D is **PASS** only if all are true:

1. Shell is the only route owner for Charles capabilities.
2. Charles requires WEC for any execution.
3. Activation gating occurs before any Charles logic.
4. Token lifecycle is shell-mediated; Charles never refreshes or reads token storage directly.
5. No standalone runtime path exists for production usage.
6. Background execution is shell-controlled via WEC.
7. Required tests exist and pass.

Any single violation is **FAIL**.

---

## 10. Change Control

- This contract is binding for Phase 4D.
- Any deviation requires an explicit contract revision (v1.1+) committed before implementation changes.
- Phase 4D completion must be tagged: `PHASE_4D_CERTIFIED` only after a hostile audit confirms invariants.

---

## 11. Deliverables (Required Artifacts)

Implementation must produce:
- `PHASE_4D_IMPLEMENTATION_REPORT.md` (what changed, why, invariant mapping)
- `PHASE_4D_AUDIT_PROMPT.md` (hostile audit instructions)
- Test evidence (CI output or command transcript)

---

**END OF CONTRACT — PHASE 4D SHELL SOVEREIGNTY (v1.0)**