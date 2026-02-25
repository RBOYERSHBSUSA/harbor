# HARBOR — ACTIVATION CONTRACT v1.0
Status: BINDING (Phase 3 Certified Baseline)
Date: 2026-02-25
Applies To: Harbor Control Plane (All Apps Requiring QBO Entitlement)
Scope: Activation Bootstrap Orchestration (License + QBO Connection Coordination)

-------------------------------------------------------------------------------
## 1. PURPOSE

This contract defines the only valid behavior for Harbor “Activation.”

Activation is a deterministic orchestration layer that coordinates:

- Phase 1: Licensing / Entitlement
- Phase 2: QBO Connection State Machine

Activation does NOT modify Phase 1 or Phase 2 invariants.
Activation does NOT introduce new state machine behavior.
Activation records a one-time completion marker only.

-------------------------------------------------------------------------------
## 2. PHASE BOUNDARIES (NON-NEGOTIABLE)

### Phase 1 Owns (Authoritative)
- Workspace creation
- Membership insertion
- Licensing
- Entitlement enforcement
- Multi-tenant isolation guarantees for licensing objects

### Phase 2 Owns (Authoritative)
- QBO OAuth start
- OAuth callback
- Token exchange and encryption
- Realm binding
- QBO connection persistence
- QBO state machine transitions
- Concurrency discipline for QBO state mutation

### Activation MUST NOT
- Modify Phase 1 tables or schemas
- Modify Phase 2 schema or constraints
- Modify token encryption logic
- Modify QBO state machine transitions
- Introduce background workers
- Introduce Redis, queues, or event buses
- Introduce entitlement caching
- Introduce distributed locking

Activation MAY
- Read Phase 1 entitlement state using canonical functions
- Read Phase 2 QBO connection state
- Persist a single activation completion row per workspace

-------------------------------------------------------------------------------
## 3. DEFINITIONS

### 3.1 Activation Readiness (Derived, Not Stored)

A workspace is activation_ready if and only if:

    require_qbo_entitlement(workspace_id) succeeds
AND
    qbo_connections.status == "CONNECTED"

Readiness MUST be computed at query time.
Readiness MUST NOT be persisted as a boolean.
No caching is permitted.

### 3.2 Activation Completion (Persisted)

A workspace is activation_completed if and only if:

    A row exists in workspace_activations for that workspace.

Presence-of-row semantics only.
No status column.
No soft delete.
No deactivation behavior defined in v1.0.

-------------------------------------------------------------------------------
## 4. DATA MODEL (CANONICAL)

### Table: workspace_activations

Invariants:
- Exactly one row per workspace.
- Presence-of-row == activated.
- Referential integrity to workspaces.
- No flags or status fields.

Canonical DDL:

CREATE TABLE IF NOT EXISTS workspace_activations (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    UUID        NOT NULL UNIQUE
                                REFERENCES workspaces(id)
                                ON DELETE CASCADE,
    activated_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-------------------------------------------------------------------------------
## 5. API SURFACE (CANONICAL)

### 5.1 GET /v1/workspaces/{workspace_id}/activation/status

Authorization:
- Caller MUST be authenticated using Harbor’s canonical auth posture.
- Caller MUST be authorized for the workspace.
- No spoofable identity headers are permitted.

Response Shape:

{
  "entitlement_valid": true|false,
  "qbo_status": "CONNECTED|...|NULL",
  "activation_ready": true|false,
  "activation_completed": true|false
}

Semantics:
- entitlement_valid: derived from Phase 1 (non-raising check)
- qbo_status: read from qbo_connections.status (or null)
- activation_ready: entitlement_valid AND qbo_status == "CONNECTED"
- activation_completed: presence-of-row in workspace_activations

No caching.
No hidden flags.
No side effects.

-------------------------------------------------------------------------------
### 5.2 POST /v1/workspaces/{workspace_id}/activation/complete

Authorization:
Same as status endpoint.

Preconditions (Must Hold at Commit Time):
1. Phase 1 entitlement valid (raising mode)
2. Phase 2 qbo_connections.status == "CONNECTED"

Response Shape:

{
  "activation_completed": true,
  "already_completed": true|false
}

Idempotency:
- Multiple calls MUST be safe.
- No duplicate rows.
- No state mutation outside workspace_activations.
- activated_at is immutable after first insert.

-------------------------------------------------------------------------------
## 6. CONCURRENCY & TRANSACTION DISCIPLINE

### 6.1 Unified Workspace Advisory Lock (MANDATORY)

Activation MUST use the SAME workspace lock key derivation as Phase 2.

Rule:
- Both Activation and any QBO connection mutation
  MUST acquire the same pg_advisory_xact_lock(workspace_key).

Prohibited:
- Independent lock derivations (e.g., hashtext()).
- Separate lock namespaces for activation.

Purpose:
- Serialize activation against:
  - OAuth callback
  - connect-start
  - disconnect
  - revoke
  - refresh error transitions

-------------------------------------------------------------------------------
### 6.2 Row-Level Locking (Defense in Depth)

After acquiring advisory lock, activation MUST read QBO state with:

SELECT status
FROM qbo_connections
WHERE workspace_id = $1
FOR UPDATE;

If no row exists:
- Activation is NOT ready.

Purpose:
- Prevent TOCTOU inside transaction.
- Prevent concurrent mutation during activation evaluation.

-------------------------------------------------------------------------------
### 6.3 Transaction Ownership

Activation logic MUST:
- Acquire advisory lock inside active transaction.
- Perform entitlement check under lock.
- Perform QBO status read under lock.
- Perform insert under lock.
- Commit only on success.

Activation domain logic SHOULD NOT:
- Call rollback() on precondition failures.
- Release advisory lock prematurely.

Rollback must occur via request teardown / connection lifecycle.

-------------------------------------------------------------------------------
## 7. ACTIVATION INSERT SEMANTICS (CANONICAL)

INSERT INTO workspace_activations (workspace_id, activated_at)
VALUES ($1, NOW())
ON CONFLICT (workspace_id) DO NOTHING
RETURNING id;

Required Behavior:
- First call: row inserted → already_completed = false
- Subsequent calls: no-op → already_completed = true
- UNIQUE(workspace_id) is authoritative invariant.

-------------------------------------------------------------------------------
## 8. ERROR MODEL

Activation MUST use HarborError.

Required Error Codes:
- ACTIVATION_NOT_READY (409)
  - entitlement invalid
  - QBO not CONNECTED
  - no qbo_connections row
- WORKSPACE_ACCESS_DENIED (403)
- AUTHENTICATION_REQUIRED (401)

Prohibited:
- Raw DB error leakage.
- Cross-workspace metadata exposure.

-------------------------------------------------------------------------------
## 9. MULTI-TENANT ISOLATION

All operations MUST:
- Scope by workspace_id.
- Enforce canonical authentication.
- Enforce workspace authorization prior to mutation.

Identity must not rely on caller-supplied unsigned headers.

-------------------------------------------------------------------------------
## 10. NON-GOALS

Activation v1.0 does NOT:
- Auto-activate on OAuth callback.
- Provision downstream app resources.
- Introduce activation states beyond presence-of-row.
- Cache readiness.
- Modify QBO state.
- Implement deactivation.
- Introduce async processing.

-------------------------------------------------------------------------------
## 11. COMPLIANCE CHECKLIST (HOSTILE AUDIT)

A compliant implementation MUST satisfy:

[ ] Readiness derived, not stored.
[ ] Completion = presence-of-row in workspace_activations.
[ ] Entitlement valid AND QBO CONNECTED required at commit.
[ ] Shared advisory lock namespace with Phase 2.
[ ] QBO status read FOR UPDATE under advisory lock.
[ ] Idempotent insert (ON CONFLICT DO NOTHING).
[ ] No Phase 1 modifications.
[ ] No Phase 2 modifications.
[ ] No encryption modifications.
[ ] No state machine modifications.
[ ] Canonical auth enforced.
[ ] No raw DB error leakage.

-------------------------------------------------------------------------------
## 12. VERSIONING & CHANGE CONTROL

This contract is binding.

Any modification requires:
- Version increment (v1.1, v2.0, etc.)
- Explicit documentation of changes
- Hostile audit pass
- Phase boundary re-certification

-------------------------------------------------------------------------------

END OF ACTIVATION CONTRACT v1.0