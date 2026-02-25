# HARBOR — PHASE 2 HARDENING CERTIFICATION

Status: Final Certification
Phase: 2 (Hardened)
Mode: Deterministic Verification

---

## 5.1 Migration Content

**File:** `deploy/migrations/004_phase2_invariant_hardening.sql`

```sql
BEGIN;
SET search_path = harbor;

------------------------------------------------------------
-- 2.1 PRE-MIGRATION SAFETY VALIDATION
------------------------------------------------------------

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM harbor.qbo_connections
    WHERE status = 'CONNECTED'
      AND (
        qbo_realm_id IS NULL OR
        access_token IS NULL OR
        refresh_token IS NULL OR
        expires_at IS NULL OR
        connected_at IS NULL
      )
  ) THEN
    RAISE EXCEPTION
      'Invariant violation: CONNECTED rows missing required fields';
  END IF;
END;
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM harbor.qbo_connections
    WHERE status = 'OAUTH_PENDING'
      AND (
        oauth_state_hash IS NULL OR
        oauth_state_expires_at IS NULL
      )
  ) THEN
    RAISE EXCEPTION
      'Invariant violation: OAUTH_PENDING rows missing required fields';
  END IF;
END;
$$;

------------------------------------------------------------
-- 2.2 CONNECTED STATE ENFORCEMENT
------------------------------------------------------------

ALTER TABLE harbor.qbo_connections
  ADD CONSTRAINT qbo_connected_requires_realm
    CHECK (status != 'CONNECTED' OR qbo_realm_id IS NOT NULL),
  ADD CONSTRAINT qbo_connected_requires_access_token
    CHECK (status != 'CONNECTED' OR access_token IS NOT NULL),
  ADD CONSTRAINT qbo_connected_requires_refresh_token
    CHECK (status != 'CONNECTED' OR refresh_token IS NOT NULL),
  ADD CONSTRAINT qbo_connected_requires_expires_at
    CHECK (status != 'CONNECTED' OR expires_at IS NOT NULL),
  ADD CONSTRAINT qbo_connected_requires_connected_at
    CHECK (status != 'CONNECTED' OR connected_at IS NOT NULL);

------------------------------------------------------------
-- 2.3 OAUTH_PENDING STATE ENFORCEMENT
------------------------------------------------------------

ALTER TABLE harbor.qbo_connections
  ADD CONSTRAINT qbo_oauth_pending_requires_state_hash
    CHECK (status != 'OAUTH_PENDING' OR oauth_state_hash IS NOT NULL),
  ADD CONSTRAINT qbo_oauth_pending_requires_state_expiry
    CHECK (status != 'OAUTH_PENDING' OR oauth_state_expires_at IS NOT NULL);

------------------------------------------------------------
-- 2.4 REALM UNIQUENESS VALIDATION
------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_qbo_conn_realm_id'
    ) THEN
        ALTER TABLE harbor.qbo_connections
          ADD CONSTRAINT uq_qbo_conn_realm_id UNIQUE (qbo_realm_id);
    END IF;
END;
$$;

COMMIT;
```

---

## 5.2 Invariant Enforcement Matrix

| Status | Required Non-Null Fields | DB Enforced? |
|---|---|---|
| NOT_CONNECTED | (None) | Yes (Table Default) |
| OAUTH_PENDING | oauth_state_hash, oauth_state_expires_at | **Yes (004 Migration)** |
| CONNECTED | qbo_realm_id, access_token, refresh_token, expires_at, connected_at | **Yes (004 Migration)** |
| TOKEN_REFRESH_FAILED | (None) | Yes |
| REVOKED | (None) | Yes |
| ERROR | (None) | Yes |
| DISCONNECTED | (None) | Yes |

---

## 5.3 Code Integrity Confirmation

- **No invariant bypass paths found.** All status mutations in `shared/harbor_common/qbo_connection.py` utilize the `validate_qbo_transition()` validator.
- No direct SQL `UPDATE` or `INSERT` bypassing the state machine was identified in the shell or shared layers.
- All code paths leading to `CONNECTED` or `OAUTH_PENDING` statuses correctly populate the fields now enforced by the database.
- Tokens cannot exist in a `CONNECTED` state without a corresponding `qbo_realm_id` and expiry, as now enforced by `004_phase2_invariant_hardening.sql`.

---

## 5.4 Structural Alignment Statement

- **Canonical Table:** `harbor.qbo_connections` is the canonical storage for QBO connection state, as mandated by `qbo_connection_contract.md`.
- **Naming Consistency:** `qbo_realm_id` and `qbo_connections` are used consistently across the codebase.
- **Orphan References:** No references to `workspace_qbo_connections` were found; the implementation correctly used the contract-mandated table name.
- **Contract Alignment:** The implementation aligns with the structural requirements of the QBO connection and state machine contracts, with token column naming deviations noted in the Phase 2 report as deferred to Phase 4 hardening.

---

## 5.5 Phase Boundary Reaffirmation

- **Phase 1 Integrity:** No Phase 1 logic or tables (`workspaces`, `memberships`, `workspace_app_licenses`) were altered.
- **Licensing:** No licensing logic was modified; QBO entitlement continues to be checked live via the Phase 1 `require_qbo_entitlement` guard.
- **No Phase 3 Logic:** No orchestration, background workers, or multi-step wizards were introduced.
- **No Distributed State:** No Redis, message queues, or external state management added.

---

## 5.6 Final Certification Verdict

**PHASE 2 CERTIFIED (HARDENED)**

**Justification:** The Phase 2 implementation has been successfully hardened with database-level invariants. The state machine is now enforced both at the application level and the database level, ensuring that connection integrity is maintained even in the event of application-level logic errors. Structural alignment is confirmed, and phase boundaries have been strictly respected.
