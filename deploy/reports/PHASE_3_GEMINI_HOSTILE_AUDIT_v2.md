# PHASE 3 HOSTILE AUDIT (V2 CLEAN SESSION) — REPORT

**Date:** 2026-02-25
**Auditor:** Gemini CLI (Hostile Audit Mode)
**Subject:** Harbor Phase 3 — Activation Bootstrap Orchestration
**Status:** Post-Remediation Verification

---

## 1. Executive Summary

The Phase 3 Activation implementation has been subjected to a hostile audit focusing on concurrency, multi-tenant isolation, and transaction safety. The remediation successfully addresses the critical flaws identified in previous audits (v1). Specifically, the unification of the advisory lock namespace and the addition of row-level locking (`FOR UPDATE`) on the QBO connection state have eliminated the TOCTOU (Time-of-Check to Time-of-Use) race conditions and serialization gaps. Multi-tenant isolation is maintained through strict workspace-scoping of all queries and locks.

---

## 2. Concurrency Analysis

The system utilizes a dual-locking strategy to ensure deterministic state transitions under high concurrency.

### 2.1 Unified Advisory Locking
Phase 2 (OAuth/Connection) and Phase 3 (Activation) now share a canonical lock-key derivation function in `harbor_common/locks.py`. This ensures that all operations affecting a workspace's QBO state or activation status are serialized.

**Trace: Activation vs. OAuth Callback (Q2)**
| Step | Thread A (Activation) | Thread B (OAuth Callback) | Impact |
|:---:|:---|:---|:---|
| 1 | `BEGIN` | | |
| 2 | `SELECT pg_advisory_xact_lock(W1)` | | T1 acquires lock |
| 3 | | `BEGIN` | |
| 4 | | `SELECT pg_advisory_xact_lock(W1)` | **T2 BLOCKS** |
| 5 | `SELECT status FROM qbo_connections FOR UPDATE` | (waiting) | T1 checks status (e.g., NOT_CONNECTED) |
| 6 | `RAISE ACTIVATION_NOT_READY` | (waiting) | T1 fails, lock held until teardown |
| 7 | `ROLLBACK` (via teardown) | (waiting) | T1 releases lock |
| 8 | | (Acquires Lock) | T2 proceeds |
| 9 | | `UPDATE qbo_connections SET status='CONNECTED'` | |
| 10 | | `COMMIT` | |

**Result:** Serialization is guaranteed. There is no window where Activation can "observe" a pending connection state change.

### 2.2 Row-Level Locking (FOR UPDATE)
The addition of `SELECT ... FOR UPDATE` on `qbo_connections` inside the advisory lock provides "Defense in Depth." Even if a future code path fails to use the advisory lock, any attempt to mutate the connection status will block until the activation transaction completes (and vice versa).

---

## 3. Multi-Tenant Isolation Analysis

Multi-tenant isolation is preserved through the following mechanisms:

1.  **Workspace-Scoped Queries:** Every database operation in the activation flow (entitlement check, status check, and activation insert) is strictly filtered by `workspace_id`.
2.  **Workspace-Scoped Locking:** Advisory locks are derived from the `workspace_id`, ensuring that contention is isolated to a single tenant.
3.  **Auth Posture:** In accordance with the "existing Harbor auth posture," identity verification is delegated to the infrastructure layer. The removal of the spoofable `X-User-Id` header (Fixed R3) aligns the implementation with the system's security architecture, though it places total reliance on the reverse proxy for path-based authorization.

---

## 4. Transaction Discipline Analysis

The implementation follows a clean "Commit on Success, Teardown on Error" pattern.

-   **Success:** `db.commit()` is called only after all preconditions and the activation insert have succeeded.
-   **Error:** `HarborError` is raised immediately upon precondition failure. No explicit `db.rollback()` is called (Fixed R4), ensuring the advisory lock is held until the Flask request context is torn down and the connection is closed. This eliminates the "lock release gap" where concurrent requests could observe inconsistent transient states.

---

## 5. Idempotency Analysis

Activation idempotency is correctly implemented via:
```sql
INSERT INTO workspace_activations (workspace_id, activated_at)
VALUES (%s, NOW())
ON CONFLICT (workspace_id) DO NOTHING
RETURNING id
```
-   **First Call:** Inserts row, returns ID. `already_completed = false`.
-   **Subsequent Calls:** No-op, returns NULL. `already_completed = true`.
-   **Safety:** The `UNIQUE(workspace_id)` constraint is the source of truth, preventing duplicate activations regardless of application-layer race conditions.

---

## 6. Critical Findings

**NONE.** All critical serialization and TOCTOU issues from v1 are resolved.

---

## 7. Major Findings

### 7.1 Missing Disconnect/Revoke Implementation
While the state machine defined in `qbo_connection.py` allows transitions to `DISCONNECTED` and `REVOKED`, there are no corresponding functions in the codebase to trigger these states. While this does not affect the *safety* of activation, it limits the *testability* of the `CONNECTED -> DISCONNECTED` interleavings in a live environment.

### 7.2 Total Reliance on Infrastructure Auth
The application layer performs zero validation of user-to-workspace membership. It assumes that if a request reaches the route, it has been authorized by the gateway. While this matches the described "posture," it represents a "Fail-Open" design if the infrastructure layer is bypassed or misconfigured.

---

## 8. Hardening Recommendations

1.  **Application-Layer Auth Guard:** Implement a membership check (`SELECT 1 FROM memberships WHERE workspace_id = %s AND user_id = %s`) that consumes a trusted, non-spoofable identity header (e.g., JWT or signature-verified header) provided by the infrastructure layer.
2.  **Complete State Machine:** Implement the `disconnect` and `revoke` functions to ensure the connection lifecycle is fully manageable and that the `ACTIVATION_NOT_READY` paths can be empirically verified in production.

---

## 9. Certification Verdict: PASS

The Phase 3 Activation implementation is **CERTIFIED** for production use. It successfully enforces deterministic state transitions, ensures workspace isolation, and handles high-concurrency scenarios safely through unified advisory locking and row-level serialization.

**Verdict: PASS**
