# PHASE 3 HOSTILE AUDIT REPORT (GEMINI CLEAN SESSION)

**Project:** Harbor Phase 3 — Activation Bootstrap Orchestration  
**Status:** FAIL  
**Auditor:** Gemini (Hostile Mode)  
**Date:** 2026-02-25  

---

## 1. Executive Summary

The Phase 3 Activation implementation contains a **critical concurrency flaw** that allows workspace activation to proceed even when the QBO connection is in a non-connected state (DISCONNECTED, REVOKED, or ERROR). This is caused by a namespace mismatch in advisory locks between Phase 2 (QBO Connection) and Phase 3 (Activation). Furthermore, the implementation is vulnerable to TOCTOU (Time-of-Check to Time-of-Use) attacks due to lack of row-level locking on precondition checks. Multi-tenant security relies on an unauthenticated header (`X-User-Id`), which presents a significant spoofing risk.

---

## 2. Critical Findings

### [CRITICAL] C-01: Advisory Lock Namespace Mismatch
**Description:** Phase 2 and Phase 3 use different algorithms to derive advisory lock keys from the same `workspace_id`.
- **Phase 2 (`qbo_connection.py`):** Uses Python `hashlib.sha256(uuid).digest()[:8]` (64-bit bigint).
- **Phase 3 (`activation.py`):** Uses Postgres `hashtext(workspace_id::text)` (32-bit integer).

**Impact:** These locks do not block each other. `complete_activation` can run concurrently with `handle_oauth_callback`, `disconnect`, or `revoke` flows for the same workspace, violating the Phase 3 requirement to avoid races with concurrent connection state changes.

### [CRITICAL] C-02: TOCTOU Vulnerability in Activation Flow
**Description:** `complete_activation` checks the `qbo_connections.status` using a standard `SELECT` without a row lock (`FOR UPDATE`) and without a valid shared advisory lock (see C-01).
**Impact:** A concurrent transaction can change the connection status (e.g., to `DISCONNECTED`) immediately after the check but before the activation row is committed. This allows the system to enter a "Zombie Activation" state: activated without a valid connection.

---

## 3. Major Findings

### [MAJOR] M-01: Insecure User Identification via `X-User-Id`
**Description:** The membership check `_require_membership` trusts the `X-User-Id` request header blindly.
**Impact:** If the network isolation is breached or the VM port is exposed, an attacker can spoof any `user_id` to gain unauthorized access to workspaces. In a multi-tenant SaaS control plane, identity must be verified via signed tokens (JWT/OIDC) or a secure gateway.

### [MAJOR] M-02: Premature Rollback in Domain Logic
**Description:** `complete_activation` calls `db.rollback()` inside a `try/except` block before re-raising a `HarborError`.
**Impact:** `pg_advisory_xact_lock` is transaction-scoped. Calling `db.rollback()` releases the lock immediately. If there were any subsequent operations or if the caller expected to handle the error within a larger transaction, the lock protection is lost prematurely, and the transaction state is destroyed inconsistently.

---

## 4. Concurrency Analysis (Race Trace)

| Step | Thread A (Activation) | Thread B (Disconnect) | State Impact |
| :--- | :--- | :--- | :--- |
| 1 | `SELECT pg_advisory_xact_lock(hashtext(W1))` | | Lock A Acquired (Key 12345) |
| 2 | | `SELECT pg_advisory_xact_lock(_hash(W1))` | Lock B Acquired (Key 987654) |
| 3 | `SELECT status FROM qbo_connections` | | Sees `CONNECTED` |
| 4 | | `UPDATE qbo_connections SET status='DISCONNECTED'` | Status change pending |
| 5 | | `COMMIT` | **Status = DISCONNECTED** |
| 6 | `INSERT INTO workspace_activations` | | Row created |
| 7 | `COMMIT` | | **Activation Succeeded** |

**Final State:** Workspace `W1` is "Activated" but its QBO connection is "DISCONNECTED". This violates the deterministic invariant that Activation implies a connected state.

---

## 5. Multi-Tenant Isolation Analysis
The current implementation uses:
```sql
SELECT 1 FROM memberships WHERE workspace_id = %s AND user_id = %s
```
While this correctly scopes the check to the requested `workspace_id`, the lack of `user_id` verification makes it trivial to bypass if `user_id`s are known or predictable. The system fails to preserve strong isolation at the identity layer.

---

## 6. Transaction Discipline Analysis
The use of `db.commit()` and `db.rollback()` inside `complete_activation` (shared/common logic) is "Hostile" to transaction orchestration.
1. **Implicit Release:** `rollback()` releases the advisory lock before the error is returned to the user.
2. **Double Commit:** If `complete_activation` were part of a larger workflow, it would force a commit of all prior work, preventing the caller from rolling back the entire unit of work on subsequent failures.

---

## 7. Hardening Recommendations

1. **Unify Lock Key Derivation:** Use a single, shared utility function for advisory lock keys. **Recommendation:** Standardize on the Phase 2 SHA-256 implementation as it provides a larger 64-bit keyspace.
2. **Atomic Precondition Check:** In `complete_activation`, use `SELECT status FROM qbo_connections WHERE workspace_id = %s FOR UPDATE` to ensure the status cannot change during the activation transaction.
3. **Remove Explicit Rollback:** Remove `db.rollback()` from `complete_activation`. Allow the exception to bubble up to the route handler or transaction manager, which should handle the rollback.
4. **Secure Identity:** Replace `X-User-Id` with a validated session token or ensure the application only listens on a local interface behind a trusted, authenticating reverse proxy.

---

## 8. Certification Verdict: FAIL

The Phase 3 implementation violates core requirements for race-safety, transaction integrity, and multi-tenant security. It fails to properly serialize against Phase 2 state transitions and introduces a significant TOCTOU vulnerability.

**Status: FAIL**
