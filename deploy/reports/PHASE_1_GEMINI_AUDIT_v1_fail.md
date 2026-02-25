# HARBOR — PHASE 1 GEMINI AUDIT

## VERDICT

FAIL

---

## CRITICAL VIOLATIONS

1. **Cross-Workspace License Mutation (Authorization Bypass):** The `attach_license` endpoint in `shell/app/routes.py` implements an unsafe `ON CONFLICT (purchase_id) DO UPDATE` clause. It fails to verify that the incoming `workspace_id` matches the `workspace_id` of the existing record associated with that `purchase_id`. Consequently, any caller can modify the license status and expiration dates of another workspace's license by simply reusing its `purchase_id`.
2. **License Identity Divergence:** In the event of a `purchase_id` conflict, the `DO UPDATE` clause only updates status and time-related fields. It does not update (nor validate the consistency of) the `workspace_id`, `app_key`, or `customer_id`. This allows the database state to diverge from the request intent (e.g., a request to attach a purchase to App B will instead update the dates for App A if the `purchase_id` was previously linked to App A) and causes the API to return a 200 OK containing the wrong identity metadata.
3. **Information Leakage:** When a `purchase_id` conflict occurs across workspaces, the `RETURNING *` clause returns the existing row (belonging to the original workspace) to the new, unauthorized caller. This leaks the `workspace_id` and `customer_id` of the original license holder to the second caller.

---

## RISK FINDINGS

1. **Implicit Customer ID Trust:** The `customer_id` is derived from the workspace during the initial insert but is never updated or verified during subsequent idempotent calls. If a workspace were to change owners or if a purchase were validly reassigned, the `customer_id` would remain stale.
2. **Time Synchronization Sensitivity:** The system relies on Python's `datetime.now(timezone.utc)` for application-layer validity checks and Postgres `now()` for the `v_workspace_qbo_connect_allowed` view. While both use UTC, significant clock drift between the application and database servers could lead to inconsistent entitlement states.
3. **Database-Only Constraint Enforcement:** The application relies entirely on catching database `UniqueViolation` and `ForeignKeyViolation` exceptions for business logic (e.g., preventing duplicate app licenses). While functionally correct, it forces a hard dependency on specific Postgres exception names and reduces the portability of the service layer logic.

---

## PHASE BOUNDARY STATUS

Phase 1 boundary respected.
