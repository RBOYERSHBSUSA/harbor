# Harbor Licensing Model Contract

Status: Authoritative  
Scope: Workspace-scoped application licensing and QBO entitlement gating  
Applies To: All Harbor apps (Charles, Emily, future apps)

---

## 1. Core Invariants

1. Licensing is scoped to a Workspace.
2. Billing is scoped to a Customer.
3. A Workspace may hold multiple App Licenses.
4. Each App License is unique per (workspace_id, app_key).
5. Licensing is operationally independent from QBO connection state.
6. Licensing determines entitlement to use an app and, where applicable, entitlement to connect to QBO.

---

## 2. License Storage Model

Licenses are stored in:

harbor.workspace_app_licenses

Required fields:

- id (uuid)
- customer_id
- workspace_id
- app_key
- status
- starts_at
- ends_at (nullable)
- trial_ends_at (nullable)
- purchase_id (UNIQUE)
- created_at
- updated_at

Constraint:

UNIQUE (workspace_id, app_key)

---

## 3. License Status Semantics

Valid statuses:

- trial
- active
- expired
- canceled
- past_due (optional)

A license is considered VALID if:

- status IN ('trial', 'active')
AND
- (ends_at IS NULL OR ends_at > now())
AND
- (trial_ends_at IS NULL OR trial_ends_at > now())

All other states are NON-VALID.

---

## 4. QBO-Dependent Apps

Each app defines:

requires_qbo (boolean)

An app may only initiate QBO connection if:

requires_qbo = true

---

## 5. Workspace QBO Entitlement Rule

A workspace has QBO entitlement if:

EXISTS license WHERE
- workspace_id = ?
- app.requires_qbo = true
- license is VALID

This rule is authoritative and must be enforced:

- Before QBO connect start
- At OAuth callback time
- Before any QBO API call
- During token refresh

---

## 6. Expiration Behavior

If a license becomes invalid:

- QBO connection is NOT automatically deleted.
- All QBO operations must be blocked.
- System returns LICENSE_REQUIRED error.

No background processes are required.

---

## 7. Idempotency

purchase_id MUST be UNIQUE.

Repeated purchase webhooks or activation calls MUST NOT create duplicate licenses.

---

End of Contract