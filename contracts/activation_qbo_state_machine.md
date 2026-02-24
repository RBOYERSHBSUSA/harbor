# Harbor Activation + QBO State Machine Contract

Status: Authoritative  
Scope: Workspace bootstrap, licensing attachment, QBO connect lifecycle

---

# PART I — Workspace Activation States

Derived States:

W0 — No workspace  
W1 — Workspace exists, no valid QBO license  
W2 — Workspace has valid QBO-dependent license  
W3 — Workspace has valid QBO license and QBO connected  

---

# PART II — Activation Transitions

## A1 — Create Workspace

From: W0  
To: W1  

DB:
- Insert workspace
- Insert membership (owner)

---

## A2 — Attach License

From: W1 → W2 (if QBO-dependent)
From: W2 → W2
From: W3 → W3

DB:
- Insert or upsert workspace_app_licenses

---

## A3 — License Expiration

From: W2 → W1  
From: W3 → W1  

No QBO disconnection occurs automatically.

---

# PART III — QBO Connection States

QC0 — NOT_CONNECTED  
QC1 — OAUTH_PENDING  
QC2 — CONNECTED  
QC3 — TOKEN_REFRESH_FAILED  
QC4 — REVOKED  
QC5 — ERROR  
QC6 — DISCONNECTED  

---

# PART IV — QBO Transitions

## Q1 — Start Connect

Guard:
- Workspace has QBO entitlement

From:
QC0, QC5, QC6, QC3 → QC1

DB:
- Upsert qbo_connections
- Store oauth_state_hash
- status = OAUTH_PENDING

---

## Q2 — OAuth Success

Guard:
- State valid
- Entitlement valid
- Advisory lock(workspace_id)

QC1 → QC2

DB:
- Store encrypted tokens
- Bind qbo_realm_id (UNIQUE)
- status = CONNECTED

---

## Q3 — OAuth Failure

QC1 → QC5

DB:
- status = ERROR
- Set last_error fields

---

## Q4 — Refresh Success

QC2 or QC3 → QC2

---

## Q5 — Refresh Failure (transient)

QC2 → QC3

---

## Q6 — Revoked

QC2 or QC3 → QC4

Tokens cleared.

---

## Q7 — Disconnect

Any → QC6

Tokens cleared.

---

# PART V — Advisory Lock Rule

All token refresh and OAuth callback writes MUST use:

pg_advisory_xact_lock(workspace_id)

---

# PART VI — Idempotency

- purchase_id is UNIQUE
- workspace_id is UNIQUE in qbo_connections
- qbo_realm_id is UNIQUE
- Duplicate activation calls must be safe

---

End of Contract