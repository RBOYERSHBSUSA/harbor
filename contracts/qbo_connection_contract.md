# Harbor QBO Connection Contract

Status: Authoritative  
Scope: Workspace-scoped QuickBooks Online connection lifecycle

---

## 1. Core Invariants

1. Each Workspace may have at most one QBO connection.
2. Each QBO Realm ID may be connected to at most one Workspace.
3. QBO connection is workspace-scoped.
4. QBO tokens are never stored outside harbor.qbo_connections.
5. QBO operations are license-gated.

---

## 2. Storage Model

Table: harbor.qbo_connections

Required fields:

- workspace_id (PK, FK)
- status
- qbo_realm_id (UNIQUE)
- access_token_encrypted
- refresh_token_encrypted
- access_token_expires_at
- refresh_token_expires_at (nullable)
- oauth_state_hash
- oauth_state_expires_at
- last_error_code
- last_error_message
- last_error_at
- connected_at
- updated_at

---

## 3. Connection States

Allowed statuses:

- NOT_CONNECTED
- OAUTH_PENDING
- CONNECTED
- TOKEN_REFRESH_FAILED
- REVOKED
- ERROR
- DISCONNECTED

Only these states are valid.

---

## 4. Realm Binding Rule

qbo_realm_id must be globally UNIQUE across all workspaces.

Attempting to bind an already-bound realm must:

- Fail transaction
- Set status = ERROR
- Set last_error_code = REALM_ALREADY_BOUND

---

## 5. Token Refresh Model

- Refresh is synchronous.
- No background queue exists.
- Refresh occurs during QBO API calls.
- Advisory lock must be taken per workspace during refresh.

---

## 6. Revocation Handling

If refresh returns invalid_grant:

- status → REVOKED
- Tokens cleared
- Reconnect required

---

## 7. Disconnect Semantics

User-initiated disconnect:

- status → DISCONNECTED
- Tokens cleared
- Realm ID may be cleared (recommended)

---

## 8. Licensing Gate

QBO connect start and OAuth callback MUST verify workspace QBO entitlement.

No exceptions.

---

End of Contract