# PHASE 2 ENTITLEMENT CALLBACK FIX

## A) Vulnerability Summary
- **Finding:** Hostile audit v3 FAILED due to missing callback entitlement verification in the QBO OAuth flow.
- **Exploit:** A user could initiate an OAuth connection (connect-start) while holding a valid license, but if that license expired during the OAuth redirect window (the state TTL), the callback would still proceed to bind the QBO company and mark the connection as `CONNECTED`.
- **Impact:** Allows unauthorized QBO connections without an active, entitled license, violating core system mandates.

## B) Exact Code Change

### Import Section
```python
from harbor_common.db import get_db
from harbor_common.errors import HarborError
from harbor_common.licensing import require_qbo_entitlement
```

### Updated handle_oauth_callback Function
```python
def handle_oauth_callback(code: str, realm_id: str, state_token: str) -> dict:
    """Q2 — OAuth callback handler.

    Validates state, exchanges code for tokens, binds realm, persists connection.
    All writes happen in a single transaction.
    """
    db = get_db()
    cur = db.cursor()

    # 1. Validate and consume state token (inside transaction)
    workspace_id = consume_oauth_state(state_token)

    # Guard: entitlement must be valid at callback time (contract compliance)
    require_qbo_entitlement(workspace_id)

    # 2. Advisory lock per contract Part V
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (_workspace_id_hash(workspace_id),))

    # 3. Exchange code for tokens (external HTTP call — happens inside our txn,
    #    but before any connection writes; if this fails, nothing is written)
    token_data = exchange_code_for_tokens(code)
    now = datetime.now(timezone.utc)
    access_token_plain = token_data["access_token"]
    refresh_token_plain = token_data["refresh_token"]
    expires_in = token_data["expires_in"]
    token_expires_at = now + timedelta(seconds=expires_in)

    # 4. Encrypt tokens at application layer before storage
    access_token_enc = encrypt_str(access_token_plain)
    refresh_token_enc = encrypt_str(refresh_token_plain)

    # 5. Lock and read current connection row
    cur.execute(
        """
        SELECT id, workspace_id, status, qbo_realm_id
          FROM qbo_connections
         WHERE workspace_id = %s
           FOR UPDATE
        """,
        (str(workspace_id),),
    )
    conn_row = cur.fetchone()

    if conn_row is None:
        raise HarborError(
            code="INVALID_STATE_TRANSITION",
            message="No connection record found for this workspace.",
            metadata={"workspace_id": str(workspace_id)},
        )

    # 6. Validate state transition: must be OAUTH_PENDING → CONNECTED
    validate_qbo_transition(conn_row["status"], "CONNECTED")

    # 7. Check realm binding — realm_id must not be bound to another workspace
    cur.execute(
        """
        SELECT workspace_id FROM qbo_connections
         WHERE qbo_realm_id = %s AND workspace_id != %s
        """,
        (realm_id, str(workspace_id)),
    )
    conflict = cur.fetchone()
    if conflict is not None:
        # Per contract Section 4: set status = ERROR, set last_error_code
        # Q3 transition: OAUTH_PENDING → ERROR
        validate_qbo_transition(conn_row["status"], "ERROR")
        cur.execute(
            """
            UPDATE qbo_connections
               SET status = 'ERROR',
                   last_error_code = 'REALM_ALREADY_BOUND',
                   last_error_message = %s,
                   last_error_at = %s,
                   updated_at = now()
             WHERE workspace_id = %s
            """,
            (
                f"Realm {realm_id} is already bound to workspace {conflict['workspace_id']}.",
                now,
                str(workspace_id),
            ),
        )
        db.commit()
        raise HarborError(
            code="QBO_REALM_ALREADY_BOUND",
            message="This QBO company is already connected to another workspace.",
            metadata={
                "realm_id": realm_id,
                "workspace_id": str(workspace_id),
            },
            status_code=409,
        )

    # 8. Update connection row — bind realm, store tokens (encrypted), set CONNECTED
    cur.execute(
        """
        UPDATE qbo_connections
           SET status = 'CONNECTED',
               qbo_realm_id = %s,
               access_token = %s,
               refresh_token = %s,
               expires_at = %s,
               connected_at = %s,
               oauth_state_hash = NULL,
               oauth_state_expires_at = NULL,
               last_error_code = NULL,
               last_error_message = NULL,
               last_error_at = NULL,
               updated_at = now()
         WHERE workspace_id = %s
        """,
        (
            realm_id,
            access_token_enc,
            refresh_token_enc,
            token_expires_at,
            now,
            str(workspace_id),
        ),
    )

    db.commit()

    return {
        "workspace_id": str(workspace_id),
        "realm_id": realm_id,
        "status": "CONNECTED",
        "connected_at": now.isoformat(),
    }
```

## C) Contract Compliance Statement
- Entitlement is now rigorously verified at both **connect-start** (in `qbo_connect_start` route) and **callback** (in `handle_oauth_callback`).
- This satisfies the contract requirement for callback-time guardrails with no exceptions.

## D) Boundary Confirmation
- **No Phase 1 changes:** Core schema and initial workspace logic remain untouched.
- **No licensing logic changes:** `licensing.py` remains a read-only dependency.
- **No schema/migration changes:** No DDL was required for this fix.
- **No state machine changes:** State transitions remain exactly as specified in the contract.
- **No encryption changes:** Fernet encryption logic is preserved and runs after the entitlement guard.
- **No Phase 3 logic introduced:** No background workers or orchestration were added.

## E) Ready-for-Reaudit Statement
- Phase 2 implementation is now fully compliant with the entitlement guard mandates.
- The codebase is ready for hostile audit v3 re-execution.
