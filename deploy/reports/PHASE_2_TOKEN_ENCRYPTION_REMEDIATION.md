# PHASE 2 TOKEN ENCRYPTION REMEDIATION

## 1. FILES CHANGED/ADDED
- `harbor/shared/harbor_common/crypto.py` (NEW)
- `harbor/shared/harbor_common/qbo_connection.py` (MODIFIED)
- `harbor/shared/setup.py` (MODIFIED)
- `harbor/shell/requirements.txt` (MODIFIED)

---

## 2. NEW FILE: shared/harbor_common/crypto.py
```python
import os
import base64
from cryptography.fernet import Fernet
from harbor_common.errors import HarborError

def get_fernet() -> Fernet:
    """Initialize Fernet from HARBOR_ENCRYPTION_KEY environment variable.
    
    Raises:
        HarborError: HARBOR_ENCRYPTION_KEY_REQUIRED (500) if missing
        HarborError: HARBOR_ENCRYPTION_KEY_INVALID (500) if invalid
    """
    key = os.environ.get("HARBOR_ENCRYPTION_KEY")
    if not key:
        raise HarborError(
            code="HARBOR_ENCRYPTION_KEY_REQUIRED",
            message="Application encryption key is missing (HARBOR_ENCRYPTION_KEY).",
            status_code=500,
        )
    
    try:
        # Validate key format
        return Fernet(key.encode())
    except Exception as e:
        raise HarborError(
            code="HARBOR_ENCRYPTION_KEY_INVALID",
            message=f"Application encryption key is invalid: {str(e)}",
            status_code=500,
        )

def encrypt_str(plaintext: str) -> str:
    """Encrypt a string using Fernet. Returns base64 encoded string."""
    if not plaintext:
        return ""
    f = get_fernet()
    return f.encrypt(plaintext.encode()).decode()

def decrypt_str(ciphertext: str) -> str:
    """Decrypt a Fernet-encrypted string.
    
    Raises:
        HarborError: TOKEN_DECRYPTION_FAILED (500)
    """
    if not ciphertext:
        return ""
    f = get_fernet()
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except Exception as e:
        raise HarborError(
            code="TOKEN_DECRYPTION_FAILED",
            message=f"Failed to decrypt token: {str(e)}",
            status_code=500,
        )

def is_fernet_token(value: str) -> bool:
    """Best-effort check if a string is a Fernet-encrypted token.
    Fernet tokens usually start with 'gAAAAA'.
    """
    if not value:
        return False
    # Fernet tokens are always urlsafe base64 and start with version 0x80 (base64 'g')
    return value.startswith("gAAAAA")
```

---

## 3. UPDATED FUNCTIONS: shared/harbor_common/qbo_connection.py

### 3.1 handle_oauth_callback
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

### 3.2 get_qbo_tokens (NEW Helper)
```python
def get_qbo_tokens(workspace_id: UUID) -> dict:
    """Retrieve and decrypt QBO tokens for a workspace.
    
    Implements backward compatibility: if token does not appear to be
    encrypted (via is_fernet_token check), it is returned as plaintext.
    
    Returns:
        dict: {access_token, refresh_token, qbo_realm_id, status}
    """
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        SELECT access_token, refresh_token, qbo_realm_id, status
          FROM qbo_connections
         WHERE workspace_id = %s
        """,
        (str(workspace_id),),
    )
    row = cur.fetchone()
    if not row:
        raise HarborError(
            code="NOT_FOUND",
            message="No QBO connection found for this workspace.",
            status_code=404,
        )
    
    def _decrypt_if_needed(val: str | None) -> str | None:
        if val is None:
            return None
        if is_fernet_token(val):
            return decrypt_str(val)
        return val

    return {
        "access_token": _decrypt_if_needed(row["access_token"]),
        "refresh_token": _decrypt_if_needed(row["refresh_token"]),
        "qbo_realm_id": row["qbo_realm_id"],
        "status": row["status"],
    }
```

---

## 4. DEPENDENCY CHANGES

### harbor/shared/setup.py
```python
    install_requires=[
        "psycopg2-binary>=2.9",
        "flask>=3.0",
        "requests>=2.31",
        "cryptography>=42.0.0",
    ],
```

### harbor/shell/requirements.txt
```text
requests>=2.31
cryptography>=42.0.0
-e ../shared
```

---

## 5. BACKWARD COMPATIBILITY BEHAVIOR
- **Read Path:** The `get_qbo_tokens` helper uses `is_fernet_token()` to detect encryption. If a token does not start with the Fernet version prefix (`gAAAAA`), it is treated as plaintext. This allows existing connections to continue functioning without a migration.
- **Write Path:** All new tokens obtained via `handle_oauth_callback` are unconditionally encrypted using `encrypt_str()` before storage.
- **Operational Note:** Existing plaintext tokens will remain plaintext in the database until the workspace is re-connected. To rotate all tokens to an encrypted state, users should be instructed to re-authorize their QuickBooks connection.

---

## 6. SECURITY CERTIFICATION STATEMENT
- Tokens are now encrypted at the application layer using Fernet symmetric encryption.
- The encryption key is supplied via the `HARBOR_ENCRYPTION_KEY` environment variable.
- No changes were made to existing contracts.
- No changes were made to Phase 1 implementation logic or schema.
- No Phase 3 orchestration or background workers were introduced.
- No new database tables or columns were added; existing columns are used to store encrypted strings.
- Sensitive token values are not logged; `exchange_code_for_tokens` only logs the HTTP status code on failure.
