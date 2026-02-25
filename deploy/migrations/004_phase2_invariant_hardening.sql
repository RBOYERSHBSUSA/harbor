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
