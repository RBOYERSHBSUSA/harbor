/* Harbor Phase 2 — QBO Connection lifecycle + OAuth state storage
   Aligns harbor.qbo_connections with qbo_connection_contract.md and
   activation_qbo_state_machine.md.
   Creates harbor.qbo_oauth_states for server-side OAuth state persistence.

   Run after 002_workspace_licenses.sql:
     psql -h localhost -U harbor_app -d harbor -v ON_ERROR_STOP=1 -f 003_qbo_connections_phase2.sql
*/

BEGIN;

SET search_path = harbor;

-- -------------------------------------------------------------------
-- qbo_connections — add contract-required columns
-- -------------------------------------------------------------------

-- 1. Add status column with CHECK constraint matching contract states
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'NOT_CONNECTED'
    CHECK (status IN (
      'NOT_CONNECTED',
      'OAUTH_PENDING',
      'CONNECTED',
      'TOKEN_REFRESH_FAILED',
      'REVOKED',
      'ERROR',
      'DISCONNECTED'
    ));

-- 2. Add realm_id column (contract field: qbo_realm_id, UNIQUE across all workspaces)
--    Note: workspaces table already has qbo_realm_id UNIQUE from 001.
--    The contract says qbo_connections stores qbo_realm_id with UNIQUE constraint.
--    We add it here for the connection table per contract Section 2.
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS qbo_realm_id TEXT NULL;

ALTER TABLE harbor.qbo_connections
  ADD CONSTRAINT uq_qbo_conn_realm_id UNIQUE (qbo_realm_id);

ALTER TABLE harbor.qbo_connections
  ADD CONSTRAINT qbo_conn_realm_id_nonempty
    CHECK (qbo_realm_id IS NULL OR length(trim(qbo_realm_id)) > 0);

-- 3. Add OAuth state hash and expiry (for state validation on callback)
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS oauth_state_hash TEXT NULL;

ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS oauth_state_expires_at TIMESTAMPTZ NULL;

-- 4. Add error tracking columns
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS last_error_code TEXT NULL;

ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS last_error_message TEXT NULL;

ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS last_error_at TIMESTAMPTZ NULL;

-- 5. Add connected_at timestamp
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS connected_at TIMESTAMPTZ NULL;

-- 6. Add refresh_token_expires_at (nullable per contract)
ALTER TABLE harbor.qbo_connections
  ADD COLUMN IF NOT EXISTS refresh_token_expires_at TIMESTAMPTZ NULL;

-- -------------------------------------------------------------------
-- qbo_oauth_states — server-side OAuth state persistence
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.qbo_oauth_states (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id    UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  state           TEXT NOT NULL UNIQUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at      TIMESTAMPTZ NOT NULL,
  consumed_at     TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_qbo_oauth_states_workspace
  ON harbor.qbo_oauth_states(workspace_id);

CREATE INDEX IF NOT EXISTS idx_qbo_oauth_states_expires
  ON harbor.qbo_oauth_states(expires_at);

COMMIT;
