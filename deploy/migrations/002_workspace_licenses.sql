/* Harbor Phase 1 — Licensing schema alignment
   Brings workspace_app_licenses into conformance with the licensing contract.
   Creates apps reference table and workspace creation idempotency table.

   Run after 001_harbor_core.sql:
     psql -h localhost -U harbor_app -d harbor -v ON_ERROR_STOP=1 -f 002_workspace_licenses.sql
*/

BEGIN;

SET search_path = harbor;

-- -------------------------------------------------------------------
-- apps (reference table for registered Harbor applications)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.apps (
  app_key       TEXT PRIMARY KEY,
  display_name  TEXT NOT NULL,
  requires_qbo  BOOLEAN NOT NULL DEFAULT false,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO harbor.apps (app_key, display_name, requires_qbo) VALUES
  ('charles', 'Charles', true),
  ('emily',   'Emily',   true)
ON CONFLICT (app_key) DO NOTHING;

-- -------------------------------------------------------------------
-- workspace_app_licenses — align columns with licensing contract
-- -------------------------------------------------------------------

-- 1. Add customer_id (billing entity, derived from workspace on insert)
ALTER TABLE harbor.workspace_app_licenses
  ADD COLUMN IF NOT EXISTS customer_id UUID REFERENCES harbor.customers(id) ON DELETE RESTRICT;

-- 2. Rename purchase_ref → purchase_id and enforce uniqueness
ALTER TABLE harbor.workspace_app_licenses
  RENAME COLUMN purchase_ref TO purchase_id;

ALTER TABLE harbor.workspace_app_licenses
  ALTER COLUMN purchase_id SET NOT NULL;

ALTER TABLE harbor.workspace_app_licenses
  ADD CONSTRAINT uq_wal_purchase_id UNIQUE (purchase_id);

-- 3. Rename period columns to match contract naming
ALTER TABLE harbor.workspace_app_licenses
  RENAME COLUMN current_period_start TO starts_at;

ALTER TABLE harbor.workspace_app_licenses
  RENAME COLUMN current_period_end TO ends_at;

-- 4. Fix status CHECK — replace 'suspended' with 'past_due' per contract
ALTER TABLE harbor.workspace_app_licenses
  DROP CONSTRAINT workspace_app_licenses_status_check;

ALTER TABLE harbor.workspace_app_licenses
  ADD CONSTRAINT workspace_app_licenses_status_check
  CHECK (status IN ('trial', 'active', 'expired', 'canceled', 'past_due'));

-- 5. Replace app_key CHECK with FK to apps table
ALTER TABLE harbor.workspace_app_licenses
  DROP CONSTRAINT workspace_app_licenses_app_key_check;

ALTER TABLE harbor.workspace_app_licenses
  ADD CONSTRAINT fk_wal_app_key
  FOREIGN KEY (app_key) REFERENCES harbor.apps(app_key);

-- -------------------------------------------------------------------
-- workspace_creation_requests (idempotency for POST /v1/workspaces)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.workspace_creation_requests (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key   TEXT NOT NULL UNIQUE,
  workspace_id      UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  response_payload  JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------------
-- Update convenience view to use renamed columns
-- -------------------------------------------------------------------
CREATE OR REPLACE VIEW harbor.v_workspace_qbo_connect_allowed AS
SELECT
  w.id AS workspace_id,
  (COUNT(*) FILTER (
    WHERE l.deleted_at IS NULL
      AND l.status IN ('trial', 'active')
      AND (l.ends_at IS NULL OR l.ends_at > now())
      AND (l.trial_ends_at IS NULL OR l.trial_ends_at > now())
      AND a.requires_qbo = true
  ) > 0) AS connect_allowed
FROM harbor.workspaces w
LEFT JOIN harbor.workspace_app_licenses l
  ON l.workspace_id = w.id
LEFT JOIN harbor.apps a
  ON a.app_key = l.app_key
WHERE w.deleted_at IS NULL
GROUP BY w.id;

COMMIT;
