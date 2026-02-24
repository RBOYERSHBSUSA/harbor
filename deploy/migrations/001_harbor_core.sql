/* Harbor core schema v0.1
   Run with:
     psql -h localhost -U harbor_app -d harbor -v ON_ERROR_STOP=1 -f harbor_core_v0_1.sql
   Or paste directly into psql.
*/

BEGIN;

-- Required for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Ensure we are creating objects in the harbor schema
SET search_path = harbor;

-- -------------------------------------------------------------------
-- customers (billing entity)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.customers (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name                TEXT NOT NULL,
  status              TEXT NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active','suspended','closed')),
  billing_plan_key    TEXT NULL,            -- e.g. 'standard', 'pro' (optional now)
  billing_metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,  -- Stripe ids, invoice prefs, etc.
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at          TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_customers_status ON harbor.customers(status) WHERE deleted_at IS NULL;

-- -------------------------------------------------------------------
-- users (Harbor login identity)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.users (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email               public.citext NOT NULL UNIQUE,
  display_name        TEXT NULL,
  password_hash       TEXT NULL,            -- if using password auth; NULL if OAuth-only later
  status              TEXT NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active','disabled','closed')),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at          TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_users_status ON harbor.users(status) WHERE deleted_at IS NULL;

-- -------------------------------------------------------------------
-- workspaces (operational tenant) 1:1 with QBO Realm (when connected)
-- belongs to one customer; may have multiple licensed apps
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.workspaces (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id         UUID NOT NULL REFERENCES harbor.customers(id) ON DELETE RESTRICT,
  name                TEXT NOT NULL,

  -- QBO realm becomes known after OAuth connect; remains immutable once set
  qbo_realm_id        TEXT NULL UNIQUE,

  status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','active','suspended','closed')),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at          TIMESTAMPTZ NULL,

  -- basic guardrails (optional but useful)
  CONSTRAINT workspaces_realm_id_nonempty CHECK (qbo_realm_id IS NULL OR length(trim(qbo_realm_id)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_workspaces_customer ON harbor.workspaces(customer_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_workspaces_status   ON harbor.workspaces(status)      WHERE deleted_at IS NULL;

-- -------------------------------------------------------------------
-- memberships (RBAC) user ↔ workspace
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.memberships (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id         UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,
  user_id              UUID NOT NULL REFERENCES harbor.users(id) ON DELETE CASCADE,
  role                 TEXT NOT NULL DEFAULT 'member'
                         CHECK (role IN ('owner','admin','editor','viewer','consultant','member')),
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at           TIMESTAMPTZ NULL,

  UNIQUE (workspace_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_memberships_user      ON harbor.memberships(user_id)      WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_memberships_workspace ON harbor.memberships(workspace_id) WHERE deleted_at IS NULL;

-- -------------------------------------------------------------------
-- qbo_connections (1:1 with workspace)
-- Store tokens/metadata for a single QBO app registration (Harbor-wide)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.qbo_connections (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id            UUID NOT NULL UNIQUE REFERENCES harbor.workspaces(id) ON DELETE CASCADE,

  -- App registration identity (if you later support multiple Intuit app registrations)
  qbo_app_key             TEXT NOT NULL DEFAULT 'harbor'
                            CHECK (qbo_app_key IN ('harbor')),

  -- OAuth token material (store encrypted-at-rest in application layer)
  access_token            TEXT NULL,
  refresh_token           TEXT NULL,
  token_type              TEXT NULL,
  expires_at              TIMESTAMPTZ NULL,

  -- operational metadata
  connected_at            TIMESTAMPTZ NULL,
  last_refresh_at         TIMESTAMPTZ NULL,
  revoked_at              TIMESTAMPTZ NULL,

  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qbo_connections_expires ON harbor.qbo_connections(expires_at);

-- -------------------------------------------------------------------
-- workspace_app_licenses (N per workspace; UNIQUE(workspace, app))
-- This is the Harbor multi-app licensing layer.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS harbor.workspace_app_licenses (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id            UUID NOT NULL REFERENCES harbor.workspaces(id) ON DELETE CASCADE,

  app_key                 TEXT NOT NULL
                            CHECK (app_key IN ('charles','emily')),

  status                  TEXT NOT NULL DEFAULT 'trial'
                            CHECK (status IN ('trial','active','expired','canceled','suspended')),

  plan_key                TEXT NULL,          -- e.g. 'monthly', 'annual'
  quantity                INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),

  trial_ends_at           TIMESTAMPTZ NULL,
  current_period_start    TIMESTAMPTZ NULL,
  current_period_end      TIMESTAMPTZ NULL,

  purchase_ref            TEXT NULL,          -- link to billing system later
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at              TIMESTAMPTZ NULL,

  UNIQUE (workspace_id, app_key)
);

CREATE INDEX IF NOT EXISTS idx_ws_app_licenses_workspace ON harbor.workspace_app_licenses(workspace_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_ws_app_licenses_status    ON harbor.workspace_app_licenses(status)      WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_ws_app_licenses_app       ON harbor.workspace_app_licenses(app_key)     WHERE deleted_at IS NULL;

-- -------------------------------------------------------------------
-- Convenience view: "is QBO connect allowed?"
-- Rule: workspace may connect to QBO iff it has ≥1 license for a QBO-dependent app in trial/active
-- (Charles and Emily both count here)
-- -------------------------------------------------------------------
CREATE OR REPLACE VIEW harbor.v_workspace_qbo_connect_allowed AS
SELECT
  w.id AS workspace_id,
  (COUNT(*) FILTER (WHERE l.deleted_at IS NULL AND l.status IN ('trial','active')) > 0) AS connect_allowed
FROM harbor.workspaces w
LEFT JOIN harbor.workspace_app_licenses l
  ON l.workspace_id = w.id
WHERE w.deleted_at IS NULL
GROUP BY w.id;

COMMIT;

/* Post-run sanity checks (run manually):
   \dn
   \dt harbor.*
   SELECT * FROM harbor.v_workspace_qbo_connect_allowed LIMIT 10;
*/