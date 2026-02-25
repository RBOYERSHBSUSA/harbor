-- ============================================================================
-- Migration 005: Workspace Activations (Phase 3)
--
-- Creates the workspace_activations table for deterministic activation
-- bootstrap orchestration.
--
-- Invariants:
--   - UNIQUE(workspace_id): one activation per workspace
--   - Presence of row == activated (no status column, no soft deletes)
--   - workspace_id references workspaces(id) with CASCADE delete
-- ============================================================================

SET search_path TO harbor, public;

CREATE TABLE IF NOT EXISTS workspace_activations (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    UUID        NOT NULL UNIQUE
                                REFERENCES workspaces(id)
                                ON DELETE CASCADE,
    activated_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
