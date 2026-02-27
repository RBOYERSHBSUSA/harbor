# Phase 4B2 Commit A: Workspace-Scoped Sync Locks

Date: 2026-02-27
Branch: phase4b2-workspace-hardening

## Changes

### apps/charles/shared/sync_lifecycle.py
- Constructor: `__init__(db_connection, company_id)` -> `__init__(db_connection, workspace_id, company_id)`. Stores `self.workspace_id`.
- Removed import: `from workspace_helpers import get_workspace_id_from_company_id`.
- Removed workspace_id derivation fallback (forbidden inference eliminated).
- Lock table DDL: `company_sync_locks(company_id PK)` -> `workspace_sync_locks(workspace_id PK)`.
- All lock SQL (6 refs): table `company_sync_locks` -> `workspace_sync_locks`, column `company_id` -> `workspace_id`, params use `self.workspace_id`.
- Factory: `get_sync_manager(db_connection, company_id)` -> `get_sync_manager(db_connection, workspace_id, company_id)`.

### apps/charles/shared/workspace_sync_orchestrator.py
- Call site: `SyncRunManager(self.conn, company_id)` -> `SyncRunManager(self.conn, self.workspace_id, company_id)`.

### apps/charles/shared/job_queue.py
- 6 references: `company_sync_locks` -> `workspace_sync_locks` (mechanical rename only).

## Verification Results

1. `rg "company_sync_locks" apps/charles --glob "*.py"` -> ZERO hits
2. `rg "workspace_sync_locks" apps/charles --glob "*.py"` -> 12 hits (6 sync_lifecycle, 6 job_queue)
3. `rg "get_workspace_id_from_company_id" apps/charles/shared/sync_lifecycle.py` -> ZERO hits
4. All `SyncRunManager(` calls include workspace_id arg
5. `python3 -m py_compile` -> all three files pass

## Commit Message
```
Phase4B2 Commit A: workspace-scoped sync locks
```
