# PHASE 4B2 TODO (workspace scoping)

## Commit A: Replace company_sync_locks with workspace_sync_locks (workspace-scoped locks)
Files:
- apps/charles/shared/sync_lifecycle.py
- apps/charles/shared/workspace_sync_orchestrator.py
- apps/charles/shared/job_queue.py

Verification:
- rg "company_sync_locks" apps/charles  -> ZERO
- rg "workspace_sync_locks" apps/charles -> expected hits

