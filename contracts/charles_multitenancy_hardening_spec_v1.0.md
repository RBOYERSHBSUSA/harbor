# CHARLES MULTITENANCY HARDENING SPECIFICATION v1.0
Status: Mandatory for Phase 4 Extraction  
Scope: Harbor Charles (Single SQLite, Multi-Tenant)  
Date: 2026-02-26  

-------------------------------------------------------------------------------

## 1. TOPOLOGY (CONFIRMED)

Charles SHALL use:

- Single SQLite database file
- WAL mode enabled
- Multi-tenant partitioning via workspace_id

Per-workspace SQLite files are explicitly rejected for Phase 4.

-------------------------------------------------------------------------------

## 2. UNIVERSAL WORKSPACE REQUIREMENT

Every domain table MUST include:

    workspace_id TEXT NOT NULL

No exceptions.

This includes but is not limited to:

- sync_runs
- api_keys
- sync_locks
- processor_config
- payout_batches
- canonical_facts
- raw_events
- idempotency_keys
- reconciliation tables
- audit tables
- match tables

-------------------------------------------------------------------------------

## 3. SCHEMA HARDENING RULES

### 3.1 NOT NULL Enforcement

workspace_id MUST be:

- NOT NULL
- Indexed
- Included in composite UNIQUE constraints where appropriate

### 3.2 Composite Key Reinforcement

Any table with a primary UUID key MUST enforce:

    PRIMARY KEY (workspace_id, id)

OR

    UNIQUE(workspace_id, id)

Standalone primary keys without workspace_id are prohibited.

### 3.3 Foreign Keys

All foreign keys MUST include workspace_id.

Example:

BAD:
    FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id)

GOOD:
    FOREIGN KEY (workspace_id, sync_run_id)
        REFERENCES sync_runs(workspace_id, id)

-------------------------------------------------------------------------------

## 4. QUERY ENFORCEMENT RULE

No query may access data without explicit workspace_id scoping.

Every:

- SELECT
- UPDATE
- DELETE
- JOIN
- UPSERT

MUST include:

    WHERE workspace_id = ?

No reliance on:

- company_id alone
- primary key alone
- UUID uniqueness alone

Terminal update queries must reinforce workspace_id.

Example:

BAD:
    UPDATE payout_batches SET status='posted' WHERE id = ?

GOOD:
    UPDATE payout_batches
    SET status='posted'
    WHERE workspace_id = ?
      AND id = ?

-------------------------------------------------------------------------------

## 5. SERVICE LAYER RULES

company_id → workspace_id resolution bridges are prohibited in Phase 4.

All Charles services MUST receive:

    workspace_id

company_id may exist only as a domain attribute.

Authorization MUST be enforced at workspace boundary.

No service may execute solely on company_id.

-------------------------------------------------------------------------------

## 6. IDENTITY & TOKEN SAFETY

Charles SHALL NOT:

- Store OAuth tokens
- Resolve workspace via token lookup
- Use company_id to infer workspace ownership

QBO access MUST be injected via Harbor QBO interface.

-------------------------------------------------------------------------------

## 7. IDEMPOTENCY HARDENING

Idempotency uniqueness MUST be:

    UNIQUE(workspace_id, idempotency_key)

Terminal updates MUST include workspace_id in WHERE clause.

-------------------------------------------------------------------------------

## 8. SYNC & LOCK TABLE RESTRUCTURE

The following tables MUST be rewritten:

- sync_runs
- sync_locks
- api_keys

They MUST:

- Include workspace_id
- Remove reliance on company_id for isolation
- Enforce composite uniqueness per workspace

-------------------------------------------------------------------------------

## 9. CONCURRENCY MODEL

SQLite remains global.

Harbor SHALL acquire workspace-scoped advisory lock before invoking write operations.

Charles SHALL NOT implement its own global mutex.

Blocking across workspaces is acceptable in Phase 4.

-------------------------------------------------------------------------------

## 10. PROHIBITED PATTERNS

The following patterns are prohibited:

- WHERE id = ? without workspace_id
- WHERE company_id = ? without workspace_id
- Cross-workspace JOIN
- Terminal updates without workspace reinforcement
- company-level isolation enforcement

-------------------------------------------------------------------------------

## 11. SUCCESS CRITERIA

Multitenancy is considered STRICT only when:

- Every table has workspace_id NOT NULL
- Every query enforces workspace_id
- Every update enforces workspace_id
- No service relies solely on company_id
- Idempotency is workspace-scoped
- Sync tables are workspace-scoped
- Hostile audit PASS achieved

-------------------------------------------------------------------------------

END OF SPECIFICATION