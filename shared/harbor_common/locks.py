"""Harbor — workspace-scoped advisory lock utilities.

Provides a single canonical lock-key derivation for pg_advisory_xact_lock,
shared across all phases (QBO connection lifecycle, activation, etc.).

All workspace-scoped advisory locks MUST use workspace_id_hash() to ensure
a unified lock namespace. Using different hash functions would create
independent lock namespaces that fail to serialize.
"""

import hashlib
from uuid import UUID


def workspace_id_hash(workspace_id: UUID) -> int:
    """Deterministic int64 hash of workspace_id for pg_advisory_xact_lock.

    Returns a signed 64-bit integer derived from SHA-256 of the UUID string.
    This is the ONLY lock-key derivation permitted for workspace-scoped
    advisory locks in Harbor.
    """
    h = hashlib.sha256(str(workspace_id).encode()).digest()
    return int.from_bytes(h[:8], "big", signed=True)
