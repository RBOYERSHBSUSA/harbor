from datetime import datetime, timezone
from uuid import UUID

from harbor_common.db import get_db
from harbor_common.errors import HarborError


def is_license_valid(license_row, now=None) -> bool:
    """Pure predicate — no DB, no logging, no mutation.

    A license is VALID when:
      status IN ('trial', 'active')
      AND (ends_at IS NULL OR ends_at > now)
      AND (trial_ends_at IS NULL OR trial_ends_at > now)
    """
    if now is None:
        now = datetime.now(timezone.utc)

    if license_row["status"] not in ("trial", "active"):
        return False

    ends_at = license_row.get("ends_at")
    if ends_at is not None and ends_at <= now:
        return False

    trial_ends_at = license_row.get("trial_ends_at")
    if trial_ends_at is not None and trial_ends_at <= now:
        return False

    return True


def workspace_has_qbo_entitlement(workspace_id: UUID) -> bool:
    """Read-only check: does this workspace hold a valid QBO-dependent license?"""
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        SELECT l.status, l.ends_at, l.trial_ends_at
          FROM workspace_app_licenses l
          JOIN apps a ON a.app_key = l.app_key
         WHERE l.workspace_id = %s
           AND a.requires_qbo = true
           AND l.deleted_at IS NULL
        """,
        (str(workspace_id),),
    )
    now = datetime.now(timezone.utc)
    for row in cur:
        if is_license_valid(row, now=now):
            return True
    return False


def require_qbo_entitlement(workspace_id: UUID):
    """Guard — raises HarborError when workspace lacks QBO entitlement."""
    if not workspace_has_qbo_entitlement(workspace_id):
        raise HarborError(
            code="QBO_ENTITLEMENT_REQUIRED",
            message="Workspace does not have an active QBO-entitled license.",
            metadata={"workspace_id": str(workspace_id)},
            status_code=403,
        )
