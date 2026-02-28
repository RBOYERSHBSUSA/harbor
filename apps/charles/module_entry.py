"""CharlesModule — Harbor-facing entry surface for Charles.

Phase 4D (INV-4D-2): All operations require a shell-issued
WorkspaceExecutionContext.  Charles never constructs WEC.

No HTTP logic.  No OAuth logic.  No licensing logic.
"""

from charles.database.module2_database import get_db_manager
from charles.domain.sync_manager import SyncManager
from charles.security.wec_guard import require_wec


class CharlesModule:
    """Harbor-facing entry surface.

    Requires shell-issued WorkspaceExecutionContext for all operations.
    No HTTP logic.  No OAuth logic.  No licensing logic.
    """

    def __init__(self, db_path: str):
        self.db_manager = get_db_manager(db_path)

    def run_sync(self, wec):
        """Execute sync for a workspace.

        Args:
            wec: Shell-issued WorkspaceExecutionContext (required).

        Raises:
            ValueError: If wec is None or has empty workspace_id.
            TypeError:  If wec is not a WorkspaceExecutionContext.
            NotImplementedError: Domain function not yet implemented.
        """
        require_wec(wec)
        raise NotImplementedError(
            "run_sync domain function not yet implemented"
        )

    def build_deposit(self, wec, payout_id: str):
        """Build a deposit for a specific payout.

        Args:
            wec:       Shell-issued WorkspaceExecutionContext (required).
            payout_id: Payout identifier.

        Raises:
            ValueError: If wec is None or has empty workspace_id.
            TypeError:  If wec is not a WorkspaceExecutionContext.
            NotImplementedError: Domain function not yet implemented.
        """
        require_wec(wec)
        raise NotImplementedError(
            "build_deposit domain function not yet implemented"
        )
