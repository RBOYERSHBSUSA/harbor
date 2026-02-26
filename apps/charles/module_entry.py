from charles.database.module2_database import get_db_manager
from charles.domain.sync_manager import SyncManager
from charles.domain.match_payout import build_deposit as build_deposit_domain


class CharlesModule:
    """
    Harbor-facing entry surface.
    No HTTP logic.
    No OAuth logic.
    No licensing logic.
    """

    def __init__(self, db_path: str):
        self.db_manager = get_db_manager(db_path)

    def run_sync(self, context):
        sync_manager = SyncManager(self.db_manager)
        return sync_manager.run(context)

    def build_deposit(self, context, payout_id: str):
        return build_deposit_domain(self.db_manager, context, payout_id)
