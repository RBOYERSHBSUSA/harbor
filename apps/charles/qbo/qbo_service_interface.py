from __future__ import annotations

from typing import Any, Protocol


class QBOServiceInterface(Protocol):
    """
    Harbor-owned QBO service boundary.

    Charles MUST NOT:
      - look up tokens
      - refresh tokens
      - persist tokens
      - call OAuth endpoints
      - read OAuth tables
    """

    def fetch_payments(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def create_deposit(self, *args: Any, **kwargs: Any) -> Any:
        ...

    def create_payment(self, *args: Any, **kwargs: Any) -> Any:
        ...
