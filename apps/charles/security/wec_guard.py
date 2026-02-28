"""WEC Guard — Charles-side enforcement of shell-issued execution context.

Phase 4D (INV-4D-2): Charles MUST reject execution if WEC is absent.

Shell builds WEC.  Charles guards it.  Shared modules do neither.

This module imports the WorkspaceExecutionContext TYPE only.
It never imports build_wec() and never instantiates WEC directly.
"""

from shell.core.wec import WorkspaceExecutionContext


def require_wec(wec) -> WorkspaceExecutionContext:
    """Assert that a valid WorkspaceExecutionContext is present.

    Must be called at every Charles entry point before any business logic.

    Raises:
        ValueError: WEC is None or has empty workspace_id.
        TypeError:  WEC is not a WorkspaceExecutionContext instance.
    """
    if wec is None:
        raise ValueError(
            "WorkspaceExecutionContext is required "
            "— Charles cannot execute without shell-issued WEC"
        )
    if not isinstance(wec, WorkspaceExecutionContext):
        raise TypeError(
            "Expected WorkspaceExecutionContext, got: "
            + type(wec).__name__
        )
    if not wec.workspace_id:
        raise ValueError("WEC.workspace_id is required")
    return wec
