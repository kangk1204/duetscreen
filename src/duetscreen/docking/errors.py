from __future__ import annotations


class DockingError(RuntimeError):
    pass


class ExternalToolError(DockingError):
    pass


class InternalLogicError(DockingError):
    pass
