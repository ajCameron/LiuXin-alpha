"""Expose maintenance failures without importing a runtime or implementation."""


class MaintenanceFailure(RuntimeError):
    """Required work failed or cannot run with the installed implementation."""


class MaintenanceUnavailable(MaintenanceFailure):
    """A delivery is unavailable without a failed handler attempt."""
