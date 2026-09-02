"""Status bar view."""

from __future__ import annotations

from typing import Any


class StatusBar:
    """Display transient application and background-task status messages."""

    def __init__(self, parent: Any, *, tk: Any, ttk: Any, status_var: Any) -> None:
        self.label = ttk.Label(parent, textvariable=status_var, anchor=tk.W, style="Status.TLabel")


__all__ = ["StatusBar"]
