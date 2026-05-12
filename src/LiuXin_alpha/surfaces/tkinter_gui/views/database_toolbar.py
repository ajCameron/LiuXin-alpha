"""Database toolbar view."""

from __future__ import annotations

from typing import Any, Callable


class DatabaseToolbar:
    def __init__(
        self,
        parent: Any,
        *,
        ttk: Any,
        database_var: Any,
        on_open: Callable[[], None],
        on_reload: Callable[[], None],
    ) -> None:
        self.frame = ttk.Frame(parent, style="Toolbar.TFrame")
        self.entry = ttk.Entry(self.frame, textvariable=database_var)
        self.entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self.open_button = ttk.Button(self.frame, text="Open", command=on_open)
        self.open_button.pack(side="left", padx=(0, 4))
        self.reload_button = ttk.Button(self.frame, text="Reload", command=on_reload)
        self.reload_button.pack(side="left")

    def set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        self.open_button.configure(state=state)
        self.reload_button.configure(state=state)


__all__ = ["DatabaseToolbar"]
