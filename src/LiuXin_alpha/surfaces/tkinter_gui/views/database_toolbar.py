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
        read_source_var: Any,
        cache_type_var: Any,
        on_open: Callable[[], None],
        on_reload: Callable[[], None],
        on_refresh_source: Callable[[], None],
    ) -> None:
        self.frame = ttk.Frame(parent, style="Toolbar.TFrame")
        self.entry = ttk.Entry(self.frame, textvariable=database_var)
        self.entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self.source_label = ttk.Label(self.frame, text="Source")
        self.source_label.pack(side="left", padx=(0, 4))
        self.source_combo = ttk.Combobox(
            self.frame,
            textvariable=read_source_var,
            state="readonly",
            values=("direct", "cache"),
            width=8,
        )
        self.source_combo.pack(side="left", padx=(0, 6))
        self.cache_type_entry = ttk.Entry(self.frame, textvariable=cache_type_var, width=14)
        self.cache_type_entry.pack(side="left", padx=(0, 6))
        self.open_button = ttk.Button(self.frame, text="Open", command=on_open)
        self.open_button.pack(side="left", padx=(0, 4))
        self.reload_button = ttk.Button(self.frame, text="Reload", command=on_reload)
        self.reload_button.pack(side="left", padx=(0, 4))
        self.refresh_source_button = ttk.Button(
            self.frame,
            text="Refresh Source",
            command=on_refresh_source,
        )
        self.refresh_source_button.pack(side="left")

    def set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        self.open_button.configure(state=state)
        self.reload_button.configure(state=state)
        self.refresh_source_button.configure(state=state)
        self.source_combo.configure(state="disabled" if busy else "readonly")
        self.cache_type_entry.configure(state=state)

    def set_source_refresh_enabled(self, enabled: bool) -> None:
        self.refresh_source_button.configure(state="normal" if enabled else "disabled")


__all__ = ["DatabaseToolbar"]
