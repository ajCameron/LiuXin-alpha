"""Metadata hydration panel view."""

from __future__ import annotations

from typing import Any, Callable

from .inspector import set_readonly_text


class MetadataPanel:
    def __init__(
        self,
        parent: Any,
        *,
        tk: Any,
        ttk: Any,
        on_hydrate: Callable[[], None],
    ) -> None:
        self.tk = tk
        self.frame = ttk.Frame(parent)
        self.toolbar = ttk.Frame(self.frame)
        self.toolbar.pack(side=tk.TOP, fill=tk.X)
        self.hydrate_button = ttk.Button(self.toolbar, text="Hydrate", command=on_hydrate)
        self.hydrate_button.pack(side=tk.LEFT)
        self.text = tk.Text(self.frame, height=9, wrap="word", state=tk.DISABLED)
        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.text.yview)
        self.text.configure(yscrollcommand=self.scrollbar.set)
        self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.set_hydrate_enabled(False)

    def set_text(self, text: str) -> None:
        set_readonly_text(self.tk, self.text, text)

    def set_hydrate_enabled(self, enabled: bool) -> None:
        self.hydrate_button.configure(state="normal" if enabled else "disabled")


__all__ = ["MetadataPanel"]
