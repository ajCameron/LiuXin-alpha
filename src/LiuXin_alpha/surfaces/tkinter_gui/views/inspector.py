"""Row detail inspector view."""

from __future__ import annotations

from typing import Any


def set_readonly_text(tk: Any, widget: Any, text: str) -> None:
    widget.configure(state=tk.NORMAL)
    widget.delete("1.0", tk.END)
    widget.insert("1.0", text)
    widget.configure(state=tk.DISABLED)


class DetailInspector:
    def __init__(self, parent: Any, *, tk: Any, ttk: Any) -> None:
        self.tk = tk
        self.frame = ttk.Frame(parent)
        self.text = tk.Text(self.frame, height=9, wrap="word", state=tk.DISABLED)
        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.text.yview)
        self.text.configure(yscrollcommand=self.scrollbar.set)
        self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def set_text(self, text: str) -> None:
        set_readonly_text(self.tk, self.text, text)


__all__ = ["DetailInspector", "set_readonly_text"]
