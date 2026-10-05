"""
Inspect selected records in the Tk interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise inspector through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from typing import Any


def set_readonly_text(tk: Any, widget: Any, text: str) -> None:
    """
    Replace a Tk text widget's content while preserving read-only state.

    Example:
        Exercise set readonly text through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py


    :param tk: Value supplied for tk under the utility contract.
    :param widget: Value supplied for widget under the utility contract.
    :param text: Text parsed, normalized or rendered.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    widget.configure(state=tk.NORMAL)
    widget.delete("1.0", tk.END)
    widget.insert("1.0", text)
    widget.configure(state=tk.DISABLED)


class DetailInspector:
    """
    Render selected row and schema details without owning application state.

    Example:
        Exercise DetailInspector through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    def __init__(self, parent: Any, *, tk: Any, ttk: Any) -> None:
        """
        Initialize and validate the detailinspector state.

        Example:
            Exercise DetailInspector.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param parent: Value supplied for parent under the utility contract.
        :param tk: Value supplied for tk under the utility contract.
        :param ttk: Value supplied for ttk under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tk = tk
        self.frame = ttk.Frame(parent)
        self.text = tk.Text(self.frame, height=9, wrap="word", state=tk.DISABLED)
        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.text.yview)
        self.text.configure(yscrollcommand=self.scrollbar.set)
        self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def set_text(self, text: str) -> None:
        """
        Set text under the format's safety and compatibility rules.

        Example:
            Exercise DetailInspector.set text through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set_readonly_text(self.tk, self.text, text)


__all__ = ["DetailInspector", "set_readonly_text"]
