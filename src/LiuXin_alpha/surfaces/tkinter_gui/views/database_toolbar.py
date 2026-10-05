"""
Provide Tk database toolbar controls.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise database toolbar through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from typing import Any, Callable


class DatabaseToolbar:
    """
    Toolbar view for database selection, refresh, and navigation actions.

    Example:
        Exercise DatabaseToolbar through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

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
        """
        Initialize and validate the databasetoolbar state.

        Example:
            Exercise DatabaseToolbar.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param parent: Value supplied for parent under the utility contract.
        :param ttk: Value supplied for ttk under the utility contract.
        :param database_var: Value supplied for database var under the utility contract.
        :param read_source_var: Value supplied for read source var under the utility
            contract.
        :param cache_type_var: Value supplied for cache type var under the utility contract.
        :param on_open: Value supplied for on open under the utility contract.
        :param on_reload: Value supplied for on reload under the utility contract.
        :param on_refresh_source: Value supplied for on refresh source under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
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
        """
        Set busy under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseToolbar.set busy through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param busy: Value supplied for busy under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        state = "disabled" if busy else "normal"
        self.open_button.configure(state=state)
        self.reload_button.configure(state=state)
        self.refresh_source_button.configure(state=state)
        self.source_combo.configure(state="disabled" if busy else "readonly")
        self.cache_type_entry.configure(state=state)

    def set_source_refresh_enabled(self, enabled: bool) -> None:
        """
        Set source refresh enabled under the format's safety and compatibility rules.

        Example:
            Exercise DatabaseToolbar.set source refresh enabled through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param enabled: Value supplied for enabled under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.refresh_source_button.configure(state="normal" if enabled else "disabled")


__all__ = ["DatabaseToolbar"]
