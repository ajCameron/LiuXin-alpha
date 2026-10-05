"""
Navigate library tables in Tk.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise table sidebar through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from typing import Any, Callable

from ..state import TableSummary
from .inspector import set_readonly_text


class TableSidebar:
    """
    Selectable table list that delegates navigation to the controller.

    Example:
        Exercise TableSidebar through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    def __init__(
        self,
        parent: Any,
        *,
        tk: Any,
        ttk: Any,
        filter_var: Any,
        on_filter_changed: Callable[[], None],
        on_selected: Callable[[object | None], None],
    ) -> None:
        """
        Initialize and validate the tablesidebar state.

        Example:
            Exercise TableSidebar.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param parent: Value supplied for parent under the utility contract.
        :param tk: Value supplied for tk under the utility contract.
        :param ttk: Value supplied for ttk under the utility contract.
        :param filter_var: Value supplied for filter var under the utility contract.
        :param on_filter_changed: Value supplied for on filter changed under the utility
            contract.
        :param on_selected: Value supplied for on selected under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tk = tk
        self.filter_var = filter_var
        self.visible_table_names: list[str] = []
        self.frame = ttk.Frame(parent)
        self.frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.filter_entry = ttk.Entry(self.frame, textvariable=filter_var)
        self.filter_entry.pack(side=tk.TOP, fill=tk.X, pady=(0, 6))
        filter_var.trace_add("write", lambda *_args: on_filter_changed())
        self.list_frame = ttk.Frame(self.frame)
        self.list_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.listbox = tk.Listbox(self.list_frame, exportselection=False, activestyle="dotbox")
        self.scrollbar = ttk.Scrollbar(self.list_frame, orient=tk.VERTICAL, command=self.listbox.yview)
        self.listbox.configure(yscrollcommand=self.scrollbar.set)
        self.listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.listbox.bind("<<ListboxSelect>>", on_selected)
        self.schema_label = ttk.Label(self.frame, text="Schema", anchor=tk.W)
        self.schema_label.pack(side=tk.TOP, fill=tk.X, pady=(8, 2))
        self.schema_text = tk.Text(self.frame, height=8, wrap="none", state=tk.DISABLED)
        self.schema_text.pack(side=tk.TOP, fill=tk.X)

    def set_tables(self, summaries: tuple[TableSummary, ...], *, filter_text: str = "") -> None:
        """
        Set tables under the format's safety and compatibility rules.

        Example:
            Exercise TableSidebar.set tables through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param summaries: Value supplied for summaries under the utility contract.
        :param filter_text: Value supplied for filter text under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        text = str(filter_text or "").strip().lower()
        self.listbox.delete(0, self.tk.END)
        self.visible_table_names = []
        for summary in summaries:
            if text and text not in summary.name.lower():
                continue
            self.visible_table_names.append(summary.name)
            label = summary.name if summary.record_count is None else f"{summary.name} ({summary.record_count})"
            self.listbox.insert(self.tk.END, label)

    def set_enabled(self, enabled: bool) -> None:
        """
        Set enabled under the format's safety and compatibility rules.

        Example:
            Exercise TableSidebar.set enabled through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param enabled: Value supplied for enabled under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        state = "normal" if enabled else "disabled"
        self.filter_entry.configure(state=state)
        self.listbox.configure(state=state)

    def set_schema_text(self, text: str) -> None:
        """
        Set schema text under the format's safety and compatibility rules.

        Example:
            Exercise TableSidebar.set schema text through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set_readonly_text(self.tk, self.schema_text, text)

    def select_first(self) -> None:
        """
        Perform the select first operation under explicit file-format and conversion rules.

        Example:
            Exercise TableSidebar.select first through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.visible_table_names:
            return
        self.listbox.selection_clear(0, self.tk.END)
        self.listbox.selection_set(0)
        self.listbox.activate(0)

    def selected_table(self) -> str | None:
        """
        Perform the selected table operation under explicit file-format and conversion rules.

        Example:
            Exercise TableSidebar.selected table through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        selection = self.listbox.curselection()
        if not selection:
            return None
        index = int(selection[0])
        if index >= len(self.visible_table_names):
            return None
        return self.visible_table_names[index]


__all__ = ["TableSidebar"]
