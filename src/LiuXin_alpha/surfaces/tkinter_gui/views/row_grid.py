"""
Display tabular library rows in Tk.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise row grid through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from typing import Any, Callable

from ..backend import TkGuiBackend
from ..state import RowPage


class RowGrid:
    """
    Tabular row browser with selection and paging callbacks.

    Example:
        Exercise RowGrid through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    def __init__(
        self,
        parent: Any,
        *,
        tk: Any,
        ttk: Any,
        search_column_var: Any,
        search_text_var: Any,
        on_search: Callable[[], None],
        on_clear: Callable[[], None],
        on_previous: Callable[[], None],
        on_next: Callable[[], None],
        on_row_selected: Callable[[object | None], None],
    ) -> None:
        """
        Initialize and validate the rowgrid state.

        Example:
            Exercise RowGrid.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param parent: Value supplied for parent under the utility contract.
        :param tk: Value supplied for tk under the utility contract.
        :param ttk: Value supplied for ttk under the utility contract.
        :param search_column_var: Value supplied for search column var under the utility
            contract.
        :param search_text_var: Value supplied for search text var under the utility
            contract.
        :param on_search: Value supplied for on search under the utility contract.
        :param on_clear: Value supplied for on clear under the utility contract.
        :param on_previous: Value supplied for on previous under the utility contract.
        :param on_next: Value supplied for on next under the utility contract.
        :param on_row_selected: Value supplied for on row selected under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tk = tk
        self.ttk = ttk

        self.toolbar = ttk.Frame(parent)
        self.toolbar.pack(side=tk.TOP, fill=tk.X, pady=(0, 6))
        self.search_column_combo = ttk.Combobox(
            self.toolbar,
            textvariable=search_column_var,
            state="readonly",
            width=28,
        )
        self.search_column_combo.pack(side=tk.LEFT, padx=(0, 6))
        self.search_entry = ttk.Entry(self.toolbar, textvariable=search_text_var, width=34)
        self.search_entry.pack(side=tk.LEFT, padx=(0, 6))
        self.search_button = ttk.Button(self.toolbar, text="Search", command=on_search)
        self.search_button.pack(side=tk.LEFT, padx=(0, 4))
        self.clear_button = ttk.Button(self.toolbar, text="Clear", command=on_clear)
        self.clear_button.pack(side=tk.LEFT, padx=(0, 12))
        self.previous_button = ttk.Button(self.toolbar, text="Previous", command=on_previous)
        self.previous_button.pack(side=tk.LEFT, padx=(0, 4))
        self.next_button = ttk.Button(self.toolbar, text="Next", command=on_next)
        self.next_button.pack(side=tk.LEFT)

        self.frame = ttk.Frame(parent)
        self.frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.tree = ttk.Treeview(self.frame, show="headings", selectmode="browse")
        self.y_scroll = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.x_scroll = ttk.Scrollbar(self.frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.y_scroll.set, xscrollcommand=self.x_scroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.y_scroll.grid(row=0, column=1, sticky="ns")
        self.x_scroll.grid(row=1, column=0, sticky="ew")
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)
        self.tree.bind("<<TreeviewSelect>>", on_row_selected)
        self.set_controls_enabled(False)

    def set_search_columns(self, columns: tuple[str, ...]) -> None:
        """
        Set search columns under the format's safety and compatibility rules.

        Example:
            Exercise RowGrid.set search columns through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param columns: Value supplied for columns under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.search_column_combo.configure(values=columns)

    def render_rows(self, page: RowPage, *, backend: TkGuiBackend | None) -> None:
        """
        Perform the render rows operation under explicit file-format and conversion rules.

        Example:
            Exercise RowGrid.render rows through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param page: Value supplied for page under the utility contract.
        :param backend: Value supplied for backend under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.tree.delete(*self.tree.get_children())
        columns = page.columns or ("row",)
        self.tree.configure(columns=columns)
        for column in columns:
            self.tree.heading(column, text=column)
            self.tree.column(column, width=160, minwidth=80, stretch=True, anchor=self.tk.W)
        for index, row in enumerate(page.rows):
            values = backend.row_values(page.table, row) if backend is not None else ()
            self.tree.insert("", self.tk.END, iid=str(index), values=values)
        self.set_page_state(page)

    def clear_rows(self) -> None:
        """
        Perform the clear rows operation under explicit file-format and conversion rules.

        Example:
            Exercise RowGrid.clear rows through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.tree.delete(*self.tree.get_children())
        self.tree.configure(columns=())
        self.search_column_combo.configure(values=())
        self.set_controls_enabled(False)

    def set_controls_enabled(
        self,
        enabled: bool,
        *,
        has_previous: bool = False,
        has_next: bool = False,
    ) -> None:
        """
        Set controls enabled under the format's safety and compatibility rules.

        Example:
            Exercise RowGrid.set controls enabled through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param enabled: Value supplied for enabled under the utility contract.
        :param has_previous: Value supplied for has previous under the utility contract.
        :param has_next: Value supplied for has next under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        search_state = "normal" if enabled else "disabled"
        combo_state = "readonly" if enabled else "disabled"
        self.search_column_combo.configure(state=combo_state)
        self.search_entry.configure(state=search_state)
        self.search_button.configure(state=search_state)
        self.clear_button.configure(state=search_state)
        self.previous_button.configure(state="normal" if enabled and has_previous else "disabled")
        self.next_button.configure(state="normal" if enabled and has_next else "disabled")

    def set_page_state(self, page: RowPage | None) -> None:
        """
        Set page state under the format's safety and compatibility rules.

        Example:
            Exercise RowGrid.set page state through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param page: Value supplied for page under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if page is None:
            self.set_controls_enabled(False)
            return
        self.set_controls_enabled(
            True,
            has_previous=page.has_previous,
            has_next=page.has_next,
        )

    def selected_index(self) -> int | None:
        """
        Perform the selected index operation under explicit file-format and conversion rules.

        Example:
            Exercise RowGrid.selected index through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        selection = self.tree.selection()
        if not selection:
            return None
        try:
            return int(selection[0])
        except Exception:
            return None


__all__ = ["RowGrid"]
