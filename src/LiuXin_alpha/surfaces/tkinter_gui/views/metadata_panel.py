"""
Display and edit metadata in Tk.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise metadata panel through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from typing import Any, Callable

from ..metadata_editing import METADATA_EDIT_FIELDS
from .inspector import set_readonly_text


class MetadataPanel:
    """
    Present editable metadata fields and delegate mutations to callbacks.

    Example:
        Exercise MetadataPanel through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    def __init__(
        self,
        parent: Any,
        *,
        tk: Any,
        ttk: Any,
        on_hydrate: Callable[[], None],
        edit_field_var: Any,
        edit_value_var: Any,
        on_replace: Callable[[], None],
    ) -> None:
        """
        Initialize and validate the metadatapanel state.

        Example:
            Exercise MetadataPanel.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param parent: Value supplied for parent under the utility contract.
        :param tk: Value supplied for tk under the utility contract.
        :param ttk: Value supplied for ttk under the utility contract.
        :param on_hydrate: Value supplied for on hydrate under the utility contract.
        :param edit_field_var: Value supplied for edit field var under the utility contract.
        :param edit_value_var: Value supplied for edit value var under the utility contract.
        :param on_replace: Value supplied for on replace under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tk = tk
        self.frame = ttk.Frame(parent)
        self.toolbar = ttk.Frame(self.frame)
        self.toolbar.pack(side=tk.TOP, fill=tk.X)
        self.hydrate_button = ttk.Button(self.toolbar, text="Hydrate", command=on_hydrate)
        self.hydrate_button.pack(side=tk.LEFT, padx=(0, 8))
        self.edit_field_combo = ttk.Combobox(
            self.toolbar,
            textvariable=edit_field_var,
            values=METADATA_EDIT_FIELDS,
            state="readonly",
            width=14,
        )
        self.edit_field_combo.pack(side=tk.LEFT, padx=(0, 4))
        self.edit_entry = ttk.Entry(self.toolbar, textvariable=edit_value_var, width=42)
        self.edit_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        self.replace_button = ttk.Button(self.toolbar, text="Replace", command=on_replace)
        self.replace_button.pack(side=tk.LEFT)
        self.text = tk.Text(self.frame, height=9, wrap="word", state=tk.DISABLED)
        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.text.yview)
        self.text.configure(yscrollcommand=self.scrollbar.set)
        self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.set_hydrate_enabled(False)
        self.set_edit_enabled(False)

    def set_text(self, text: str) -> None:
        """
        Set text under the format's safety and compatibility rules.

        Example:
            Exercise MetadataPanel.set text through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set_readonly_text(self.tk, self.text, text)

    def set_hydrate_enabled(self, enabled: bool) -> None:
        """
        Set hydrate enabled under the format's safety and compatibility rules.

        Example:
            Exercise MetadataPanel.set hydrate enabled through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param enabled: Value supplied for enabled under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.hydrate_button.configure(state="normal" if enabled else "disabled")

    def set_edit_enabled(self, enabled: bool) -> None:
        """
        Set edit enabled under the format's safety and compatibility rules.

        Example:
            Exercise MetadataPanel.set edit enabled through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param enabled: Value supplied for enabled under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        entry_state = "normal" if enabled else "disabled"
        self.edit_field_combo.configure(state="readonly" if enabled else "disabled")
        self.edit_entry.configure(state=entry_state)
        self.replace_button.configure(state=entry_state)


__all__ = ["MetadataPanel"]
