"""
Display Tk application status.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise status bar through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from typing import Any


class StatusBar:
    """
    Display transient application and background-task status messages.

    Example:
        Exercise StatusBar through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    def __init__(self, parent: Any, *, tk: Any, ttk: Any, status_var: Any) -> None:
        """
        Initialize and validate the statusbar state.

        Example:
            Exercise StatusBar.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param parent: Value supplied for parent under the utility contract.
        :param tk: Value supplied for tk under the utility contract.
        :param ttk: Value supplied for ttk under the utility contract.
        :param status_var: Value supplied for status var under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.label = ttk.Label(parent, textvariable=status_var, anchor=tk.W, style="Status.TLabel")


__all__ = ["StatusBar"]
