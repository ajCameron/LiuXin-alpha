"""
Launch the Tk desktop interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   main   through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.tkinter_gui import main


if __name__ == "__main__":
    raise SystemExit(main())
