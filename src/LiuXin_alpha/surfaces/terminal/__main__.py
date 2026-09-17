"""
Run the terminal application's shared main function when this package is executed with python -m.

Importing the module only binds main; direct package execution raises SystemExit
with its result. Argument parsing, UI selection, Core composition, and session
cleanup remain in the terminal application and its browser owners.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.app import main

if __name__ == "__main__":
    raise SystemExit(main())
