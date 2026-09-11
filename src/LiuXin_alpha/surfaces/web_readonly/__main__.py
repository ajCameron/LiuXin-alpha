"""
Run the generic web surface when invoked with python -m.

The main-name guard forwards process arguments to the package runner and turns
its result into SystemExit. Importing this module under its package name does
not start the server; application exceptions are not intercepted here.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.web_readonly import main


if __name__ == "__main__":
    raise SystemExit(main())
