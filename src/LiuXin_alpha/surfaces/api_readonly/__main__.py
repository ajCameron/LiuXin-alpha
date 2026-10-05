"""
Invoke the JSON API runner when this package is executed with python -m.

The main-name guard forwards process arguments through the runner and turns its
return code into SystemExit. Importing this module under its package name is
inert; runner exceptions and interrupts are not intercepted here.
"""

from __future__ import annotations

from .app import main


if __name__ == "__main__":
    raise SystemExit(main())
