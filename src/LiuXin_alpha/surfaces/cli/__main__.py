"""
Run the operator CLI when invoked as ``python -m LiuXin_alpha.surfaces.cli``.

Ordinary import only binds the package's lazy dispatcher. Main-name execution
calls it with process arguments and forwards its return value through SystemExit;
argument-parser exits and other uncaught exceptions are not intercepted here.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.cli.app import main

if __name__ == "__main__":
    raise SystemExit(main())
