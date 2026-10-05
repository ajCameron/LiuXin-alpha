"""
Run the read/write web application when invoked through python -m.

The main-name guard passes process arguments to the runner and converts its
result to SystemExit. Importing this module under its package name is inert;
runner failures and interrupts are not caught here.
"""

from __future__ import annotations

from .app import main


if __name__ == "__main__":
    raise SystemExit(main())
