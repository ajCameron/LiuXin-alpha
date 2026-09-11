"""
Run the standalone OPDS server when this package is invoked with python -m.

The application runner owns argument parsing, Core-session lifetime, and the
WSGI server. Its return value becomes the process exit status; argument errors,
startup failures, and interrupts are not intercepted here. Importing this module
does not start a server because execution is guarded by the module name.
"""

from __future__ import annotations

from .app import main

if __name__ == "__main__":
    raise SystemExit(main())
