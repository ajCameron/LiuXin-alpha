"""
Invoke the Calibre-style web runner and turn its result into a process exit status.

Use python -m LiuXin_alpha.surfaces.web_calibre_readonly for this entrypoint.
Execution is unconditional: importing this __main__ module also parses process
arguments and calls the runner. Import the package or app module for library use.
Parser exits, startup failures, and interrupts propagate without handling here.
"""

from __future__ import annotations

from .app import main

raise SystemExit(main())
