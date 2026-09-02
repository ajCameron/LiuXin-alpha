"""Command-line entry point for the read-only HTTP API surface."""

from __future__ import annotations

from .app import main


if __name__ == "__main__":
    raise SystemExit(main())
