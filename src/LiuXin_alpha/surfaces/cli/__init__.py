from __future__ import annotations

def main(argv: list[str] | None = None) -> int:
    """Load the packaged CLI lazily so subcommands can import independently."""

    from LiuXin_alpha.surfaces.cli.app import main as application_main

    return application_main(argv)

__all__ = ["main"]
