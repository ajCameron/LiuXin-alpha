"""Execute the packaged CLI using the separately owned command grammar."""

from __future__ import annotations

import argparse
import sys

from LiuXin_alpha.surfaces.cli.completion import build_completion_parser
from LiuXin_alpha.surfaces.cli.parsers import create_parser


def build_parser() -> argparse.ArgumentParser:
    """Preserve the installed parser entry point, including shell completion."""
    return create_parser(register_completion=build_completion_parser)


def _normalise_shortcuts(argv: list[str]) -> list[str]:
    """Expand concise operator forms before argparse sees subcommands."""

    if len(argv) < 2 or argv[0] != "ingest":
        return argv
    if argv[1] in {"disk", "formats", "remote-html", "runs", "-h", "--help"}:
        return argv
    if "--source" in argv:
        index = argv.index("--source")
        if index + 1 >= len(argv):
            return argv
        source = argv[index + 1]
        remainder = [*argv[1:index], *argv[index + 2 :]]
        return [
            "storage",
            "ingest",
            "--source-root",
            source,
            *remainder,
        ]
    if not argv[1].startswith("-"):
        return [
            "storage",
            "ingest",
            "--source-root",
            argv[1],
            *argv[2:],
        ]
    return argv


def _hoist_global_selectors(argv: list[str]) -> list[str]:
    """Allow global profile selectors before or after a subcommand."""

    selected: list[str] = []
    remainder: list[str] = []
    index = 0
    while index < len(argv):
        value = argv[index]
        matched = next(
            (
                option
                for option in ("--system-root", "--profile")
                if value == option or value.startswith(option + "=")
            ),
            None,
        )
        if matched is None:
            remainder.append(value)
            index += 1
            continue
        if value == matched:
            if index + 1 >= len(argv):
                return argv
            selected.extend((matched, argv[index + 1]))
            index += 2
        else:
            selected.append(value)
            index += 1
    return [*selected, *_normalise_shortcuts(remainder)]


def main(argv: list[str] | None = None) -> int:
    """Apply operator shortcuts/selectors and execute the selected command.

    Argument errors retain argparse's exits. Command failures are reported to
    stderr and return 2; a successful handler supplies its own exit status.
    """
    parser = build_parser()
    selected = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(_hoist_global_selectors(list(selected)))
    global_root = getattr(args, "global_system_root", None)
    global_profile = getattr(args, "global_profile", None)
    if global_root and global_profile:
        parser.error("--system-root and --profile are mutually exclusive")
    if global_root:
        if getattr(args, "system_root", None):
            parser.error("--system-root was provided more than once")
        args.system_root = global_root
    if global_profile:
        if getattr(args, "profile", None):
            parser.error("--profile was provided more than once")
        args.profile = global_profile
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 2
    try:
        return int(handler(args))
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


__all__ = ["build_parser", "main"]
