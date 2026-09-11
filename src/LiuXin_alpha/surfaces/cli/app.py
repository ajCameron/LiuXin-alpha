"""
Normalize operator shortcuts and dispatch the separately owned CLI grammar.

Parser construction registers handlers but does not execute a Core operation.
Global profile selectors are moved ahead of commands, then concise ingest forms
are expanded. Argparse errors retain SystemExit; ordinary handler or result-int
conversion failures print an ERROR line to stderr and return status two.
"""

from __future__ import annotations

import argparse
import sys

from LiuXin_alpha.surfaces.cli.completion import build_completion_parser
from LiuXin_alpha.surfaces.cli.parsers import create_parser


def build_parser() -> argparse.ArgumentParser:
    """
    Build the complete command grammar with the standalone completion registrar.

    Example:
        >>> args = build_parser().parse_args(['completion', 'fish'])
        >>> (args.surface, args.shell, args.output)
        ('completion', 'fish', '-')


    :return: A fresh argparse parser; no command handler has run.
    """
    return create_parser(register_completion=build_completion_parser)


def _normalise_shortcuts(argv: list[str]) -> list[str]:
    """
    Expand a nonreserved ingest shortcut into the storage-ingest command path.

    Recognized ingest subcommands and help remain unchanged. Otherwise the
    first exact --source token and its next token take precedence over a
    positional source. An absent source value or an unrecognized option-first
    form is left for argparse. Equals-form --source and option terminators are
    not interpreted specially. The input list is never mutated.

    Example:
        >>> _normalise_shortcuts(['ingest', '/media/books', '--detach'])
        ['storage', 'ingest', '--source-root', '/media/books', '--detach']
        >>> _normalise_shortcuts(['ingest', 'formats'])
        ['ingest', 'formats']


    :param argv: Command tokens without the executable name, matched case-sensitively.
    :return: Expanded token list, or the original list object when no rewrite applies.
    """

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
    """
    Move profile/system-root selectors ahead of shortcut-normalized command tokens.

    Recognize separate and equals-form values anywhere in the list, preserving
    selector order and repetitions. This scan does not respect an option
    terminator or know which tokens are other options' values. A final bare
    selector without a value returns the original list immediately, without
    shortcut expansion. Mutual-exclusion checks belong to main, not this helper.

    Example:
        >>> _hoist_global_selectors(['completion', 'bash', '--profile=home'])
        ['--profile=home', 'completion', 'bash']


    :param argv: Argument tokens to scan without mutating the input list.
    :return: Hoisted and normalized tokens, or the original list for a missing value.
    """

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
    """
    Normalize selectors/shortcuts, parse arguments, and invoke the selected handler.

    Truthy global root/profile selectors are mutually exclusive and are copied
    to the command namespace unless that same destination is already truthy.
    Repeated global options ordinarily retain argparse's last-value behavior;
    this is not a general duplicate-token detector. A missing handler prints
    help and returns two. Ordinary handler and integer-result conversion errors
    print ERROR to stderr and return two. Parser/construction errors and
    BaseException subclasses such as SystemExit or KeyboardInterrupt propagate.

    Example:
        >>> main(['completion', 'fish', '--output', 'liuxin.fish'])  # doctest: +SKIP


    :param argv: Explicit tokens copied before normalization, or None for sys.argv[1:].
    :return: Integer-coerced handler status, or two for a missing/failed handler.
    :raises SystemExit: Argparse handles help/version or rejects arguments/selectors.
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
