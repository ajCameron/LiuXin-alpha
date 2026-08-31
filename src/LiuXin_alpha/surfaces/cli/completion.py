"""Generate shell completion scripts from the installed argparse tree."""

from __future__ import annotations

import argparse
import shlex

from collections.abc import Mapping

from LiuXin_alpha.surfaces.cli.common import emit_bytes


def _command_tree(
    parser: argparse.ArgumentParser,
) -> dict[tuple[str, ...], tuple[str, ...]]:
    """Return command-path suggestions, including options at each node."""

    tree: dict[tuple[str, ...], tuple[str, ...]] = {}

    def visit(current: argparse.ArgumentParser, path: tuple[str, ...]) -> None:
        subcommands: Mapping[str, argparse.ArgumentParser] = {}
        options: set[str] = set()
        for action in current._actions:
            options.update(action.option_strings)
            if isinstance(action, argparse._SubParsersAction):
                subcommands = action.choices
        tree[path] = tuple(sorted({*options, *subcommands}))
        for name, child in sorted(subcommands.items()):
            # Argparse aliases share a parser object, but each spelling is a
            # distinct shell path and therefore needs its own completion row.
            visit(child, (*path, name))

    visit(parser, ())
    return tree


def _bash(tree: Mapping[tuple[str, ...], tuple[str, ...]]) -> str:
    paths = sorted(" ".join(path) for path in tree if path)
    cases = []
    for path, values in sorted(tree.items()):
        label = " ".join(path)
        words = " ".join(values)
        cases.append(
            "    {} ) candidates={} ;;".format(
                shlex.quote(label), shlex.quote(words)
            )
        )
    return """# bash completion for liuxin
_liuxin_complete() {
  local current path candidate word i candidates
  current="${COMP_WORDS[COMP_CWORD]}"
  path=""
  for ((i=1; i<COMP_CWORD; i++)); do
    word="${COMP_WORDS[i]}"
    [[ "$word" == -* ]] && continue
    candidate="${path:+$path }$word"
    case "$candidate" in
      __PATHS__ ) path="$candidate" ;;
    esac
  done
  case "$path" in
__CASES__
    * ) candidates="" ;;
  esac
  COMPREPLY=( $(compgen -W "$candidates" -- "$current") )
}
complete -F _liuxin_complete liuxin
""".replace("__PATHS__", " | ".join(shlex.quote(value) for value in paths)).replace(
        "__CASES__", "\n".join(cases)
    )


def _zsh(tree: Mapping[tuple[str, ...], tuple[str, ...]]) -> str:
    top = [value for value in tree.get((), ()) if not value.startswith("-")]
    cases: list[str] = []
    for name in top:
        values = [
            value
            for value in tree.get((name,), ())
            if not value.startswith("-")
        ]
        if values:
            cases.append(
                "    {} ) _values 'command' {} ;;".format(
                    shlex.quote(name),
                    " ".join(shlex.quote(value) for value in values),
                )
            )
    return """#compdef liuxin
_liuxin() {
  _arguments '1:command:((__TOP__))' '*::argument:->arguments'
  case "$words[2]" in
__CASES__
  esac
}
compdef _liuxin liuxin
""".replace("__TOP__", " ".join(top)).replace("__CASES__", "\n".join(cases))


def _fish(tree: Mapping[tuple[str, ...], tuple[str, ...]]) -> str:
    lines = ["# fish completion for liuxin", "complete -c liuxin -f"]
    top = [value for value in tree.get((), ()) if not value.startswith("-")]
    for name in top:
        lines.append(
            "complete -c liuxin -n '__fish_use_subcommand' -a {}".format(
                shlex.quote(name)
            )
        )
        for child in tree.get((name,), ()):
            if child.startswith("-"):
                continue
            lines.append(
                "complete -c liuxin -n '__fish_seen_subcommand_from {}' -a {}"
                .format(shlex.quote(name), shlex.quote(child))
            )
    return "\n".join(lines) + "\n"


def cmd_completion(args: argparse.Namespace) -> int:
    """Write a completion script for the selected shell."""

    from LiuXin_alpha.surfaces.cli.app import build_parser

    tree = _command_tree(build_parser())
    script = {
        "bash": _bash,
        "zsh": _zsh,
        "fish": _fish,
    }[args.shell](tree)
    emit_bytes(
        script.encode("utf-8"),
        output=args.output,
        replace=bool(args.replace_output),
    )
    return 0


def build_completion_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "completion", help="Generate shell completion for the installed CLI."
    )
    parser.add_argument("shell", choices=("bash", "zsh", "fish"))
    parser.add_argument("--output", default="-")
    parser.add_argument("--replace-output", action="store_true")
    parser.set_defaults(handler=cmd_completion)


__all__ = ["build_completion_parser", "cmd_completion"]
