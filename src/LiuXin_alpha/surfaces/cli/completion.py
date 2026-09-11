"""
Generate completion text from the installed command grammar without dispatching it.

The tree retains aliases and option spellings at every depth. Bash uses those
paths and options; the simpler zsh/fish renderers expose root commands and their
immediate child commands only. None provides option-value or filesystem-value
completion. Generated text targets this project's trusted command names, not
arbitrary shell identifiers, and output publication belongs to the common helper.
"""

from __future__ import annotations

import argparse
import shlex
from collections.abc import Mapping

from LiuXin_alpha.surfaces.cli.common import emit_bytes
from LiuXin_alpha.surfaces.cli.parser_types import CompletionSubparsers
from LiuXin_alpha.surfaces.cli.parsers import create_parser


def _command_tree(
    parser: argparse.ArgumentParser,
) -> dict[tuple[str, ...], tuple[str, ...]]:
    """
    Collect sorted option spellings and child-command names at each parser path.

    Traverse argparse's private action lists without modifying them. Alias
    spellings receive separate paths even when they share a parser. If a custom
    parser has multiple subparser actions, only the last action's children are
    traversed. There is no cycle detection, argument-value expansion, or filtering
    of help-suppressed option names.

    Example:
        >>> parser = argparse.ArgumentParser(add_help=False)
        >>> child = parser.add_subparsers().add_parser('scan', add_help=False)
        >>> option = child.add_argument('--quick', action='store_true')
        >>> _command_tree(parser)
        {(): ('scan',), ('scan',): ('--quick',)}


    :param parser: Root of the argparse command tree to inspect.
    :return: New mapping from command-name tuples to sorted unique suggestion tuples.
    """

    tree: dict[tuple[str, ...], tuple[str, ...]] = {}

    def visit(current: argparse.ArgumentParser, path: tuple[str, ...]) -> None:
        """
        Record one parser's suggestions and recurse through sorted child spellings.

        Options from every action accumulate; the last subparser action supplies
        children. Mutate the enclosing tree, retaining distinct alias paths.

        Example:
            >>> visit(parser, ())  # doctest: +SKIP


        :param current: Parser whose private action list is being inspected.
        :param path: Command spellings from the root to this parser, excluding the executable.
        :return: None after recording this node and all recursively reached children.
        """
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
    """
    Render a bash completion function for known command paths and option names.

    The generated scanner skips dash-prefixed words and extends its path only
    when a non-option word matches a known command path. It does not track
    option arity, so an option value resembling a command may affect traversal.
    Candidate strings are word lists, not arbitrary whitespace-bearing tokens.
    No shell is executed or completion installed by this renderer.

    Example:
        >>> _bash({(): ('scan',), ('scan',): ('--quick',)}).splitlines()[0]
        '# bash completion for liuxin'


    :param tree: Trusted command paths and suggestions, normally from _command_tree.
    :return: Newline-terminated bash script registering _liuxin_complete for liuxin.
    """
    paths = sorted(" ".join(path) for path in tree if path)
    cases = []
    for path, values in sorted(tree.items()):
        label = " ".join(path)
        words = " ".join(values)
        cases.append(f"    {shlex.quote(label)} ) candidates={shlex.quote(words)} ;;")
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
    """
    Render a shallow zsh completion script for root and immediate child commands.

    Drop dash-prefixed suggestions and ignore deeper paths; this does not emit
    option-name, option-value, or complete nested-command support. Names come
    from the trusted installed grammar. Rendering does not invoke zsh.

    Example:
        >>> _zsh({(): ('scan',), ('scan',): ('files', '--quick')}).splitlines()[0]
        '#compdef liuxin'


    :param tree: Command-path suggestion mapping; absent root or child paths act empty.
    :return: Newline-terminated script defining and registering _liuxin.
    """
    top = [value for value in tree.get((), ()) if not value.startswith("-")]
    cases: list[str] = []
    for name in top:
        values = [value for value in tree.get((name,), ()) if not value.startswith("-")]
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
    """
    Render fish rules for root commands and their immediate non-option children.

    Disable default file completion. Child rules check whether the parent
    spelling has appeared, not an exact complete command path. Deeper paths and
    option names/values are not represented; no fish process is started here.

    Example:
        >>> _fish({(): ('scan',), ('scan',): ('files',)}).splitlines()[1]
        'complete -c liuxin -f'


    :param tree: Trusted installed-command suggestions, with missing paths treated as empty.
    :return: Newline-terminated fish completion declarations for liuxin.
    """
    lines = ["# fish completion for liuxin", "complete -c liuxin -f"]
    top = [value for value in tree.get((), ()) if not value.startswith("-")]
    for name in top:
        lines.append(
            f"complete -c liuxin -n '__fish_use_subcommand' -a {shlex.quote(name)}"
        )
        for child in tree.get((name,), ()):
            if child.startswith("-"):
                continue
            lines.append(
                f"complete -c liuxin -n '__fish_seen_subcommand_from {shlex.quote(name)}' -a {shlex.quote(child)}"
            )
    return "\n".join(lines) + "\n"


def cmd_completion(args: argparse.Namespace) -> int:
    """
    Rebuild the standalone grammar and publish the selected shell's UTF-8 script.

    Dispatch renderers by exact shell key; direct callers bypassing argparse
    receive KeyError for an unsupported shell. Output is buffered and follows
    the common no-clobber/replacement policy. Errors propagate to the CLI owner.

    Example:
        >>> cmd_completion(argparse.Namespace(shell='bash', output='liuxin.bash', replace_output=False))  # doctest: +SKIP


    :param args: Namespace with shell, output, and replace_output attributes.
    :return: Zero after successful script generation and output publication.
    """

    tree = _command_tree(create_parser(register_completion=build_completion_parser))
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
    subparsers: CompletionSubparsers,
) -> None:
    """
    Add the completion command with shell selection and output-file controls.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> build_completion_parser(parser.add_subparsers())
        >>> args = parser.parse_args(['completion', 'bash'])
        >>> (args.shell, args.output, args.replace_output)
        ('bash', '-', False)


    :param subparsers: Root registration collection satisfying the completion protocol.
    :return: None; add the parser and bind cmd_completion as its handler.
    """
    parser = subparsers.add_parser(
        "completion", help="Generate shell completion for the installed CLI."
    )
    parser.add_argument("shell", choices=("bash", "zsh", "fish"))
    parser.add_argument("--output", default="-")
    parser.add_argument("--replace-output", action="store_true")
    parser.set_defaults(handler=cmd_completion)


__all__ = ["build_completion_parser", "cmd_completion"]
