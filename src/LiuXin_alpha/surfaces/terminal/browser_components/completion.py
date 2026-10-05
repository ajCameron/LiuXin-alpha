"""
Route whitespace-tokenized terminal input to command-specific completion sources.

Completion describes replacement spans and suggestions without executing commands.
It intentionally uses simple whitespace boundaries rather than shell-quote parsing.
"""

from __future__ import annotations

from collections.abc import Sequence

from LiuXin_alpha.surfaces.terminal.commands import (
    TerminalCommandAPI,
)

from .contracts import BrowserState
from .models import CommandCompletion as _CommandCompletion


class CompletionMixin[HostT](BrowserState[HostT]):
    """
    Select completion sources using registered root commands, groups, and argument positions.

    The source owner supplies candidate values; this owner decides which source is
    appropriate and applies final prefix filtering. It is not a command validator.

    Example:
        >>> completion = browser.command_completion_candidates("browse wo")  # doctest: +SKIP
    """

    def _completion_candidates_for_direct_command(
        self,
        command: TerminalCommandAPI[HostT],
        args_before_current: Sequence[str],
        current_token: str,
    ) -> list[str]:
        """
        Dispatch argument completion by the implementation's normalized primary command name.

        Aliases inherit their command's handler. Commands outside the fixed handler
        map return no candidates; extension execution is never invoked here.

        Example:
            >>> matches = browser._completion_candidates_for_direct_command(  # doctest: +SKIP
            ...     command, ["works"], "1"
            ... )


        :param command: Resolved command object whose primary name selects the handler.
        :param args_before_current: Completed arguments, excluding the root command and current token.
        :param current_token: Partially entered argument passed to the selected source.
        :return: Candidate strings from the handler, or an empty list for unsupported commands.
        """
        command_name = self._normalize_command_token(command.name)
        handlers = {
            "help": self._complete_help_arguments,
            "use": self._complete_table_arguments,
            "schema": self._complete_table_arguments,
            "count": self._complete_table_arguments,
            "browse": self._complete_table_arguments,
            "top": self._complete_table_arguments,
            "search": self._complete_table_arguments,
            "row": self._complete_row_arguments,
            "set": self._complete_set_arguments,
            "edit": self._complete_edit_arguments,
            "delete": self._complete_delete_arguments,
            "links": self._complete_links_arguments,
            "link": self._complete_link_arguments,
            "unlink": self._complete_link_arguments,
        }
        handler = handlers.get(command_name)
        return [] if handler is None else handler(args_before_current, current_token)

    def _complete_help_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Suggest help targets from completed arguments, leaving prefix filtering to the caller.

        Example:
            >>> matches = browser._complete_help_arguments(["show"], "n")  # doctest: +SKIP


        :param args_before_current: Completed help-target tokens before the current argument.
        :param current_token: Unused prefix; the shared help source receives only completed tokens.
        :return: Root/group/subcommand suggestions appropriate to the completed help context.
        """
        return self._completion_candidates_for_help(args_before_current)

    def _complete_table_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Suggest tables only at the first argument position of a table-oriented command.

        Example:
            >>> matches = browser._complete_table_arguments([], "wo")  # doctest: +SKIP


        :param args_before_current: Completed arguments; any existing argument disables this handler.
        :param current_token: Table prefix forwarded to the table-candidate source.
        :return: Table suggestions for the first argument, otherwise an empty list.
        """
        if len(args_before_current) == 0:
            return self._table_token_completion_candidates(current_token)
        return []

    def _complete_row_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Suggest a row reference first, then IDs scoped to a preceding table token.

        Example:
            >>> matches = browser._complete_row_arguments(["works"], "1")  # doctest: +SKIP


        :param args_before_current: Completed row-command arguments before the edited token.
        :param current_token: Partial reference or ID forwarded to the corresponding source.
        :return: Reference/ID suggestions for the first two positions, otherwise an empty list.
        """
        if len(args_before_current) == 0:
            return self._row_ref_token_completion_candidates(current_token)
        if len(args_before_current) == 1:
            return self._table_scoped_id_completion_candidates(
                args_before_current[0], current_token
            )
        return []

    def _complete_set_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Suggest the set command's target and single column, but not its value.

        A compact reference moves directly to column completion. Split table/ID
        syntax inserts an ID position first; later positions have no suggestions.

        Example:
            >>> matches = browser._complete_set_arguments(["works:1"], "ti")  # doctest: +SKIP


        :param args_before_current: Completed target/column tokens before the current argument.
        :param current_token: Partial reference, ID, or column passed to its source.
        :return: Position-appropriate suggestions, or an empty list after the column position.
        """
        if len(args_before_current) == 0:
            return self._row_ref_token_completion_candidates(current_token)
        first_token = args_before_current[0]
        if self._looks_like_compact_row_ref_prefix(first_token):
            if len(args_before_current) == 1:
                return self._table_column_completion_candidates(
                    first_token.split(":", 1)[0], current_token
                )
            return []
        if len(args_before_current) == 1:
            return self._table_scoped_id_completion_candidates(
                first_token, current_token
            )
        if len(args_before_current) == 2:
            return self._table_column_completion_candidates(first_token, current_token)
        return []

    def _complete_edit_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Suggest an edit target, then columns at every following argument position.

        Compact references begin column suggestions immediately; split references
        first suggest IDs. Already-entered column tokens are not validated or excluded.

        Example:
            >>> matches = browser._complete_edit_arguments(["works", "1"], "ti")  # doctest: +SKIP


        :param args_before_current: Completed edit arguments used to locate the target table.
        :param current_token: Partial target or column token forwarded to its source.
        :return: Reference/ID suggestions before the target is filled, otherwise column suggestions.
        """
        if len(args_before_current) == 0:
            return self._row_ref_token_completion_candidates(current_token)
        first_token = args_before_current[0]
        if self._looks_like_compact_row_ref_prefix(first_token):
            return self._table_column_completion_candidates(
                first_token.split(":", 1)[0], current_token
            )
        if len(args_before_current) == 1:
            return self._table_scoped_id_completion_candidates(
                first_token, current_token
            )
        return self._table_column_completion_candidates(first_token, current_token)

    def _complete_delete_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Suggest the delete target only, accepting compact or split reference shapes.

        Example:
            >>> matches = browser._complete_delete_arguments(["works"], "1")  # doctest: +SKIP


        :param args_before_current: Completed delete arguments before the edited token.
        :param current_token: Partial row reference or split-table ID to complete.
        :return: Target suggestions, or an empty list after a compact reference or split ID.
        """
        if len(args_before_current) == 0:
            return self._row_ref_token_completion_candidates(current_token)
        first_token = args_before_current[0]
        if self._looks_like_compact_row_ref_prefix(first_token):
            return []
        if len(args_before_current) == 1:
            return self._table_scoped_id_completion_candidates(
                first_token, current_token
            )
        return []

    def _complete_links_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Suggest a links source row followed by a table token for filtering its relationships.

        One preceding compact reference selects tables; one plain token selects
        scoped IDs. Two preceding arguments always select tables; later ones return none.

        Example:
            >>> matches = browser._complete_links_arguments(["works:1"], "no")  # doctest: +SKIP


        :param args_before_current: Completed source-reference and optional filter arguments.
        :param current_token: Partial reference, ID, or table token to pass to its source.
        :return: Suggestions for the recognized argument position, without validating prior tokens.
        """
        if len(args_before_current) == 0:
            return self._row_ref_token_completion_candidates(current_token)
        if len(args_before_current) == 1:
            if self._looks_like_compact_row_ref_prefix(args_before_current[0]):
                return self._table_token_completion_candidates(current_token)
            return self._table_scoped_id_completion_candidates(
                args_before_current[0], current_token
            )
        if len(args_before_current) == 2:
            return self._table_token_completion_candidates(current_token)
        return []

    def _complete_link_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Complete the two row references for link/unlink, allowing an intervening ``to``.

        Each side may be compact or split. A ``to`` where a split source ID belongs
        yields no candidates; other ID tokens are consumed without validation.
        Nothing is suggested after the destination reference has been consumed.

        Example:
            >>> matches = browser._complete_link_arguments(["works:1", "to"], "no")  # doctest: +SKIP


        :param args_before_current: Completed link/unlink arguments, copied before consuming tokens.
        :param current_token: Partial source/destination reference or scoped ID.
        :return: Suggestions for the next reference position, or an empty list when filled/unsupported.
        """
        remaining = list(args_before_current)
        if not remaining:
            return self._row_ref_token_completion_candidates(current_token)

        first_token = remaining.pop(0)
        if not self._looks_like_compact_row_ref_prefix(first_token):
            if not remaining:
                return self._table_scoped_id_completion_candidates(
                    first_token, current_token
                )
            next_token = remaining.pop(0)
            if self._normalize_command_token(next_token) == "to":
                return []

        if remaining and self._normalize_command_token(remaining[0]) == "to":
            remaining.pop(0)

        if not remaining:
            return self._row_ref_token_completion_candidates(current_token)

        second_token = remaining.pop(0)
        if self._looks_like_compact_row_ref_prefix(second_token):
            return []
        if not remaining:
            return self._table_scoped_id_completion_candidates(
                second_token, current_token
            )
        return []

    def _completion_candidates_for_group_command(
        self, group_name: str, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        """
        Suggest group subcommands, with row-target completion for registered show/on/off commands.

        Once a subcommand is entered, only those three groups suggest targets:
        a reference after the subcommand, then IDs after one table token.

        Example:
            >>> matches = browser._completion_candidates_for_group_command(  # doctest: +SKIP
            ...     "show", ["note"], "wo"
            ... )


        :param group_name: Canonical group name resolved from the root token.
        :param args_before_current: Completed subcommand/target tokens after the group name.
        :param current_token: Partial token forwarded to the applicable row-target source.
        :return: Group/target suggestions, or an empty list for unknown or unsupported contexts.
        """
        if not args_before_current:
            return self._group_completion_tokens(group_name)

        group_map = self._command_groups.get(group_name, {})
        subcommand_token = self._normalize_command_token(args_before_current[0])
        subcommand = group_map.get(subcommand_token)
        if subcommand is None:
            return []

        if group_name in {"show", "on", "off"}:
            if len(args_before_current) == 1:
                return self._row_ref_token_completion_candidates(current_token)
            if len(args_before_current) == 2:
                return self._table_scoped_id_completion_candidates(
                    args_before_current[1], current_token
                )

        return []

    def command_completion_candidates(
        self, line: str, *, cursor: int | None = None
    ) -> _CommandCompletion:
        """
        Describe candidates and the token-prefix span ending at a clamped input cursor.

        Split preceding input on whitespace, without interpreting shell quotes.
        Resolve groups before direct commands, then choose positional sources.
        Ordinary candidates must start with the normalized prefix. For compact
        references only the table prefix is rechecked here; ID filtering belongs
        to the source. Candidate order is preserved, with no extra deduplication.

        Example:
            >>> completion = browser.command_completion_candidates(  # doctest: +SKIP
            ...     "browse works", cursor=9
            ... )


        :param line: Input text to inspect without executing or replacing it.
        :param cursor: Character offset clamped into the line; ``None`` selects its end.
        :return: Replacement start/end, original prefix, and ordered candidate tuple; suffix text is excluded.
        """
        text = str(line)
        cursor_pos = (
            len(text) if cursor is None else max(0, min(len(text), int(cursor)))
        )
        before_cursor = text[:cursor_pos]

        token_start = cursor_pos
        while token_start > 0 and not before_cursor[token_start - 1].isspace():
            token_start -= 1
        token_prefix = before_cursor[token_start:cursor_pos]
        previous_tokens = [
            token for token in before_cursor[:token_start].split() if token
        ]
        normalized_prefix = self._normalize_command_token(token_prefix)

        candidates: list[str] = []
        if not previous_tokens:
            candidates = self._root_completion_tokens()
        else:
            root_token = self._normalize_command_token(previous_tokens[0])
            group_name = self._group_alias_to_group.get(root_token)
            if group_name is not None and len(previous_tokens) == 1:
                candidates = self._group_completion_tokens(group_name)
            elif group_name is not None:
                candidates = self._completion_candidates_for_group_command(
                    group_name,
                    previous_tokens[1:],
                    token_prefix,
                )
            else:
                command = self._commands.get(root_token)
                if command is not None:
                    candidates = self._completion_candidates_for_direct_command(
                        command,
                        previous_tokens[1:],
                        token_prefix,
                    )

        if normalized_prefix:
            if ":" in token_prefix:
                table_prefix = self._normalize_command_token(
                    token_prefix.split(":", 1)[0]
                )
                candidates = [
                    candidate
                    for candidate in candidates
                    if self._normalize_command_token(
                        candidate.split(":", 1)[0]
                    ).startswith(table_prefix)
                ]
            else:
                candidates = [
                    candidate
                    for candidate in candidates
                    if candidate.startswith(normalized_prefix)
                ]

        return _CommandCompletion(
            token_start=token_start,
            token_end=cursor_pos,
            prefix=token_prefix,
            candidates=tuple(candidates),
        )
