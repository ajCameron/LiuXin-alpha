"""Input-token parsing and command-specific completion routing."""

from __future__ import annotations

from collections.abc import Sequence

from LiuXin_alpha.surfaces.terminal.commands import (
    TerminalCommandAPI,
)

from .contracts import BrowserState
from .models import CommandCompletion as _CommandCompletion


class CompletionMixin[HostT](BrowserState[HostT]):
    """Input-token parsing and command-specific completion routing."""

    def _completion_candidates_for_direct_command(
        self,
        command: TerminalCommandAPI[HostT],
        args_before_current: Sequence[str],
        current_token: str,
    ) -> list[str]:
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
        return self._completion_candidates_for_help(args_before_current)

    def _complete_table_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
        if len(args_before_current) == 0:
            return self._table_token_completion_candidates(current_token)
        return []

    def _complete_row_arguments(
        self, args_before_current: Sequence[str], current_token: str
    ) -> list[str]:
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
        Return token completion candidates for the current input line.

        Completion is intentionally command-focused:
        - root prompt: direct commands + command groups/aliases
        - grouped commands: subcommands + aliases
        - `help`: commands/groups, then grouped subcommands
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
