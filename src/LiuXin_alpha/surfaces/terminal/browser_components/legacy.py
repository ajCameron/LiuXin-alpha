"""
Translate historical on/off/show argument order into registered command calls.

Compatibility recognition preserves the original validation order: compact
targets precede split targets, and a registered kind is selected before its
target is validated by the eventual command implementation. No mutation is
performed by these argument-rewriting helpers.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.presentation import (
    looks_like_id_selector as _looks_like_id_selector,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    safe_int as _safe_int,
)

from .contracts import BrowserState

_SHOW_KINDS = "tags, notes, genres, subjects, language, series, all"
_SINGLE_SHOW_ID_ERROR = "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."


class LegacyMixin[HostT](BrowserState[HostT]):
    """
    Recognize historical row-target spellings without changing error precedence.

    Concrete browser composition supplies table resolution, token normalization,
    and registered command maps. Unrecognized shapes return ``None`` so ordinary
    dispatch can handle them; recognized invalid forms raise specific errors.

    Example:
        >>> LegacyMixin._compact_legacy_target('works:7')
        ('works', '7')
    """

    def _legacy_table_exists(self, table: str) -> bool:
        """
        Check whether a legacy table token resolves through the browser's aliases.

        Only validation failures become false; other resolution failures propagate.

        Example:
            >>> exists = browser._legacy_table_exists('works')  # doctest: +SKIP


        :param table: Table name or alias whose recognizability should be checked.
        :return: Whether resolution succeeds without ``ValueError``.
        """
        try:
            self._resolve_table(table)
        except ValueError:
            return False
        return True

    @staticmethod
    def _compact_legacy_target(token: str) -> tuple[str, str] | None:
        """
        Split a compact legacy target at its final colon without validating either part.

        Only whitespace around the complete token is stripped. Table resolution
        and selector validation belong to later compatibility or command checks.

        Example:
            >>> LegacyMixin._compact_legacy_target(' works:7 ')
            ('works', '7')


        :param token: Candidate compact table/selector token.
        :return: Table and selector text, or ``None`` when no colon is present.
        """
        compact = str(token).strip()
        if ":" not in compact:
            return None
        table, selector = compact.rsplit(":", 1)
        return table, selector

    def _rewrite_on_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """
        Select an on-group command from a legacy target-first argument sequence.

        Example:
            >>> rewrite = browser._rewrite_on_legacy_group_args(commands, ['works', '7', 'tag', 'history'])  # doctest: +SKIP


        :param group_map: On-group kind and alias tokens mapped to command implementations.
        :param args: Arguments after ``on``, with a compact or split target before the kind.
        :return: Selected command and arguments without its kind token, or ``None``.
        :raises ValueError: If a recognizable target is followed by an unsupported kind.
        """
        return self._rewrite_legacy_mutation_group("on", group_map, args)

    def _rewrite_off_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """
        Select an off-group command from a legacy target-first argument sequence.

        Example:
            >>> rewrite = browser._rewrite_off_legacy_group_args(commands, ['works:7', 'tag', 'history'])  # doctest: +SKIP


        :param group_map: Off-group kind and alias tokens mapped to command implementations.
        :param args: Arguments after ``off``, preserving the target and value tokens.
        :return: Selected command and arguments without its kind token, or ``None``.
        :raises ValueError: If a recognizable target is followed by an unsupported kind.
        """
        return self._rewrite_legacy_mutation_group("off", group_map, args)

    def _rewrite_legacy_mutation_group(
        self,
        group: str,
        group_map: dict[str, TerminalCommandAPI[HostT]],
        args: list[str],
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """
        Resolve a legacy mutation kind at the compact-target or split-target position.

        A registered kind wins before target validation. Otherwise, validate the
        possible target before trying the next position, retaining historical
        unsupported-kind errors and their precedence.

        Example:
            >>> rewrite = browser._rewrite_legacy_mutation_group('on', commands, ['works:7', 'tag', 'history'])  # doctest: +SKIP


        :param group: Group label used in unsupported-kind error messages.
        :param group_map: Normalized kind/alias tokens mapped to implementations.
        :param args: Unmodified group arguments containing target, kind, and values.
        :return: Selected implementation and a new argument list, or ``None``.
        :raises ValueError: If a valid-looking target identifies an unsupported kind.
        """
        # Compact interpretation always precedes split interpretation, including
        # lookup of a registered kind before validating its preceding target.
        for kind_index in (1, 2):
            if len(args) <= kind_index:
                continue
            kind = self._normalize_command_token(args[kind_index])
            command = group_map.get(kind)
            if command is not None:
                return command, args[:kind_index] + args[kind_index + 1 :]
            target = (
                self._compact_legacy_target(args[0])
                if kind_index == 1
                else (args[0], args[1])
            )
            if target is not None:
                self._validate_mutation_kind(group, target, args[kind_index])
        return None

    def _validate_mutation_kind(
        self, group: str, target: tuple[str, str], kind: str
    ) -> None:
        """
        Raise an unsupported-kind error only when the preceding mutation target is recognizable.

        Example:
            >>> browser._validate_mutation_kind('on', ('works', '7'), 'unknown')  # doctest: +SKIP


        :param group: Mutation group whose name should appear in the error.
        :param target: Unresolved table token and row-selector text.
        :param kind: Original unsupported kind text retained for diagnostics.
        :return: ``None`` when the selector or table does not identify a legacy target.
        :raises ValueError: If both the table and selector are recognizable.
        """
        table, selector = target
        if _looks_like_id_selector(selector) and self._legacy_table_exists(table):
            raise ValueError(
                f"Unsupported `{group}` kind: {kind!r}. Expected one of: note, tag, genre, subject, language, series."
            )

    def _default_show_target(self, args: list[str]) -> list[str] | None:
        """
        Recognize a lone show target that should default to the all-linked-data command.

        Accept one compact token or two split tokens only. Require a single
        integer ID, but leave row existence and final validation to the command.

        Example:
            >>> target = browser._default_show_target(['works:7'])  # doctest: +SKIP


        :param args: Complete arguments after ``show`` when no explicit kind is supplied.
        :return: A copy of recognizable target arguments, or ``None`` for other shapes.
        :raises ValueError: If a known table is paired with an unsupported multi-ID selector.
        """
        target = None
        if len(args) == 1:
            target = self._compact_legacy_target(args[0])
        elif len(args) == 2:
            target = (args[0], args[1])
        if target is None:
            return None
        table, selector = target
        if _safe_int(selector) is not None:
            return list(args) if self._legacy_table_exists(table) else None
        if _looks_like_id_selector(selector) and self._legacy_table_exists(table):
            raise ValueError(_SINGLE_SHOW_ID_ERROR)
        return None

    def _validate_show_kind(self, target: tuple[str, str], kind: str) -> None:
        """
        Choose the historical selector or unknown-kind error for a recognizable show target.

        Multi-ID selectors take precedence over an unknown linked-data kind.
        Unrecognized table/selector shapes are left for ordinary dispatch.

        Example:
            >>> browser._validate_show_kind(('works', '7'), 'unknown')  # doctest: +SKIP


        :param target: Unresolved table token and row-selector text.
        :param kind: Original linked-data kind text retained in an unknown-kind error.
        :return: ``None`` when no specific legacy validation error applies.
        :raises ValueError: If a known table has a multi-ID selector or unknown kind.
        """
        table, selector = target
        if _safe_int(selector) is None and _looks_like_id_selector(selector):
            if self._legacy_table_exists(table):
                raise ValueError(_SINGLE_SHOW_ID_ERROR)
        elif _safe_int(selector) is not None:
            if self._legacy_table_exists(table):
                raise ValueError(
                    f"Unknown linked kind/table {kind!r}. Try: {_SHOW_KINDS}."
                )

    def _rewrite_show_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """
        Select a show command from default-all or explicit target-first legacy syntax.

        Try default-all for a lone target before inspecting explicit kinds. Then
        prefer the compact-target position to the split-target position without
        validating a recognized command's target prematurely.

        Example:
            >>> rewrite = browser._rewrite_show_legacy_group_args(commands, ['works:7', 'tags'])  # doctest: +SKIP


        :param group_map: Show-group kind/alias tokens mapped to implementations.
        :param args: Arguments after ``show`` in compact or split target form.
        :return: Selected command and rewritten arguments, or ``None`` if unrecognized.
        :raises ValueError: If a recognizable target has an invalid selector or kind.
        """
        if not args:
            return None
        all_impl = group_map.get("all")
        if all_impl is not None:
            default_args = self._default_show_target(args)
            if default_args is not None:
                return all_impl, default_args

        for kind_index in (1, 2):
            if len(args) <= kind_index:
                continue
            kind = self._normalize_command_token(args[kind_index])
            command = group_map.get(kind)
            if command is not None:
                return command, args[:kind_index] + args[kind_index + 1 :]
            target = (
                self._compact_legacy_target(args[0])
                if kind_index == 1
                else (args[0], args[1])
            )
            if target is not None:
                self._validate_show_kind(target, args[kind_index])
        return None
