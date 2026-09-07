"""Historical on/off/show spellings translated into registered commands."""

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
    """Translate historical argument order without changing validation precedence."""

    def _legacy_table_exists(self, table: str) -> bool:
        try:
            self._resolve_table(table)
        except ValueError:
            return False
        return True

    @staticmethod
    def _compact_legacy_target(token: str) -> tuple[str, str] | None:
        compact = str(token).strip()
        if ":" not in compact:
            return None
        table, selector = compact.rsplit(":", 1)
        return table, selector

    def _rewrite_on_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """Compatibility for legacy `on <table> <id> <kind> <value...>` syntax."""
        return self._rewrite_legacy_mutation_group("on", group_map, args)

    def _rewrite_off_legacy_group_args(
        self, group_map: dict[str, TerminalCommandAPI[HostT]], args: list[str]
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
        """Compatibility for legacy `off <table> <id> <kind> <value...>` syntax."""
        return self._rewrite_legacy_mutation_group("off", group_map, args)

    def _rewrite_legacy_mutation_group(
        self,
        group: str,
        group_map: dict[str, TerminalCommandAPI[HostT]],
        args: list[str],
    ) -> tuple[TerminalCommandAPI[HostT], list[str]] | None:
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
        table, selector = target
        if _looks_like_id_selector(selector) and self._legacy_table_exists(table):
            raise ValueError(
                f"Unsupported `{group}` kind: {kind!r}. Expected one of: note, tag, genre, subject, language, series."
            )

    def _default_show_target(self, args: list[str]) -> list[str] | None:
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
        """Compatibility for legacy `show <table> <id> <kind>` syntax."""
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
