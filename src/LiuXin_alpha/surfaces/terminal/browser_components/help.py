"""Command and group help rendered from the live registration metadata."""

from __future__ import annotations

from collections.abc import Sequence

from LiuXin_alpha.surfaces.terminal.commands import (
    TerminalCommandAPI,
)

from .contracts import BrowserState


class HelpMixin[HostT](BrowserState[HostT]):
    """Command and group help rendered from the live registration metadata."""

    def _format_command_aliases(self, aliases: Sequence[str]) -> str:
        normalized = [self._normalize_command_token(alias) for alias in aliases]
        filtered = [alias for alias in normalized if alias]
        if not filtered:
            return ""
        return " (aliases: {})".format(", ".join(filtered))

    def _normalized_command_tokens(self, tokens: Sequence[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in tokens:
            token = self._normalize_command_token(raw)
            if not token or token in seen:
                continue
            seen.add(token)
            normalized.append(token)
        return normalized

    def _group_aliases(self, group_name: str) -> list[str]:
        aliases: list[str] = []
        for alias, target in sorted(self._group_alias_to_group.items()):
            if target == group_name and alias != group_name:
                aliases.append(alias)
        return aliases

    def _write_group_help(self, group_name: str) -> None:
        commands = dict(self._command_groups.get(group_name, {}))
        if not commands:
            raise ValueError(f"Unknown command group: {group_name!r}.")

        self._write(f"Command group: {group_name}")
        aliases = self._group_aliases(group_name)
        if aliases:
            self._write("Aliases: {}".format(", ".join(aliases)))
        self._write("Subcommands:")

        by_id: dict[int, TerminalCommandAPI[HostT]] = {}
        for command in commands.values():
            by_id[id(command)] = command
        for command in sorted(by_id.values(), key=lambda c: c.name):
            usage = command.usage or f"{group_name} {command.name}"
            alias_text = self._format_command_aliases(command.aliases)
            self._write(f"  {usage:<34} {command.summary}{alias_text}")

        self._write(f"Use `help {group_name} <subcommand>` for details.")

    def _write_command_help(
        self, command: TerminalCommandAPI[HostT], *, group_name: str | None = None
    ) -> None:
        canonical_name = command.name
        if group_name:
            canonical_name = f"{group_name} {command.name}"

        self._write(f"Command: {canonical_name}")
        summary = str(getattr(command, "summary", "") or "").strip()
        if summary:
            self._write(f"Summary: {summary}")

        usage = str(getattr(command, "usage", "") or "").strip() or canonical_name
        self._write(f"Usage: {usage}")

        if group_name:
            direct_names: list[str] = []
            if bool(getattr(command, "expose_direct", True)):
                direct_names = self._normalized_command_tokens(
                    [command.name] + list(command.aliases)
                )
            if direct_names:
                self._write("Direct names: {}".format(", ".join(direct_names)))
            group_aliases = self._group_aliases(group_name)
            if group_aliases:
                self._write("Group aliases: {}".format(", ".join(group_aliases)))
        else:
            aliases = self._normalized_command_tokens(list(command.aliases))
            if aliases:
                self._write("Aliases: {}".format(", ".join(aliases)))

    def _print_help(self, args: Sequence[str] | None = None) -> None:
        help_args = [str(arg) for arg in (args or []) if str(arg).strip()]
        if len(help_args) > 2:
            raise ValueError("Usage: help [command] [subcommand]")

        if len(help_args) == 1:
            target = self._normalize_command_token(help_args[0])
            group_name = self._group_alias_to_group.get(target)
            if group_name is not None:
                self._write_group_help(group_name)
                return

            command = self._commands.get(target)
            if command is None:
                raise ValueError(f"Unknown command or group: {help_args[0]!r}.")
            self._write_command_help(command)
            return

        if len(help_args) == 2:
            group_token = self._normalize_command_token(help_args[0])
            group_name = self._group_alias_to_group.get(group_token)
            if group_name is None:
                raise ValueError(f"Unknown command group: {help_args[0]!r}.")

            subcommand_token = self._normalize_command_token(help_args[1])
            command = self._command_groups.get(group_name, {}).get(subcommand_token)
            if command is None:
                raise ValueError(f"Unknown subcommand {group_name} {help_args[1]}.")
            self._write_command_help(command, group_name=group_name)
            return

        self._write_help_overview()

    def _write_help_overview(self) -> None:
        self._write("Commands:")
        self._write(
            "  Use `help <command>` or `help <group> <subcommand>` for details."
        )
        grouped_command_ids: set[int] = set()
        grouped = self.iter_registered_command_groups()
        if grouped:
            self._write("  -- grouped --")
            for group_name, commands in grouped:
                self._write(f"  {group_name} <subcommand>")
                for command in commands:
                    grouped_command_ids.add(id(command))
                    usage = command.usage or f"{group_name} {command.name}"
                    alias_text = self._format_command_aliases(command.aliases)
                    self._write(f"    {usage:<32} {command.summary}{alias_text}")

        self._write("  -- direct --")
        for command in self.iter_registered_commands():
            if id(command) in grouped_command_ids:
                continue
            usage = command.usage or command.name
            alias_text = self._format_command_aliases(command.aliases)
            self._write(f"  {usage:<34} {command.summary}{alias_text}")
