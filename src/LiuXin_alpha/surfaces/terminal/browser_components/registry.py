"""Command registration, group dispatch, and extension ordering."""

from __future__ import annotations

import shlex

from LiuXin_alpha.surfaces.terminal.commands import (
    TerminalCommandAPI,
)
from LiuXin_alpha.surfaces.terminal.plugins import TerminalLifecyclePluginAPI

from .contracts import BrowserState


class RegistryMixin[HostT](BrowserState[HostT]):
    """Command registration, group dispatch, and extension ordering."""

    def execute_line(self, line: str) -> bool:
        """Execute one command line; returns False when session should exit."""
        stripped = line.strip()
        if not stripped:
            return True

        tokens = shlex.split(stripped)
        if not tokens:
            return True

        command = tokens[0].lower()
        args = tokens[1:]

        group_name = self._group_alias_to_group.get(command)
        if group_name is not None:
            return self._execute_group_command(group_name, args)

        command_impl = self._commands.get(command)
        if command_impl is not None:
            return self._execute_command(command, command_impl, args)

        self._write(f"Unknown command: {command}. Type `help`.")
        return True

    def _execute_command(
        self,
        command_token: str,
        command_impl: TerminalCommandAPI[HostT],
        args: list[str],
    ) -> bool:
        should_continue = bool(command_impl.execute(self._extension_host, args))
        if bool(getattr(command_impl, "mutates_data", False)):
            self.notify_write_completed()
        if not should_continue and self._shutdown_reason is None:
            self.request_shutdown(f"command:{command_token}")
        return should_continue

    def _execute_group_command(self, group_name: str, args: list[str]) -> bool:
        if not args:
            self._write_subcommand_listing(group_name)
            return True

        subcommand_token = args[0].strip().lower()
        if not subcommand_token:
            raise ValueError(f"Missing subcommand for group {group_name!r}.")

        group_map = self._command_groups.get(group_name, {})
        command_impl = group_map.get(subcommand_token)
        rewritten_group_args: list[str] | None = None
        if command_impl is None and ":" in subcommand_token:
            # Convenience compact form for grouped commands:
            #   <group> <subcommand>:<arg0> [arg1...]
            # Example:
            #   sync store:1 --no-refresh
            compact_subcommand, compact_first_arg = subcommand_token.split(":", 1)
            compact_subcommand = self._normalize_command_token(compact_subcommand)
            compact_first_arg = str(compact_first_arg).strip()
            if compact_subcommand:
                compact_impl = group_map.get(compact_subcommand)
                if compact_impl is not None and compact_first_arg:
                    command_impl = compact_impl
                    rewritten_group_args = [compact_first_arg] + args[1:]
        legacy_handlers = {
            "on": self._rewrite_on_legacy_group_args,
            "off": self._rewrite_off_legacy_group_args,
            "show": self._rewrite_show_legacy_group_args,
        }
        legacy_handler = legacy_handlers.get(group_name)
        if command_impl is None and legacy_handler is not None:
            legacy_rewrite = legacy_handler(group_map, args)
            if legacy_rewrite is not None:
                command_impl, rewritten_args = legacy_rewrite
                return self._execute_command(
                    f"{group_name} {command_impl.name}",
                    command_impl,
                    rewritten_args,
                )
        if command_impl is None:
            available = sorted(group_map.keys())
            raise ValueError(
                "Unknown subcommand {} {}. Available: {}".format(
                    group_name,
                    subcommand_token,
                    ", ".join(available) if available else "<none>",
                )
            )

        if rewritten_group_args is not None:
            return self._execute_command(
                f"{group_name} {command_impl.name}",
                command_impl,
                rewritten_group_args,
            )

        return self._execute_command(
            f"{group_name} {subcommand_token}", command_impl, args[1:]
        )

    def _write_subcommand_listing(self, group_name: str) -> None:
        group_map = self._command_groups.get(group_name, {})
        if not group_map:
            raise ValueError(
                f"Command group {group_name!r} has no subcommands registered."
            )
        unique: dict[int, TerminalCommandAPI[HostT]] = {}
        for command in group_map.values():
            unique[id(command)] = command
        self._write(f"Available `{group_name}` subcommands:")
        for command in sorted(unique.values(), key=lambda c: c.name):
            usage = command.usage or f"{group_name} {command.name}"
            self._write(f"  {usage:<34} {command.summary}")

    def register_command(self, command: TerminalCommandAPI[HostT]) -> None:
        """Register a command implementation (name + aliases)."""
        names = [command.name] + list(command.aliases)
        if bool(getattr(command, "expose_direct", True)):
            self._register_direct_command_names(command, names)

        raw_group = getattr(command, "group", None)
        group_name = self._normalize_command_token(raw_group)
        if not group_name:
            return

        self._register_group_alias(group_name, group_name)
        # Convenience alias: `new <thing>` behaves like `add <thing>`.
        if group_name == "add":
            self._register_group_alias("new", group_name)
        for raw_alias in getattr(command, "group_aliases", ()) or ():
            alias = self._normalize_command_token(raw_alias)
            if alias:
                self._register_group_alias(alias, group_name)

        group_map = self._command_groups.setdefault(group_name, {})
        for raw_name in names:
            name = self._normalize_command_token(raw_name)
            if not name:
                continue
            existing = group_map.get(name)
            if existing is not None and existing is not command:
                raise ValueError(
                    f"Command {group_name} subcommand already registered: {name!r}"
                )
            group_map[name] = command

    def _register_direct_command_names(
        self, command: TerminalCommandAPI[HostT], names: list[str]
    ) -> None:
        for raw_name in names:
            name = self._normalize_command_token(raw_name)
            if not name:
                continue
            existing = self._commands.get(name)
            if existing is not None and existing is not command:
                raise ValueError(f"Command name already registered: {name!r}")
            self._commands[name] = command

    @staticmethod
    def _normalize_command_token(token: str | None) -> str:
        if token is None:
            return ""
        return str(token).strip().lower()

    def _register_group_alias(self, alias: str, group_name: str) -> None:
        existing = self._group_alias_to_group.get(alias)
        if existing is not None and existing != group_name:
            raise ValueError(
                f"Command group alias collision: {alias!r} maps to {existing!r} and {group_name!r}"
            )
        self._group_alias_to_group[alias] = group_name

    def register_lifecycle_plugin(
        self, plugin: TerminalLifecyclePluginAPI[HostT]
    ) -> None:
        """Register a lifecycle plugin for startup/shutdown events."""
        plugin_name = str(
            getattr(plugin, "name", plugin.__class__.__name__) or ""
        ).strip()
        if not plugin_name:
            plugin_name = plugin.__class__.__name__
        for existing in self._lifecycle_plugins:
            existing_name = str(
                getattr(existing, "name", existing.__class__.__name__) or ""
            ).strip()
            if existing_name == plugin_name and existing is not plugin:
                raise ValueError(
                    f"Lifecycle plugin already registered: {plugin_name!r}"
                )
        self._lifecycle_plugins.append(plugin)

    def iter_lifecycle_plugins(self) -> list[TerminalLifecyclePluginAPI[HostT]]:
        """Return lifecycle plugins in registration order."""
        return list(self._lifecycle_plugins)

    def iter_registered_commands(self) -> list[TerminalCommandAPI[HostT]]:
        """Return unique command instances sorted by primary command name."""
        by_id: dict[int, TerminalCommandAPI[HostT]] = {}
        for command in self._commands.values():
            by_id[id(command)] = command
        return sorted(by_id.values(), key=lambda c: c.name)

    def iter_registered_command_groups(
        self,
    ) -> list[tuple[str, list[TerminalCommandAPI[HostT]]]]:
        """Return command groups as (group_name, unique_commands) tuples."""
        groups: list[tuple[str, list[TerminalCommandAPI[HostT]]]] = []
        for group_name in sorted(self._command_groups.keys()):
            by_id: dict[int, TerminalCommandAPI[HostT]] = {}
            for command in self._command_groups[group_name].values():
                by_id[id(command)] = command
            commands = sorted(by_id.values(), key=lambda c: c.name)
            groups.append((group_name, commands))
        return groups
