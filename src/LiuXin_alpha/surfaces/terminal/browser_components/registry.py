"""
Register terminal extensions and dispatch parsed command lines through the host boundary.

Command and group namespaces are separate, with group aliases taking dispatch
precedence. Registration is incremental rather than transactional.
"""

from __future__ import annotations

import shlex

from LiuXin_alpha.surfaces.terminal.commands import (
    TerminalCommandAPI,
)
from LiuXin_alpha.surfaces.terminal.plugins import TerminalLifecyclePluginAPI

from .contracts import BrowserState


class RegistryMixin[HostT](BrowserState[HostT]):
    """
    Own command lookup, group aliases, and ordered lifecycle-plugin registration.

    The browser initializes the registries and supplies the extension host, output,
    write-notification, shutdown, and legacy-syntax hooks used during dispatch.

    Example:
        >>> RegistryMixin._normalize_command_token(" SHOW ")
        'show'
    """

    def execute_line(self, line: str) -> bool:
        """
        Shell-split a command line and dispatch its lowercase root token, groups first.

        Blank input continues without output. Unknown roots print a hint and continue;
        malformed quoting and command/group errors propagate to the session owner.

        Example:
            >>> browser.execute_line("help 'show'")  # doctest: +SKIP


        :param line: Input line whose quoting is interpreted by ``shlex.split``.
        :return: Whether the session should continue after handling the line.
        """
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
        """
        Execute through the extension host, notify declared writes, and record exit intent.

        Write notification follows a normal command return even when it is false.
        A false result records a reason only if none exists; it does not close the
        session here. Command or notification exceptions skip subsequent steps.

        Example:
            >>> keep_running = browser._execute_command("quit", command, [])  # doctest: +SKIP


        :param command_token: Display token used in a newly recorded shutdown reason.
        :param command_impl: Registered implementation to invoke.
        :param args: Parsed arguments forwarded to the implementation unchanged.
        :return: Boolean interpretation of the implementation's return value.
        """
        should_continue = bool(command_impl.execute(self._extension_host, args))
        if bool(getattr(command_impl, "mutates_data", False)):
            self.notify_write_completed()
        if not should_continue and self._shutdown_reason is None:
            self.request_shutdown(f"command:{command_token}")
        return should_continue

    def _execute_group_command(self, group_name: str, args: list[str]) -> bool:
        """
        Resolve a grouped command, including compact syntax and on/off/show legacy forms.

        Empty arguments list subcommands and continue. Lookup tries a direct
        subcommand token, then ``subcommand:arg``, then the group's legacy rewrite.
        The compact token is lowercased before splitting, including its embedded
        first argument; later argument strings retain their original case.

        Example:
            >>> browser._execute_group_command("sync", ["store:1"])  # doctest: +SKIP


        :param group_name: Canonical group name, already resolved from any root alias.
        :param args: Subcommand/legacy target tokens followed by command arguments.
        :return: Whether to continue, as reported by dispatch or an empty-argument listing.
        :raises ValueError: If the subcommand is blank, unknown, or invalid under legacy parsing.
        """
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
        """
        Print one usage/summary row per unique command in the requested group.

        Aliases are deduplicated by command identity; primary names determine order.

        Example:
            >>> browser._write_subcommand_listing("show")  # doctest: +SKIP


        :param group_name: Canonical registry group to list.
        :return: ``None`` after writing the heading and command rows.
        :raises ValueError: If the group has no registered subcommands.
        """
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
        """
        Register normalized command names and, when specified, group names and aliases.

        Direct exposure follows ``expose_direct``; group ``add`` also reserves ``new``.
        Reusing a name for this same object is allowed. A later collision can leave
        earlier insertions intact, and group aliases can shadow direct command names.

        Example:
            >>> browser.register_command(command)  # doctest: +SKIP


        :param command: Extension supplying primary name, aliases, group, and exposure metadata.
        :return: ``None``; registry mappings are updated in place.
        :raises ValueError: If a name/group alias is already bound incompatibly.
        """
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
        """
        Insert nonblank normalized direct names, rejecting a different object at the same key.

        Earlier insertions are retained if a later name collides.

        Example:
            >>> browser._register_direct_command_names(command, ["browse", "ls"])  # doctest: +SKIP


        :param command: Implementation to associate with every accepted name.
        :param names: Primary name and aliases to normalize in order.
        :return: ``None``; the direct-command mapping is extended in place.
        :raises ValueError: If a normalized name already identifies another command object.
        """
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
        """
        Convert a token to stripped lowercase text, treating ``None`` as empty.

        Example:
            >>> RegistryMixin._normalize_command_token("  Browse ")
            'browse'


        :param token: Name or alias to normalize for registry lookup.
        :return: Lowercase string with surrounding whitespace removed, or an empty string.
        """
        if token is None:
            return ""
        return str(token).strip().lower()

    def _register_group_alias(self, alias: str, group_name: str) -> None:
        """
        Bind an already-normalized alias to a group without checking direct-command names.

        Example:
            >>> browser._register_group_alias("new", "add")  # doctest: +SKIP


        :param alias: Canonical group token or alias to use directly as the lookup key.
        :param group_name: Canonical target group name, stored without normalization.
        :return: ``None`` after inserting or reaffirming the same binding.
        :raises ValueError: If the alias currently identifies a different group.
        """
        existing = self._group_alias_to_group.get(alias)
        if existing is not None and existing != group_name:
            raise ValueError(
                f"Command group alias collision: {alias!r} maps to {existing!r} and {group_name!r}"
            )
        self._group_alias_to_group[alias] = group_name

    def register_lifecycle_plugin(
        self, plugin: TerminalLifecyclePluginAPI[HostT]
    ) -> None:
        """
        Append a lifecycle plugin after checking its name against existing registrations.

        The incoming name is stripped, falling back to its class name when blank.
        Re-registering the same object is allowed and appends another entry; hooks
        are not run here. Distinct objects with matching stored names are rejected.

        Example:
            >>> browser.register_lifecycle_plugin(plugin)  # doctest: +SKIP


        :param plugin: Extension supplying startup/shutdown hooks and optional name metadata.
        :return: ``None``; the plugin is appended in registration order.
        :raises ValueError: If an existing distinct plugin has the same compared name.
        """
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
        """
        Copy the lifecycle-plugin list, retaining registration order and repeated objects.

        Example:
            >>> plugins = browser.iter_lifecycle_plugins()  # doctest: +SKIP


        :return: New list containing the original registered plugin objects.
        """
        return list(self._lifecycle_plugins)

    def iter_registered_commands(self) -> list[TerminalCommandAPI[HostT]]:
        """
        List directly exposed command objects once each, sorted by their primary names.

        Group-only commands are absent unless also registered in the direct namespace.

        Example:
            >>> commands = browser.iter_registered_commands()  # doctest: +SKIP


        :return: New list deduplicated by command identity rather than name equality.
        """
        by_id: dict[int, TerminalCommandAPI[HostT]] = {}
        for command in self._commands.values():
            by_id[id(command)] = command
        return sorted(by_id.values(), key=lambda c: c.name)

    def iter_registered_command_groups(
        self,
    ) -> list[tuple[str, list[TerminalCommandAPI[HostT]]]]:
        """
        List canonical groups alphabetically, with unique command objects sorted by name.

        Deduplication is within each group; a shared command may appear in several groups.

        Example:
            >>> groups = browser.iter_registered_command_groups()  # doctest: +SKIP


        :return: New group/command-list pairs containing the original command instances.
        """
        groups: list[tuple[str, list[TerminalCommandAPI[HostT]]]] = []
        for group_name in sorted(self._command_groups.keys()):
            by_id: dict[int, TerminalCommandAPI[HostT]] = {}
            for command in self._command_groups[group_name].values():
                by_id[id(command)] = command
            commands = sorted(by_id.values(), key=lambda c: c.name)
            groups.append((group_name, commands))
        return groups
