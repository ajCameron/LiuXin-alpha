"""
Render terminal help from registered command objects and group/alias mappings.

Help reads command metadata, not Python docstrings. Aliases may resolve to the
same command object; listings deduplicate those objects where noted below.
"""

from __future__ import annotations

from collections.abc import Sequence

from LiuXin_alpha.surfaces.terminal.commands import (
    TerminalCommandAPI,
)

from .contracts import BrowserState


class HelpMixin[HostT](BrowserState[HostT]):
    """
    Supply overview, group, and individual-command help to a composed browser.

    The registry owner supplies commands and normalized tokens; the host owner
    supplies output. Rendering help does not execute commands or query table data.

    Example:
        >>> browser.execute_line("help browse")  # doctest: +SKIP
    """

    def _format_command_aliases(self, aliases: Sequence[str]) -> str:
        """
        Format normalized nonblank aliases as a parenthesized listing suffix.

        Input order and repeated aliases are retained; this helper does not deduplicate.

        Example:
            >>> browser._format_command_aliases([" LS ", "", "ls"])  # doctest: +SKIP
            ' (aliases: ls, ls)'


        :param aliases: Alias strings to strip, lowercase, and display.
        :return: Space-prefixed alias suffix, or an empty string when all aliases are blank.
        """
        normalized = [self._normalize_command_token(alias) for alias in aliases]
        filtered = [alias for alias in normalized if alias]
        if not filtered:
            return ""
        return " (aliases: {})".format(", ".join(filtered))

    def _normalized_command_tokens(self, tokens: Sequence[str]) -> list[str]:
        """
        Normalize tokens and retain the first occurrence of each nonblank result.

        Example:
            >>> browser._normalized_command_tokens([" LS ", "ls", ""])  # doctest: +SKIP
            ['ls']


        :param tokens: Command or alias tokens in the desired display order.
        :return: New ordered list of unique normalized nonblank tokens.
        """
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
        """
        Find registered aliases for a canonical group, excluding the group's own name.

        Example:
            >>> aliases = browser._group_aliases("add")  # doctest: +SKIP


        :param group_name: Canonical group name matched directly against registry targets.
        :return: Alphabetically sorted aliases for that group, possibly empty.
        """
        aliases: list[str] = []
        for alias, target in sorted(self._group_alias_to_group.items()):
            if target == group_name and alias != group_name:
                aliases.append(alias)
        return aliases

    def _write_group_help(self, group_name: str) -> None:
        """
        Print a group's aliases, unique subcommands, and detailed-help hint.

        Commands are deduplicated by identity and sorted by primary name. A command's
        usage metadata takes precedence over the generated group/name fallback.

        Example:
            >>> browser._write_group_help("show")  # doctest: +SKIP


        :param group_name: Canonical registry group whose help should be printed.
        :return: ``None``; formatted lines are sent to the browser output hook.
        :raises ValueError: If the group is absent or has no registered subcommands.
        """
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
        """
        Print one command's name, optional summary, usage, and applicable aliases.

        Grouped help includes direct names only when direct exposure is enabled,
        plus aliases of the group. Ungrouped help instead lists command aliases.

        Example:
            >>> browser._write_command_help(command, group_name="show")  # doctest: +SKIP


        :param command: Registered implementation supplying help metadata.
        :param group_name: Optional canonical group prefix for grouped help.
        :return: ``None``; help is written without invoking the command.
        """
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
        """
        Route zero, one, or two nonblank help arguments to the appropriate listing.

        No arguments prints an overview. One token resolves group aliases before
        direct commands; two tokens select a group and subcommand, accepting aliases.
        Arguments are separate tokens here, not a command line to shell-parse.

        Example:
            >>> browser._print_help(["show", "note"])  # doctest: +SKIP


        :param args: Optional command/group and subcommand tokens; blank entries are ignored.
        :return: ``None`` after writing the selected help.
        :raises ValueError: If more than two arguments survive or a requested name is unknown.
        """
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
        """
        Print grouped commands first, then direct commands not already shown in a group.

        Shared command instances are excluded from the direct section by identity.
        Registry iterators supply group/name ordering, while each row includes usage,
        summary, and any nonblank command aliases.

        Example:
            >>> browser._write_help_overview()  # doctest: +SKIP


        :return: ``None``; the overview and detailed-help hint are written to output.
        """
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
