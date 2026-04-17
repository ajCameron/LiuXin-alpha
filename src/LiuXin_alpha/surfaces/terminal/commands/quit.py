"""Quit command for terminal text browser."""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class QuitCommand(TerminalCommandAPI):
    """Exit the current browser session."""

    name = "quit"
    aliases = ("exit", "q")
    summary = "Exit the browser."
    usage = "quit"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))
        browser.request_shutdown("command:quit")
        return False
