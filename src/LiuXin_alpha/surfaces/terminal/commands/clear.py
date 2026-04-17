"""Clear output command for the terminal surface."""

from __future__ import annotations

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class ClearCommand(TerminalCommandAPI):
    """Clear terminal output or windowed console buffer."""

    name = "clear"
    aliases = ("cls",)
    summary = "Clear terminal output."
    usage = "clear"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))
        browser.clear_output()
        return True
