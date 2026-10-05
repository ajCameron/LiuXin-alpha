"""
Define the standard-library-only seam between grammar assembly and completion.

CompletionSubparsers names the one registration operation consumed by the
completion builder. CompletionRegistrar is the callback type injected into
grammar construction, avoiding an import back into the application dispatcher.
"""

import argparse
from collections.abc import Callable
from typing import Protocol


class CompletionSubparsers(Protocol):
    """
    Describe the minimal subparser registration surface needed by completion.

    This structural protocol is not runtime-checkable and supplies no parser
    implementation. An argparse subparser collection satisfies the intended seam.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> children = root.add_subparsers()
        >>> isinstance(children.add_parser('completion', help='Shell scripts'), argparse.ArgumentParser)
        True
    """

    def add_parser(self, name: str, *, help: str) -> argparse.ArgumentParser:
        """
        Register a named command and return the parser used to configure it.

        Example:
            >>> command = subparsers.add_parser('completion', help='Shell scripts')  # doctest: +SKIP


        :param name: Command spelling to add to the receiving subparser collection.
        :param help: Short description displayed in the parent's command listing.
        :return: Parser accepting the new command's options and handler defaults.
        """
        ...


type CompletionRegistrar = Callable[[CompletionSubparsers], None]
