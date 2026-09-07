"""Standard-library-only contracts shared by parser composition and completion."""

import argparse
from collections.abc import Callable
from typing import Protocol


class CompletionSubparsers(Protocol):
    """The public registration operation completion needs from argparse."""

    def add_parser(self, name: str, *, help: str) -> argparse.ArgumentParser: ...


type CompletionRegistrar = Callable[[CompletionSubparsers], None]
