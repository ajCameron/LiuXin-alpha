"""Tests for the file-format annotation migration tool."""

from __future__ import annotations

import ast

import pytest


pytest.importorskip("libcst")

from scripts.annotate_file_formats import annotate_source  # noqa: E402


def test_annotate_source_adds_conservative_complete_signatures() -> None:
    source = '''#!/usr/bin/env python3
"""Example module."""
from __future__ import division

class Converter:
    def __init__(self, enabled=True):
        self.enabled = enabled

    @classmethod
    def build(cls, value):
        return cls(value)

    def values(self, limit=2):
        for value in range(limit):
            yield value

def label(value, suffix="!"):
    if value:
        return f"{value}{suffix}"
    return None
'''

    annotated, changed = annotate_source(source)

    assert changed
    assert "from __future__ import annotations" in annotated
    assert "import typing as _typing" in annotated
    assert (
        "def __init__(self: _typing.Self, enabled: bool = True) -> None:"
        in annotated
    )
    assert (
        "def build(cls: type[_typing.Self], value: _typing.Any) -> _typing.Any:"
        in annotated
    )
    assert (
        "def values("
        "self: _typing.Self, limit: int = 2"
        ") -> _typing.Iterator[_typing.Any]:"
        in annotated
    )
    assert (
        'def label(value: _typing.Any, suffix: str = "!") -> str | None:'
        in annotated
    )
    ast.parse(annotated)


def test_annotate_source_is_idempotent() -> None:
    source = "def empty():\n    return None\n"

    annotated, _changed = annotate_source(source)
    repeated, changed_again = annotate_source(annotated)

    assert repeated == annotated
    assert not changed_again


def test_annotate_source_terminates_empty_modules_with_a_newline() -> None:
    annotated, changed = annotate_source("")

    assert changed
    assert annotated == "from __future__ import annotations\n"
