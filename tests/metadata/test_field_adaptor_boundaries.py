"""Protect adapter ownership and the contracts shared by both factory interfaces."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from LiuXin_alpha.errors import InvalidUpdate
from LiuXin_alpha.metadata.field_adaptors import (
    cc_adapt_datetime,
    cc_adapt_number,
    cc_adapt_text,
    get_adapter,
    get_adapter_from_name_and_dt,
)
from LiuXin_alpha.utils.adaptors import adapt_number, multiple_text, single_text


@pytest.mark.parametrize(
    "module",
    ["LiuXin_alpha.utils.adaptors", "LiuXin_alpha.metadata.field_adaptors"],
)
def test_adaptors_import_without_loading_their_consumers(module: str) -> None:
    """Utilities must not load metadata; neither adapter owner may load writers."""
    source = Path(__file__).resolve().parents[2] / "src"
    script = """
import importlib
import importlib.abc
import sys

sys.path.insert(0, sys.argv[1])
module = sys.argv[2]
blocked = (
    'LiuXin_alpha.databases', 'LiuXin_alpha.catalog',
    'LiuXin_alpha.caches', 'LiuXin_alpha.library',
)
if module == 'LiuXin_alpha.utils.adaptors':
    blocked += ('LiuXin_alpha.metadata',)

class RejectConsumerImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(blocked):
            raise AssertionError('Adapter loaded consumer: ' + fullname)

sys.meta_path.insert(0, RejectConsumerImports())
importlib.import_module(module)
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(source), module],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    ("name", "datatype", "value", "expected"),
    [
        ("title", "text", None, "Unknown"),
        ("author_sort", "text", None, ""),
        ("series_index", "float", None, 1.0),
        ("series_index", "float", "2.5", 2.5),
        ("count", "int", "42", 42),
        ("flag", "bool", "none", None),
        ("rating", "rating", "12", 10),
        ("status", "enumeration", "  Active  ", "Active"),
        ("comments", "comments", "  Note  ", "Note"),
        ("series", "series", "  Cycle  ", "Cycle"),
        ("payload", "composite", {"x": 1}, {"x": 1}),
    ],
)
def test_factory_interfaces_share_conversion_and_field_defaults(
    name: str, datatype: str, value: object, expected: object,
) -> None:
    descriptor = {"datatype": datatype, "is_multiple": False}
    assert get_adapter(name, descriptor)(value) == expected
    assert get_adapter_from_name_and_dt(name, datatype)(value) == expected


@pytest.mark.parametrize(
    ("name", "value", "expected"),
    [
        ("authors", "Doe|Jane; Other", ("Doe,Jane", "Other")),
        ("languages", "eng; eng; und", ("English", "Undetermined")),
        ("identifiers", "ISBN: 123; doi: abc", {"isbn": "123", "doi": "abc"}),
    ],
)
def test_explicit_factory_applies_field_wrappers_after_multiple_text_conversion(
    name: str, value: str, expected: object,
) -> None:
    separators = {"ui_to_list": ";", "list_to_ui": "; "}
    descriptor = {"datatype": "text", "is_multiple": separators}
    assert get_adapter(name, descriptor)(value) == expected
    explicit = get_adapter_from_name_and_dt(name, "text", True, ";", "; ")
    assert explicit(value) == expected


def test_explicit_factory_preserves_missing_separator_behavior() -> None:
    adapter = get_adapter_from_name_and_dt("tags", "text", True)
    assert adapter(None) == ()
    with pytest.raises(AttributeError):
        adapter("alpha beta")
    assert get_adapter_from_name_and_dt(
        "tags", "text", True, None, ", "
    )("alpha beta") == ("alpha", "beta")


def test_factory_rejects_bad_descriptors_and_unknown_datatypes() -> None:
    with pytest.raises(KeyError, match="is_multiple"):
        get_adapter("title", {"datatype": "text"})
    with pytest.raises(NotImplementedError):
        get_adapter("field", {"datatype": "unknown"})
    with pytest.raises(NotImplementedError):
        get_adapter_from_name_and_dt("field", "unknown")


def test_custom_field_converters_keep_distinct_generic_contracts() -> None:
    assert single_text("  Note  ") == "Note"
    assert cc_adapt_text("  Note  ", {"is_multiple": False}) == "  Note  "
    assert multiple_text(",", ", ", None) == ()
    assert cc_adapt_text(None, {"is_multiple": True}) == []
    assert adapt_number(int, True) == 1
    with pytest.raises(InvalidUpdate):
        cc_adapt_number(True, {"datatype": "int"})
    assert cc_adapt_datetime(None, {}) is None
    with pytest.raises(InvalidUpdate):
        cc_adapt_datetime(123, {})
