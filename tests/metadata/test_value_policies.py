"""Check pure policy imports and the comparison/merge contracts they describe."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.text_identity import normalize_identity_text
from LiuXin_alpha.metadata.value_policies import (
    ColumnMergePolicy,
    DisplayValueKind,
    display_value_policy,
)


def test_policy_import_does_not_load_persistence_or_catalog() -> None:
    """A schema generator can use the vocabulary without loading higher layers."""
    source = Path(__file__).resolve().parents[2] / "src"
    script = """
import importlib.abc
import sys

sys.path.insert(0, sys.argv[1])

class RejectPersistenceImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith((
            'LiuXin_alpha.databases',
            'LiuXin_alpha.catalog',
            'LiuXin_alpha.caches',
        )):
            raise AssertionError('Policy import loaded ' + fullname)

sys.meta_path.insert(0, RejectPersistenceImports())
from LiuXin_alpha.metadata.value_policies import DisplayValueKind, display_value_policy
assert display_value_policy(DisplayValueKind.TITLE).semantic_role.value == 'title'
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(source)],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    ("kind", "text", "expected", "case_sensitive", "merge"),
    [
        (DisplayValueKind.TITLE, "  Cafe\u0301  ", "café", False, "replace"),
        (DisplayValueKind.ALIASES, "  Jane Doe  ", "jane doe", False, "set_union"),
        (DisplayValueKind.TAXONOMY_TERM, " Sci-Fi! ", "sci-fi!", False, "set_union"),
        (DisplayValueKind.COMMENT, " Cafe\u0301 ", " Café ", True, "append"),
        (DisplayValueKind.NOTE, " A NOTE ", " A NOTE ", True, "append"),
        (DisplayValueKind.SYNOPSIS, " A STORY ", " A STORY ", True, "append"),
        (DisplayValueKind.VERBATIM_TEXT, " My Note ", " My Note ", True, "replace"),
        (DisplayValueKind.RESOURCE_NAME, " Cafe\u0301.epub ",
         " Cafe\u0301.epub ", True, "replace"),
    ],
)
def test_display_policy_preserves_meaningful_text_distinctions(
    kind: DisplayValueKind,
    text: str,
    expected: str,
    case_sensitive: bool,
    merge: str,
) -> None:
    policy = display_value_policy(kind)
    assert normalize_identity_text(text, policy.normalization_profile.value) == expected
    assert policy.case_sensitive is case_sensitive
    assert policy.merge_policy is ColumnMergePolicy(merge)
