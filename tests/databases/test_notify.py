"""Tests for LiuXin_alpha.databases.notify.

The notify module contains two lightweight placeholder functions:
dummy_notify and dummy_dirtied. Tests confirm their import contract and
runtime behaviour for both the embed=False and embed=True paths.
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.databases.notify import dummy_dirtied, dummy_notify


class _FakeCCClassNotEmbedded:
    embed = False


class _FakeCCClassEmbedded:
    embed = True


class TestDummyNotify:
    def test_no_op_when_not_embedded(self) -> None:
        # Should return None silently
        result = dummy_notify("added", [1, 2, 3], _FakeCCClassNotEmbedded())
        assert result is None

    def test_raises_not_implemented_when_embedded(self) -> None:
        with pytest.raises(NotImplementedError):
            dummy_notify("added", [1], _FakeCCClassEmbedded())

    def test_accepts_arbitrary_event_names(self) -> None:
        dummy_notify("deleted", [], _FakeCCClassNotEmbedded())
        dummy_notify("updated", [99], _FakeCCClassNotEmbedded())

    def test_accepts_empty_ids_list(self) -> None:
        dummy_notify("any_event", [], _FakeCCClassNotEmbedded())


class TestDummyDirtied:
    def test_no_op_when_not_embedded(self) -> None:
        result = dummy_dirtied([1, 2], commit=True, cc_class=_FakeCCClassNotEmbedded())
        assert result is None

    def test_raises_not_implemented_when_embedded(self) -> None:
        with pytest.raises(NotImplementedError):
            dummy_dirtied([1], commit=False, cc_class=_FakeCCClassEmbedded())

    def test_accepts_empty_ids(self) -> None:
        dummy_dirtied([], commit=False, cc_class=_FakeCCClassNotEmbedded())

    def test_commit_flag_does_not_affect_non_embed_path(self) -> None:
        dummy_dirtied([1], commit=True, cc_class=_FakeCCClassNotEmbedded())
        dummy_dirtied([1], commit=False, cc_class=_FakeCCClassNotEmbedded())
