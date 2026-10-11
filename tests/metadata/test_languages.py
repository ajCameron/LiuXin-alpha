"""Exercise metadata-owned bibliographic language normalization policy."""

from __future__ import annotations

import pytest

import LiuXin_alpha.metadata.languages as language_policy
from LiuXin_alpha.metadata.languages import canonicalize_languages


def test_canonicalize_languages_preserves_order_and_removes_duplicates() -> None:
    assert canonicalize_languages(("eng", "spa", "eng")) == (
        "English",
        "Spanish; Castilian",
    )


def test_canonicalize_languages_preserves_historical_undetermined_result() -> None:
    assert canonicalize_languages(("und",)) == ("Undetermined",)


def test_canonicalize_languages_filters_false_and_literal_special_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(language_policy, "canonicalize_lang", lambda value: value)

    assert canonicalize_languages((None, "und", "zxx", "mis", "mul", "English")) == (
        "English",
    )


def test_canonicalize_languages_propagates_canonicalizer_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_value: str) -> str:
        raise RuntimeError("language failure")

    monkeypatch.setattr(language_policy, "canonicalize_lang", fail)

    with pytest.raises(RuntimeError, match="language failure"):
        canonicalize_languages(("eng",))
