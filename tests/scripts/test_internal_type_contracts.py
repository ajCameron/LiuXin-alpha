"""Keep negative type checks from passing because of unrelated diagnostics."""

from pathlib import Path

import pytest

from scripts.check_internal_type_contracts import (
    Diagnostic,
    _diagnostic_failures,
    _expected_errors,
)


def test_missing_diagnostics_cannot_pass_a_negative_example(tmp_path: Path) -> None:
    failures = _diagnostic_failures({8: "arg-type"}, (), tmp_path / "probe.py")
    assert len(failures) == 1
    assert "checker accepted this mistake" in failures[0]


def test_unrelated_errors_do_not_satisfy_an_expectation(tmp_path: Path) -> None:
    fixture = tmp_path / "probe.py"
    errors = (
        Diagnostic(fixture, 8, "import-not-found", "a missing import"),
        Diagnostic(tmp_path / "other.py", 8, "arg-type", "another file"),
    )
    failures = _diagnostic_failures({8: "arg-type"}, errors, fixture)
    assert len(failures) == 3


def test_positive_lines_must_remain_clean(tmp_path: Path) -> None:
    fixture = tmp_path / "probe.py"
    errors = (
        Diagnostic(fixture, 8, "arg-type", "the intended error"),
        Diagnostic(fixture, 3, "arg-type", "a valid call was rejected"),
    )
    failures = _diagnostic_failures({8: "arg-type"}, errors, fixture)
    assert len(failures) == 1
    assert ":3:" in failures[0]


def test_matching_inherited_diagnostics_are_accepted(tmp_path: Path) -> None:
    fixture = tmp_path / "probe.py"
    errors = (
        Diagnostic(fixture, 8, "override", "first base"),
        Diagnostic(fixture, 8, "override", "second base"),
    )
    assert _diagnostic_failures({8: "override"}, errors, fixture) == []


def test_fixture_must_contain_real_negative_examples() -> None:
    with pytest.raises(ValueError, match="no expected errors"):
        _expected_errors("pass\n", "mypy")
    source = "value()  # expect-error: reportArgumentType arg-type\n"
    assert _expected_errors(source, "basedpyright") == {1: "reportArgumentType"}
    assert _expected_errors(source, "mypy") == {1: "arg-type"}
