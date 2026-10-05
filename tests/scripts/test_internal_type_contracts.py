"""
Provide test internal type contracts utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test internal type contracts through a consuming regression::

        python -m pytest -q tests/scripts/test_internal_type_contracts.py
"""

from pathlib import Path

import pytest

from scripts.check_internal_type_contracts import (
    Diagnostic,
    _diagnostic_failures,
    _expected_errors,
)


def test_missing_diagnostics_cannot_pass_a_negative_example(tmp_path: Path) -> None:
    """
    Perform the test missing diagnostics cannot pass a negative example operation under explicit file-format and conversion rules.

    Example:
        Exercise test missing diagnostics cannot pass a negative example through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    failures = _diagnostic_failures({8: "arg-type"}, (), tmp_path / "probe.py")
    assert len(failures) == 1
    assert "checker accepted this mistake" in failures[0]


def test_unrelated_errors_do_not_satisfy_an_expectation(tmp_path: Path) -> None:
    """
    Perform the test unrelated errors do not satisfy an expectation operation under explicit file-format and conversion rules.

    Example:
        Exercise test unrelated errors do not satisfy an expectation through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture = tmp_path / "probe.py"
    errors = (
        Diagnostic(fixture, 8, "import-not-found", "a missing import"),
        Diagnostic(tmp_path / "other.py", 8, "arg-type", "another file"),
    )
    failures = _diagnostic_failures({8: "arg-type"}, errors, fixture)
    assert len(failures) == 3


def test_positive_lines_must_remain_clean(tmp_path: Path) -> None:
    """
    Perform the test positive lines must remain clean operation under explicit file-format and conversion rules.

    Example:
        Exercise test positive lines must remain clean through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture = tmp_path / "probe.py"
    errors = (
        Diagnostic(fixture, 8, "arg-type", "the intended error"),
        Diagnostic(fixture, 3, "arg-type", "a valid call was rejected"),
    )
    failures = _diagnostic_failures({8: "arg-type"}, errors, fixture)
    assert len(failures) == 1
    assert ":3:" in failures[0]


def test_matching_inherited_diagnostics_are_accepted(tmp_path: Path) -> None:
    """
    Perform the test matching inherited diagnostics are accepted operation under explicit file-format and conversion rules.

    Example:
        Exercise test matching inherited diagnostics are accepted through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture = tmp_path / "probe.py"
    errors = (
        Diagnostic(fixture, 8, "override", "first base"),
        Diagnostic(fixture, 8, "override", "second base"),
    )
    assert _diagnostic_failures({8: "override"}, errors, fixture) == []


def test_fixture_must_contain_real_negative_examples() -> None:
    """
    Perform the test fixture must contain real negative examples operation under explicit file-format and conversion rules.

    Example:
        Exercise test fixture must contain real negative examples through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ValueError, match="no expected errors"):
        _expected_errors("pass\n", "mypy")
    source = "value()  # expect-error: reportArgumentType arg-type\n"
    assert _expected_errors(source, "basedpyright") == {1: "reportArgumentType"}
    assert _expected_errors(source, "mypy") == {1: "arg-type"}


def test_multiline_call_keeps_each_checkers_exact_diagnostic_location(
    tmp_path: Path,
) -> None:
    """
    Perform the test multiline call keeps each checkers exact diagnostic location operation under explicit file-format and conversion rules.

    Example:
        Exercise test multiline call keeps each checkers exact diagnostic location through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    source = (
        "register(  # expect-error: - call-arg\n"
        '    "name", unexpected=True  # expect-error: reportCallIssue -\n'
        ")\n"
    )
    assert _expected_errors(source, "mypy") == {1: "call-arg"}
    assert _expected_errors(source, "basedpyright") == {2: "reportCallIssue"}
    fixture = tmp_path / "probe.py"
    misplaced = (Diagnostic(fixture, 1, "reportCallIssue", "wrong line"),)
    failures = _diagnostic_failures(
        _expected_errors(source, "basedpyright"), misplaced, fixture
    )
    assert len(failures) == 2


@pytest.mark.parametrize("checker", ["mypy", "basedpyright"])
def test_dash_only_markers_cannot_pass_without_negative_examples(checker: str) -> None:
    """
    Perform the test dash only markers cannot pass without negative examples operation under explicit file-format and conversion rules.

    Example:
        Exercise test dash only markers cannot pass without negative examples through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param checker: Value supplied for checker under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ValueError, match="no expected errors"):
        _expected_errors("value()  # expect-error: - -\n", checker)
