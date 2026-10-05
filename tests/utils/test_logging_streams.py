"""
Provide test logging streams utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test logging streams through a consuming regression::

        python -m pytest -q tests/utils/test_logging_streams.py
"""

from __future__ import annotations

from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_warning_print


def test_warning_output_does_not_contaminate_machine_readable_stdout(
    capsys,
) -> None:
    """
    Perform the test warning output does not contaminate machine readable stdout utility operation under explicit compatibility rules.

    Example:
        Exercise test warning output does not contaminate machine readable stdout through a consuming regression::

            python -m pytest -q tests/utils/test_logging_streams.py


    :param capsys: Value supplied for capsys under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    LiuXin_warning_print("fallback", "detail")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "fallback\ndetail\n"


def test_ordinary_legacy_print_remains_on_stdout(capsys) -> None:
    """
    Perform the test ordinary legacy print remains on stdout utility operation under explicit compatibility rules.

    Example:
        Exercise test ordinary legacy print remains on stdout through a consuming regression::

            python -m pytest -q tests/utils/test_logging_streams.py


    :param capsys: Value supplied for capsys under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    LiuXin_print("receipt")

    captured = capsys.readouterr()
    assert captured.out == "receipt\n"
    assert captured.err == ""
