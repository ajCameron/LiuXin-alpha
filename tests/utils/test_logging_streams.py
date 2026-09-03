"""Stream ownership tests for legacy-compatible logging helpers."""

from __future__ import annotations

from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_warning_print


def test_warning_output_does_not_contaminate_machine_readable_stdout(
    capsys,
) -> None:
    LiuXin_warning_print("fallback", "detail")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "fallback\ndetail\n"


def test_ordinary_legacy_print_remains_on_stdout(capsys) -> None:
    LiuXin_print("receipt")

    captured = capsys.readouterr()
    assert captured.out == "receipt\n"
    assert captured.err == ""
