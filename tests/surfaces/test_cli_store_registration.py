"""Keep standalone disk registration presentation above Core ownership."""

from __future__ import annotations

import json
from typing import Any

import pytest

from LiuXin_alpha.ingest.native_disk import NativeDiskReport
from LiuXin_alpha.surfaces.cli import store_registration


@pytest.mark.parametrize("as_json", (False, True))
@pytest.mark.parametrize(
    "fail_on_errors,errors,status",
    ((False, ["bad file"], 0), (True, ["bad file"], 2), (True, [], 0)),
)
def test_cli_registration_report_and_exit_status(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    as_json: bool,
    fail_on_errors: bool,
    errors: list[str],
    status: int,
) -> None:
    """Preserve flags, counters and opt-in file-error exit status after the move."""
    report = NativeDiskReport(
        "00000000-0000-0000-0000-000000000007",
        "/books",
        "Books",
        inserted_assets=2,
        errors=errors,
        duration_seconds=1.5,
    )

    def register(**kwargs: Any) -> NativeDiskReport:
        assert kwargs == {
            "database_path": "catalogue.sqlite",
            "disk_path": "/books",
            "db_type": "SQLite_APSW",
            "store_name": "Books",
            "compute_hash": True,
            "follow_symlinks": True,
            "attach_store_links": True,
        }
        return report

    monkeypatch.setattr(
        store_registration, "register_existing_disk_with_database_path", register
    )
    args = [
        "--database",
        "catalogue.sqlite",
        "--disk",
        "/books",
        "--db-type",
        "SQLite_APSW",
        "--store-name",
        "Books",
        "--follow-symlinks",
    ]
    if as_json:
        args.append("--json")
    if fail_on_errors:
        args.append("--fail-on-errors")
    assert store_registration.main(args) == status
    output = capsys.readouterr().out
    if as_json:
        assert json.loads(output) == report.to_dict()
    else:
        assert "Store: Books (/books)" in output
        assert "Inserted Assets: 2" in output
        assert f"Errors: {len(errors)}" in output
        assert "Duration (seconds): 1.500" in output


def test_cli_does_not_print_success_after_lifecycle_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Registration or teardown failure escapes without a misleading report."""

    def fail(**kwargs: Any) -> NativeDiskReport:
        raise RuntimeError("library close failed")

    monkeypatch.setattr(
        store_registration, "register_existing_disk_with_database_path", fail
    )
    with pytest.raises(RuntimeError, match="library close failed"):
        store_registration.main(
            ["--database", "catalogue.sqlite", "--disk", "/books", "--json"]
        )
    assert capsys.readouterr().out == ""


def test_cli_help_never_opens_a_graph(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The relocated module exposes help without opening a library."""

    def unexpected(**kwargs: Any) -> NativeDiskReport:
        pytest.fail("help attempted registration")

    monkeypatch.setattr(
        store_registration, "register_existing_disk_with_database_path", unexpected
    )
    with pytest.raises(SystemExit) as error:
        store_registration.main(["--help"])
    assert error.value.code == 0
    assert "--database" in capsys.readouterr().out
