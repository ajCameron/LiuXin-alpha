from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def test_web_sources_cli_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.cli as cli

    assert cli is not None


def test_web_sources_cli_no_results_exits(monkeypatch, capsys) -> None:
    import LiuXin_alpha.metadata.web_sources.cli as cli

    monkeypatch.setattr(cli, "identify", lambda *args, **kwargs: [])

    with pytest.raises(SystemExit) as exc:
        cli.main(["prog", "--title", "Unknown Book"])
    assert exc.value.code == 1

    captured = capsys.readouterr()
    assert "No results found" in captured.err


def test_web_sources_cli_text_output_and_cover(monkeypatch, tmp_path: Path, capsys) -> None:
    import LiuXin_alpha.metadata.web_sources.cli as cli

    mi = calibreMetaInformation("CLI Test Title", ["CLI Author"])
    mi.set_identifier("isbn", "9780306406157")
    monkeypatch.setattr(cli, "identify", lambda *args, **kwargs: [mi])
    monkeypatch.setattr(cli, "download_cover", lambda *args, **kwargs: (object(), 100, 120, "jpeg", b"cover-bytes"))
    monkeypatch.setattr(cli, "save_cover_data_to", lambda data, path: Path(path).write_bytes(data))

    cover_path = tmp_path / "cover.jpg"
    rc = cli.main(
        [
            "prog",
            "--title",
            "CLI Test Title",
            "--authors",
            "CLI Author",
            "--cover",
            str(cover_path),
            "--identifier",
            "asin:B0082BAJA0",
        ]
    )
    assert rc == 0
    assert cover_path.read_bytes() == b"cover-bytes"

    captured = capsys.readouterr()
    assert "CLI Test Title" in captured.out
    assert "Cover" in captured.out


def test_web_sources_cli_opf_output(monkeypatch, capsys) -> None:
    import LiuXin_alpha.metadata.web_sources.cli as cli

    mi = calibreMetaInformation("OPF Title", ["Author"])
    monkeypatch.setattr(cli, "identify", lambda *args, **kwargs: [mi])
    monkeypatch.setattr(cli, "metadata_to_opf", lambda _mi: b"<opf>ok</opf>")

    rc = cli.main(["prog", "--title", "OPF Title", "--opf"])
    assert rc == 0

    captured = capsys.readouterr()
    assert "<opf>ok</opf>" in captured.out


def test_web_sources_cli_invalid_identifier_rejected() -> None:
    import LiuXin_alpha.metadata.web_sources.cli as cli

    with pytest.raises(SystemExit, match="Not a valid identifier"):
        cli.main(["prog", "--identifier", "bad_identifier"])
