from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_script():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "torrent_ebook_inventory.py"
    spec = importlib.util.spec_from_file_location("torrent_ebook_inventory", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _bencode(value):
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii") + b"e"
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return str(len(raw)).encode("ascii") + b":" + raw
    if isinstance(value, list):
        return b"l" + b"".join(_bencode(item) for item in value) + b"e"
    if isinstance(value, dict):
        parts = []
        for key in sorted(value):
            parts.append(_bencode(key))
            parts.append(_bencode(value[key]))
        return b"d" + b"".join(parts) + b"e"
    raise TypeError("unsupported bencode value {!r}".format(value))


def test_analyze_single_file_torrent_identifies_ebook_file() -> None:
    script = _load_script()
    torrent_bytes = _bencode(
        {
            b"announce": b"https://tracker.example/announce",
            b"info": {
                b"length": 12345,
                b"name": b"Example Book.epub",
                b"piece length": 16384,
                b"pieces": b"abc",
            },
        }
    )

    payload = script.analyze_torrent_bytes(torrent_bytes)

    assert payload["torrent"]["name"] == "Example Book.epub"
    assert payload["torrent"]["file_count"] == 1
    assert payload["torrent"]["ebook_file_count"] == 1
    assert len(payload["ebook_files"]) == 1
    assert payload["ebook_files"][0]["path"] == "Example Book.epub"
    assert payload["groups"][0]["primary_path"] == "Example Book.epub"


def test_analyze_multi_file_torrent_groups_multiformat_books() -> None:
    script = _load_script()
    torrent_bytes = _bencode(
        {
            b"announce": b"https://tracker.example/announce",
            b"info": {
                b"name": b"Library Dump",
                b"piece length": 32768,
                b"pieces": b"def",
                b"files": [
                    {b"length": 100, b"path": [b"Book One", b"Book One.epub"]},
                    {b"length": 150, b"path": [b"Book One", b"Book One.pdf"]},
                    {b"length": 25, b"path": [b"Book One", b"cover.jpg"]},
                    {b"length": 80, b"path": [b"Book Two", b"Book Two.txt"]},
                    {b"length": 90, b"path": [b"Misc", b"Alpha.epub"]},
                    {b"length": 95, b"path": [b"Misc", b"Beta.pdf"]},
                ],
            },
        }
    )

    payload = script.analyze_torrent_bytes(torrent_bytes)

    assert payload["torrent"]["file_count"] == 6
    assert payload["torrent"]["ebook_file_count"] == 5
    assert payload["torrent"]["group_count"] == 4
    assert payload["torrent"]["directory_group_count"] == 3
    assert payload["torrent"]["multi_variant_group_count"] == 1
    assert payload["torrent"]["multi_stem_directory_group_count"] == 1

    groups = {entry["group_key"]: entry for entry in payload["groups"]}
    assert groups["Book One:book one"]["variant_count"] == 2
    assert groups["Book One:book one"]["likely_multiformat_book"] is True
    assert groups["Book One:book one"]["extensions"] == ["epub", "pdf"]
    assert groups["Book Two:book two"]["variant_count"] == 1
    assert groups["Book Two:book two"]["primary_path"] == "Book Two/Book Two.txt"
    assert groups["Misc:alpha"]["variant_count"] == 1
    assert groups["Misc:beta"]["variant_count"] == 1

    directory_groups = {entry["group_key"]: entry for entry in payload["directory_groups"]}
    assert directory_groups["Book One"]["file_count"] == 2
    assert directory_groups["Book One"]["stem_count"] == 1
    assert directory_groups["Misc"]["file_count"] == 2
    assert directory_groups["Misc"]["stem_count"] == 2
    assert directory_groups["Misc"]["stems"] == ["alpha", "beta"]


def test_main_writes_json_output_file(tmp_path: Path, capsys) -> None:
    script = _load_script()
    torrent_path = tmp_path / "sample.torrent"
    output_path = tmp_path / "inventory.json"
    torrent_path.write_bytes(
        _bencode(
            {
                b"announce": b"https://tracker.example/announce",
                b"info": {
                    b"length": 42,
                    b"name": b"Standalone.pdf",
                    b"piece length": 16384,
                    b"pieces": b"ghi",
                },
            }
        )
    )

    exit_code = script.main([str(torrent_path), "--output", str(output_path)])

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["torrent"]["ebook_file_count"] == 1
    assert payload["torrent"]["directory_group_count"] == 1
    captured = capsys.readouterr()
    assert "wrote torrent inventory:" in captured.out


def test_render_text_report_lists_groups_and_files() -> None:
    script = _load_script()
    torrent_bytes = _bencode(
        {
            b"announce": b"https://tracker.example/announce",
            b"info": {
                b"name": b"Library Dump",
                b"piece length": 32768,
                b"pieces": b"def",
                b"files": [
                    {b"length": 100, b"path": [b"Book One", b"Book One.epub"]},
                    {b"length": 150, b"path": [b"Book One", b"Book One.pdf"]},
                    {b"length": 80, b"path": [b"Book Two", b"Book Two.txt"]},
                    {b"length": 90, b"path": [b"Misc", b"Alpha.epub"]},
                    {b"length": 95, b"path": [b"Misc", b"Beta.pdf"]},
                ],
            },
        }
    )

    payload = script.analyze_torrent_bytes(torrent_bytes)
    report = script.render_text_report(payload)

    assert "Torrent" in report
    assert "Likely Books" in report
    assert "Messy Directories" in report
    assert "Ebook Files" in report
    assert "Book One | 2 variants | epub, pdf" in report
    assert "Misc | 2 files | 2 stems" in report
    assert "Book One/Book One.epub | epub | 100 B" in report


def test_main_emits_text_report_to_stdout(tmp_path: Path, capsys) -> None:
    script = _load_script()
    torrent_path = tmp_path / "sample.torrent"
    torrent_path.write_bytes(
        _bencode(
            {
                b"announce": b"https://tracker.example/announce",
                b"info": {
                    b"length": 42,
                    b"name": b"Standalone.pdf",
                    b"piece length": 16384,
                    b"pieces": b"ghi",
                },
            }
        )
    )

    exit_code = script.main([str(torrent_path), "--report", "text"])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Torrent" in captured.out
    assert "Likely Books" in captured.out
    assert "Standalone.pdf | pdf | 42 B" in captured.out
