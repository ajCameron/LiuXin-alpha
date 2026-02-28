from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest


class _Log:
    def __call__(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    warn = warning

    def error(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


def _opts(input_encoding: str = "utf-8") -> SimpleNamespace:
    return SimpleNamespace(input_encoding=input_encoding, debug_pipeline=False)


def _mobi_paths(md_test_files_by_ext: dict[str, list[Path]]) -> list[Path]:
    paths = list(md_test_files_by_ext.get("mobi", []))
    if not paths:
        pytest.skip("No .mobi fixtures found in optional LiuXin_alpha_data corpus")
    return paths


def _assert_valid_opf(path: Path) -> None:
    assert path.exists(), f"missing output OPF: {path}"
    root = ET.parse(path).getroot()
    assert root.tag.endswith("package")

    rendered = path.read_text(encoding="utf-8", errors="replace")
    assert ".html" in rendered or ".xhtml" in rendered


def test_mobi_input_end_to_end_on_real_fixtures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, md_test_files_by_ext: dict[str, list[Path]]
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.mobi_input import MOBIInput

    for idx, mobi_path in enumerate(_mobi_paths(md_test_files_by_ext)):
        work = tmp_path / f"mobi_case_{idx}"
        work.mkdir()
        monkeypatch.chdir(work)

        with mobi_path.open("rb") as stream:
            out = MOBIInput(None).convert(stream, _opts(), "mobi", _Log(), {})

        out_path = Path(out) if Path(out).is_absolute() else work / out
        _assert_valid_opf(out_path)


def test_mobi_input_handles_non_utf8_option(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, md_test_files_by_ext: dict[str, list[Path]]
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.mobi_input import MOBIInput

    mobi_path = _mobi_paths(md_test_files_by_ext)[0]
    work = tmp_path / "mobi_cp1252"
    work.mkdir()
    monkeypatch.chdir(work)

    with mobi_path.open("rb") as stream:
        out = MOBIInput(None).convert(stream, _opts("cp1252"), "mobi", _Log(), {})

    out_path = Path(out) if Path(out).is_absolute() else work / out
    _assert_valid_opf(out_path)
