from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def test_worker_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.worker as worker

    assert worker is not None


def test_metadata_from_formats_prefers_opf_when_available(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import worker

    txt = tmp_path / "book.txt"
    opf = tmp_path / "book.opf"
    txt.write_bytes(b"text")
    opf.write_bytes(b"opf")

    txt_md = calibreMetaInformation("TXT Title", ["Txt Author"])
    opf_md = calibreMetaInformation("OPF Title", ["Opf Author"])
    calls = []

    def fake_get_metadata(path, force_type=False):
        ext = str(force_type or Path(path).suffix.lstrip(".")).lower()
        calls.append(ext)
        if ext == "opf":
            return opf_md
        return txt_md

    monkeypatch.setattr(worker, "get_file_metadata", fake_get_metadata)

    out = worker.metadata_from_formats([txt, opf])
    assert out.title == "OPF Title"
    assert _values(out.authors) == ["Opf Author"]
    assert calls[0] == "opf"


def test_serialize_metadata_for_writes_cover_and_opf(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import worker

    md = calibreMetaInformation("Serialized", ["Author"])
    md.cover_data = ("jpg", b"\xff\xd8\xff")
    md.application_id = None

    monkeypatch.setattr(worker, "metadata_from_formats", lambda _paths: md)
    monkeypatch.setattr(worker, "_metadata_to_opf_bytes", lambda _mi, _tdir: b"<package/>")

    out_mi, opf, has_cover = worker.serialize_metadata_for(["dummy.txt"], str(tmp_path), "42")

    assert out_mi.application_id == "__calibre_dummy__"
    assert opf == b"<package/>"
    assert has_cover is True
    assert (tmp_path / "42.cdata").read_bytes() == b"\xff\xd8\xff"
    assert out_mi.cover_data == (None, None)


def test_run_import_plugins_flattens_groups_and_preserves_basename(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import worker

    a = tmp_path / "a.txt"
    b = tmp_path / "b.epub"
    converted = tmp_path / "converted.mobi"
    a.write_text("a", encoding="utf-8")
    b.write_text("b", encoding="utf-8")
    converted.write_text("converted", encoding="utf-8")

    def fake_run_plugins(path):
        if path.endswith("b.epub"):
            return str(converted)
        return path

    monkeypatch.setattr(worker, "run_plugins_on_import", fake_run_plugins)

    out = worker.run_import_plugins([str(a), [str(b)]], group_id="7", tdir=str(tmp_path))
    assert out[0] == str(a)
    expected = tmp_path / "7" / "b.mobi"
    assert out[1] == str(expected)
    assert expected.read_text("utf-8") == "converted"


def test_read_metadata_sets_duplicate_info_from_common_data(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import worker

    md = calibreMetaInformation("The Book", ["A"])
    monkeypatch.setattr(worker, "run_import_plugins", lambda paths, _gid, _tdir: list(paths))
    monkeypatch.setattr(worker, "serialize_metadata_for", lambda _paths, _tdir, _gid: (md, b"<opf/>", False))

    paths, opf, has_cover, duplicate = worker.read_metadata(
        [str(tmp_path / "book.epub")],
        group_id="1",
        tdir=str(tmp_path),
        common_data={"the book"},
    )
    assert paths == [str(tmp_path / "book.epub")]
    assert opf == b"<opf/>"
    assert has_cover is False
    assert duplicate is True


def test_read_metadata_bulk_returns_requested_parts(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import worker

    md = calibreMetaInformation("Bulk", ["A"])
    md.cover_data = ("jpg", b"cover-bytes")
    md.application_id = None

    monkeypatch.setattr(worker, "metadata_from_formats", lambda _paths: md)
    monkeypatch.setattr(worker, "_metadata_to_opf_bytes", lambda _mi, _tdir: b"<opf-bulk/>")

    ans = worker.read_metadata_bulk(get_opf=True, get_cover=True, paths=["dummy.txt"])
    assert ans["opf"] == b"<opf-bulk/>"
    assert ans["cdata"] == b"cover-bytes"


def test_worker_metadata_to_opf_falls_back_to_opfcreator(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import worker

    md = calibreMetaInformation("Fallback", ["Author"])
    monkeypatch.setattr(worker, "metadata_to_opf", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))

    out = worker._metadata_to_opf_bytes(md, str(tmp_path))
    assert isinstance(out, (bytes, bytearray))
    assert b"<package" in bytes(out)


def test_read_metadata_in_job_serial(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources import worker

    p = tmp_path / "job.txt"
    p.write_text("Job Title\n\n\nby Job Author\n", encoding="utf-8")

    paths, opf, has_cover, duplicate = worker.read_metadata_in_job(
        [str(p)],
        group_id="1",
        tdir=str(tmp_path),
        common_data={"job title"},
        backend="serial",
        no_output=True,
    )

    assert paths == [str(p)]
    assert isinstance(opf, (bytes, bytearray))
    assert b"<package" in bytes(opf)
    assert has_cover is False
    assert duplicate is True


def test_read_metadata_bulk_in_job_process(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources import worker

    p = tmp_path / "job_bulk.txt"
    p.write_text("Bulk Title\n\n\nby Bulk Author\n", encoding="utf-8")

    ans = worker.read_metadata_bulk_in_job(
        get_opf=True,
        get_cover=True,
        paths=[str(p)],
        backend="process",
        no_output=True,
        timeout=30,
    )

    assert isinstance(ans, dict)
    assert isinstance(ans.get("opf"), (bytes, bytearray))
    assert b"<package" in bytes(ans["opf"])
    assert ans.get("cdata") is None
