from __future__ import annotations

import io
from pathlib import Path

import pytest


def _encode_vwi(value: int) -> bytes:
    parts = [value & 0x7F]
    value >>= 7
    while value:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    return bytes(reversed(parts))


def _build_topaz_bytes(title: str, authors: str) -> bytes:
    fields = [
        ("Title", title.encode("utf-8", "replace")),
        ("Authors", authors.encode("utf-8", "replace")),
    ]
    metadata_block = bytearray()
    metadata_block.extend(_encode_vwi(len(b"metadata")))
    metadata_block.extend(b"metadata")
    metadata_block.append(0)
    metadata_block.append(len(fields))
    for key, value in fields:
        key_bytes = key.encode("ascii")
        metadata_block.extend(_encode_vwi(len(key_bytes)))
        metadata_block.extend(key_bytes)
        metadata_block.extend(_encode_vwi(len(value)))
        metadata_block.extend(value)

    len_uncomp = max(0, len(metadata_block) - (len("metadata") + 2))
    header = bytearray(b"TPZ0")
    header.extend(_encode_vwi(1))
    header.extend(b"c")
    header.extend(_encode_vwi(len(b"metadata")))
    header.extend(b"metadata")
    header.extend(_encode_vwi(1))
    header.extend(_encode_vwi(0))
    header.extend(_encode_vwi(len_uncomp))
    header.extend(_encode_vwi(len_uncomp))
    header.extend(b"d")
    return bytes(header) + bytes(metadata_block)


def test_metadata_file_sources_dispatcher_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources as dispatcher

    assert dispatcher is not None


def test_metadata_dispatcher_get_plugins_for_extension_pdb() -> None:
    from LiuXin_alpha.metadata.file_sources import get_plugins_for_extension

    plugins = get_plugins_for_extension("pdb")
    names = [plugin.module_name for plugin in plugins]
    assert "PDBMetadataReader" in names


def test_metadata_dispatcher_reads_pdb_from_path(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    fixture = md_test_fixture(file_ext="pdb", file_num=1, verify_hash=True)
    md = get_metadata(fixture)
    assert md.title == "20_000 Leagues Under the Sea"
    assert md.authors == ["Unknown"]


def test_metadata_dispatcher_reads_pdb_from_stream_with_force_type(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    fixture = md_test_fixture(file_ext="pdb", file_num=2, verify_hash=True)
    with fixture.open("rb") as stream:
        md = get_metadata(stream, force_type="pdb")
    assert md.title == "20_000_Leagues_Under_the_Sea"


def test_metadata_dispatcher_requires_force_type_for_stream_without_name() -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    with pytest.raises(ValueError, match="Could not infer extension"):
        get_metadata(io.BytesIO(b"not a named stream"))


def test_metadata_dispatcher_invalid_extension_raises_clean_error(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources import InvalidMetadataExtractor, get_metadata

    target = tmp_path / "unknown.foo"
    target.write_bytes(b"abc")
    with pytest.raises(InvalidMetadataExtractor, match="No metadata reader plugin"):
        get_metadata(target)


def test_metadata_dispatcher_filter_plugin_sources_compat() -> None:
    from LiuXin_alpha.metadata.file_sources import filter_plugin_sources

    result = filter_plugin_sources(["__init__.py", "foo.py", "bar.pyc", "baz.py"])
    assert result == ["foo.py", "baz.py"]


def test_metadata_dispatcher_supports_positional_force_type_arg(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    fixture = md_test_fixture(file_ext="pdb", file_num=1, verify_hash=True)
    with fixture.open("rb") as stream:
        md = get_metadata(stream, "pdb")
    assert md.title == "20_000 Leagues Under the Sea"


def test_metadata_dispatcher_reads_rtf_when_reader_plugin_is_available(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    fixture = md_test_fixture(file_ext="rtf", file_num=1, verify_hash=True)
    with fixture.open("rb") as stream:
        md = get_metadata(stream, "rtf")
    assert md.title == "20,000 Leagues Under the Sea"
    authors = getattr(md, "authors", None)
    if isinstance(authors, dict):
        author_values = list(authors.keys())
    else:
        author_values = list(authors or [])
    assert author_values == ["Jules Verne"]


def test_metadata_dispatcher_reads_snb_when_reader_plugin_is_available(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    fixture = md_test_fixture(file_ext="snb", file_num=1, verify_hash=True)
    with fixture.open("rb") as stream:
        md = get_metadata(stream, "snb")
    assert md.title == "20,000 Leagues Under the Sea"
    authors = getattr(md, "authors", None)
    if isinstance(authors, dict):
        author_values = list(authors.keys())
    else:
        author_values = list(authors or [])
    assert author_values == ["Jules Verne"]


def test_metadata_dispatcher_reads_topaz_when_reader_plugin_is_available(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    payload = _build_topaz_bytes("Dispatcher Topaz", "Alice; Bob")
    path = tmp_path / "dispatcher.tpz"
    path.write_bytes(payload)

    md = get_metadata(path)
    assert md.title == "Dispatcher Topaz"
    authors = getattr(md, "authors", None)
    if isinstance(authors, dict):
        author_values = list(authors.keys())
    else:
        author_values = list(authors or [])
    assert author_values == ["Alice", "Bob"]


def test_metadata_dispatcher_reads_txt_when_reader_plugin_is_available(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    payload = b"Dispatcher TXT Title\n\n\nDispatcher TXT Author\nBody\n"
    path = tmp_path / "dispatcher.txt"
    path.write_bytes(payload)

    md = get_metadata(path)
    assert md.title == "Dispatcher TXT Title"
    authors = getattr(md, "authors", None)
    if isinstance(authors, dict):
        author_values = list(authors.keys())
    else:
        author_values = list(authors or [])
    assert author_values == ["Dispatcher TXT Author"]


def test_metadata_dispatcher_reads_txtz_when_reader_plugin_is_available(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources import get_metadata

    fixture = md_test_fixture(file_ext="txtz", file_num=1, verify_hash=True)
    with fixture.open("rb") as stream:
        md = get_metadata(stream, "txtz")
    assert md.title == "20,000 Leagues Under the Sea"
    authors = getattr(md, "authors", None)
    if isinstance(authors, dict):
        author_values = list(authors.keys())
    else:
        author_values = list(authors or [])
    assert author_values == ["Jules Verne"]
