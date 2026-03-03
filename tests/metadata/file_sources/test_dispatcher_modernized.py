from __future__ import annotations

import io
from pathlib import Path

import pytest


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


def test_metadata_dispatcher_reports_missing_reader_plugin_cleanly_for_rtf(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources import InvalidMetadataExtractor, get_metadata

    fixture = md_test_fixture(file_ext="rtf", file_num=1, verify_hash=True)
    with fixture.open("rb") as stream:
        with pytest.raises(InvalidMetadataExtractor, match="No metadata reader plugin is registered"):
            get_metadata(stream, "rtf")
