"""
Verify PDB fixture integrity across supported subreader families.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test pdb metadata fixtures through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py
"""
from __future__ import annotations

import io
import shutil
import uuid
from pathlib import Path

import pytest

from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins
from LiuXin_alpha.customize.builtins.metadata_writers import get_metadata_set_plugins
from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader
from LiuXin_alpha.metadata.file_sources.pdb import PdbFormatError, get_metadata, get_pheader_ident, set_metadata
from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _get_reader_plugin_cls():
    """
    Return reader plugin cls from deterministic test state.

    Example:
        Exercise get reader plugin cls through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return next((plugin for plugin in get_metadata_reader_plugins() if plugin.__name__ == "PDBMetadataReader"), None)


def _get_writer_plugin_cls():
    """
    Return writer plugin cls from deterministic test state.

    Example:
        Exercise get writer plugin cls through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return next((plugin for plugin in get_metadata_set_plugins() if plugin.__name__ == "PDBMetadataWriter"), None)


def _pdb_fixture(md_test_fixture, file_num: int) -> Path:
    """
    Perform the pdb fixture test-helper operation with deterministic inputs.

    Example:
        Exercise pdb fixture through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :param file_num: Value supplied for file num in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return md_test_fixture(file_ext="pdb", file_num=file_num, verify_hash=True)


def test_pdb_reader_plugin_loads_and_reads_hashed_fixtures(md_test_fixtures_for_ext) -> None:
    """
    Verify pdb reader plugin loads and reads hashed fixtures.

    Example:
        Exercise test pdb reader plugin loads and reads hashed fixtures through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixtures_for_ext: Value supplied for md test fixtures for ext in the
        focused test operation.
    :return: None; the function records state or raises through its assertions.
    """
    reader_cls = _get_reader_plugin_cls()
    assert reader_cls is not None
    reader = reader_cls(None)

    for fixture in md_test_fixtures_for_ext(file_ext="pdb", verify_hash=True):
        with fixture.open("rb") as stream:
            md_from_stream = reader.get_metadata(stream=stream, ftype="pdb")
        md_from_inplace = reader.get_metadata_inplace(file_path=fixture, ftype="pdb")

        assert md_from_stream is not None
        assert md_from_inplace is not None
        assert md_from_stream.title == md_from_inplace.title


def test_pdb_titles_match_legacy_expectations(md_test_fixture) -> None:
    """
    Verify pdb titles match legacy expectations.

    Example:
        Exercise test pdb titles match legacy expectations through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    expected_titles = {
        1: "20_000 Leagues Under the Sea",
        2: "20_000_Leagues_Under_the_Sea",
        3: "20_000 Leagues Under the Sea",
        4: "20_000 Leagues Under the Sea",
    }
    for file_num, expected_title in expected_titles.items():
        fixture = _pdb_fixture(md_test_fixture, file_num)
        md = get_metadata(fixture, extract_cover=False)
        assert md.title == expected_title


def test_pdb_authors_default_to_unknown_for_legacy_fixtures(md_test_fixture) -> None:
    """
    Verify pdb authors default to unknown for legacy fixtures.

    Example:
        Exercise test pdb authors default to unknown for legacy fixtures through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    for file_num in (1, 2, 3, 4):
        fixture = _pdb_fixture(md_test_fixture, file_num)
        md = get_metadata(fixture, extract_cover=False)
        assert md.authors == ["Unknown"]


def test_pdb_tags_are_empty_for_legacy_fixtures(md_test_fixture) -> None:
    """
    Verify pdb tags remain empty for legacy fixtures.

    Example:
        Exercise test pdb tags are empty for legacy fixtures through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    for file_num in (1, 2, 3, 4):
        fixture = _pdb_fixture(md_test_fixture, file_num)
        md = get_metadata(fixture, extract_cover=False)
        assert md.tags == []


def test_pdb_get_pheader_ident_matches_legacy_expectations(md_test_fixture) -> None:
    """
    Verify pdb get pheader ident matches legacy expectations.

    Example:
        Exercise test pdb get pheader ident matches legacy expectations through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    expected_idents = {
        1: "zTXTGPlm",
        2: "TEXtREAd",
        3: "SDocSilX",
        4: "PNRdPPrs",
    }
    for file_num, expected_ident in expected_idents.items():
        fixture = _pdb_fixture(md_test_fixture, file_num)

        assert get_pheader_ident(fixture) == expected_ident
        with fixture.open("rb") as stream:
            assert get_pheader_ident(stream) == expected_ident


def test_pdb_writer_plugin_loads() -> None:
    """
    Verify pdb writer plugin loads.

    Example:
        Exercise test pdb writer plugin loads through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :return: None; the function records state or raises through its assertions.
    """
    assert _get_writer_plugin_cls() is not None


def test_pdb_set_metadata_updates_header_title_for_non_ereader_fixture(md_test_fixture, tmp_path: Path) -> None:
    """
    Verify pdb set metadata updates header title for non ereader fixture.

    Example:
        Exercise test pdb set metadata updates header title for non ereader fixture through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    source = _pdb_fixture(md_test_fixture, 1)
    target = tmp_path / source.name
    shutil.copy2(source, target)

    desired_title = "Header-only update " + str(uuid.uuid4())[:8]
    set_metadata(target, calibreMetaInformation(desired_title, ["Ignored Author"]))

    md = get_metadata(target, extract_cover=False)
    assert md.title == desired_title
    assert md.authors == ["Unknown"]


def test_pdb_writer_plugin_updates_title_on_ereader_fixture(md_test_fixture, tmp_path: Path) -> None:
    """
    Verify pdb writer plugin updates title on ereader fixture.

    Example:
        Exercise test pdb writer plugin updates title on ereader fixture through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    source = _pdb_fixture(md_test_fixture, 4)
    target = tmp_path / source.name
    shutil.copy2(source, target)

    reader_cls = _get_reader_plugin_cls()
    writer_cls = _get_writer_plugin_cls()
    assert reader_cls is not None
    assert writer_cls is not None
    reader = reader_cls(None)
    writer = writer_cls(None)

    desired_title = "Writer title " + str(uuid.uuid4())[:8]
    update = calibreMetaInformation(desired_title, ["Writer One", "Writer Two"])

    with target.open("rb+") as stream:
        writer.set_metadata(stream, update, "pdb")

    with target.open("rb") as stream:
        md = reader.get_metadata(stream=stream, ftype="pdb")
    assert md.title == desired_title
    # This legacy fixture does not support author-body metadata writes yet.
    assert md.authors == ["Unknown"]


def test_pdb_writer_sanitizes_header_title_characters(md_test_fixture, tmp_path: Path) -> None:
    """
    Verify pdb writer sanitizes header title characters.

    Example:
        Exercise test pdb writer sanitizes header title characters through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    source = _pdb_fixture(md_test_fixture, 4)
    target = tmp_path / source.name
    shutil.copy2(source, target)

    raw_title = "A/B:C*D?E"
    set_metadata(target, calibreMetaInformation(raw_title, ["Ignored"]))

    with target.open("rb") as stream:
        header_title = PdbHeaderReader(stream).title
    assert header_title == "A_B_C_D_E"

    md = get_metadata(target, extract_cover=False)
    assert md.title == "A_B_C_D_E"


def test_pdb_get_metadata_handles_truncated_stream_sensibly() -> None:
    """
    Verify pdb get metadata handles truncated stream sensibly.

    Example:
        Exercise test pdb get metadata handles truncated stream sensibly through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(PdbFormatError):
        get_metadata(io.BytesIO(b"\x00\x01\x02"), extract_cover=False)

    md = get_metadata(io.BytesIO(b"\x00\x01\x02"), extract_cover=False, fallback_on_parse_error=True)
    assert md.title == "Unknown"
    assert md.authors == ["Unknown"]


def test_pdb_get_metadata_uses_filename_hint_for_corrupt_path(tmp_path: Path) -> None:
    """
    Verify pdb get metadata uses filename hint for corrupt path.

    Example:
        Exercise test pdb get metadata uses filename hint for corrupt path through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    broken = tmp_path / "broken_fixture.pdb"
    broken.write_bytes(b"\x00\x01")

    with pytest.raises(PdbFormatError):
        get_metadata(broken, extract_cover=False)

    md = get_metadata(broken, extract_cover=False, fallback_on_parse_error=True)
    assert md.title == "broken_fixture"
    assert md.authors == ["Unknown"]


def test_pdb_get_pheader_ident_raises_clean_value_error_on_corrupt_input() -> None:
    """
    Verify pdb get pheader ident raises clean value error on corrupt input.

    Example:
        Exercise test pdb get pheader ident raises clean value error on corrupt input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(ValueError, match="Unable to parse PDB header identity"):
        get_pheader_ident(io.BytesIO(b""))


def test_pdb_set_metadata_raises_clean_value_error_on_corrupt_input() -> None:
    """
    Verify pdb set metadata raises clean value error on corrupt input.

    Example:
        Exercise test pdb set metadata raises clean value error on corrupt input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_fixtures.py


    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(ValueError, match="invalid or corrupt PDB header"):
        set_metadata(io.BytesIO(b"\x00"), calibreMetaInformation("x", ["y"]))
