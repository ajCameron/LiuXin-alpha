"""
Provide test opf golden corpus scaffold utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test opf golden corpus scaffold through a consuming regression::

        python -m pytest -q tests/file_formats/opf/test_opf_golden_corpus_scaffold.py
"""
from __future__ import annotations

import importlib
import json
import hashlib
from pathlib import Path

import pytest

pytest.importorskip("lxml")
from lxml import etree

_THIS_DIR = Path(__file__).resolve().parent
_GOLDEN_DIR = _THIS_DIR.parent.parent / "fixtures" / "opf_golden"
_MANIFEST_PATH = _GOLDEN_DIR / "manifest.json"


@pytest.fixture()
def opf_mod(legacy_liuxin_alias):
    """
    Perform the opf mod operation under explicit file-format and conversion rules.

    Example:
        Exercise opf mod through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf_golden_corpus_scaffold.py


    :param legacy_liuxin_alias: Value supplied for legacy liuxin alias under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return importlib.import_module("LiuXin_alpha.file_formats.opf.opf")


def _load_manifest() -> dict:
    """
    Perform the load manifest operation under explicit file-format and conversion rules.

    Example:
        Exercise  load manifest through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf_golden_corpus_scaffold.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    """
    Perform the sha256 file operation under explicit file-format and conversion rules.

    Example:
        Exercise  sha256 file through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf_golden_corpus_scaffold.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _case_ids() -> list[str]:
    """
    Perform the case ids operation under explicit file-format and conversion rules.

    Example:
        Exercise  case ids through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf_golden_corpus_scaffold.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    manifest = _load_manifest()
    return [str(case["name"]) for case in manifest.get("cases", [])]


@pytest.mark.parametrize("case_name", _case_ids())
def test_golden_opf_corpus_scaffold(case_name: str, opf_mod) -> None:
    """
    Perform the test golden opf corpus scaffold operation under explicit file-format and conversion rules.

    Example:
        Exercise test golden opf corpus scaffold through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf_golden_corpus_scaffold.py


    :param case_name: Value supplied for case name under the utility contract.
    :param opf_mod: Value supplied for opf mod under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    manifest = _load_manifest()
    cases = {str(case["name"]): case for case in manifest.get("cases", [])}
    case = cases[case_name]
    expected = case.get("expected", {})
    path = _GOLDEN_DIR / case["path"]
    assert path.exists(), f"Missing golden OPF fixture: {path}"
    assert "sha256" in case
    assert case["sha256"] == _sha256_file(path)

    mi, ver, *_ = opf_mod.get_metadata(path)
    assert ver.major == expected.get("version_major")
    assert str(getattr(mi, "title", "")) == expected.get("title")
    assert [str(x) for x in getattr(mi, "authors", [])] == expected.get("authors")
    assert str(getattr(mi, "series", "")) == expected.get("series")
    assert float(getattr(mi, "series_index", 0.0)) == float(expected.get("series_index", 0.0))

    # Golden scaffold invariants: parseable + deterministic output for same input.
    out1, ver1, _ = opf_mod.set_metadata(path, mi)
    out2, ver2, _ = opf_mod.set_metadata(path, mi)
    assert ver1.major == ver2.major == ver.major
    assert bytes(out1) == bytes(out2)
    etree.fromstring(bytes(out1))


def test_golden_manifest_has_unique_names_and_paths() -> None:
    """
    Perform the test golden manifest has unique names and paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test golden manifest has unique names and paths through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf_golden_corpus_scaffold.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    manifest = _load_manifest()
    cases = manifest.get("cases", [])
    names = [str(case["name"]) for case in cases]
    paths = [str(case["path"]) for case in cases]
    assert len(names) == len(set(names))
    assert len(paths) == len(set(paths))
