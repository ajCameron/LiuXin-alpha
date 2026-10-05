"""
Verify the optional PDF golden-corpus scaffold and fixture discovery policy.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test pdf golden corpus scaffold through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py
"""
from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
from typing import Any

import pytest

_THIS_DIR = Path(__file__).resolve().parent
_GOLDEN_DIR = _THIS_DIR.parent.parent / "fixtures" / "pdf_golden"
_MANIFEST_PATH = _GOLDEN_DIR / "manifest.json"


def _load_manifest() -> dict[str, Any]:
    """
    Perform the load manifest test-helper operation with deterministic inputs.

    Example:
        Exercise load manifest through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    """
    Perform the sha256 file test-helper operation with deterministic inputs.

    Example:
        Exercise sha256 file through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :param path: Value supplied for path in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _normalize_text(raw: Any) -> str:
    """
    Normalize text for stable comparison.

    Example:
        Exercise normalize text through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return " ".join(str(raw or "").split()).strip()


def _values(raw: Any) -> list[str]:
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [str(x) for x in raw.keys()]
    if isinstance(raw, str):
        return [raw]
    try:
        return [str(x) for x in list(raw)]
    except Exception:
        return [str(raw)]


def _normalized_list(raw: Any) -> list[str]:
    """
    Normalize normalized list for stable comparison.

    Example:
        Exercise normalized list through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    vals: list[str] = []
    for value in _values(raw):
        item = _normalize_text(value)
        if item:
            vals.append(item)
    return sorted(set(vals), key=str.casefold)


def _first_normalized(raw: Any) -> str | None:
    """
    Perform the first normalized test-helper operation with deterministic inputs.

    Example:
        Exercise first normalized through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    vals = _normalized_list(raw)
    return vals[0] if vals else None


def _identifier_value(md: Any, scheme: str) -> str | None:
    """
    Perform the identifier value test-helper operation with deterministic inputs.

    Example:
        Exercise identifier value through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :param md: Value supplied for md in the focused test operation.
    :param scheme: Value supplied for scheme in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    try:
        ids = md.get_identifiers()
    except Exception:
        ids = {}

    if isinstance(ids, dict):
        vals = _normalized_list(ids.get(scheme))
        if vals:
            return vals[0]

    vals = _normalized_list(getattr(md, scheme, None))
    if vals:
        return vals[0]

    return None


def _extract_expected(pdf_mod, pdf_path: Path) -> dict[str, Any]:
    """
    Perform the extract expected test-helper operation with deterministic inputs.

    Example:
        Exercise extract expected through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :param pdf_mod: Value supplied for pdf mod in the focused test operation.
    :param pdf_path: Value supplied for pdf path in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    md = pdf_mod.get_metadata_inplace(pdf_path)
    return {
        "title": _normalize_text(getattr(md, "title", "") or ""),
        "authors": _normalized_list(getattr(md, "authors", None)),
        "tags": _normalized_list(getattr(md, "tags", None)),
        "comments": _first_normalized(getattr(md, "comments", None)),
        "publisher": _first_normalized(getattr(md, "publisher", None)),
        "producers": _normalized_list(getattr(md, "producers", None)),
        "identifiers": {
            "isbn": _identifier_value(md, "isbn"),
            "doi": _identifier_value(md, "doi"),
            "uuid": _first_normalized(getattr(md, "uuid", None)),
        },
    }


def _case_ids() -> list[str]:
    """
    Perform the case ids test-helper operation with deterministic inputs.

    Example:
        Exercise case ids through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :return: The deterministic value, row, identity or collection described above.
    """
    manifest = _load_manifest()
    return [str(case["name"]) for case in manifest.get("cases", [])]


@pytest.fixture()
def pdf_md_mod():
    """
    Perform the pdf md mod test-helper operation with deterministic inputs.

    Example:
        Exercise pdf md mod through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return importlib.import_module("LiuXin_alpha.metadata.file_sources.pdf")


@pytest.mark.parametrize("case_name", _case_ids())
def test_golden_pdf_corpus_scaffold(case_name: str, pdf_md_mod) -> None:
    """
    Verify golden pdf corpus scaffold.

    Example:
        Exercise test golden pdf corpus scaffold through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :param case_name: Value supplied for case name in the focused test operation.
    :param pdf_md_mod: Value supplied for pdf md mod in the focused test operation.
    :return: None; the function records state or raises through its assertions.
    """
    manifest = _load_manifest()
    cases = {str(case["name"]): case for case in manifest.get("cases", [])}
    case = cases[case_name]
    expected = case.get("expected", {})

    path = _GOLDEN_DIR / str(case["path"])
    assert path.exists(), f"Missing golden PDF fixture: {path}"
    assert case.get("sha256") == _sha256_file(path)

    actual_1 = _extract_expected(pdf_md_mod, path)
    actual_2 = _extract_expected(pdf_md_mod, path)
    assert actual_1 == actual_2
    assert actual_1 == expected


def test_pdf_golden_manifest_has_unique_names_and_paths() -> None:
    """
    Verify pdf golden manifest has unique names and paths.

    Example:
        Exercise test pdf golden manifest has unique names and paths through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py


    :return: None; the function records state or raises through its assertions.
    """
    manifest = _load_manifest()
    cases = manifest.get("cases", [])
    names = [str(case["name"]) for case in cases]
    paths = [str(case["path"]) for case in cases]
    assert len(names) == len(set(names))
    assert len(paths) == len(set(paths))
