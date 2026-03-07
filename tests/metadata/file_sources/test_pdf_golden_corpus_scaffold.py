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
    return json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _normalize_text(raw: Any) -> str:
    return " ".join(str(raw or "").split()).strip()


def _values(raw: Any) -> list[str]:
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
    vals: list[str] = []
    for value in _values(raw):
        item = _normalize_text(value)
        if item:
            vals.append(item)
    return sorted(set(vals), key=str.casefold)


def _first_normalized(raw: Any) -> str | None:
    vals = _normalized_list(raw)
    return vals[0] if vals else None


def _identifier_value(md: Any, scheme: str) -> str | None:
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
    manifest = _load_manifest()
    return [str(case["name"]) for case in manifest.get("cases", [])]


@pytest.fixture()
def pdf_md_mod():
    return importlib.import_module("LiuXin_alpha.metadata.file_sources.pdf")


@pytest.mark.parametrize("case_name", _case_ids())
def test_golden_pdf_corpus_scaffold(case_name: str, pdf_md_mod) -> None:
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
    manifest = _load_manifest()
    cases = manifest.get("cases", [])
    names = [str(case["name"]) for case in cases]
    paths = [str(case["path"]) for case in cases]
    assert len(names) == len(set(names))
    assert len(paths) == len(set(paths))
