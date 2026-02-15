"""Helpers for working with *zipped* Calibre fixture libraries.

The fixture libraries live in the separate `LiuXin_alpha_data` repository.
In a typical checkout, that data repo is present as:

    <repo_root>/LiuXin_alpha_data/

This module is test-only. It:

* discovers fixture zips + expected snapshots
* extracts them into a temp dir for tests
* normalizes path-ish fields so snapshots compare across OS and temp dirs

Notes
-----

The generator currently captures some path fields (e.g. drift contexts and
warning strings) as *absolute* paths. Those are not stable across machines.
The normalizer here strips any leading temp prefix and keeps the suffix from
`calibre_library/` onward, which is stable for our generated zips.
"""

from __future__ import annotations

import json
import os
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True, slots=True)
class CalibreFixtureSpec:
    schema_key: str
    name: str
    fixture_dir: Path
    library_zip: Path
    expected_json: Path

    def id(self) -> str:
        return f"{self.schema_key}/{self.name}"


def repo_root_from_here() -> Path:
    # tests/databases/<thisfile> -> repo_root
    return Path(__file__).resolve().parents[2]


def find_data_repo_root(repo_root: Optional[Path] = None) -> Optional[Path]:
    """Return the LiuXin_alpha_data root, or None if not present.

    Resolution order:

    1. $LIUXIN_ALPHA_DATA_ROOT (or legacy $LIUXIN_ALPHA_DATA_DIR)
    2. <repo_root>/LiuXin_alpha_data
    """

    rr = repo_root or repo_root_from_here()

    env = os.environ.get("LIUXIN_ALPHA_DATA_ROOT", "").strip() or os.environ.get("LIUXIN_ALPHA_DATA_DIR", "").strip()
    candidates: List[Path] = []
    if env:
        candidates.append(Path(env))
    candidates.append(rr / "LiuXin_alpha_data")

    for c in candidates:
        try:
            if c.exists() and (c / "calibre_libraries").exists():
                return c.resolve()
        except Exception:
            continue
    return None


def discover_calibre_fixtures(data_repo_root: Path) -> List[CalibreFixtureSpec]:
    root = data_repo_root / "calibre_libraries"
    if not root.exists():
        return []

    specs: List[CalibreFixtureSpec] = []

    for schema_dir in sorted(root.iterdir(), key=lambda p: p.name.casefold()):
        if not schema_dir.is_dir():
            continue
        if schema_dir.name.startswith(".") or schema_dir.name == "_build":
            continue
        if not schema_dir.name.startswith("uv"):
            continue

        for fixture_dir in sorted(schema_dir.iterdir(), key=lambda p: p.name.casefold()):
            if not fixture_dir.is_dir():
                continue
            if fixture_dir.name.startswith("_"):
                continue

            library_zip = fixture_dir / "library.zip"
            expected_json = fixture_dir / "expected.json"
            if not (library_zip.exists() and expected_json.exists()):
                continue

            specs.append(
                CalibreFixtureSpec(
                    schema_key=schema_dir.name,
                    name=fixture_dir.name,
                    fixture_dir=fixture_dir,
                    library_zip=library_zip,
                    expected_json=expected_json,
                )
            )

    return specs


def load_expected_snapshot(spec: CalibreFixtureSpec) -> Dict[str, Any]:
    return json.loads(spec.expected_json.read_text(encoding="utf-8"))


def extract_library_zip(spec: CalibreFixtureSpec, dst_dir: Path) -> Path:
    """Extract `library.zip` into dst_dir and return the extracted library root.

    The extracted root is discovered by locating `metadata.db`.
    """

    dst_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(spec.library_zip, "r") as zf:
        zf.extractall(dst_dir)

    md = next(dst_dir.rglob("metadata.db"), None)
    if md is None:
        raise RuntimeError(f"Fixture zip {spec.library_zip} did not contain metadata.db")
    return md.parent


def snapshot_calibre_library(library_root: Path) -> Dict[str, Any]:
    """Project a Calibre library into the same snapshot shape as `expected.json`."""

    from LiuXin_alpha.databases.calibre_emulation.readers import CalibreReader

    reader = CalibreReader.from_root(library_root)
    schema = reader.db.schema_info(best_effort=True)

    def _rel(root: Path, p: Path) -> str:
        try:
            return p.relative_to(root).as_posix()
        except Exception:
            return str(p).replace("\\", "/")

    books: List[Dict[str, Any]] = []
    for b in reader.iter_book_payloads(
        include_formats=True,
        include_files=False,
        include_cover_path=True,
        filesystem_reconcile=True,
        include_orphan_formats=False,
        best_effort=True,
    ):
        books.append(
            {
                "calibre_book_id": b.calibre_book_id,
                "title": b.title,
                "authors": list(b.authors),
                "tags": list(b.tags),
                "languages": list(b.languages),
                "identifiers": dict(b.identifiers),
                "series": None
                if b.series is None
                else {"name": b.series.name, "index": b.series.index},
                "comments_html": b.comments_html,
                "cover_path": None if b.cover_path is None else _rel(library_root, Path(b.cover_path)),
                "formats": [
                    {
                        "fmt": f.fmt,
                        "path": _rel(library_root, Path(f.file_path)),
                        "size_bytes": f.size_bytes,
                    }
                    for f in b.formats
                ],
                "custom": dict(b.custom_values),
                "warnings": list(b.warnings),
                "drift": [
                    {
                        "severity": d.severity,
                        "code": d.code,
                        "message": d.message,
                        "context": dict(d.context or {}),
                    }
                    for d in (b.drift_events or ())
                ],
            }
        )

    unique_authors = set()
    unique_tags = set()
    total_formats = 0
    total_drift = 0
    for b in books:
        for a in b.get("authors", []) or []:
            unique_authors.add(a)
        for t in b.get("tags", []) or []:
            unique_tags.add(t)
        total_formats += len(b.get("formats", []) or [])
        total_drift += len(b.get("drift", []) or [])

    return {
        "schema": {
            "application_id": schema.application_id,
            "user_version": schema.user_version,
            "has_fts": schema.has_fts,
            "has_notes": schema.has_notes,
            "custom_columns": [
                {
                    "num": c.num,
                    "label": c.label,
                    "datatype": c.datatype,
                    "is_multiple": c.is_multiple,
                    "normalized": c.normalized,
                    "value_table": c.value_table,
                    "link_table": c.link_table,
                }
                for c in schema.custom_columns
            ],
            "issues": [
                {
                    "severity": i.severity,
                    "code": i.code,
                    "message": i.message,
                    "context": dict(i.context or {}),
                }
                for i in schema.issues
            ],
            "version_plan": None
            if schema.version_plan is None
            else {
                "status": schema.version_plan.status,
                "action": schema.version_plan.action,
                "warnings": list(schema.version_plan.warnings),
                "expected_application_id": schema.version_plan.expected_application_id,
                "latest_supported_user_version": schema.version_plan.latest_supported_user_version,
            },
        },
        "counts": {
            "books": len(books),
            "formats_total": int(total_formats),
            "authors_unique": int(len(unique_authors)),
            "tags_unique": int(len(unique_tags)),
            "custom_columns": int(len(schema.custom_columns)),
            "drift_events_total": int(total_drift),
        },
        "books": books,
    }


def normalize_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize snapshot dicts for cross-platform comparisons."""

    import copy

    out: Dict[str, Any] = copy.deepcopy(snapshot)

    # Drop non-structural fields if present.
    out.pop("fixture", None)

    # Normalize schema issue contexts.
    schema = out.get("schema")
    if isinstance(schema, dict):
        issues = schema.get("issues")
        if isinstance(issues, list):
            for i in issues:
                if isinstance(i, dict):
                    _normalize_context_dict(i.get("context"))

    books = out.get("books")
    if isinstance(books, list):
        for b in books:
            if not isinstance(b, dict):
                continue

            # Normalize warning strings.
            ws = b.get("warnings")
            if isinstance(ws, list):
                b["warnings"] = [_normalize_warning(w) for w in ws]

            # Normalize drift contexts.
            drift = b.get("drift")
            if isinstance(drift, list):
                for d in drift:
                    if isinstance(d, dict):
                        _normalize_context_dict(d.get("context"))

    return out


def _normalize_warning(w: Any) -> Any:
    if not isinstance(w, str):
        return w

    s = w.replace("\\", "/")
    idx = s.lower().find("calibre_library/")
    if idx == -1:
        return s

    # Keep only the warning code prefix (up to the first ':'), but strip any
    # unstable temp components.
    prefix = s.split(":", 1)[0] + ":" if ":" in s else ""
    suffix = s[idx:]
    return prefix + suffix


def _normalize_context_dict(ctx: Any) -> None:
    if not isinstance(ctx, dict):
        return

    for k, v in list(ctx.items()):
        if not isinstance(v, str):
            continue
        s = v.replace("\\", "/")
        idx = s.lower().find("calibre_library/")
        if idx != -1:
            ctx[k] = s[idx:]
        else:
            ctx[k] = s


def fixture_ids(specs: Iterable[CalibreFixtureSpec]) -> List[str]:
    return [s.id() for s in specs]
