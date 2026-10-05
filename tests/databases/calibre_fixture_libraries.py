"""
Discover, extract and compare zipped Calibre libraries from the data checkout.

Fixtures live under LiuXin_alpha_data/calibre_libraries/uv*/<name>, each with
library.zip and expected.json. Archives are trusted test data and extraction writes
beneath a caller-supplied destination. Snapshot normalization deep-copies input but
retains the context helper's documented scalar limitation.
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
    """
    Describe one zipped library and its expected snapshot.

    Frozen, slotted fields hold schema/name labels and paths without checking that files
    exist. The ID is a slash-separated test label, not a filesystem path.

    Example:
        >>> p = Path('/fixtures/uv1/small')
        >>> spec = CalibreFixtureSpec('uv1', 'small', p, p / 'library.zip', p / 'expected.json')
        >>> spec.id()
        'uv1/small'
    """
    schema_key: str
    name: str
    fixture_dir: Path
    library_zip: Path
    expected_json: Path

    def id(self) -> str:
        """
        Combine the schema key and fixture name into a pytest display ID.

        Example:
            >>> p = Path('.')
            >>> CalibreFixtureSpec('uv1', 'tiny', p, p, p).id()
            'uv1/tiny'


        :return: Schema key and fixture name joined by one slash.
        """
        return f"{self.schema_key}/{self.name}"


def repo_root_from_here() -> Path:
    # tests/databases/<thisfile> -> repo_root
    """
    Locate the checkout by walking two parent levels above this module.

    Resolves symlinks first and assumes the tests/databases layout; it does not search
    for a Git marker.

    Example:
        >>> repo_root_from_here() == Path(__file__).resolve().parents[2]
        True


    :return: Resolved repository root Path.
    """
    return Path(__file__).resolve().parents[2]


def find_data_repo_root(repo_root: Optional[Path] = None) -> Optional[Path]:
    """
    Find a data checkout containing a calibre_libraries entry.

    Tries the stripped LIUXIN_ALPHA_DATA_ROOT value, or legacy LIUXIN_ALPHA_DATA_DIR
    only when the first is empty, then the checkout-local LiuXin_alpha_data path.
    Candidate filesystem errors are ignored. Merely checks existence, not directory type
    or fixture completeness.

    Example:
        >>> from tempfile import TemporaryDirectory
        >>> from unittest.mock import patch
        >>> with TemporaryDirectory() as tmp, patch.dict(os.environ, {'LIUXIN_ALPHA_DATA_ROOT': '', 'LIUXIN_ALPHA_DATA_DIR': ''}):
        ...     root = Path(tmp)
        ...     (root / 'LiuXin_alpha_data' / 'calibre_libraries').mkdir(parents=True)
        ...     found = find_data_repo_root(root)
        ...     print(found == (root / 'LiuXin_alpha_data').resolve())
        True


    :param repo_root: Checkout root override; None uses the location of this helper
        module.
    :return: First resolved matching candidate, or None.
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
    """
    List complete fixture pairs in case-insensitive schema/name order.

    Requires schema directories beginning uv and skips hidden schemas and _build. Skips
    fixture directories beginning underscore, and accepts only entries with both
    library.zip and expected.json present. Does not inspect archive or JSON content.
    Missing calibre_libraries yields an empty list; other traversal errors propagate.

    Example:
        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory() as tmp:
        ...     print(discover_calibre_fixtures(Path(tmp)))
        []


    :param data_repo_root: Data checkout containing the calibre_libraries directory.
    :return: Ordered list of fixture specifications.
    """
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
    """
    Decode a fixture's expected.json as UTF-8 JSON.

    Filesystem and JSON errors propagate; the decoded root is not validated against the
    dict annotation.

    Example:
        >>> expected = load_expected_snapshot(spec)  # doctest: +SKIP


    :param spec: Fixture specification naming the expected JSON file.
    :return: Decoded JSON value, normally a snapshot dict.
    """
    return json.loads(spec.expected_json.read_text(encoding="utf-8"))


def extract_library_zip(spec: CalibreFixtureSpec, dst_dir: Path) -> Path:
    """
    Extract a trusted fixture archive and find its metadata.db parent.

    Creates the destination and may overwrite existing extracted files. Returns the
    parent of the first recursive metadata.db match, without requiring uniqueness.
    Raises RuntimeError if none is found; archive/filesystem errors propagate and
    extracted files remain caller-owned.

    Example:
        >>> library_root = extract_library_zip(spec, tmp_path)  # doctest: +SKIP


    :param spec: Specification naming the trusted library archive.
    :param dst_dir: Extraction destination; created with missing parents when needed.
    :return: Extracted library root containing the first discovered metadata.db.
    """

    dst_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(spec.library_zip, "r") as zf:
        zf.extractall(dst_dir)

    md = next(dst_dir.rglob("metadata.db"), None)
    if md is None:
        raise RuntimeError(f"Fixture zip {spec.library_zip} did not contain metadata.db")
    return md.parent


def snapshot_calibre_library(library_root: Path) -> Dict[str, Any]:
    """
    Project a Calibre library into schema, counts and book snapshot sections.

    Uses best-effort schema/payload reading with formats, covers and filesystem
    reconciliation enabled, excluding orphan formats. Paths inside the library become
    relative POSIX strings; outside paths retain their location with normalized
    separators. Includes custom columns, version-plan details, warnings and drift
    contexts. Reads the library and filesystem; does not normalize machine-specific
    warning/context prefixes here.

    Example:
        >>> snapshot = snapshot_calibre_library(library_root)  # doctest: +SKIP
        >>> comparable = normalize_snapshot(snapshot)  # doctest: +SKIP


    :param library_root: Existing Calibre library root to read.
    :return: Snapshot dict with schema details, aggregate counts and book payload
        projections.
    """

    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import CalibreReader

    reader = CalibreReader.from_root(library_root)
    schema = reader.db.schema_info(best_effort=True)

    def _rel(root: Path, p: Path) -> str:
        """
        Express a path relative to the library when possible, otherwise retain its location.

        Any relative_to failure falls back to the original string with backslashes replaced
        by forward slashes; no path resolution is performed.

        Example:
            >>> _rel(Path('/library'), Path('/library/book.epub'))  # doctest: +SKIP
            'book.epub'


        :param root: Library root used for lexical relative-path conversion.
        :param p: Payload path to display.
        :return: Relative POSIX path or separator-normalized original path.
        """
        try:
            return p.relative_to(root).as_posix()
        except Exception:
            return str(p).replace("\\", "/")

    books: List[Dict[str, Any]] = []
    for b in reader.iter_book_payloads(
        include_formats=True,
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
    """
    Copy a snapshot and normalize selected machine-dependent path fields.

    Drops the top-level fixture field, processes schema issue contexts, book warnings
    and drift contexts, and leaves the original untouched. Delegates context handling to
    _normalize_context_dict, including its existing non-string scalar bug; other fields
    are retained without general recursive normalization.

    Example:
        >>> original = {'fixture': 'local', 'books': [{'warnings': ['missing:/tmp/run/calibre_library/a.epub']}]}
        >>> normalized = normalize_snapshot(original)
        >>> normalized['books'][0]['warnings']
        ['missing:calibre_library/a.epub']
        >>> 'fixture' in original, 'fixture' in normalized
        (True, False)


    :param snapshot: Snapshot mapping to copy and compare across extraction directories.
    :return: Deep-copied snapshot with the selected fields normalized.
    """

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
    """
    Normalize warning separators and trim prefixes before calibre_library/.

    Searches for the anchor case-insensitively but preserves its original spelling.
    Retains everything before the first colon as a warning-code prefix, even when that
    colon belongs to a drive path. Non-string inputs pass through unchanged.

    Example:
        >>> _normalize_warning('missing:/tmp/a/calibre_library/book.epub')
        'missing:calibre_library/book.epub'
        >>> _normalize_warning(None) is None
        True


    :param w: Warning value to normalize.
    :return: Normalized string, or the original non-string value.
    """
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
    """
    Rewrite selected path-like context values in place.

    Non-dicts are ignored. String values are rewritten only when a calibre_library/
    anchor is found; unanchored strings remain unchanged, including their separators.
    List strings always get separator normalization and optional prefix trimming; other
    list items remain unchanged. A non-string, non-list scalar is incorrectly assigned
    the last local string, or raises UnboundLocalError when no such string exists. This
    helper is therefore not a general context normalizer.

    Example:
        >>> context = {'path': '/tmp/run/calibre_library/a', 'items': ['/tmp/calibre_library/b', 7]}
        >>> _normalize_context_dict(context)
        >>> context
        {'path': 'calibre_library/a', 'items': ['calibre_library/b', 7]}
        >>> try:
        ...     _normalize_context_dict({'count': 1})
        ... except UnboundLocalError:
        ...     print('unsupported scalar context')
        unsupported scalar context


    :param ctx: Context value; supported path fields are strings and lists.
    :return: None; mutates a dict argument and may fail after partial updates.
    """
    if not isinstance(ctx, dict):
        return

    for k, v in list(ctx.items()):
        if isinstance(v, str):
            s = v.replace("\\", "/")
            idx = s.lower().find("calibre_library/")
            if idx != -1:
                ctx[k] = s[idx:]
            continue

        if isinstance(v, list):
            normalized = []
            for item in v:
                if isinstance(item, str):
                    s = item.replace("\\", "/")
                    idx = s.lower().find("calibre_library/")
                    normalized.append(s[idx:] if idx != -1 else s)
                else:
                    normalized.append(item)
            ctx[k] = normalized
        else:
            ctx[k] = s


def fixture_ids(specs: Iterable[CalibreFixtureSpec]) -> List[str]:
    """
    Collect display IDs from fixture specifications in iteration order.

    Example:
        >>> p = Path('.')
        >>> fixture_ids([CalibreFixtureSpec('uv1', 'tiny', p, p, p)])
        ['uv1/tiny']


    :param specs: Iterable of specifications, consumed once.
    :return: List of schema/name strings, retaining duplicates.
    """
    return [s.id() for s in specs]
