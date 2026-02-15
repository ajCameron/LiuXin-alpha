#!/usr/bin/env python3
"""Generate a small corpus of *realistic* Calibre library fixtures.

This script is intended to populate the separate data repository:

    ./LiuXin_alpha_data/

with zipped Calibre libraries (metadata.db + book folders) that stress the
calibre_emulation reader/import stack.

It tries hard to:
- Keep fixtures small enough to commit.
- Exercise nasty corners (custom columns, drift, unicode paths, mangled schema).
- Emit a JSON snapshot of what LiuXin's CalibreReader observes.

Run from the main repo root:

    python scripts/generate_calibre_fixture_libraries.py

Or specify an explicit output directory:

    python scripts/generate_calibre_fixture_libraries.py --out /path/to/LiuXin_alpha_data

Note: The data repo is optional; if it can't be found automatically, the script
will error with a clear message.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


# -------------------------------------------------------------------------------------------------
# Path discovery / imports
# -------------------------------------------------------------------------------------------------


def _find_repo_root(start: Path) -> Optional[Path]:
    start = start.resolve()
    for p in [start] + list(start.parents):
        if (p / "src" / "LiuXin_alpha").is_dir() and (p / "tests").is_dir():
            return p
    return None


def _ensure_importable(repo_root: Path) -> None:
    # Make local sources importable when running as a script.
    root_str = str(repo_root)
    src_str = str(repo_root / "src")
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


def _resolve_data_repo_root(repo_root: Path, explicit: Optional[Path]) -> Path:
    if explicit is not None:
        p = Path(explicit).expanduser()
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        if not p.is_dir():
            raise SystemExit(f"--out does not exist or is not a directory: {p}")
        return p

    env = os.environ.get("LIUXIN_ALPHA_DATA_DIR")
    if env:
        p = Path(env).expanduser()
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        if p.is_dir():
            return p

    p1 = repo_root / "LiuXin_alpha_data"
    if p1.is_dir():
        return p1
    p2 = repo_root.parent / "LiuXin_alpha_data"
    if p2.is_dir():
        return p2

    raise SystemExit(
        "Could not locate LiuXin_alpha_data. "
        "Set LIUXIN_ALPHA_DATA_DIR or pass --out /path/to/LiuXin_alpha_data."
    )


def _resolve_md_corpus_dir(data_repo_root: Path) -> Optional[Path]:
    for name in ("md_test_files", "md_test_books"):
        p = data_repo_root / name
        if p.is_dir():
            return p
    return None


# -------------------------------------------------------------------------------------------------
# Fixture model
# -------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class FixtureSpec:
    name: str
    description: str
    notes_db: bool = False
    fts_db: bool = False


FIXTURES: Tuple[FixtureSpec, ...] = (
    FixtureSpec(
        name="01_minimal",
        description="One book, one author, one EPUB, no custom columns populated.",
    ),
    FixtureSpec(
        name="02_customs_stress",
        description="Custom columns across datatypes (single/multi, series, enum, datetime, etc.).",
    ),
    FixtureSpec(
        name="03_filesystem_drift",
        description="Case mismatch folder, missing referenced format, duplicates, orphan file, missing cover.",
    ),
    FixtureSpec(
        name="04_unicode_chaos",
        description="Unicode titles/authors/tags, RTL, emoji, combining marks, and unicode paths.",
    ),
    FixtureSpec(
        name="05_mangled_optional_tables",
        description="Drops optional tables (tags/series/comments/identifiers/languages) but keeps core tables.",
    ),
    FixtureSpec(
        name="06_schema_drift_pragmas",
        description="Tweaks PRAGMA application_id/user_version to trigger version-policy warnings.",
    ),
)


# -------------------------------------------------------------------------------------------------
# Small helpers
# -------------------------------------------------------------------------------------------------


def _read_any_file_bytes(corpus_dir: Path, *, prefer_exts: Sequence[str]) -> bytes:
    # Pick the first matching file for a given extension preference.
    # This keeps the script robust even if the corpus contents change.
    by_ext: Dict[str, List[Path]] = {}
    for p in corpus_dir.rglob("*"):
        if p.is_dir() or p.name.startswith("."):
            continue
        ext = p.suffix.lower().lstrip(".")
        by_ext.setdefault(ext, []).append(p)
    for ext, ps in by_ext.items():
        ps.sort(key=lambda x: x.name.casefold())

    for ext in prefer_exts:
        ps = by_ext.get(ext.lower())
        if ps:
            return ps[0].read_bytes()

    # Fall back to *any* file.
    all_files: List[Path] = []
    for ps in by_ext.values():
        all_files.extend(ps)
    if not all_files:
        raise SystemExit(f"Corpus dir appears empty: {corpus_dir}")
    all_files.sort(key=lambda x: x.name.casefold())
    return all_files[0].read_bytes()


def _default_cover_bytes(repo_root: Path) -> bytes:
    p = repo_root / "LiuXin_resources" / "calibre_resources" / "catalog" / "DefaultCover.jpg"
    if p.exists():
        return p.read_bytes()
    # fallback: tiny jpeg header-ish bytes (tests should treat as opaque)
    return b"\xff\xd8\xff\xe0" + b"J" * 128 + b"\xff\xd9"


def _write_opf(
    opf_path: Path,
    *,
    title: str,
    authors: Sequence[str],
    tags: Sequence[str] = (),
    languages: Sequence[str] = ("eng",),
    identifiers: Mapping[str, str] = (),
    comments_html: Optional[str] = None,
    series: Optional[Tuple[str, Optional[float]]] = None,
    user_metadata: Optional[dict] = None,
) -> None:
    # A deliberately small OPF that our robust sidecar parser can read.
    # Namespace prefixes are intentionally minimal.
    from xml.sax.saxutils import escape

    def e(s: str) -> str:
        return escape(str(s), entities={"\"": "&quot;"})

    meta_lines: List[str] = []
    if series is not None:
        name, idx = series
        meta_lines.append(f'<meta name="calibre:series" content="{e(name)}"/>')
        if idx is not None:
            meta_lines.append(f'<meta name="calibre:series_index" content="{e(idx)}"/>')
    if user_metadata is not None:
        # OPF2-style per-field payloads.
        for k, v in user_metadata.items():
            try:
                js = json.dumps(v, ensure_ascii=False)
            except Exception:
                js = json.dumps(str(v), ensure_ascii=False)
            meta_lines.append(f'<meta name="calibre:user_metadata:{e(k)}" content="{e(js)}"/>')

    id_lines: List[str] = []
    if isinstance(identifiers, Mapping):
        for k, v in identifiers.items():
            id_lines.append(f'<dc:identifier opf:scheme="{e(k)}">{e(v)}</dc:identifier>')

    desc = "" if not comments_html else f"<dc:description>{e(comments_html)}</dc:description>"

    xml = """<?xml version="1.0" encoding="utf-8"?>
<package version="2.0" xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">
  <metadata>
    <dc:title>{title}</dc:title>
{creators}
{langs}
{subjects}
{idents}
    {desc}
{meta}
  </metadata>
</package>
"""
    creators = "\n".join(f"    <dc:creator>{e(a)}</dc:creator>" for a in authors)
    langs = "\n".join(f"    <dc:language>{e(l)}</dc:language>" for l in languages)
    subjects = "\n".join(f"    <dc:subject>{e(t)}</dc:subject>" for t in tags)
    idents = "\n".join(f"    {x}" for x in id_lines)
    meta = "\n".join(f"    {x}" for x in meta_lines)
    opf_path.write_text(
        xml.format(
            title=e(title),
            creators=creators,
            langs=langs,
            subjects=subjects,
            idents=idents,
            desc=desc,
            meta=meta,
        ),
        encoding="utf-8",
        errors="replace",
    )


def _zip_dir(src_dir: Path, dst_zip: Path) -> None:
    dst_zip.parent.mkdir(parents=True, exist_ok=True)
    if dst_zip.exists():
        dst_zip.unlink()

    with zipfile.ZipFile(dst_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        base = src_dir.parent
        files: List[Path] = []
        for p in src_dir.rglob("*"):
            if p.is_dir():
                continue
            files.append(p)
        files.sort(key=lambda x: str(x.relative_to(base)).casefold())
        for p in files:
            arcname = p.relative_to(base).as_posix()
            zf.write(p, arcname)


def _relpath(root: Path, p: Path) -> str:
    try:
        return p.relative_to(root).as_posix()
    except Exception:
        return str(p)


# -------------------------------------------------------------------------------------------------
# Fixture creation
# -------------------------------------------------------------------------------------------------


def _create_base_library(tmp_root: Path, *, name: str, notes_db: bool, fts_db: bool) -> Path:
    from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import (
        create_calibre_library_skeleton,
    )

    lib_root = tmp_root / name
    create_calibre_library_skeleton(
        lib_root,
        overwrite=True,
        validate=True,
        ensure_library_uuid=True,
        library_uuid="00000000-0000-0000-0000-000000000000",
        create_notes_db=notes_db,
        create_fts_db=fts_db,
        best_effort_aux_dbs=True,
    )
    return lib_root


def _snapshot_library(lib_root: Path) -> dict[str, Any]:
    from LiuXin_alpha.databases.calibre_emulation.readers import CalibreReader

    reader = CalibreReader.from_root(lib_root)
    schema = reader.db.schema_info(best_effort=True)

    books: List[dict[str, Any]] = []
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
                "formats": [
                    {
                        "fmt": f.fmt,
                        "path": _relpath(lib_root, Path(f.file_path)),
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
        },
        "books": books,
    }


def _populate_fixture(
    spec: FixtureSpec,
    *,
    repo_root: Path,
    lib_root: Path,
    md_corpus_dir: Path,
    cover_bytes: bytes,
) -> None:
    from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder

    builder = CalibreLibraryBuilder(lib_root)

    # Choose representative format bytes from the corpus.
    epub_bytes = _read_any_file_bytes(md_corpus_dir, prefer_exts=("epub", "txt", "pdf"))
    pdf_bytes = _read_any_file_bytes(md_corpus_dir, prefer_exts=("pdf", "epub", "txt"))
    mobi_bytes = _read_any_file_bytes(md_corpus_dir, prefer_exts=("mobi", "azw3", "epub", "txt"))

    if spec.name == "01_minimal":
        b = builder.add_book(
            title="Minimal Fixture",
            authors=("Ada Example",),
            tags=("fixture", "minimal"),
            identifiers={"isbn": "9780000000002"},
            comments_html="<p>Minimal library fixture.</p>",
            formats={"EPUB": epub_bytes},
            cover_bytes=cover_bytes,
        )
        _write_opf(
            b.folder_path / "metadata.opf",
            title=b.title,
            authors=b.authors,
            tags=("fixture", "minimal"),
            identifiers={"isbn": "9780000000002"},
            comments_html="Minimal library fixture.",
        )
        return

    if spec.name == "02_customs_stress":
        # Create a representative set of custom columns.
        builder.create_custom_column(label="txt1", name="Text One", datatype="text")
        builder.create_custom_column(label="txtm", name="Text Multi", datatype="text", is_multiple=True)
        builder.create_custom_column(label="flag", name="Flag", datatype="bool")
        builder.create_custom_column(label="count", name="Count", datatype="int")
        builder.create_custom_column(label="ratio", name="Ratio", datatype="float")
        builder.create_custom_column(label="when", name="When", datatype="datetime")
        builder.create_custom_column(label="serx", name="Series X", datatype="series")
        builder.create_custom_column(label="enum", name="Enum", datatype="enumeration", display={"enum_values": ["red", "green", "blue"]})
        builder.create_custom_column(label="rate", name="Rate", datatype="rating")
        builder.create_custom_column(label="comp", name="Composite", datatype="composite")

        custom = {
            "txt1": "hello",
            "txtm": ["alpha", "beta", "gamma"],
            "flag": True,
            "count": 42,
            "ratio": 0.125,
            "when": "2020-01-02 03:04:05+00:00",
            "serx": ("Custom Series", 2.5),
            "enum": "blue",
            "rate": 7,
            "comp": "precomputed:42",
        }

        b = builder.add_book(
            title="Custom Columns Fixture",
            authors=("Beatrice Builder", "Chris Column"),
            languages=("eng", "fra"),
            tags=("fixture", "custom", "stress"),
            series=("Built-in Series", 1.0),
            publisher="The Test Press",
            identifiers={"doi": "10.0000/example"},
            comments_html="<p>Custom columns stress fixture.</p>",
            formats={"EPUB": epub_bytes, "PDF": pdf_bytes},
            cover_bytes=cover_bytes,
            custom_values=custom,
        )
        _write_opf(
            b.folder_path / "metadata.opf",
            title=b.title,
            authors=b.authors,
            tags=("fixture", "custom", "stress"),
            languages=("eng", "fra"),
            identifiers={"doi": "10.0000/example"},
            comments_html="Custom columns stress fixture.",
            series=("Built-in Series", 1.0),
            user_metadata={
                "#txt1": {"#value#": "hello"},
                "#txtm": {"#value#": ["alpha", "beta", "gamma"]},
            },
        )
        return

    if spec.name == "03_filesystem_drift":
        b = builder.add_book(
            title="Filesystem Drift Fixture",
            authors=("Case Sensitive",),
            tags=("fixture", "drift"),
            formats={"EPUB": epub_bytes, "MOBI": mobi_bytes},
            cover_bytes=cover_bytes,
        )
        # Add OPF.
        _write_opf(
            b.folder_path / "metadata.opf",
            title=b.title,
            authors=b.authors,
            tags=("fixture", "drift"),
        )

        # 1) Case-mismatch: rename top author folder to different case.
        author_dir = b.folder_path.parent
        alt_author_dir = author_dir.parent / (author_dir.name.swapcase() or (author_dir.name + "_alt"))
        if not alt_author_dir.exists():
            author_dir.rename(alt_author_dir)
            # book folder path changed; books.path now points to missing-case path.

        # 2) Missing referenced format: delete the EPUB file.
        for fr in b.formats.values():
            if fr.format.upper() == "EPUB":
                try:
                    Path(fr.file_path).unlink()
                except Exception:
                    pass

        # 3) Duplicate format files: create two MOBI files with different mtimes.
        mobi_paths = []
        for fr in b.formats.values():
            if fr.format.upper() == "MOBI":
                mobi_paths.append(Path(fr.file_path))
        if mobi_paths:
            p0 = mobi_paths[0]
            dup = p0.with_name(p0.stem + "_copy" + p0.suffix)
            dup.write_bytes(p0.read_bytes())
            os.utime(dup, None)

        # 4) Orphan file: add an extra file not referenced by DB.
        # Place it in the *actual* folder.
        real_folder = alt_author_dir / b.folder_path.name
        (real_folder / "orphan.bin").write_bytes(b"orphan")

        # 5) Missing cover: set has_cover=1 but remove cover.jpg.
        cover = real_folder / "cover.jpg"
        if cover.exists():
            cover.unlink()
        # IMPORTANT: Calibre's metadata.db schema includes triggers that call
        # custom SQL functions (e.g. title_sort, uuid4). Use the builder's
        # connection helper so those minimal UDFs are registered.
        conn = builder.connect()
        try:
            conn.execute("UPDATE books SET has_cover=1 WHERE id=?", (b.book_id,))
            conn.commit()
        finally:
            conn.close()
        return

    if spec.name == "04_unicode_chaos":
        title = "Zo\u00eb\u2019s Caf\u00e9 \u2615\ufe0f \u2014 \u05e9\u05dc\u05d5\u05dd / \u0627\u0644\u0633\u0644\u0627\u0645 \u2014 e\u0301"
        authors = ("\u0627\u0644\u064a\u0651", "\u05d3\u05d1\u05d5\u05e8\u05d4 \U0001f680")
        tags = ("unic\u00f4de", "\u5de8\u05db\u05d1\u05d5\u05ea", "\U0001f4da")
        b = builder.add_book(
            title=title,
            authors=authors,
            languages=("ara", "heb", "eng"),
            tags=tags,
            series=("\u0421\u0435\u0440\u0438\u044f", 3.0),
            identifiers={"uuid": "urn:uuid:00000000-0000-0000-0000-000000000001"},
            comments_html="<p>Unicode fixture \U0001f4a5 with combining marks.</p>",
            formats={"EPUB": epub_bytes},
            cover_bytes=cover_bytes,
        )
        _write_opf(
            b.folder_path / "metadata.opf",
            title=title,
            authors=authors,
            tags=tags,
            languages=("ara", "heb", "eng"),
            identifiers={"uuid": "urn:uuid:00000000-0000-0000-0000-000000000001"},
            comments_html="Unicode fixture with combining marks.",
            series=("\u0421\u0435\u0440\u0438\u044f", 3.0),
        )
        return

    if spec.name == "05_mangled_optional_tables":
        # Build one normal book first.
        b = builder.add_book(
            title="Mangled Optional Tables Fixture",
            authors=("Schema Breaker",),
            tags=("fixture", "mangled"),
            formats={"EPUB": epub_bytes},
            cover_bytes=cover_bytes,
        )
        _write_opf(
            b.folder_path / "metadata.opf",
            title=b.title,
            authors=b.authors,
            tags=("fixture", "mangled"),
        )
        # Now drop tables that the reader treats as optional.
        conn = builder.connect()
        try:
            for t in ("tags", "books_tags_link", "series", "books_series_link", "comments", "identifiers", "languages", "books_languages_link"):
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {t}")
                except Exception:
                    pass
            conn.commit()
        finally:
            conn.close()
        return

    if spec.name == "06_schema_drift_pragmas":
        b = builder.add_book(
            title="Schema Drift Pragmas Fixture",
            authors=("Version Wanderer",),
            tags=("fixture", "schema"),
            formats={"EPUB": epub_bytes},
            cover_bytes=cover_bytes,
        )
        _write_opf(
            b.folder_path / "metadata.opf",
            title=b.title,
            authors=b.authors,
            tags=("fixture", "schema"),
        )
        conn = builder.connect()
        try:
            # Intentionally drift: mismatched application_id + slightly newer user_version.
            conn.execute("PRAGMA application_id = 0")
            # Increment user_version, but keep it plausible.
            uv = int(conn.execute("PRAGMA user_version").fetchone()[0])
            conn.execute(f"PRAGMA user_version = {uv + 1}")
            conn.commit()
        finally:
            conn.close()
        return

    raise ValueError(f"Unknown fixture: {spec.name}")


def generate_all(
    *,
    repo_root: Path,
    data_repo_root: Path,
    fixtures: Sequence[str] | None,
    clean: bool,
) -> Path:
    from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator.database_generator import (
        calibre_metadata_schema_info,
    )

    info = calibre_metadata_schema_info()
    out_root = data_repo_root / "calibre_libraries" / f"uv{info.user_version}_{info.sha256[:10]}"
    out_root.mkdir(parents=True, exist_ok=True)
    (out_root / "_build").mkdir(parents=True, exist_ok=True)

    md_dir = _resolve_md_corpus_dir(data_repo_root)
    if md_dir is None:
        raise SystemExit(
            "md_test_books/ (or md_test_files/) not found in LiuXin_alpha_data. "
            "These are used as realistic format payloads for the Calibre fixtures."
        )

    cover_bytes = _default_cover_bytes(repo_root)

    selected = {s.strip() for s in (fixtures or []) if s.strip()}
    specs = [s for s in FIXTURES if (not selected or s.name in selected)]
    if not specs:
        raise SystemExit(f"No fixtures selected. Available: {[s.name for s in FIXTURES]}")

    for spec in specs:
        fixture_dir = out_root / spec.name
        if clean and fixture_dir.exists():
            shutil.rmtree(fixture_dir)
        fixture_dir.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix=f"liuxin_calibre_fixture_{spec.name}_") as td:
            td_path = Path(td)
            lib_root = _create_base_library(td_path, name="calibre_library", notes_db=spec.notes_db, fts_db=spec.fts_db)
            _populate_fixture(spec, repo_root=repo_root, lib_root=lib_root, md_corpus_dir=md_dir, cover_bytes=cover_bytes)

            # Snapshot what the reader sees.
            snap = _snapshot_library(lib_root)
            snap.update({"fixture": {"name": spec.name, "description": spec.description}})
            (fixture_dir / "expected.json").write_text(
                json.dumps(snap, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            # Zip the whole library folder.
            _zip_dir(lib_root, fixture_dir / "library.zip")

    # Ensure a README exists to explain the layout.
    readme = out_root.parent / "README.md"
    if not readme.exists():
        readme.write_text(
            """# Calibre fixture libraries

This folder contains **zipped Calibre libraries** used by LiuXin-alpha tests.

Layout
------

- `uv<user_version>_<schemahash>/` groups fixtures by Calibre schema snapshot
- Each fixture folder contains:
  - `library.zip` (unzip to get a Calibre library root with `metadata.db`)
  - `expected.json` (snapshot of what LiuXin's CalibreReader observed)

Regenerate
----------

Run from the main repo:

    python scripts/generate_calibre_fixture_libraries.py
""",
            encoding="utf-8",
        )
    return out_root


# -------------------------------------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Generate Calibre library fixtures into LiuXin_alpha_data.")
    ap.add_argument(
        "--out",
        type=str,
        default=None,
        help="Path to LiuXin_alpha_data repo root (defaults to auto-discovery or LIUXIN_ALPHA_DATA_DIR).",
    )
    ap.add_argument(
        "--fixtures",
        type=str,
        default=None,
        help="Comma-separated fixture names to generate (default: all).",
    )
    ap.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing fixture directories before writing.",
    )

    ns = ap.parse_args(argv)

    here = Path(__file__).resolve()
    repo_root = _find_repo_root(here)
    if repo_root is None:
        raise SystemExit("Could not locate LiuXin-alpha repo root (expected src/LiuXin_alpha + tests/).")
    _ensure_importable(repo_root)

    data_root = _resolve_data_repo_root(repo_root, Path(ns.out) if ns.out else None)
    fixture_list = [x.strip() for x in (ns.fixtures.split(",") if ns.fixtures else []) if x.strip()]

    out_root = generate_all(repo_root=repo_root, data_repo_root=data_root, fixtures=fixture_list or None, clean=bool(ns.clean))
    print(f"Wrote fixtures under: {out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
