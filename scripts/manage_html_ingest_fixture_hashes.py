#!/usr/bin/env python3
"""
Manage hash baseline for HTML ingest fixtures.

This script operates on:
  - fixture dir: tests/fixtures/html_ingest
  - manifest:    tests/support/html_ingest_fixture_hashes.py

Typical usage:
  - Revalidate current baseline:
      python scripts/manage_html_ingest_fixture_hashes.py --revalidate

  - Add/update one newly-added fixture:
      python scripts/manage_html_ingest_fixture_hashes.py --add html_ingest_case_007_new.html

  - Do both:
      python scripts/manage_html_ingest_fixture_hashes.py --add x.html --revalidate
"""

from __future__ import annotations

import argparse
import hashlib
import runpy
import sys
from pathlib import Path
from typing import Iterable

DEFAULT_MANIFEST_REL = Path("tests/support/html_ingest_fixture_hashes.py")
DEFAULT_FIXTURES_REL = Path("tests/fixtures/html_ingest")


def find_repo_root(start: Path) -> Path:
    start = start.resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "src" / "LiuXin_alpha").is_dir() and (candidate / "tests").is_dir():
            return candidate
    return start


def resolve_fixtures_dir(repo_root: Path, explicit_dir: str | None) -> Path:
    if explicit_dir:
        p = Path(explicit_dir).expanduser()
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        if p.is_dir():
            return p
        raise FileNotFoundError(f"--fixtures-dir does not exist: {p}")

    p = (repo_root / DEFAULT_FIXTURES_REL).resolve()
    if p.is_dir():
        return p
    raise FileNotFoundError(f"Fixture directory not found: {p}")


def legacy_sha512_size_hash(path: Path) -> str:
    hasher = hashlib.sha512()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest() + str(path.stat().st_size)


def load_manifest(path: Path) -> dict[str, str]:
    scope = runpy.run_path(str(path))
    data = scope.get("EXPECTED_HTML_INGEST_FIXTURE_HASHES")
    if not isinstance(data, dict):
        raise TypeError(f"{path} does not define EXPECTED_HTML_INGEST_FIXTURE_HASHES as a dict")

    out: dict[str, str] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise TypeError("EXPECTED_HTML_INGEST_FIXTURE_HASHES must be dict[str, str]")
        out[k] = v
    return out


def render_manifest(mapping: dict[str, str]) -> str:
    lines: list[str] = [
        "from __future__ import annotations\n",
        "\n",
        "import hashlib\n",
        "from pathlib import Path\n",
        "\n",
        "# Hash format is intentionally consistent with legacy fixture manifests:\n",
        "#   sha512(file_bytes).hexdigest() + str(file_size_in_bytes)\n",
        "EXPECTED_HTML_INGEST_FIXTURE_HASHES: dict[str, str] = {\n",
    ]
    for filename in sorted(mapping):
        lines.append(f'    "{filename}": "{mapping[filename]}",\n')
    lines.extend(
        [
            "}\n",
            "\n",
            "\n",
            "def legacy_sha512_size_hash(path: Path) -> str:\n",
            '    """Return the historical LiuXin file hash format used by fixture tests."""\n',
            "    hasher = hashlib.sha512()\n",
            '    with path.open(\"rb\") as stream:\n',
            "        while True:\n",
            "            chunk = stream.read(1024 * 1024)\n",
            "            if not chunk:\n",
            "                break\n",
            "            hasher.update(chunk)\n",
            "    return hasher.hexdigest() + str(path.stat().st_size)\n",
        ]
    )
    return "".join(lines)


def write_manifest(path: Path, mapping: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_manifest(mapping), encoding="utf-8")


def _is_within(base: Path, target: Path) -> bool:
    base = base.resolve()
    target = target.resolve()
    return target == base or base in target.parents


def resolve_add_target(raw: str, fixtures_dir: Path, repo_root: Path) -> Path:
    raw_path = Path(raw).expanduser()
    candidates: list[Path] = []

    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.append(fixtures_dir / raw_path)
        candidates.append(repo_root / raw_path)
        if raw_path.name != raw:
            candidates.append(fixtures_dir / raw_path.name)

    for candidate in candidates:
        if candidate.is_file():
            resolved = candidate.resolve()
            if not _is_within(fixtures_dir, resolved):
                raise ValueError(f"--add target must live under {fixtures_dir} (got {resolved})")
            return resolved

    attempted = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Could not resolve --add target '{raw}'. Tried: {attempted}")


def revalidate_existing_hashes(
    mapping: dict[str, str],
    fixtures_dir: Path,
    *,
    strict_set: bool = False,
) -> bool:
    missing: list[str] = []
    mismatches: list[tuple[str, str, str]] = []

    for filename, expected in sorted(mapping.items()):
        path = fixtures_dir / filename
        if not path.is_file():
            missing.append(filename)
            continue
        actual = legacy_sha512_size_hash(path)
        if actual != expected:
            mismatches.append((filename, expected, actual))

    actual_files = {p.name for p in fixtures_dir.iterdir() if p.is_file() and not p.name.startswith(".")}
    expected_files = set(mapping.keys())
    extras = sorted(actual_files - expected_files)

    if not missing and not mismatches:
        print(f"OK: validated {len(mapping)} tracked fixture hashes in {fixtures_dir}")
    if extras:
        message = f"Untracked fixture files ({len(extras)}): {extras}"
        if strict_set:
            print("ERROR:", message)
        else:
            print("WARN:", message)

    if missing:
        print(f"ERROR: missing fixture files ({len(missing)}): {missing}")
    for filename, expected, actual in mismatches:
        print(f"ERROR: hash mismatch for {filename}")
        print(f"  expected: {expected}")
        print(f"  actual:   {actual}")

    return not missing and not mismatches and (not strict_set or not extras)


def add_or_update_hash_entries(
    mapping: dict[str, str],
    add_targets: Iterable[str],
    *,
    fixtures_dir: Path,
    repo_root: Path,
) -> bool:
    changed = False
    for raw in add_targets:
        path = resolve_add_target(raw, fixtures_dir=fixtures_dir, repo_root=repo_root)
        filename = path.name
        new_hash = legacy_sha512_size_hash(path)
        old_hash = mapping.get(filename)
        mapping[filename] = new_hash

        if old_hash is None:
            print(f"ADDED: {filename}")
            changed = True
        elif old_hash != new_hash:
            print(f"UPDATED: {filename}")
            changed = True
        else:
            print(f"UNCHANGED: {filename}")
    return changed


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage HTML ingest fixture hash manifest")
    parser.add_argument(
        "--add",
        action="append",
        metavar="FILE",
        help="Add/update hash entry for one fixture file (path or filename). Repeat for multiple files.",
    )
    parser.add_argument(
        "--revalidate",
        action="store_true",
        help="Revalidate existing hash entries against files on disk.",
    )
    parser.add_argument(
        "--strict-set",
        action="store_true",
        help="With --revalidate, also fail if extra untracked files are present in fixture dir.",
    )
    parser.add_argument(
        "--manifest",
        help=f"Path to manifest file (default: {DEFAULT_MANIFEST_REL.as_posix()})",
    )
    parser.add_argument(
        "--fixtures-dir",
        help=f"Path to fixture directory (default: {DEFAULT_FIXTURES_REL.as_posix()})",
    )
    parser.add_argument(
        "--repo-root",
        help="Path to LiuXin-alpha repo root (default: auto-detect from CWD).",
    )
    args = parser.parse_args(argv)

    if not args.add and not args.revalidate:
        parser.error("No action requested. Use --add and/or --revalidate.")

    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    repo_root = Path(args.repo_root).expanduser().resolve() if args.repo_root else find_repo_root(Path.cwd())
    manifest = Path(args.manifest).expanduser() if args.manifest else DEFAULT_MANIFEST_REL
    if not manifest.is_absolute():
        manifest = (repo_root / manifest).resolve()

    fixtures_dir = resolve_fixtures_dir(repo_root, args.fixtures_dir)

    if not manifest.is_file():
        raise FileNotFoundError(f"Manifest not found: {manifest}")
    mapping = load_manifest(manifest)

    if args.add:
        changed = add_or_update_hash_entries(
            mapping,
            args.add,
            fixtures_dir=fixtures_dir,
            repo_root=repo_root,
        )
        if changed:
            write_manifest(manifest, mapping)
            print(f"WROTE: {manifest}")
        else:
            print("No hash changes detected; manifest left untouched.")

    if args.revalidate:
        ok = revalidate_existing_hashes(mapping, fixtures_dir, strict_set=args.strict_set)
        if not ok:
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
