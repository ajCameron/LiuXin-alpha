#!/usr/bin/env python3
"""
Manage the OPF golden-corpus manifest.

Default locations:
  - fixtures dir: tests/fixtures/opf_golden
  - manifest:     tests/fixtures/opf_golden/manifest.json

Actions:
  - --verify: validate manifest entries against fixture files
  - --rebuild: recompute expected metadata + sha256 for all tracked entries
  - --add FILE: add/update one fixture entry (repeatable)

Examples:
  python scripts/manage_opf_golden_manifest.py --verify
  python scripts/manage_opf_golden_manifest.py --add fake_realworld_001.opf
  python scripts/manage_opf_golden_manifest.py --rebuild --verify
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any

DEFAULT_FIXTURE_DIR_REL = Path("tests/fixtures/opf_golden")
DEFAULT_MANIFEST_REL = DEFAULT_FIXTURE_DIR_REL / "manifest.json"


def find_repo_root(start: Path) -> Path:
    start = start.resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "src" / "LiuXin_alpha").is_dir() and (candidate / "tests").is_dir():
            return candidate
    return start


def _is_within(base: Path, target: Path) -> bool:
    base = base.resolve()
    target = target.resolve()
    return target == base or base in target.parents


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def load_manifest(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    cases = data.get("cases", [])
    if not isinstance(cases, list):
        raise TypeError(f"Manifest {path} has invalid 'cases' (expected list).")
    out: list[dict[str, Any]] = []
    for case in cases:
        if not isinstance(case, dict):
            raise TypeError("Every manifest case must be a JSON object.")
        out.append(case)
    return out


def _sorted_cases(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(cases, key=lambda c: str(c.get("name", "")).lower())


def write_manifest(path: Path, cases: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"cases": _sorted_cases(cases)}
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _ensure_src_on_path(repo_root: Path) -> None:
    src = (repo_root / "src").resolve()
    src_text = str(src)
    if src_text not in sys.path:
        sys.path.insert(0, src_text)


def load_opf_module(repo_root: Path):
    _ensure_src_on_path(repo_root)
    return importlib.import_module("LiuXin_alpha.file_formats.opf.opf")


def _authors_list(authors: Any) -> list[str]:
    if authors is None:
        return []
    if isinstance(authors, dict):
        return [str(x) for x in authors.keys()]
    if isinstance(authors, (list, tuple)):
        return [str(x) for x in authors]
    try:
        return [str(x) for x in list(authors)]
    except Exception:
        return [str(authors)]


def _series_index_value(raw: Any) -> Any:
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except Exception:
        return str(raw)


def extract_expected(opf_mod, opf_path: Path) -> dict[str, Any]:
    mi, ver, *_ = opf_mod.get_metadata(opf_path)
    return {
        "version_major": int(getattr(ver, "major", 0) or 0),
        "title": str(getattr(mi, "title", "") or ""),
        "authors": _authors_list(getattr(mi, "authors", None)),
        "series": None if getattr(mi, "series", None) in (None, "") else str(getattr(mi, "series")),
        "series_index": _series_index_value(getattr(mi, "series_index", None)),
    }


def normalize_case(case: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(case["name"]),
        "path": str(case["path"]),
        "sha256": str(case.get("sha256", "")),
        "expected": {
            "version_major": int((case.get("expected") or {}).get("version_major", 0)),
            "title": str((case.get("expected") or {}).get("title", "")),
            "authors": [str(x) for x in (case.get("expected") or {}).get("authors", [])],
            "series": (case.get("expected") or {}).get("series", None),
            "series_index": (case.get("expected") or {}).get("series_index", None),
        },
    }


def _expected_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if int(left.get("version_major", 0)) != int(right.get("version_major", 0)):
        return False
    if str(left.get("title", "")) != str(right.get("title", "")):
        return False
    if [str(x) for x in left.get("authors", [])] != [str(x) for x in right.get("authors", [])]:
        return False
    if left.get("series", None) != right.get("series", None):
        return False
    lsi = left.get("series_index", None)
    rsi = right.get("series_index", None)
    if lsi in (None, "") and rsi in (None, ""):
        return True
    try:
        return float(lsi) == float(rsi)
    except Exception:
        return lsi == rsi


def _resolve_target_path(raw: str, *, fixture_dir: Path, repo_root: Path) -> Path:
    p = Path(raw).expanduser()
    candidates: list[Path] = []
    if p.is_absolute():
        candidates.append(p)
    else:
        candidates.append(fixture_dir / p)
        candidates.append(repo_root / p)
        if p.name != raw:
            candidates.append(fixture_dir / p.name)
    for candidate in candidates:
        if candidate.is_file():
            resolved = candidate.resolve()
            if not _is_within(fixture_dir, resolved):
                raise ValueError(f"Fixture must be under {fixture_dir} (got {resolved})")
            return resolved
    attempted = ", ".join(str(c) for c in candidates)
    raise FileNotFoundError(f"Could not resolve fixture path: {raw}. Tried: {attempted}")


def _next_name(base_name: str, used_names: set[str]) -> str:
    if base_name not in used_names:
        return base_name
    i = 2
    while True:
        candidate = f"{base_name}_{i}"
        if candidate not in used_names:
            return candidate
        i += 1


def add_or_update_cases(
    cases: list[dict[str, Any]],
    add_targets: list[str],
    *,
    fixture_dir: Path,
    repo_root: Path,
    opf_mod,
) -> bool:
    changed = False
    by_path = {str(case.get("path", "")): case for case in cases}
    used_names = {str(case.get("name", "")) for case in cases}

    for raw in add_targets:
        path = _resolve_target_path(raw, fixture_dir=fixture_dir, repo_root=repo_root)
        rel_path = path.resolve().relative_to(fixture_dir.resolve()).as_posix()
        expected = extract_expected(opf_mod, path)
        digest = sha256_file(path)

        case = by_path.get(rel_path)
        if case is None:
            base_name = path.stem
            name = _next_name(base_name, used_names)
            used_names.add(name)
            case = {"name": name, "path": rel_path}
            cases.append(case)
            by_path[rel_path] = case
            action = "ADDED"
            changed = True
        else:
            action = "UPDATED"

        old_norm = normalize_case(case)
        case["sha256"] = digest
        case["expected"] = expected
        new_norm = normalize_case(case)
        if old_norm != new_norm:
            changed = True
            print(f"{action}: {case['name']} ({case['path']})")
        else:
            print(f"UNCHANGED: {case['name']} ({case['path']})")

    return changed


def rebuild_cases(cases: list[dict[str, Any]], *, fixture_dir: Path, opf_mod) -> bool:
    changed = False
    for case in cases:
        rel_path = str(case.get("path", ""))
        path = (fixture_dir / rel_path).resolve()
        if not path.is_file():
            print(f"WARN: missing fixture referenced by manifest: {rel_path}")
            continue
        old = normalize_case(case)
        case["sha256"] = sha256_file(path)
        case["expected"] = extract_expected(opf_mod, path)
        new = normalize_case(case)
        if old != new:
            changed = True
            print(f"REBUILT: {case.get('name', rel_path)}")
    return changed


def verify_cases(
    cases: list[dict[str, Any]],
    *,
    fixture_dir: Path,
    opf_mod,
    strict_set: bool = False,
) -> bool:
    ok = True

    seen_paths: set[str] = set()
    seen_names: set[str] = set()
    for case in cases:
        name = str(case.get("name", ""))
        rel_path = str(case.get("path", ""))
        if not name:
            print(f"ERROR: case missing name: {case}")
            ok = False
        if not rel_path:
            print(f"ERROR: case missing path: {case}")
            ok = False
            continue
        if name in seen_names:
            print(f"ERROR: duplicate case name: {name}")
            ok = False
        seen_names.add(name)
        if rel_path in seen_paths:
            print(f"ERROR: duplicate case path: {rel_path}")
            ok = False
        seen_paths.add(rel_path)

        path = (fixture_dir / rel_path).resolve()
        if not _is_within(fixture_dir, path):
            print(f"ERROR: fixture escapes fixture dir: {rel_path}")
            ok = False
            continue
        if not path.is_file():
            print(f"ERROR: missing fixture file: {rel_path}")
            ok = False
            continue

        actual_sha = sha256_file(path)
        expected_sha = str(case.get("sha256", "") or "")
        if expected_sha and expected_sha != actual_sha:
            print(f"ERROR: sha256 mismatch for {name}")
            print(f"  manifest: {expected_sha}")
            print(f"  actual:   {actual_sha}")
            ok = False

        expected = (case.get("expected") or {})
        if not isinstance(expected, dict):
            print(f"ERROR: expected block for {name} is not a dict")
            ok = False
            continue
        actual = extract_expected(opf_mod, path)
        if not _expected_equal(expected, actual):
            print(f"ERROR: expected metadata mismatch for {name}")
            print("  manifest:", json.dumps(expected, ensure_ascii=False, sort_keys=True))
            print("  actual:  ", json.dumps(actual, ensure_ascii=False, sort_keys=True))
            ok = False

    if strict_set:
        tracked = {str(case.get("path", "")) for case in cases}
        actual_files = {
            p.resolve().relative_to(fixture_dir.resolve()).as_posix()
            for p in fixture_dir.rglob("*.opf")
            if p.is_file()
        }
        extras = sorted(actual_files - tracked)
        if extras:
            print(f"ERROR: untracked OPF fixtures ({len(extras)}): {extras}")
            ok = False

    if ok:
        print(f"OK: validated {len(cases)} OPF golden cases in {fixture_dir}")
    return ok


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage OPF golden fixture manifest")
    parser.add_argument("--verify", action="store_true", help="Validate manifest against fixture files.")
    parser.add_argument("--rebuild", action="store_true", help="Recompute expected metadata + sha256 for tracked cases.")
    parser.add_argument(
        "--add",
        action="append",
        metavar="FILE",
        help="Add or update one fixture entry (path or filename). Repeat for multiple.",
    )
    parser.add_argument(
        "--strict-set",
        action="store_true",
        help="With --verify, fail if fixture dir contains untracked *.opf files.",
    )
    parser.add_argument("--repo-root", help="Repo root path (default: auto-detect).")
    parser.add_argument("--fixture-dir", help=f"Fixture dir (default: {DEFAULT_FIXTURE_DIR_REL.as_posix()}).")
    parser.add_argument("--manifest", help=f"Manifest path (default: {DEFAULT_MANIFEST_REL.as_posix()}).")
    args = parser.parse_args(argv)

    if not args.verify and not args.rebuild and not args.add:
        parser.error("No action requested. Use --verify, --rebuild and/or --add.")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    repo_root = Path(args.repo_root).expanduser().resolve() if args.repo_root else find_repo_root(Path.cwd())
    fixture_dir = (
        Path(args.fixture_dir).expanduser().resolve()
        if args.fixture_dir
        else (repo_root / DEFAULT_FIXTURE_DIR_REL).resolve()
    )
    manifest_path = (
        Path(args.manifest).expanduser().resolve()
        if args.manifest
        else (repo_root / DEFAULT_MANIFEST_REL).resolve()
    )

    if not fixture_dir.is_dir():
        raise FileNotFoundError(f"Fixture dir does not exist: {fixture_dir}")

    opf_mod = load_opf_module(repo_root)
    cases = load_manifest(manifest_path)

    changed = False
    if args.add:
        if add_or_update_cases(
            cases,
            args.add,
            fixture_dir=fixture_dir,
            repo_root=repo_root,
            opf_mod=opf_mod,
        ):
            changed = True

    if args.rebuild:
        if rebuild_cases(cases, fixture_dir=fixture_dir, opf_mod=opf_mod):
            changed = True

    if changed:
        write_manifest(manifest_path, cases)
        print(f"WROTE: {manifest_path}")

    if args.verify:
        if not verify_cases(cases, fixture_dir=fixture_dir, opf_mod=opf_mod, strict_set=args.strict_set):
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
