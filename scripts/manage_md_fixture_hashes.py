#!/usr/bin/env python3
"""
Provide manage md fixture hashes utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise manage md fixture hashes through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
"""

from __future__ import annotations

import argparse
import hashlib
import os
import runpy
import sys
from pathlib import Path
from typing import Iterable

DEFAULT_MANIFEST_REL = Path("tests/support/md_test_fixture_hashes.py")


def find_repo_root(start: Path) -> Path:
    """
    Find repo root under the format's safety and compatibility rules.

    Example:
        Exercise find repo root through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param start: Value supplied for start under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    start = start.resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "src" / "LiuXin_alpha").is_dir() and (candidate / "tests").is_dir():
            return candidate
    return start


def resolve_data_repo_root(repo_root: Path, explicit_data_root: str | None) -> Path:
    """
    Perform the resolve data repo root operation under explicit file-format and conversion rules.

    Example:
        Exercise resolve data repo root through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param repo_root: Value supplied for repo root under the utility contract.
    :param explicit_data_root: Value supplied for explicit data root under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if explicit_data_root:
        p = Path(explicit_data_root).expanduser()
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        if p.is_dir():
            return p
        raise FileNotFoundError(f"--data-root does not exist: {p}")

    env = os.environ.get("LIUXIN_ALPHA_DATA_DIR")
    if env:
        p = Path(env).expanduser()
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        if p.is_dir():
            return p
        raise FileNotFoundError(f"LIUXIN_ALPHA_DATA_DIR does not exist: {p}")

    candidates = [
        repo_root / "LiuXin_alpha_data",
        repo_root.parent / "LiuXin_alpha_data",
    ]
    for p in candidates:
        if p.is_dir():
            return p
    raise FileNotFoundError(
        "Could not find LiuXin_alpha_data. Pass --data-root or set LIUXIN_ALPHA_DATA_DIR."
    )


def resolve_md_fixture_dir(data_root: Path) -> Path:
    """
    Perform the resolve md fixture dir operation under explicit file-format and conversion rules.

    Example:
        Exercise resolve md fixture dir through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param data_root: Value supplied for data root under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for name in ("md_test_files", "md_test_books"):
        p = data_root / name
        if p.is_dir():
            return p
    raise FileNotFoundError(
        f"No md fixture directory under {data_root}. Expected md_test_files/ or md_test_books/."
    )


def legacy_sha512_size_hash(path: Path) -> str:
    """
    Perform the legacy sha512 size hash operation under explicit file-format and conversion rules.

    Example:
        Exercise legacy sha512 size hash through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    hasher = hashlib.sha512()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest() + str(path.stat().st_size)


def load_manifest(path: Path) -> dict[str, str]:
    """
    Perform the load manifest operation under explicit file-format and conversion rules.

    Example:
        Exercise load manifest through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    scope = runpy.run_path(str(path))
    data = scope.get("EXPECTED_MD_TEST_FILE_HASHES")
    if not isinstance(data, dict):
        raise TypeError(f"{path} does not define EXPECTED_MD_TEST_FILE_HASHES as a dict")

    out: dict[str, str] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise TypeError("EXPECTED_MD_TEST_FILE_HASHES must be dict[str, str]")
        out[k] = v
    return out


def render_manifest(mapping: dict[str, str]) -> str:
    """
    Perform the render manifest operation under explicit file-format and conversion rules.

    Example:
        Exercise render manifest through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param mapping: Value supplied for mapping under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    lines: list[str] = [
        "from __future__ import annotations\n",
        "\n",
        "import hashlib\n",
        "from pathlib import Path\n",
        "\n",
        "# Extracted from legacy LiuXin tests (`LiuXin_tests.test_constants`).\n",
        "# Hash format is intentionally legacy-compatible:\n",
        "#   sha512(file_bytes).hexdigest() + str(file_size_in_bytes)\n",
        "EXPECTED_MD_TEST_FILE_HASHES: dict[str, str] = {\n",
    ]
    for filename in sorted(mapping):
        lines.append(f'    "{filename}": "{mapping[filename]}",\n')
    lines.extend(
        [
            "}\n",
            "\n",
            "\n",
            "def legacy_sha512_size_hash(path: Path) -> str:\n",
            '    """Return the historical LiuXin file hash used by metadata fixture tests."""\n',
            "    hasher = hashlib.sha512()\n",
            '    with path.open("rb") as stream:\n',
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
    """
    Write manifest under the format's safety and compatibility rules.

    Example:
        Exercise write manifest through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param mapping: Value supplied for mapping under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_manifest(mapping), encoding="utf-8")


def _is_within(base: Path, target: Path) -> bool:
    """
    Perform the is within operation under explicit file-format and conversion rules.

    Example:
        Exercise  is within through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param base: Value supplied for base under the utility contract.
    :param target: Value supplied for target under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    base = base.resolve()
    target = target.resolve()
    return target == base or base in target.parents


def resolve_add_target(raw: str, md_dir: Path, repo_root: Path) -> Path:
    """
    Perform the resolve add target operation under explicit file-format and conversion rules.

    Example:
        Exercise resolve add target through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param raw: Value supplied for raw under the utility contract.
    :param md_dir: Value supplied for md dir under the utility contract.
    :param repo_root: Value supplied for repo root under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    raw_path = Path(raw).expanduser()
    candidates: list[Path] = []

    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.append(md_dir / raw_path)
        candidates.append(repo_root / raw_path)
        if raw_path.name != raw:
            candidates.append(md_dir / raw_path.name)

    for candidate in candidates:
        if candidate.is_file():
            resolved = candidate.resolve()
            if not _is_within(md_dir, resolved):
                raise ValueError(
                    f"--add target must live under {md_dir} (got {resolved})"
                )
            return resolved

    attempted = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Could not resolve --add target '{raw}'. Tried: {attempted}")


def revalidate_existing_hashes(
    mapping: dict[str, str],
    md_dir: Path,
    *,
    strict_set: bool = False,
) -> bool:
    """
    Perform the revalidate existing hashes operation under explicit file-format and conversion rules.

    Example:
        Exercise revalidate existing hashes through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param mapping: Value supplied for mapping under the utility contract.
    :param md_dir: Value supplied for md dir under the utility contract.
    :param strict_set: Value supplied for strict set under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    missing: list[str] = []
    mismatches: list[tuple[str, str, str]] = []

    for filename, expected in sorted(mapping.items()):
        path = md_dir / filename
        if not path.is_file():
            missing.append(filename)
            continue
        actual = legacy_sha512_size_hash(path)
        if actual != expected:
            mismatches.append((filename, expected, actual))

    actual_files = {p.name for p in md_dir.iterdir() if p.is_file() and not p.name.startswith(".")}
    expected_files = set(mapping.keys())
    extras = sorted(actual_files - expected_files)

    if not missing and not mismatches:
        print(f"OK: validated {len(mapping)} tracked fixture hashes in {md_dir}")
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
    md_dir: Path,
    repo_root: Path,
) -> bool:
    """
    Perform the add or update hash entries operation under explicit file-format and conversion rules.

    Example:
        Exercise add or update hash entries through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param mapping: Value supplied for mapping under the utility contract.
    :param add_targets: Value supplied for add targets under the utility contract.
    :param md_dir: Value supplied for md dir under the utility contract.
    :param repo_root: Value supplied for repo root under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    changed = False
    for raw in add_targets:
        path = resolve_add_target(raw, md_dir=md_dir, repo_root=repo_root)
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
    """
    Parse args under the format's safety and compatibility rules.

    Example:
        Exercise parse args through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = argparse.ArgumentParser(description="Manage md_test fixture hash manifest")
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
        "--data-root",
        help="Path to LiuXin_alpha_data checkout (overrides auto-detection).",
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
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    args = parse_args(argv or sys.argv[1:])

    repo_root = Path(args.repo_root).expanduser().resolve() if args.repo_root else find_repo_root(Path.cwd())
    manifest = Path(args.manifest).expanduser() if args.manifest else DEFAULT_MANIFEST_REL
    if not manifest.is_absolute():
        manifest = (repo_root / manifest).resolve()

    data_root = resolve_data_repo_root(repo_root, args.data_root)
    md_dir = resolve_md_fixture_dir(data_root)

    if not manifest.is_file():
        raise FileNotFoundError(f"Manifest not found: {manifest}")
    mapping = load_manifest(manifest)

    if args.add:
        changed = add_or_update_hash_entries(
            mapping,
            args.add,
            md_dir=md_dir,
            repo_root=repo_root,
        )
        if changed:
            write_manifest(manifest, mapping)
            print(f"WROTE: {manifest}")
        else:
            print("No hash changes detected; manifest left untouched.")

    if args.revalidate:
        ok = revalidate_existing_hashes(mapping, md_dir, strict_set=args.strict_set)
        if not ok:
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
