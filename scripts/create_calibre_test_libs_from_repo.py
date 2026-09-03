#!/usr/bin/env python3

"""
Extract a file (default: Calibre's test metadata.db) from every git tag and
write it to an output directory, named by tag.

Typical use (from anywhere):
  python extract_calibre_metadata_dbs.py --repo /path/to/calibre --out ./out

Defaults target Calibre's test DB path:
  src/calibre/db/tests/metadata.db

It does NOT checkout tags; it uses `git show <tag>:<path>`.

Produces:
  - One output file per tag (e.g. v7.2.0.metadata.db)
  - A manifest CSV with commit, date, sha256, size, status
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


DEFAULT_PATH_IN_REPO = "src/calibre/db/tests/metadata.db"


@dataclass(frozen=True)
class TagInfo:
    """Resolved upstream Calibre tag, commit, and publication timestamp."""

    tag: str
    commit: str
    commit_date_iso: str


class GitError(RuntimeError):
    """Raised when an upstream repository operation cannot be completed."""


def run_git(repo: Path, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    p = subprocess.run(
        ["git", "-C", str(repo), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and p.returncode != 0:
        raise GitError(
            f"git {' '.join(args)} failed (code {p.returncode}).\n"
            f"STDERR:\n{p.stderr.decode('utf-8', errors='replace')}"
        )
    return p


def git_output_text(repo: Path, args: list[str]) -> str:
    p = run_git(repo, args, check=True)
    return p.stdout.decode("utf-8", errors="replace").strip()


def list_tags(repo: Path, *, all_tags: bool = False, pattern: str = "v*") -> list[str]:
    if all_tags:
        # Try a version-ish sort first; fall back if unsupported.
        try:
            out = git_output_text(repo, ["tag", "--sort=v:refname"])
            tags = [t for t in out.splitlines() if t.strip()]
            return tags
        except GitError:
            out = git_output_text(repo, ["tag"])
            tags = [t for t in out.splitlines() if t.strip()]
            return tags

    # Prefer v* tags.
    try:
        out = git_output_text(repo, ["tag", "--list", pattern, "--sort=v:refname"])
        tags = [t for t in out.splitlines() if t.strip()]
        if tags:
            return tags
    except GitError:
        pass

    out = git_output_text(repo, ["tag", "--list", pattern])
    tags = [t for t in out.splitlines() if t.strip()]
    return tags


def get_tag_info(repo: Path, tag: str) -> TagInfo:
    commit = git_output_text(repo, ["rev-list", "-n", "1", tag])
    commit_date_iso = git_output_text(repo, ["show", "-s", "--format=%cI", commit])
    return TagInfo(tag=tag, commit=commit, commit_date_iso=commit_date_iso)


def blob_exists(repo: Path, tag: str, path_in_repo: str) -> bool:
    p = run_git(repo, ["cat-file", "-e", f"{tag}:{path_in_repo}"], check=False)
    return p.returncode == 0


def read_blob(repo: Path, tag: str, path_in_repo: str) -> bytes:
    # `git show` prints the blob content.
    p = run_git(repo, ["show", f"{tag}:{path_in_repo}"], check=True)
    return p.stdout  # bytes


def is_git_lfs_pointer(data: bytes) -> bool:
    # Typical first line: "version https://git-lfs.github.com/spec/v1"
    head = data.splitlines()[:3]
    try:
        head_text = b"\n".join(head).decode("utf-8", errors="ignore")
    except Exception:
        return False
    return head_text.startswith("version https://git-lfs.github.com/spec/v1")


def safe_filename(tag: str) -> str:
    # Make filenames stable across OSes.
    name = tag.replace("/", "_").replace("\\", "_")
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")
    return name or "tag"


def sha256_hex(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Extract a repo file from each tag into named outputs.")
    ap.add_argument("--repo", required=True, help="Path to the cloned git repository")
    ap.add_argument("--out", required=True, help="Output directory to write extracted files")
    ap.add_argument("--path", default=DEFAULT_PATH_IN_REPO, help=f"Path in repo (default: {DEFAULT_PATH_IN_REPO})")
    ap.add_argument("--all-tags", action="store_true", help="Use all tags (default: only v* tags)")
    ap.add_argument("--pattern", default="v*", help="Tag glob when not using --all-tags (default: v*)")
    ap.add_argument("--tag-regex", default=None, help="Optional regex filter applied after tag listing")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of tags processed (0 = no limit)")
    ap.add_argument("--suffix", default="metadata.db", help="Filename suffix to append (default: metadata.db)")
    ap.add_argument("--manifest", default="manifest.csv", help="Manifest filename (default: manifest.csv)")
    args = ap.parse_args(argv)

    repo = Path(args.repo).expanduser().resolve()
    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Sanity: must be a git work tree
    try:
        _ = git_output_text(repo, ["rev-parse", "--is-inside-work-tree"])
    except GitError as e:
        print(f"ERROR: not a git repository: {repo}\n{e}", file=sys.stderr)
        return 2

    tags = list_tags(repo, all_tags=args.all_tags, pattern=args.pattern)
    if args.tag_regex:
        rx = re.compile(args.tag_regex)
        tags = [t for t in tags if rx.search(t)]
    if args.limit and args.limit > 0:
        tags = tags[: args.limit]

    if not tags:
        print("No tags found after filtering.", file=sys.stderr)
        return 1

    manifest_path = out_dir / args.manifest
    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tag", "commit", "commit_date_iso", "path_in_repo", "bytes", "sha256", "status", "notes", "output_file"])

        for tag in tags:
            info = get_tag_info(repo, tag)

            if not blob_exists(repo, tag, args.path):
                w.writerow([info.tag, info.commit, info.commit_date_iso, args.path, 0, "", "missing", "", ""])
                continue

            try:
                data = read_blob(repo, tag, args.path)
            except GitError as e:
                w.writerow([info.tag, info.commit, info.commit_date_iso, args.path, 0, "", "error", str(e).replace("\n", " "), ""])
                continue

            notes = []
            status = "ok"
            if is_git_lfs_pointer(data):
                status = "lfs-pointer"
                notes.append("install git-lfs and fetch LFS objects")

            fname = f"{safe_filename(tag)}.{args.suffix}"
            out_file = out_dir / fname
            out_file.write_bytes(data)

            w.writerow(
                [
                    info.tag,
                    info.commit,
                    info.commit_date_iso,
                    args.path,
                    len(data),
                    sha256_hex(data),
                    status,
                    "; ".join(notes),
                    str(out_file.name),
                ]
            )

    print(f"Wrote {len(tags)} tag outputs to: {out_dir}")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
