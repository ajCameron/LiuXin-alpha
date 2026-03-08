from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys

import pytest

from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
    build_squashfs_from_manifest,
)


def _require_squashfs_tools() -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")


def test_build_squashfs_from_manifest_roundtrip(tmp_path: pathlib.Path) -> None:
    _require_squashfs_tools()

    source_root = tmp_path / "src"
    (source_root / "books").mkdir(parents=True, exist_ok=True)
    (source_root / "books" / "one.epub").write_bytes(b"ONE")
    (source_root / "books" / "two.mobi").write_bytes(b"TWO")

    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "files": [
                    {"source": "src/books/one.epub", "archive_path": "A/one.epub"},
                    {"source": "src/books/two.mobi", "archive_path": "B/two.mobi"},
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    archive = tmp_path / "library.squashfs"
    report = build_squashfs_from_manifest(manifest, archive, deterministic=True, force=True)
    assert report.file_count == 2
    assert report.output_bytes > 0
    assert report.manifest_sha256
    assert report.output_sha256
    assert report.mksquashfs_executable
    assert report.build_flags
    assert archive.exists() is True

    store = SquashfsReadOnlyStorageBackend(url=str(archive))
    got_one = store.get_file(str(archive.resolve()) + "/A/one.epub")
    got_two = store.get_file("B/two.mobi")
    assert got_one.as_bytes() == b"ONE"
    assert got_two.as_bytes() == b"TWO"


def test_build_squashfs_from_manifest_duplicate_target_fails(tmp_path: pathlib.Path) -> None:
    _require_squashfs_tools()

    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    a.write_text("a", encoding="utf-8")
    b.write_text("b", encoding="utf-8")

    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            [
                {"source": str(a), "archive_path": "dup.txt"},
                {"source": str(b), "archive_path": "dup.txt"},
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate archive_path"):
        build_squashfs_from_manifest(manifest, tmp_path / "out.squashfs")


def test_build_squashfs_script_smoke(tmp_path: pathlib.Path) -> None:
    _require_squashfs_tools()

    src_file = tmp_path / "demo.txt"
    src_file.write_text("demo", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{"source": str(src_file), "archive_path": "docs/demo.txt"}]), encoding="utf-8")

    script = pathlib.Path(__file__).resolve().parents[4] / "scripts" / "build_squashfs_from_manifest.py"
    out = tmp_path / "out.squashfs"
    proc = subprocess.run(
        [sys.executable, str(script), "--manifest", str(manifest), "--output", str(out), "--force"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["file_count"] == 1
    assert pathlib.Path(payload["output_archive"]) == out.resolve()
    assert payload["manifest_sha256"]
    assert payload["output_sha256"]
    assert "mksquashfs_version" in payload
    assert out.exists() is True
