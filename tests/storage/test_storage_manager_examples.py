"""Executable coverage for the public StorageManager examples."""

from __future__ import annotations

import json
import subprocess
import sys

from pathlib import Path
from typing import cast


REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPO_ROOT / "examples"


def _run_example(script_name: str, *arguments: str) -> dict[str, object]:
    completed = subprocess.run(
        [sys.executable, str(EXAMPLES / script_name), *arguments],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return cast(dict[str, object], json.loads(completed.stdout))


def test_manual_storage_manager_roundtrip_example(tmp_path: Path) -> None:
    store_root = tmp_path / "manual-store"

    result = _run_example(
        "storage_manager_manual_roundtrip_example.py",
        "--store-root",
        str(store_root),
        "--payload",
        "example payload",
    )

    assert result["store_name"] == "manual_demo_store"
    assert result["digital_asset_id"] == 1
    assert result["replica_id"] == 1
    assert result["retrieved_preview"] == "example payload"
    assert result["all_read_forms_match"] is True
    assert any(path.is_file() for path in store_root.rglob("*"))


def test_storage_manager_workflows_example(tmp_path: Path) -> None:
    result = _run_example(
        "storage_manager_workflows_example.py",
        "--work-dir",
        str(tmp_path),
    )

    assert result["stores"] == ["archive", "primary"]
    assert result["ingest_verified"] is True
    assert result["all_read_forms_match"] is True
    assert result["placement_hints_reused"] is True
    assert result["verified_replica_ids"] == [1, 2]
    assert result["composite_item_role"] == "package"
    assert result["zip_members"] == [
        "book.epub",
        "images/cover.jpg",
    ]
    assert result["exported_members"] == [
        "exported-package/book.epub",
        "exported-package/images/cover.jpg",
    ]
    assert (tmp_path / "exported-package" / "book.epub").is_file()
    assert (tmp_path / "exported-package" / "images" / "cover.jpg").is_file()
