"""Keep nested-container cache writes outside the source tree for both selectors."""

from __future__ import annotations

import dataclasses
import io
import zipfile
from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.ingest import MixedFormatIngestCoordinator


def _zip_bytes(files: dict[str, bytes]) -> bytes:
    """Create real ZIP bytes for nested-container ingestion."""
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)
    return output.getvalue()


def _source(root: Path) -> bytes:
    """Seed an outer ZIP containing another ZIP and return the nested payload."""
    root.mkdir()
    inner = _zip_bytes({"book.txt": b"book"})
    (root / "outer.zip").write_bytes(_zip_bytes({"inner.zip": inner}))
    return inner


def _snapshot(root: Path) -> dict[str, tuple[bytes | None, int, int]]:
    """Compare source names, contents, modes and modification times, excluding access time."""
    return {
        str(path.relative_to(root)): (
            path.read_bytes() if path.is_file() else None,
            path.stat().st_mode,
            path.stat().st_mtime_ns,
        )
        for path in (root, *root.rglob("*"))
    }


def _cache(manager: StorageManager, root: Path) -> api.StoreConfiguration:
    """Register an unstarted cache without creating the configured directory."""
    configuration = api.StoreConfiguration.for_backend(
        "cache",
        "filesystem",
        root,
        modes=(api.ReplicaMode.CACHE, api.ReplicaMode.UNMANAGED),
    )
    manager.create_store(configuration, startup=False)
    return configuration


@pytest.mark.parametrize("placement", ["source", "child", "missing-child", "symlink"])
def test_cache_store_id_inside_source_rejects_before_adoption(
    tmp_path: Path,
    placement: str,
) -> None:
    """Refuse IDs resolving into source before creating metadata, cache directories or bytes."""
    source = tmp_path / "source with spaces"
    _source(source)
    cache_root = source if placement == "source" else source / "cache"
    if placement in {"child", "symlink"}:
        cache_root.mkdir()
    with StorageManager() as manager:
        configuration = _cache(manager, cache_root)
        if placement == "symlink":
            alias = tmp_path / "outside-alias"
            alias.symlink_to(cache_root, target_is_directory=True)
            configuration = dataclasses.replace(
                configuration, store_root_uri=alias.as_uri()
            )
            manager.update_store(configuration.store_uuid, configuration)
        before = _snapshot(source)
        configurations = tuple(manager.iter_store_configurations())
        coordinator = MixedFormatIngestCoordinator(
            manager,
            materialization_store_ref=configuration.store_uuid,
        )
        with pytest.raises(ValueError, match="outside source_root"):
            coordinator.ingest(source)
        assert _snapshot(source) == before
        assert tuple(manager.iter_store_configurations()) == configurations
        assert tuple(manager.iter_digital_asset_records()) == ()
        assert tuple(manager.iter_replica_records()) == ()


@pytest.mark.parametrize("selector", ["root", "store_id"])
def test_external_cache_materializes_and_remains_safe_on_coordinator_reuse(
    tmp_path: Path,
    selector: str,
) -> None:
    """Allow an external cache, then reject its retained ID when a later source contains it."""
    source = tmp_path / "source"
    inner = _source(source)
    cache_root = tmp_path / "cache"
    with StorageManager() as manager:
        if selector == "store_id":
            cache_root.mkdir()
            configuration = _cache(manager, cache_root)
            coordinator = MixedFormatIngestCoordinator(
                manager,
                materialization_store_ref=configuration.store_uuid,
            )
        else:
            coordinator = MixedFormatIngestCoordinator(
                manager, materialization_root=cache_root
            )
        before = _snapshot(source)
        report = coordinator.ingest(source)
        assert report.ok, report.issues
        assert report.materialized_bytes == len(inner)
        assert report.members_adopted == 2
        assert _snapshot(source) == before
        book = next(
            record
            for record in manager.iter_digital_asset_records()
            if record.metadata.original_name == "book.txt"
        )
        assert (
            manager.read_file(
                book.digital_asset_id, replica_mode=api.ReplicaMode.ARCHIVE
            )
            == b"book"
        )
        assert coordinator.materialization_store_ref is not None
        before = _snapshot(tmp_path)
        assets = tuple(manager.iter_digital_asset_records())
        replicas = tuple(manager.iter_replica_records())
        configurations = tuple(manager.iter_store_configurations())
        with pytest.raises(ValueError, match="outside source_root"):
            coordinator.ingest(tmp_path)
        assert _snapshot(tmp_path) == before
        assert tuple(manager.iter_digital_asset_records()) == assets
        assert tuple(manager.iter_replica_records()) == replicas
        assert tuple(manager.iter_store_configurations()) == configurations


def test_cache_selection_is_rechecked_before_nested_materialization(
    tmp_path: Path,
) -> None:
    """Reject a cache ID switched after initial validation before it receives nested bytes."""
    source = tmp_path / "source"
    _source(source)
    inside = source / "cache"
    inside.mkdir()
    outside = tmp_path / "cache"
    outside.mkdir()
    with StorageManager() as manager:
        safe = _cache(manager, outside)
        unsafe = _cache(manager, inside)

        def switch_cache(event: str, details: object) -> None:
            """Simulate a caller changing the selected cache during the run."""
            if event == "source_store_ready":
                coordinator.materialization_store_ref = unsafe.store_uuid

        coordinator = MixedFormatIngestCoordinator(
            manager,
            materialization_store_ref=safe.store_uuid,
            progress_callback=switch_cache,
        )
        before = _snapshot(source)
        report = coordinator.ingest(source)
        assert not report.ok
        assert any("outside source_root" in issue.message for issue in report.issues)
        assert report.materialized_bytes == 0
        assert report.members_adopted == 1
        assert _snapshot(source) == before


def test_discovery_does_not_validate_or_start_unused_cache(tmp_path: Path) -> None:
    """Keep discovery non-mutating even when the unused cache is inside the source."""
    source = tmp_path / "source"
    _source(source)
    with StorageManager() as manager:
        cache = _cache(manager, source / "missing-cache")
        coordinator = MixedFormatIngestCoordinator(
            manager,
            materialization_store_ref=cache.store_uuid,
        )
        before = _snapshot(source)
        report = coordinator.ingest(source, discovery_only=True)
        assert report.ok
        assert report.files_examined == 1
        assert _snapshot(source) == before
        assert tuple(manager.iter_digital_asset_records()) == ()
        assert tuple(manager.iter_replica_records()) == ()
