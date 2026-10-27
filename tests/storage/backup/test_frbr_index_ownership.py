"""Existing-drive scans preserve storage-owned byte identity and observations."""

import hashlib

import pytest

from LiuXin_alpha.core.library_services import LibraryServices
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup.prototype_pipeline import _index_existing_disk_frbr


@pytest.fixture
def indexed_source(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    path = root / "book.epub"
    path.write_bytes(b"original ebook")
    with LibraryServices.open(
        database_path=tmp_path / "library.sqlite",
        create=True,
        backup=False,
        enable_maintenance=False,
    ) as services:
        yield services.library, root, path


def _scan(library, root):
    return _index_existing_disk_frbr(
        library.db,
        disk_path=root,
        store_name="source",
        ebook_extensions=("epub",),
    )


def test_scan_adopts_real_sha256_and_refreshes_owned_observations(indexed_source):
    library, root, path = indexed_source
    (root / "cover.jpg").write_bytes(b"excluded")
    first = _scan(library, root)
    assert (first.inserted_files, first.skipped_non_ebook_files) == (1, 1)
    manager = library.storage
    (asset,) = manager.iter_digital_asset_records()
    (replica,) = manager.iter_replica_records()
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    assert asset.digests == (api.Digest("sha256", expected),)
    assert asset.revision
    assert replica.state is api.ReplicaState.VERIFIED
    assert replica.revision
    assert replica.observation.observed_digests == asset.digests

    second = _scan(library, root)
    assert second.inserted_files == 0
    assert second.updated_files == 1
    assert tuple(manager.iter_digital_asset_records()) == (asset,)
    (refreshed,) = manager.iter_replica_records()
    assert refreshed.replica_id == replica.replica_id
    assert refreshed.revision != replica.revision
    # A fresh repository read must agree with the manager and SQL projections.
    manager.bind_metadata_cache(None)
    assert manager.get_replica_record(replica.replica_id) == refreshed
    row = library.db.macros.get_row("asset_replicas", replica.replica_id)
    assert row["asset_replica_observed_hash_sha256"] == expected


@pytest.mark.parametrize("replacement", [b"modified ebook", b"different length"])
def test_rescan_records_corruption_without_redefining_asset(
    indexed_source, replacement
):
    library, root, path = indexed_source
    _scan(library, root)
    manager = library.storage
    (asset,) = manager.iter_digital_asset_records()
    (replica,) = manager.iter_replica_records()
    path.write_bytes(replacement)

    with pytest.raises(api.StoragePreconditionFailed, match="reconciliation"):
        _scan(library, root)

    assert manager.get_digital_asset_record(asset.digital_asset_id) == asset
    changed = manager.get_replica_record(replica.replica_id)
    assert changed.state is api.ReplicaState.CORRUPT
    assert changed.revision != replica.revision
    assert changed.observation.observed_size_bytes == len(replacement)
    assert len(tuple(manager.iter_digital_asset_records())) == 1
    assert path.read_bytes() == replacement
