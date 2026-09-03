from __future__ import annotations

import io
import logging
import os
import tarfile
import zipfile

from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.ingest import (
    ContainerMemberContext,
    MixedFormatIngestCoordinator,
    MixedIngestBudget,
)
from LiuXin_alpha.storage.store_manager import StorageManager
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_PAYLOAD,
)


def _zip_bytes(files: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)
    return output.getvalue()


def _tar_bytes(files: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, payload in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    return output.getvalue()


def _open_database(path: Path, *, create: bool = True) -> Database:
    return Database(
        metadata={"database_path": str(path)},
        db_type="SQLite",
        create=create,
        backup=False,
        enable_storage_manager=False,
    )


def test_discovery_only_classifies_without_mutating_manager(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "named-but-broken.zip").write_bytes(b"not a zip")
    (source / "magic-only.bin").write_bytes(_zip_bytes({"book.txt": b"book"}))
    # EPUB is a terminal ebook by default even though its bytes use ZIP.
    (source / "book.epub").write_bytes(_zip_bytes({"mimetype": b"epub"}))
    (source / "ordinary.txt").write_bytes(b"ordinary")

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(manager).ingest(
            source, discovery_only=True
        )

        assert report.ok
        assert report.discovery_only
        assert report.files_examined == 4
        assert report.files_adopted == 0
        assert report.top_level_containers == 2
        assert report.loose_files == 2
        assert report.recognized_formats == (("zip", 2),)
        assert report.source_store_ref is None
        assert tuple(manager.iter_store_configurations()) == ()
        assert tuple(manager.iter_digital_asset_records()) == ()
        assert tuple(manager.iter_replica_records()) == ()


def test_mixed_and_nested_ingest_is_readable_bounded_and_restart_idempotent(
    tmp_path: Path,
) -> None:
    source = tmp_path / "mess"
    source.mkdir()
    inner = _zip_bytes({"library/book.txt": b"nested book"})
    (source / "outer.zip").write_bytes(
        _zip_bytes({"packs/inner.zip": inner, "outer-note.txt": b"outer note"})
    )
    (source / "documents.tar").write_bytes(
        _tar_bytes({"tar-note.txt": b"tar note"})
    )
    (source / "standalone.epub").write_bytes(b"terminal ebook bytes")
    database_path = tmp_path / "catalogue.sqlite"
    cache_root = tmp_path / "materialized"

    with _open_database(database_path) as database:
        manager = StorageManager(db=database, startup_on_add=True)
        events: list[str] = []
        coordinator = MixedFormatIngestCoordinator(
            manager,
            materialization_root=cache_root,
            progress_callback=lambda event, _details: events.append(event),
        )

        first = coordinator.ingest(source)

        assert first.ok, [
            (issue.stage, issue.error_type, issue.message) for issue in first.issues
        ]
        assert first.files_examined == 3
        assert first.files_adopted == 3
        assert first.loose_files == 1
        assert first.top_level_containers == 2
        assert first.containers_discovered == 3
        assert first.containers_processed == 3
        assert first.containers_deduplicated == 0
        assert first.members_discovered == 4
        assert first.members_adopted == 4
        assert first.materialized_bytes == len(inner)
        assert first.recognized_formats == (("tar", 1), ("zip", 2))
        assert events.count("container_complete") == 3
        assert cache_root.is_dir()

        nested_book = next(
            record
            for record in manager.iter_digital_asset_records()
            if record.metadata.original_name == "book.txt"
        )
        assert manager.read_file(
            nested_book.digital_asset_id, replica_mode=api.ReplicaMode.ARCHIVE
        ) == b"nested book"
        inner_report = next(
            report for report in first.containers if report.depth == 2
        )
        assert inner_report.store_ref is not None
        inner_configuration = manager.get_store_configuration(inner_report.store_ref)
        assert inner_configuration.backing is not None
        assert (
            inner_configuration.backing.materialization_store_ref
            == coordinator.materialization_store_ref
        )

        repeated = coordinator.ingest(source)

        assert repeated.ok
        assert not repeated.source_store_created
        assert repeated.assets_created == 0
        assert repeated.replicas_created == 0
        assert repeated.materialized_bytes == 0
        store_count = len(tuple(manager.iter_store_configurations()))
        asset_count = len(tuple(manager.iter_digital_asset_records()))
        replica_count = len(tuple(manager.iter_replica_records()))
        manager.close()

    with _open_database(database_path, create=False) as database:
        reloaded = StorageManager(db=database, startup_on_add=True)
        bootstrap = reloaded.load_from_database(startup=True)
        assert bootstrap.ok, bootstrap.issues

        after_restart = MixedFormatIngestCoordinator(
            reloaded,
            materialization_root=cache_root,
        ).ingest(source)

        assert after_restart.ok, [
            (issue.stage, issue.error_type, issue.message)
            for issue in after_restart.issues
        ]
        assert after_restart.assets_created == 0
        assert after_restart.replicas_created == 0
        assert after_restart.materialized_bytes == 0
        assert len(tuple(reloaded.iter_store_configurations())) == store_count
        assert len(tuple(reloaded.iter_digital_asset_records())) == asset_count
        assert len(tuple(reloaded.iter_replica_records())) == replica_count
        reloaded.close()


def test_identical_containers_are_expanded_once_but_both_sources_are_catalogued(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    payload = _zip_bytes({"one.txt": b"one"})
    (source / "first.zip").write_bytes(payload)
    (source / "second.zip").write_bytes(payload)

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(manager).ingest(source)

        assert report.ok
        assert report.files_adopted == 2
        assert report.top_level_containers == 2
        assert report.containers_discovered == 2
        assert report.containers_processed == 1
        assert report.containers_deduplicated == 1
        assert report.members_adopted == 1
        assert sum(item.duplicate_of is not None for item in report.containers) == 1
        source_replicas = tuple(
            manager.iter_replica_records(mode=api.ReplicaMode.UNMANAGED)
        )
        assert len(source_replicas) == 2
        assert len({record.digital_asset_id for record in source_replicas}) == 1


def test_corrupt_candidate_is_isolated_and_other_files_remain_catalogued(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "broken.zip").write_bytes(b"not actually zip")
    (source / "book.mobi").write_bytes(b"book")

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(manager).ingest(source)

        assert not report.ok
        assert report.files_adopted == 2
        assert report.loose_files == 1
        assert report.top_level_containers == 1
        assert report.containers_processed == 0
        assert report.containers[0].issues[0].stage == "container"
        assert "ZIP" in report.containers[0].issues[0].message
        assert len(tuple(manager.iter_digital_asset_records())) == 2
        assert len(tuple(manager.iter_replica_records())) == 2


def test_run_wide_member_limit_halts_recursive_expansion(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "many.zip").write_bytes(
        _zip_bytes({"one.txt": b"one", "two.txt": b"two"})
    )
    budget = MixedIngestBudget(max_members=1)

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(manager, budget=budget).ingest(source)

        assert not report.ok
        assert report.truncated
        assert report.halt_reason == "run-wide member limit reached: 1"
        assert report.members_discovered == 1
        assert report.members_adopted == 1
        assert any(issue.stage == "member_limit" for issue in report.issues)


def test_depth_limit_catalogues_nested_bytes_without_opening_them(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    inner = _zip_bytes({"book.txt": b"book"})
    (source / "outer.zip").write_bytes(_zip_bytes({"inner.zip": inner}))

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(
            manager,
            budget=MixedIngestBudget(max_container_depth=1),
        ).ingest(source)

        assert not report.ok
        assert report.containers_discovered == 1
        assert report.containers_processed == 1
        assert report.members_adopted == 1
        assert report.containers[0].nested_containers_discovered == 1
        assert report.containers[0].truncated
        assert any(
            issue.stage == "container_depth_limit" for issue in report.issues
        )
        # Source Store + outer ZIP Store; no cache or inner Store was created.
        assert len(tuple(manager.iter_store_configurations())) == 2


def test_materialization_root_inside_source_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()

    with StorageManager() as manager:
        coordinator = MixedFormatIngestCoordinator(
            manager, materialization_root=source / "cache"
        )
        try:
            coordinator.ingest(source)
        except ValueError as error:
            assert "outside source_root" in str(error)
        else:  # pragma: no cover - assertion spelling gives a useful failure
            raise AssertionError("an in-source materialization root was accepted")


def test_nested_container_without_cache_is_actionable_and_can_be_disabled(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    inner = _zip_bytes({"book.txt": b"book"})
    (source / "outer.zip").write_bytes(_zip_bytes({"inner.zip": inner}))

    with StorageManager() as manager:
        missing_cache = MixedFormatIngestCoordinator(manager).ingest(source)

        assert not missing_cache.ok
        assert missing_cache.containers_processed == 1
        assert missing_cache.members_adopted == 1
        assert any(
            issue.stage == "container"
            and "materialization Store" in issue.message
            for issue in missing_cache.issues
        )

    with StorageManager() as manager:
        top_level_only = MixedFormatIngestCoordinator(
            manager, recurse_containers=False
        ).ingest(source)

        assert top_level_only.ok
        assert top_level_only.containers_discovered == 1
        assert top_level_only.containers_processed == 1
        assert top_level_only.members_adopted == 1
        assert len(tuple(manager.iter_store_configurations())) == 2


def test_cumulative_expanded_byte_limit_spans_separate_archives(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.zip").write_bytes(_zip_bytes({"a.txt": b"aaa"}))
    (source / "b.zip").write_bytes(_zip_bytes({"b.txt": b"bbb"}))
    budget = MixedIngestBudget(max_total_expanded_bytes=5)

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(manager, budget=budget).ingest(source)

        assert not report.ok
        assert report.halt_reason == "run-wide expanded-byte limit would be exceeded"
        assert report.expanded_bytes == 3
        assert report.members_adopted == 1
        assert report.containers_processed == 2
        assert any(issue.stage == "expanded_byte_limit" for issue in report.issues)


def test_container_limit_still_catalogues_remaining_top_level_files(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.zip").write_bytes(_zip_bytes({"a.txt": b"a"}))
    (source / "b.zip").write_bytes(_zip_bytes({"b.txt": b"b"}))

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(
            manager, budget=MixedIngestBudget(max_containers=1)
        ).ingest(source)

        assert not report.ok
        assert report.files_adopted == 2
        assert report.top_level_containers == 2
        assert report.containers_discovered == 1
        assert report.containers_processed == 1
        assert sum(issue.stage == "container_limit" for issue in report.issues) == 1


def test_zip_bomb_ratio_and_traversal_are_rejected_without_host_writes(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "ratio.zip").write_bytes(_zip_bytes({"zeros.bin": bytes(64 * 1024)}))
    (source / "traversal.zip").write_bytes(_zip_bytes({"../escape.txt": b"escape"}))
    outside = tmp_path / "escape.txt"
    budget = MixedIngestBudget(max_container_expansion_ratio=5.0)

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(manager, budget=budget).ingest(source)

        assert not report.ok
        assert report.files_adopted == 2
        assert report.containers_processed == 0
        assert report.members_adopted == 0
        assert len(report.containers) == 2
        assert all(item.issues for item in report.containers)
        assert any("ratio" in issue.message.lower() for issue in report.issues)
        assert any(
            "unsafe" in issue.message.lower()
            or "travers" in issue.message.lower()
            or "path" in issue.message.lower()
            or "canonical" in issue.message.lower()
            for issue in report.issues
        ), [issue.message for issue in report.issues]
        assert not outside.exists()


def test_pre_cancelled_run_does_not_create_a_source_store(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "book.epub").write_bytes(b"book")

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(
            manager, cancellation_callback=lambda: True
        ).ingest(source)

        assert not report.ok
        assert report.halt_reason == "ingest cancelled by callback"
        assert report.files_examined == 0
        assert report.files_adopted == 0
        assert report.source_store_ref is None
        assert tuple(manager.iter_store_configurations()) == ()
        assert tuple(manager.iter_digital_asset_records()) == ()


def test_member_metadata_hook_receives_container_context(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "pack.zip").write_bytes(_zip_bytes({"books/original.txt": b"book"}))
    observed: list[tuple[ContainerMemberContext, str]] = []

    def metadata(
        context: ContainerMemberContext,
        entry: api.StoreInventoryEntry,
    ) -> api.DigitalAssetMetadata:
        observed.append((context, entry.location.key))
        return api.DigitalAssetMetadata(
            name="enriched book",
            original_name="original.txt",
            attributes=(("enrichment.test", "true"),),
        )

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(
            manager, member_metadata_factory=metadata
        ).ingest(source)

        assert report.ok
        [(context, member_path)] = observed
        assert context.format_name == "zip"
        assert context.depth == 1
        assert context.container_path.endswith("pack.zip")
        assert context.container_chain == (context.container_path,)
        assert member_path == "books/original.txt"
        enriched = next(
            record
            for record in manager.iter_digital_asset_records()
            if record.metadata.name == "enriched book"
        )
        assert dict(enriched.metadata.attributes) == {"enrichment.test": "true"}


def test_discovery_magic_registry_covers_every_builtin_container(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    probes = {
        "squash.bin": b"hsqs",
        "zip.bin": b"PK\x03\x04",
        "rar.bin": b"Rar!\x1a\x07\x00",
        "seven.bin": b"7z\xbc\xaf'\x1c",
        "tar.bin": bytes(257) + b"ustar",
        "iso.bin": bytes(32_769) + b"CD001",
    }
    for name, payload in probes.items():
        (source / name).write_bytes(payload)

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(manager).ingest(
            source, discovery_only=True
        )

        assert report.ok
        assert report.top_level_containers == 6
        assert report.loose_files == 0
        assert dict(report.recognized_formats) == {
            "7z": 1,
            "iso": 1,
            "rar": 1,
            "squashfs": 1,
            "tar": 1,
            "zip": 1,
        }


def test_ebook_container_expansion_is_explicit_opt_in(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "book.epub").write_bytes(_zip_bytes({"chapter.xhtml": b"chapter"}))

    with StorageManager() as manager:
        default_report = MixedFormatIngestCoordinator(manager).ingest(source)

        assert default_report.ok
        assert default_report.loose_files == 1
        assert default_report.containers_discovered == 0
        assert default_report.members_adopted == 0

    with StorageManager() as manager:
        expanded = MixedFormatIngestCoordinator(
            manager, expand_ebook_containers=True
        ).ingest(source)

        assert expanded.ok
        assert expanded.loose_files == 0
        assert expanded.recognized_formats == (("zip", 1),)
        assert expanded.containers_processed == 1
        assert expanded.members_adopted == 1


@pytest.mark.skipif(
    os.name != "posix", reason="surrogateescape is a POSIX byte-name contract"
)
def test_tortured_unicode_and_undecodable_container_paths_remain_readable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    archive_bytes = _tar_bytes(
        {
            "深い/📚 e\u0301.txt": b"unicode",
            POSIX_BAD_BYTES_FILENAME: POSIX_BAD_BYTES_PAYLOAD,
        }
    )
    raw_archive = os.path.join(
        os.fsencode(source), b"pack-bad-\xff.tar"
    )
    with open(raw_archive, "wb") as output:
        _ = output.write(archive_bytes)

    with StorageManager() as manager:
        report = MixedFormatIngestCoordinator(manager).ingest(source)

        assert report.ok, [issue.message for issue in report.issues]
        assert report.files_adopted == 1
        assert report.members_adopted == 2
        bad_asset = next(
            record
            for record in manager.iter_digital_asset_records()
            if record.metadata.original_name == POSIX_BAD_BYTES_FILENAME
        )
        assert manager.read_file(
            bad_asset.digital_asset_id, replica_mode=api.ReplicaMode.ARCHIVE
        ) == POSIX_BAD_BYTES_PAYLOAD
        [source_replica] = tuple(
            manager.iter_replica_records(mode=api.ReplicaMode.UNMANAGED)
        )
        assert os.fsencode(source_replica.location.key) == b"pack-bad-\xff.tar"


def test_ingest_emits_correlated_object_events_and_checkpoints(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "pack.zip").write_bytes(_zip_bytes({"book.txt": b"book"}))
    run_id = uuid4()

    with caplog.at_level(
        logging.DEBUG,
        logger="LiuXin_alpha.storage.ingest.mixed_format",
    ):
        with StorageManager() as manager:
            report = MixedFormatIngestCoordinator(
                manager,
                log_checkpoint_every=1,
            ).ingest(source, run_id=run_id)

    assert report.run_id == run_id
    records = [
        record
        for record in caplog.records
        if getattr(record, "liuxin_event", None) is not None
    ]
    events = [getattr(record, "liuxin_event") for record in records]
    for expected in (
        "run_started",
        "discovery_complete",
        "source_store_ready",
        "source_file_adopted",
        "source_checkpoint",
        "container_discovered",
        "container_started",
        "container_store_ready",
        "member_adopted",
        "member_checkpoint",
        "container_complete",
        "complete",
    ):
        assert expected in events
    assert all(
        getattr(record, "liuxin_context")["run_id"] == str(run_id)
        for record in records
    )
    member = next(
        record
        for record in records
        if getattr(record, "liuxin_event") == "member_adopted"
    )
    assert getattr(member, "liuxin_context")["member_path"] == "book.txt"
    assert getattr(member, "liuxin_context")["digital_asset_id"] == 2


def test_recoverable_container_failure_logs_a_traceback(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "broken.zip").write_bytes(b"not actually zip")

    with caplog.at_level(
        logging.DEBUG,
        logger="LiuXin_alpha.storage.ingest.mixed_format",
    ):
        with StorageManager() as manager:
            report = MixedFormatIngestCoordinator(manager).ingest(source)

    assert not report.ok
    failure = next(
        record
        for record in caplog.records
        if getattr(record, "liuxin_event", None) == "container_error"
    )
    assert failure.exc_info is not None
    context = getattr(failure, "liuxin_context")
    assert context["run_id"] == str(report.run_id)
    assert context["error_type"]
    assert context["path"].endswith("broken.zip")
