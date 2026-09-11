"""
Exercise mixed-file ingestion with real temporary archives and manager operations.

Most tests use in-memory manager metadata; the persistence regression explicitly
uses SQLite and reopens both database and manager in this process. ZIP/TAR fixtures
are real archives, while the six-format discovery test uses signature fragments
only. Tests cover recursion, cache requirements, cumulative limits, source/member
preservation after failures, path encoding, metadata hooks, and correlated logs.
No live remote service or external extractor is required by these cases.

Example:
    >>> test_discovery_only_classifies_without_mutating_manager(tmp_path)  # doctest: +SKIP
"""

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
    """
    Build an in-memory deflated ZIP from ordered name/payload pairs.

    Use real zipfile serialization for backend tests. Default entry timestamps mean repeated calls
    are not promised byte-for-byte identical.

    Example:
        >>> zipfile.is_zipfile(io.BytesIO(_zip_bytes({'book.txt': b'book'})))
        True


    :param files: Mapping whose iteration order determines ZIP entry order; names are not sanitized.
    :return: Complete ZIP archive bytes after the writer closes.
    """
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)
    return output.getvalue()


def _tar_bytes(files: dict[str, bytes]) -> bytes:
    """
    Build an in-memory uncompressed TAR with exact payload sizes and supplied member names.

    Entries follow mapping order and otherwise use TarInfo defaults, including the default
    timestamp. Names can intentionally contain unusual Unicode or surrogate escapes for path tests.

    Example:
        >>> bool(_tar_bytes({'book.txt': b'book'}))
        True


    :param files: Ordered member-name/payload mapping serialized by the real tarfile module.
    :return: Complete TAR bytes after final archive blocks are written.
    """
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, payload in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    return output.getvalue()


def _open_database(path: Path, *, create: bool = True) -> Database:
    """
    Open a real SQLite catalogue without backups or an automatically created StorageManager.

    The caller owns the database lifetime and attaches the manager explicitly. Passing create=False
    reopens the existing catalogue used by the persistence regression.

    Example:
        >>> database = _open_database(catalogue_path)  # doctest: +SKIP


    :param path: Filesystem path of the test catalogue.
    :param create: Whether database construction may create the catalogue.
    :return: Open Database instance supporting the caller context manager.
    """
    return Database(
        metadata={"database_path": str(path)},
        db_type="SQLite",
        create=create,
        backup=False,
        enable_storage_manager=False,
    )


def test_discovery_only_classifies_without_mutating_manager(tmp_path: Path) -> None:
    """
    Classify named and magic ZIP candidates while leaving in-memory manager collections empty.

    Use real temporary bytes, including a broken named ZIP, terminal EPUB-shaped ZIP, and ordinary
    file. The assertions establish preliminary recognition and zero adoption, not complete archive
    validity.

    Example:
        >>> test_discovery_only_classifies_without_mutating_manager(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Ingest real nested ZIP/TAR bytes through SQLite, then repeat and reopen the catalogue without
    duplicate metadata.

    Assert member readability, nested cache selection/accounting, completion events, and stable
    Store/Asset/Replica counts. The restart boundary closes and reopens the database and manager
    within this process; it does not simulate a process crash.

    Example:
        >>> test_mixed_and_nested_ingest_is_readable_bounded_and_restart_idempotent(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Adopt two paths containing the same ZIP bytes while expanding their shared container identity
    once.

    An in-memory manager retains two UNMANAGED source Replicas pointing at one Asset, with one
    member adoption and one duplicate report.

    Example:
        >>> test_identical_containers_are_expanded_once_but_both_sources_are_catalogued(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Preserve adopted source metadata when backend validation rejects a broken named ZIP.

    The ordinary MOBI file remains catalogued, both source Assets/Replicas exist in memory, and the
    failed container report identifies the ZIP error.

    Example:
        >>> test_corrupt_candidate_is_isolated_and_other_files_remain_catalogued(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Stop after one accepted member when a real ZIP contains two and max_members is one.

    Assert the aggregate halt reason, truncation, counters, and member-limit issue rather than
    relying on which ZIP member is chosen.

    Example:
        >>> test_run_wide_member_limit_halts_recursive_expansion(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Adopt nested ZIP bytes at the depth ceiling without creating its container view or cache Store.

    Assert one nested discovery, a truncated parent report, and only the source and outer ZIP
    Stores. The coordinator may still identify member format from names or probe bytes.

    Example:
        >>> test_depth_limit_catalogues_nested_bytes_without_opening_them(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject a configured cache directory contained within the source root before normal ingest.

    Use actual resolved temporary paths and check the actionable ValueError text; this does not
    exercise symlink-race resistance.

    Example:
        >>> test_materialization_root_inside_source_is_rejected(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Report missing nested materialization configuration and allow top-level-only ingestion when
    recursion is disabled.

    Separate in-memory managers process the same real ZIP tree. Both adopt the inner ZIP bytes,
    while the nonrecursive run remains successful with two Stores.

    Example:
        >>> test_nested_container_without_cache_is_actionable_and_can_be_disabled(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Apply one logical expanded-byte budget across two separate ZIP containers.

    With two three-byte members and a five-byte budget, both container Stores are processed but only
    one member is adopted and charged.

    Example:
        >>> test_cumulative_expanded_byte_limit_spans_separate_archives(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Continue adopting source files after the scheduled-container ceiling refuses another ZIP.

    Assert both sources are adopted, only one container is admitted/processed, and a single
    container-limit issue is retained.

    Example:
        >>> test_container_limit_still_catalogues_remaining_top_level_files(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject excessive ZIP expansion ratio and a parent-traversing member while preserving source
    adoption.

    Real ZIP backend validation fails before members are adopted. The filesystem assertion checks
    the specific expected escape path remains absent; it is not a monitor of every possible host
    write.

    Example:
        >>> test_zip_bomb_ratio_and_traversal_are_rejected_without_host_writes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Honor a truthy cancellation callback before source discovery or manager configuration writes.

    Assert the halt reason, zero examined/adopted files, absent source Store, and empty Store/Asset
    collections in the in-memory manager.

    Example:
        >>> test_pre_cancelled_run_does_not_create_a_source_store(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Pass parent format, depth, ancestry, and member key to a custom metadata hook.

    A real ZIP member is adopted with the hook-provided name and exact attribute mapping,
    demonstrating that the custom factory supplies the metadata rather than merging default
    provenance.

    Example:
        >>> test_member_metadata_hook_receives_container_context(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
    source = tmp_path / "source"
    source.mkdir()
    (source / "pack.zip").write_bytes(_zip_bytes({"books/original.txt": b"book"}))
    observed: list[tuple[ContainerMemberContext, str]] = []

    def metadata(
        context: ContainerMemberContext,
        entry: api.StoreInventoryEntry,
    ) -> api.DigitalAssetMetadata:
        """
        Record the supplied parent context/member key and return deterministic enriched metadata.

        Append to the test closure before constructing the value; omit built-in provenance so the
        enclosing assertion can verify the hook result exactly.

        Example:
            >>> enriched = metadata(context, entry)  # doctest: +SKIP


        :param context: Immediate parent container facts passed by the coordinator.
        :param entry: Real ZIP inventory entry supplying the Location key.
        :return: Named metadata with the single enrichment.test attribute.
        """
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
    """
    Recognize each built-in container format from minimal synthetic signature bytes.

    Discovery-only mode classifies six .bin files. These prefixes are not valid archives and do not
    test extractor installation or full backend parsing.

    Example:
        >>> test_discovery_magic_registry_covers_every_builtin_container(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Keep EPUB-named ZIP bytes terminal by default and expand them only with the explicit option.

    Use fresh in-memory managers for the two policies. The fixture tests container routing, not EPUB
    package validity.

    Example:
        >>> test_ebook_container_expansion_is_explicit_opt_in(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Preserve Unicode and POSIX undecodable names through real TAR adoption and member reads.

    Read back the bad-byte member payload and recover the exact raw source basename from its
    UNMANAGED Replica key. The decorator skips non-POSIX platforms.

    Example:
        >>> test_tortured_unicode_and_undecodable_container_paths_remain_readable(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Attach the supplied run UUID and object details to captured ingest events and per-object
    checkpoints.

    Capture DEBUG logs for a real single-member ZIP with checkpoint interval one. Assertions require
    event presence and selected member fields, not a complete event order or timing contract.

    Example:
        >>> test_ingest_emits_correlated_object_events_and_checkpoints(tmp_path, caplog)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :param caplog: Pytest log-capture fixture restoring logger capture settings after the test.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Retain traceback and run/path context when a broken ZIP becomes a recoverable container issue.

    Capture the real logger at DEBUG with an in-memory manager; assert the returned failure report
    and the container_error record metadata.

    Example:
        >>> test_recoverable_container_failure_logs_a_traceback(tmp_path, caplog)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for real source/archive/cache bytes and any explicit SQLite catalogue.
    :param caplog: Pytest log-capture fixture restoring logger capture settings after the test.
    :return: None after the stated regression assertions pass.
    """
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
