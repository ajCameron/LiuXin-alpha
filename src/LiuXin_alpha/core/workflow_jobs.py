"""Importable worker entry points used by named Core workflow commands."""

from __future__ import annotations

import dataclasses

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (str, bytes, bool, int, float)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _plain(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    method = getattr(value, "to_dict", None)
    if callable(method):
        return _plain(method())
    method = getattr(value, "to_mapping", None)
    if callable(method):
        return _plain(method())
    method = getattr(value, "to_calibre", None)
    if callable(method):
        converted = method()
        if converted is not value:
            return _plain(converted)
    method = getattr(value, "all_non_none_fields", None)
    if callable(method):
        return _plain(method())
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_plain(item) for item in value]
    return value


def run_ingest_disk_job(
    *,
    database_path: str,
    db_type: str,
    disk_path: str,
    store_name: str | None = None,
    ebook_extensions: list[str] | None = None,
    source_label: str = "on_disk_unmanaged_import",
    compute_hash: bool = True,
    follow_symlinks: bool = False,
    attach_store_links: bool = True,
    refresh_storage_manager: bool = True,
) -> dict[str, Any]:
    """Register an existing local disk and its ebook inventory."""

    from LiuXin_alpha.library import Library

    plain: Any = None
    with Library(
        database_path=database_path,
        db_type=db_type,
        create=False,
        backup=False,
    ) as library:
        report = library.register_unmanaged_disk(
            disk_path=disk_path,
            store_name=store_name,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            compute_hash=compute_hash,
            follow_symlinks=follow_symlinks,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
        )
        plain = _plain(report)
    if not isinstance(plain, dict):
        raise TypeError("Ingest report did not serialize to an object")
    return cast(dict[str, Any], plain)


def run_sync_store_job(
    *,
    database_path: str,
    db_type: str,
    mode: str,
    store_root_uri: str,
    store_name: str | None,
    store_kind: str,
    source_label: str,
    ebook_extensions: list[str] | None,
    compute_hash: bool,
    capture_hashes: bool,
    follow_symlinks: bool,
    attach_store_links: bool,
    refresh_storage_manager: bool,
    max_http_requests_per_hour: float | None,
    rclone_args: tuple[str, ...],
    crawler_recurse: bool,
    crawler_max_depth: int | None,
    crawler_timeout_s: float | None,
    crawler_no_parent: bool,
    crawler_span_hosts: bool,
    crawler_respect_robots: bool,
    crawler_user_agent: str | None,
    wget_no_verbose: bool,
    wget_args: tuple[str, ...],
    crawler_incremental_db_writes: bool = True,
    progress_output: bool = True,
    progress_every: int = 100,
) -> dict[str, Any]:
    """Reconcile one configured store in an importable Core worker."""

    from LiuXin_alpha.ingest import (
        register_native_html_readonly_with_database_path,
        register_wget_html_readonly_with_database_path,
    )
    from LiuXin_alpha.storage.reconcile import (
        register_existing_disk_with_database_path,
        register_rclone_http_readonly_with_database_path,
    )

    def progress(event: str, report: Any, details: Mapping[str, Any]) -> None:
        if not progress_output:
            return
        scanned = int(getattr(report, "scanned_files", 0) or 0)
        observed = int(getattr(report, "crawler_urls_observed", 0) or 0)
        if event in {"scan", "crawl-observation"}:
            tick = max(scanned, observed)
            if tick not in {0, 1} and tick % max(1, int(progress_every)):
                return
        if event == "crawl-log":
            line = str(details.get("line") or "").strip()
            if line:
                print("JOB sync: {}".format(line), flush=True)
            return
        if event in {"start", "scan", "crawl-observation", "error", "done"}:
            print(
                "JOB sync {}: scanned={} candidates={} inserted={} updated={} "
                "unchanged={} linked={} errors={}".format(
                    event,
                    scanned,
                    int(getattr(report, "ebook_candidates", 0) or 0),
                    int(getattr(report, "inserted_files", 0) or 0),
                    int(getattr(report, "updated_files", 0) or 0),
                    int(getattr(report, "unchanged_files", 0) or 0),
                    int(getattr(report, "linked_files", 0) or 0),
                    len(getattr(report, "errors", ()) or ()),
                ),
                flush=True,
            )

    callback = progress if progress_output else None
    normalized = str(mode or "").strip().lower()
    if normalized == "rclone":
        report = register_rclone_http_readonly_with_database_path(
            database_path=database_path,
            remote_url=store_root_uri,
            db_type=db_type,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            rclone_args=rclone_args,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            capture_hashes=capture_hashes,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            progress_callback=callback,
        )
    elif normalized == "wget":
        report = register_wget_html_readonly_with_database_path(
            database_path=database_path,
            remote_url=store_root_uri,
            db_type=db_type,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            wget_args=wget_args,
            timeout_s=crawler_timeout_s,
            recurse=crawler_recurse,
            max_depth=crawler_max_depth,
            no_parent=crawler_no_parent,
            span_hosts=crawler_span_hosts,
            respect_robots=crawler_respect_robots,
            user_agent=crawler_user_agent,
            no_verbose=wget_no_verbose,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            incremental_db_writes=bool(crawler_incremental_db_writes),
            progress_callback=callback,
        )
    elif normalized == "native":
        report = register_native_html_readonly_with_database_path(
            database_path=database_path,
            remote_url=store_root_uri,
            db_type=db_type,
            store_name=store_name,
            store_kind=store_kind,
            max_http_requests_per_hour=max_http_requests_per_hour,
            timeout_s=crawler_timeout_s,
            recurse=crawler_recurse,
            max_depth=crawler_max_depth,
            no_parent=crawler_no_parent,
            span_hosts=crawler_span_hosts,
            respect_robots=crawler_respect_robots,
            user_agent=crawler_user_agent,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            incremental_db_writes=bool(crawler_incremental_db_writes),
            progress_callback=callback,
        )
    elif normalized == "local":
        report = register_existing_disk_with_database_path(
            database_path=database_path,
            disk_path=store_root_uri,
            db_type=db_type,
            store_name=store_name,
            store_kind=store_kind,
            ebook_extensions=ebook_extensions,
            source_label=source_label,
            compute_hash=compute_hash,
            follow_symlinks=follow_symlinks,
            attach_store_links=attach_store_links,
            refresh_storage_manager=refresh_storage_manager,
            progress_callback=callback,
        )
    else:
        raise ValueError("Unknown sync mode: {!r}".format(mode))

    plain = _plain(report)
    if not isinstance(plain, dict):
        raise TypeError("Sync report did not serialize to an object")
    return cast(dict[str, Any], plain)


def run_ingest_remote_html_job(
    *,
    database_path: str,
    db_type: str,
    kind: str,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    """Register a wget/native remote HTML source and its discovered files."""

    from LiuXin_alpha.library import Library

    token = str(kind).strip().lower().replace("-", "_")
    methods = {
        "wget_html": "register_wget_html_store",
        "native_html": "register_native_html_store",
    }
    method_name = methods.get(token)
    if method_name is None:
        raise ValueError("Unsupported remote HTML ingest kind: {!r}".format(kind))
    plain: Any = None
    with Library(
        database_path=database_path,
        db_type=db_type,
        create=False,
        backup=False,
    ) as library:
        method = getattr(library, method_name)
        report = method(**dict(options))
        plain = _plain(report)
    if not isinstance(plain, dict):
        raise TypeError("Remote ingest report did not serialize to an object")
    return cast(dict[str, Any], plain)


def run_conversion_job(
    *,
    input_path: str,
    output_path: str,
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Convert one ebook using the native conversion pipeline."""

    from LiuXin_alpha.customize.conversion import OptionRecommendation
    from LiuXin_alpha.file_formats.conversion.plumber import Plumber
    from LiuXin_alpha.utils.logging import default_log

    source = str(Path(input_path).expanduser())
    destination = str(Path(output_path).expanduser())
    plumber = Plumber(source, destination, default_log)
    recommendations = [
        (str(name), value, OptionRecommendation.HIGH)
        for name, value in dict(options or {}).items()
    ]
    if recommendations:
        plumber.merge_ui_recommendations(recommendations)
    plumber.run()
    output = Path(destination)
    return {
        "input_path": source,
        "output_path": destination,
        "input_format": getattr(plumber, "input_fmt", None),
        "output_format": getattr(plumber, "output_fmt", None),
        "exists": output.exists(),
        "size_bytes": (
            output.stat().st_size
            if output.exists() and output.is_file()
            else None
        ),
    }


def backup_workflow_spec_from_mapping(payload: Mapping[str, Any]) -> Any:
    from LiuXin_alpha.storage.api.workflow_apis.backup_api import (
        BackupSourceKind,
        BackupSourceSpec,
        BackupWorkflowKind,
        BackupWorkflowSpec,
    )

    raw_sources = payload.get("sources", ())
    if not isinstance(raw_sources, (list, tuple)):
        raise TypeError("workflow_spec.sources must be an array")
    sources = []
    for raw in raw_sources:
        if not isinstance(raw, Mapping):
            raise TypeError("Every workflow source must be an object")
        sources.append(
            BackupSourceSpec(
                source_kind=BackupSourceKind(str(raw["source_kind"])),
                source_identifier=str(raw["source_identifier"]),
                archive_path=(
                    None
                    if raw.get("archive_path") is None
                    else str(raw["archive_path"])
                ),
                expected_size=(
                    None
                    if raw.get("expected_size") is None
                    else int(cast(Any, raw["expected_size"]))
                ),
                expected_hash=(
                    None
                    if raw.get("expected_hash") is None
                    else str(raw["expected_hash"])
                ),
                source_file_id=(
                    None
                    if raw.get("source_file_id") is None
                    else int(cast(Any, raw["source_file_id"]))
                ),
                source_asset_replica_id=(
                    None
                    if raw.get("source_asset_replica_id") is None
                    else int(cast(Any, raw["source_asset_replica_id"]))
                ),
                source_store_id=(
                    None
                    if raw.get("source_store_id") is None
                    else int(cast(Any, raw["source_store_id"]))
                ),
            )
        )
    raw_options = payload.get("options", ())
    if isinstance(raw_options, Mapping):
        options = tuple(
            (str(key), str(value))
            for key, value in sorted(raw_options.items())
        )
    else:
        options = tuple(
            (str(item[0]), str(item[1]))
            for item in raw_options
        )
    return BackupWorkflowSpec(
        workflow_name=str(payload["workflow_name"]),
        workflow_kind=BackupWorkflowKind(str(payload["workflow_kind"])),
        output_url=str(payload["output_url"]),
        sources=tuple(sources),
        verify_after_build=bool(payload.get("verify_after_build", True)),
        cleanup_staging_after_success=bool(
            payload.get("cleanup_staging_after_success", False)
        ),
        staging_root=(
            None
            if payload.get("staging_root") is None
            else str(payload["staging_root"])
        ),
        options=options,
    )


def run_squashfs_backup_job(
    *,
    database_path: str,
    db_type: str,
    workflow_spec: Mapping[str, Any],
    verify_after_build: bool = True,
    cleanup_staging_after_success: bool = False,
    staging_root: str | None = None,
) -> dict[str, Any]:
    """Execute a serializable SquashFS backup workflow specification."""

    from LiuXin_alpha.library import Library
    from LiuXin_alpha.storage.api.workflow_apis.backup_api import BackupWorkflowSpec
    from LiuXin_alpha.storage.backup import SquashfsBackupWorkflow

    spec: BackupWorkflowSpec = backup_workflow_spec_from_mapping(workflow_spec)
    if (
        spec.verify_after_build != bool(verify_after_build)
        or spec.cleanup_staging_after_success
        != bool(cleanup_staging_after_success)
        or staging_root is not None
    ):
        spec = dataclasses.replace(
            spec,
            verify_after_build=bool(verify_after_build),
            cleanup_staging_after_success=bool(
                cleanup_staging_after_success
            ),
            staging_root=(
                str(staging_root)
                if staging_root is not None
                else spec.staging_root
            ),
        )

    plain: Any = None
    with Library(
        database_path=database_path,
        db_type=db_type,
        create=False,
        backup=False,
    ) as library:
        workflow = SquashfsBackupWorkflow.from_spec(
            spec,
            location_loader=lambda file_url: library.retrieve_file(
                file_url=file_url
            ),
        )
        result = workflow.run_to_completion()
        plain = _plain(result)
    if not isinstance(plain, dict):
        raise TypeError("Backup result did not serialize to an object")
    return cast(dict[str, Any], plain)


def run_publish_open_squashfs_store_job(
    *,
    database_path: str,
    db_type: str,
    store_id: int,
    output_archive: str | None = None,
    compression: str = "zstd",
    deterministic: bool = False,
    force: bool = False,
    duplicate_verified_files: bool = True,
    strict: bool = False,
    refresh_storage_manager: bool = True,
) -> dict[str, Any]:
    """Publish one designated open SquashFS store through a Core job."""

    from LiuXin_alpha.library import Library

    plain: Any = None
    with Library(
        database_path=database_path,
        db_type=db_type,
        create=False,
        backup=False,
    ) as library:
        report = library.publish_open_squashfs_store(
            store_id=int(store_id),
            output_archive=output_archive,
            compression=str(compression),
            deterministic=bool(deterministic),
            force=bool(force),
            duplicate_verified_files=bool(duplicate_verified_files),
            strict=bool(strict),
            refresh_storage_manager=bool(refresh_storage_manager),
        )
        plain = _plain(report)
    if not isinstance(plain, dict):
        raise TypeError("SquashFS publish report did not serialize to an object")
    return cast(dict[str, Any], plain)


def run_publish_squashfs_files_job(
    *,
    database_path: str,
    db_type: str,
    file_ids: list[int],
    archive: str,
    store_name: str | None = None,
    compression: str = "zstd",
    deterministic: bool = False,
    force: bool = False,
    strict: bool = False,
    refresh_storage_manager: bool = True,
) -> dict[str, Any]:
    """Designate file ids and publish them through one Core job."""

    from LiuXin_alpha.library import Library

    plain: Any = None
    with Library(
        database_path=database_path,
        db_type=db_type,
        create=False,
        backup=False,
    ) as library:
        report = library.publish_squashfs_archive_from_file_ids(
            file_ids=[int(value) for value in file_ids],
            archive_path=str(archive),
            store_name=store_name,
            compression=str(compression),
            deterministic=bool(deterministic),
            force=bool(force),
            strict=bool(strict),
            refresh_storage_manager=bool(refresh_storage_manager),
        )
        plain = _plain(report)
    if not isinstance(plain, dict):
        raise TypeError("SquashFS publish report did not serialize to an object")
    return cast(dict[str, Any], plain)


def run_persisted_backup_job(
    *,
    database_path: str,
    db_type: str,
    workflow_id: int,
) -> dict[str, Any]:
    """Resume and checkpoint one database-backed backup workflow."""

    from LiuXin_alpha.library import Library
    from LiuXin_alpha.storage.api.workflow_apis.backup_api import BackupWorkflowStatus
    from LiuXin_alpha.storage.backup import (
        BackupArtifactRegistry,
        BackupWorkflowRepository,
        SquashfsBackupWorkflow,
    )

    result_payload: dict[str, Any] | None = None
    with Library(
        database_path=database_path,
        db_type=db_type,
        create=False,
        backup=False,
    ) as library:
        repository = BackupWorkflowRepository(library.db)
        resume_state = repository.load_resume_state(int(workflow_id))
        workflow = SquashfsBackupWorkflow.from_resume_state(
            resume_state,
            location_loader=lambda file_url: library.retrieve_file(
                file_url=file_url
            ),
        )
        state = workflow.progress()
        repository.save_resume_state(int(workflow_id), state)
        while state.status not in {
            BackupWorkflowStatus.COMPLETE,
            BackupWorkflowStatus.FAILED,
            BackupWorkflowStatus.CANCELLED,
        }:
            state = workflow.run_next()
            repository.save_resume_state(int(workflow_id), state)

        registered: dict[str, Any] | None = None
        if (
            state.status is BackupWorkflowStatus.COMPLETE
            and state.output_artifact_url
        ):
            registration = BackupArtifactRegistry(
                library.db
            ).register_workflow_output_as_store(
                int(workflow_id),
                artifact_url=state.output_artifact_url,
                link_sources=True,
            )
            registered_plain = _plain(registration)
            if isinstance(registered_plain, dict):
                registered = cast(dict[str, Any], registered_plain)

        result_payload = {
            "workflow_id": int(workflow_id),
            "state": _plain(state),
            "registered_output": registered,
        }
    if result_payload is None:
        raise RuntimeError("Backup workflow did not produce a result.")
    return result_payload


def run_metadata_identify_job(
    *,
    title: str | None = None,
    authors: list[str] | None = None,
    identifiers: Mapping[str, str] | None = None,
    timeout: float = 30.0,
    allowed_plugins: list[str] | None = None,
) -> dict[str, Any]:
    """Query configured online metadata sources."""

    from threading import Event

    from LiuXin_alpha.metadata import metadata_to_opf_bytes
    from LiuXin_alpha.metadata.web_sources.identify import identify
    from LiuXin_alpha.metadata.web_sources.worker import GUILog

    log = GUILog()  # type: ignore[no-untyped-call]
    results = identify(  # type: ignore[no-untyped-call]
        log,
        Event(),
        title=title,
        authors=authors,
        identifiers=dict(identifiers or {}),
        timeout=max(1, int(round(timeout))),
        allowed_plugins=allowed_plugins,
    )
    return {
        "results": [
            {
                "metadata": _plain(result.all_non_none_fields()),
                "opf": metadata_to_opf_bytes(result),
                "has_cached_cover": bool(
                    getattr(result, "has_cached_cover_url", False)
                ),
            }
            for result in results
        ],
        "count": len(results),
        "log": log.dump(),
    }


def run_metadata_cover_job(
    *,
    title: str | None = None,
    authors: list[str] | None = None,
    identifiers: Mapping[str, str] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Download the best cover from configured online sources."""

    from LiuXin_alpha.metadata.web_sources.covers import download_cover
    from LiuXin_alpha.metadata.web_sources.worker import GUILog

    log = GUILog()  # type: ignore[no-untyped-call]
    result = download_cover(
        log,
        title=title,
        authors=authors,
        identifiers=dict(identifiers or {}),
        timeout=float(timeout),
    )
    if result is None:
        return {"found": False, "cover": None, "log": log.dump()}
    plugin, width, height, image_format, data = result
    if isinstance(data, bytes):
        content = data
    elif isinstance(data, str):
        content = data.encode("utf-8")
    else:
        content = bytes(cast(Any, data))
    return {
        "found": True,
        "cover": {
            "source": str(getattr(plugin, "name", type(plugin).__name__)),
            "width": int(width),
            "height": int(height),
            "format": str(image_format),
            "content": content,
        },
        "log": log.dump(),
    }


__all__ = [
    "backup_workflow_spec_from_mapping",
    "run_conversion_job",
    "run_ingest_disk_job",
    "run_ingest_remote_html_job",
    "run_metadata_cover_job",
    "run_metadata_identify_job",
    "run_persisted_backup_job",
    "run_squashfs_backup_job",
    "run_publish_open_squashfs_store_job",
    "run_publish_squashfs_files_job",
    "run_sync_store_job",
]
