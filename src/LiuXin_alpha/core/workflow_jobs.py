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
    from LiuXin_alpha.storage.api.backup_api import (
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
    from LiuXin_alpha.storage.api.backup_api import BackupWorkflowSpec
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


def run_persisted_backup_job(
    *,
    database_path: str,
    db_type: str,
    workflow_id: int,
) -> dict[str, Any]:
    """Resume and checkpoint one database-backed backup workflow."""

    from LiuXin_alpha.library import Library
    from LiuXin_alpha.storage.api.backup_api import BackupWorkflowStatus
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
]
