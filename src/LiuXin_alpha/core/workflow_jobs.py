"""
Run named Core jobs using fresh library handles and lazily imported workflow implementations.

Database-backed workers open an existing library with creation and automatic
backup disabled, and close their handle before returning. They do not add a
transaction around filesystem, network, and database effects. Report conversion
is recursive but is not a JSON encoder: bytes and unsupported objects can remain.
Worker exceptions propagate to the job runner; returned report-level errors are
not automatically raised here.
"""

from __future__ import annotations

import dataclasses

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast
from uuid import UUID


def _plain(value: Any) -> Any:
    """
    Recursively unpack recognized report containers without requiring JSON-compatible leaves.

    Mappings take precedence over dataclass instances; conversion hooks are tried
    as ``to_dict``, ``to_mapping``, ``to_calibre``, then ``all_non_none_fields``.
    Only the Calibre hook checks for returning the original object. There is no
    cycle detection. Mapping keys are stringified, so distinct keys can collide;
    sets become lists in iteration order. Unsupported values pass through intact.

    Example:
        >>> _plain({1: (b"payload", {"ok": True})})
        {'1': [b'payload', {'ok': True}]}
        >>> marker = object()
        >>> _plain(marker) is marker
        True


    :param value: Scalar, nested report, dataclass instance, or object with a supported conversion hook.
    :return: Converted dictionaries/lists and unchanged scalar or unsupported leaves.
    """
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
    """
    Open an existing library, register an unmanaged disk inventory, and unpack its report.

    Scanning, hashing, database writes, and optional storage refresh belong to
    the library operation. This wrapper neither copies the books nor introduces
    a rollback boundary. Report conversion happens before the library closes;
    the top-level dictionary check happens afterward.

    Example:
        >>> report = run_ingest_disk_job(  # doctest: +SKIP
        ...     database_path="library.sqlite", db_type="SQLite", disk_path="books",
        ... )


    :param database_path: Existing database location understood by the selected driver.
    :param db_type: Library database driver selector.
    :param disk_path: Local directory to register and scan for ebook candidates.
    :param store_name: Requested store name, or ``None`` for the registration helper's default.
    :param ebook_extensions: Extension filter, or ``None`` for the scanner's default ebook set.
    :param source_label: Provenance label forwarded to inventory registration.
    :param compute_hash: Whether the local scanner should compute file hashes.
    :param follow_symlinks: Whether the scanner may follow filesystem symlinks.
    :param attach_store_links: Whether registered files should receive store links.
    :param refresh_storage_manager: Whether registration should refresh loaded storage state.
    :return: Recursively unpacked registration report, possibly containing per-file errors.
    :raises TypeError: If the completed report does not convert to a dictionary.
    """

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
    """
    Dispatch one store scan to its local, rclone, wget, or native-HTML reconciler.

    Mode matching strips and lowercases the token. Backend-specific options are
    forwarded only on the corresponding branch; unused options are not rejected.
    This wrapper does not independently validate paths, rates, or timeouts, and
    does not interpret report errors as job failure. Optional progress writes
    flushed lines to stdout for the job runner to capture.

    Example:
        >>> report = run_sync_store_job(**worker_options)  # doctest: +SKIP


    :param database_path: Existing database location passed to the selected reconciler.
    :param db_type: Database driver selector for the reconciler's library handle.
    :param mode: ``local``, ``rclone``, ``wget``, or ``native`` dispatch token.
    :param store_root_uri: Local disk path or remote root URL, according to mode.
    :param store_name: Requested store name, or ``None`` for backend defaults.
    :param store_kind: Store-kind metadata passed through without cross-checking mode.
    :param source_label: Provenance label for registered inventory.
    :param ebook_extensions: Candidate extension filter, or ``None`` for backend defaults.
    :param compute_hash: Local-only request to hash scanned files.
    :param capture_hashes: Rclone-only request to retain hashes from listing metadata.
    :param follow_symlinks: Local-only symlink traversal choice.
    :param attach_store_links: Whether the reconciler should link registered files to the store.
    :param refresh_storage_manager: Whether the reconciler should refresh loaded storage afterward.
    :param max_http_requests_per_hour: Remote-only rate policy; ``None`` delegates default selection.
    :param rclone_args: Additional listing arguments used only in rclone mode.
    :param crawler_recurse: Whether wget/native crawlers should traverse discovered pages.
    :param crawler_max_depth: Wget/native traversal depth limit, or ``None`` for no supplied limit.
    :param crawler_timeout_s: Wget/native timeout in seconds, or ``None`` as understood by that crawler.
    :param crawler_no_parent: Wget/native request to stay below the starting path.
    :param crawler_span_hosts: Wget/native cross-host traversal permission.
    :param crawler_respect_robots: Wget/native robots-policy selection.
    :param crawler_user_agent: Wget/native user-agent override, or ``None`` for its default.
    :param wget_no_verbose: Wget-only verbosity-suppression flag.
    :param wget_args: Additional wget-only arguments, preserved in supplied order.
    :param crawler_incremental_db_writes: Wget/native request to persist observations incrementally.
    :param progress_output: Whether to install the stdout progress callback.
    :param progress_every: Scan/observation reporting interval, clamped to at least one when used.
    :return: Recursively unpacked dictionary report from the chosen reconciler.
    :raises ValueError: For an unknown mode after normalization.
    :raises TypeError: If the returned report does not convert to a dictionary.
    """

    from LiuXin_alpha.ingest import (
        register_native_html_readonly_with_database_path,
        register_wget_html_readonly_with_database_path,
    )
    from LiuXin_alpha.storage.reconcile import (
        register_existing_disk_with_database_path,
        register_rclone_http_readonly_with_database_path,
    )

    def progress(event: str, report: Any, details: Mapping[str, Any]) -> None:
        """
        Print recognized progress events, throttling scan ticks but not crawler log lines.

        Scan/observation ticks use the larger scanned/observed count; zero, one,
        and multiples of the clamped interval are printed. Log text is stripped
        and prefixed but not otherwise sanitized. Unknown events produce no line,
        although counter conversion still happens when output is enabled.

        Example:
            >>> progress("crawl-log", report, {"line": "Fetched page"})  # doctest: +SKIP


        :param event: Reconciler event name selecting a summary, log line, or no output.
        :param report: Attribute-based cumulative counters and errors used by summary events.
        :param details: Event metadata; crawler logs read its optional ``line`` field.
        :return: ``None`` after optional flushed stdout output.
        """
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
    """
    Select a remote HTML library registration method and invoke it with copied keyword options.

    Kind matching strips, lowercases, and changes hyphens to underscores. Only
    ``wget_html`` and ``native_html`` are supported. Option names/values are not
    filtered here; the selected method owns validation and network/database work.

    Example:
        >>> run_ingest_remote_html_job(database_path="unused", db_type="SQLite", kind="other", options={})
        Traceback (most recent call last):
        ...
        ValueError: Unsupported remote HTML ingest kind: 'other'


    :param database_path: Existing database location used to open a temporary library handle.
    :param db_type: Library database driver selector.
    :param kind: Wget/native HTML registration token, with hyphenated spellings accepted.
    :param options: Keyword arguments shallow-copied into the chosen registration call.
    :return: Unpacked dictionary report after the library handle closes.
    :raises ValueError: If the kind does not select a supported registration method.
    :raises TypeError: For invalid keyword arguments or a report that is not dictionary-shaped.
    """

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
    """
    Run the native ebook converter with high-priority option recommendations and report output metadata.

    Paths expand home notation but are not resolved to absolute paths. Conversion
    and output replacement policies belong to ``Plumber``; this wrapper supplies
    no staging/rollback layer. A normal converter return need not mean an output
    exists: the result records a post-run filesystem observation, not validation
    of ebook contents. Stat errors and conversion exceptions propagate.

    Example:
        >>> report = run_conversion_job(input_path="book.epub", output_path="book.txt")  # doctest: +SKIP


    :param input_path: Source ebook path, expanded before constructing the converter.
    :param output_path: Requested destination path, expanded without creating its parent here.
    :param options: Optional name/value recommendations; names are stringified and assigned HIGH priority.
    :return: Expanded paths, converter format labels, output existence, and byte size for an observed regular file.
    """

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
            output.stat().st_size if output.exists() and output.is_file() else None
        ),
    }


def backup_workflow_spec_from_mapping(payload: Mapping[str, Any]) -> Any:
    """
    Decode backup intent with legacy field aliases and declaration-level validation.

    This is coercion, not strict JSON-schema or filesystem validation. Nonmapping
    references become strings; mapping references require a UUID and key. Current
    output/staging fields override aliases even when explicitly ``None``. Boolean
    flags use Python truthiness, so a nonempty string such as ``"false"`` is true.

    Sources must be a list/tuple of mappings. A mapping-valued expected digest
    wins over legacy ``expected_hash``; otherwise that hash means SHA-256. Options
    accept a mapping or iterable of indexable pairs. Declaration constructors
    validate their own names, paths, sizes, and uniqueness constraints; unknown
    payload keys are ignored and no backing files or stores are inspected.

    Example:
        >>> spec = backup_workflow_spec_from_mapping({
        ...     "workflow_name": "nightly", "workflow_kind": "squashfs_pack",
        ...     "output_url": "nightly.sqsh", "options": {"compression": "zstd"},
        ... })
        >>> spec.output_target, spec.option_map()
        ('nightly.sqsh', {'compression': 'zstd'})


    :param payload: Workflow name/kind, output reference, optional sources/staging/flags/options, and legacy aliases.
    :return: Immutable backup declaration with normalized nested values, without executing a workflow.
    :raises KeyError: For a required mapping field absent from the workflow, source, or digest.
    :raises TypeError: For invalid source containers, incomplete location mappings, or missing output target.
    :raises ValueError: For invalid enum/UUID/numeric conversions or declaration invariants.
    """

    from LiuXin_alpha.storage.api import (
        BackupSourceKind,
        BackupSourceDeclaration,
        BackupWorkflowKind,
        BackupWorkflowDeclaration,
        Digest,
        Location,
    )

    def reference(value: Any) -> str | Location:
        """
        Build a managed Location from a mapping, or stringify a nonmapping reference.

        ``store_ref`` wins over legacy ``store_uuid`` even if empty. Presence is
        checked before string conversion; UUID parsing and Location key validation
        then run. String references are not expanded, resolved, or existence-checked.

        Example:
            >>> reference({"store_uuid": "00000000-0000-0000-0000-000000000001", "key": "pack"})  # doctest: +SKIP


        :param value: Path-like value or mapping with a store UUID reference and opaque key.
        :return: Stringified value or validated Location, without resolving it against a manager.
        :raises TypeError: If a mapping omits or empties its store reference or key.
        :raises ValueError: For an invalid UUID or invalid Location key.
        """
        if not isinstance(value, Mapping):
            return str(value)
        store_ref = value.get("store_ref", value.get("store_uuid"))
        key = value.get("key")
        if store_ref in (None, "") or key in (None, ""):
            raise TypeError("Location references require `store_uuid` and `key`.")
        return Location(UUID(str(store_ref)), str(key))

    raw_sources = payload.get("sources", ())
    if not isinstance(raw_sources, (list, tuple)):
        raise TypeError("workflow_spec.sources must be an array")
    sources = []
    for raw in raw_sources:
        if not isinstance(raw, Mapping):
            raise TypeError("Every workflow source must be an object")
        source_kind = BackupSourceKind(str(raw["source_kind"]))
        identifier = reference(raw["source_identifier"])
        expected_digest_raw = raw.get("expected_digest")
        expected_digest = None
        if isinstance(expected_digest_raw, Mapping):
            expected_digest = Digest(
                str(expected_digest_raw.get("algorithm", "sha256")),
                str(expected_digest_raw["value"]),
            )
        elif raw.get("expected_hash") is not None:
            expected_digest = Digest("sha256", str(raw["expected_hash"]))
        sources.append(
            BackupSourceDeclaration(
                source_kind=source_kind,
                source_identifier=identifier,
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
                expected_digest=expected_digest,
                source_digital_asset_id=(
                    None
                    if raw.get("source_digital_asset_id") is None
                    else int(cast(Any, raw["source_digital_asset_id"]))
                ),
                source_replica_id=(
                    None
                    if raw.get("source_asset_replica_id") is None
                    else int(cast(Any, raw["source_asset_replica_id"]))
                ),
            )
        )
    raw_options = payload.get("options", ())
    if isinstance(raw_options, Mapping):
        options = tuple(
            (str(key), str(value)) for key, value in sorted(raw_options.items())
        )
    else:
        options = tuple((str(item[0]), str(item[1])) for item in raw_options)
    output_raw = payload.get("output_target", payload.get("output_url"))
    if output_raw is None:
        raise TypeError("workflow_spec requires `output_target`.")
    staging_raw = payload.get("staging_target", payload.get("staging_root"))
    return BackupWorkflowDeclaration(
        workflow_name=str(payload["workflow_name"]),
        workflow_kind=BackupWorkflowKind(str(payload["workflow_kind"])),
        output_target=reference(output_raw),
        sources=tuple(sources),
        verify_after_build=bool(payload.get("verify_after_build", True)),
        cleanup_staging_after_success=bool(
            payload.get("cleanup_staging_after_success", False)
        ),
        staging_target=(None if staging_raw is None else reference(staging_raw)),
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
    """
    Decode backup intent, apply explicit execution overrides, and run it with an existing library's storage manager.

    The function's verification/cleanup defaults override conflicting values in
    the mapping; omission does not preserve those mapped flags. A supplied staging
    root replaces even a managed staging Location with a string. This path does
    not load/save a workflow repository checkpoint or register an artifact through
    the separate registry. A returned failed/cancelled result is not raised here.

    Example:
        >>> report = run_squashfs_backup_job(  # doctest: +SKIP
        ...     database_path="library.sqlite", db_type="SQLite", workflow_spec=payload,
        ... )


    :param database_path: Existing database location used for the storage-enabled library handle.
    :param db_type: Library database driver selector.
    :param workflow_spec: Mapping decoded into a backup declaration before opening the library.
    :param verify_after_build: Effective verification flag, overriding the declaration after truth conversion.
    :param cleanup_staging_after_success: Effective success-cleanup flag, overriding the declaration.
    :param staging_root: Optional string staging replacement; ``None`` preserves the mapped staging target.
    :return: Unpacked dictionary result after synchronous workflow completion and library closure.
    :raises TypeError: If mapping decoding fails or the result does not convert to a dictionary.
    """

    from LiuXin_alpha.library import Library
    from LiuXin_alpha.storage.api import BackupWorkflowDeclaration
    from LiuXin_alpha.storage.backup import SquashfsBackupWorkflow

    spec: BackupWorkflowDeclaration = backup_workflow_spec_from_mapping(workflow_spec)
    if (
        spec.verify_after_build != bool(verify_after_build)
        or spec.cleanup_staging_after_success != bool(cleanup_staging_after_success)
        or staging_root is not None
    ):
        spec = dataclasses.replace(
            spec,
            verify_after_build=bool(verify_after_build),
            cleanup_staging_after_success=bool(cleanup_staging_after_success),
            staging_target=(
                str(staging_root) if staging_root is not None else spec.staging_target
            ),
        )

    plain: Any = None
    with Library(
        database_path=database_path,
        db_type=db_type,
        create=False,
        backup=False,
    ) as library:
        workflow = SquashfsBackupWorkflow.from_declaration(
            spec,
            storage_manager=library.storage,
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
    """
    Publish an existing designated SquashFS store using a fresh library handle.

    The library owns source checks, archive construction, store state, and optional
    file-row duplication. This wrapper coerces option types but does not validate
    IDs/ranges or roll back side effects if report conversion/closure fails. Report
    errors remain data unless the delegated strict policy raises them.

    Example:
        >>> report = run_publish_open_squashfs_store_job(  # doctest: +SKIP
        ...     database_path="library.sqlite", db_type="SQLite", store_id=7,
        ... )


    :param database_path: Existing database location containing the store and its designations.
    :param db_type: Library database driver selector.
    :param store_id: Store row ID converted to an integer for publication.
    :param output_archive: Optional output path override; ``None`` lets publication use the store root.
    :param compression: Compression label stringified for the archive builder.
    :param deterministic: Whether to request deterministic archive construction.
    :param force: Whether to forward the builder's forced-output-replacement option.
    :param duplicate_verified_files: Whether verified members should receive duplicated file rows in the published store.
    :param strict: Whether publication should raise its verification/persistence failures instead of only reporting them.
    :param refresh_storage_manager: Whether publication should refresh loaded storage state.
    :return: Unpacked publication report after the library closes, including any nonraised errors.
    :raises TypeError: If the completed report does not convert to a dictionary.
    """

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
    """
    Ask the library to ensure an open SquashFS store, designate file IDs, and publish the archive.

    File IDs are integer-coerced without deduplication or local range validation.
    The delegated convenience flow enables verified-file duplication. Designation
    and publication are not wrapped in an additional transaction here, and a
    returned report can contain errors without this worker raising them.

    Example:
        >>> report = run_publish_squashfs_files_job(  # doctest: +SKIP
        ...     database_path="library.sqlite", db_type="SQLite", file_ids=[1, 2], archive="books.sqsh",
        ... )


    :param database_path: Existing database location holding the source file rows.
    :param db_type: Library database driver selector.
    :param file_ids: Ordered file row identifiers converted to integers for designation.
    :param archive: Archive path stringified for the library publication helper.
    :param store_name: Optional name when ensuring the destination store.
    :param compression: Compression label forwarded as text to the archive builder.
    :param deterministic: Whether to request deterministic build settings.
    :param force: Whether to enable the archive builder's forced-replacement option.
    :param strict: Whether delegated verification/persistence failures should raise.
    :param refresh_storage_manager: Whether the publication helper should refresh loaded storage state.
    :return: Unpacked publication report after the library handle closes.
    :raises TypeError: If the completed report does not convert to a dictionary.
    """

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
    """
    Resume a saved backup, checkpoint each returned step, and register only a complete artifact.

    Saves the initial progress state, then each ``run_next`` result until terminal.
    There is no worker-level timeout, external cancellation event, or no-progress
    guard. Exceptions between a workflow effect and checkpoint save propagate;
    this adapter does not atomically couple those operations or undo either one.

    Complete results are registered with source links. Other terminal results are
    recorded in the repository without raising merely because they are incomplete.
    A registration that does not unpack to a dictionary becomes ``None`` in the
    response rather than raising a serialization error.

    Example:
        >>> result = run_persisted_backup_job(  # doctest: +SKIP
        ...     database_path="library.sqlite", db_type="SQLite", workflow_id=3,
        ... )


    :param database_path: Existing database containing the durable workflow/checkpoint records.
    :param db_type: Library database driver selector.
    :param workflow_id: Workflow row ID, integer-coerced for repository and registration calls.
    :return: Workflow ID, unpacked last checkpoint state, and optional registered-output dictionary.
    :raises RuntimeError: If no result payload was assembled before leaving the library context.
    """

    from LiuXin_alpha.library import Library
    from LiuXin_alpha.storage.api import WorkflowStatus
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
        checkpoint = repository.load_checkpoint(int(workflow_id))
        workflow = SquashfsBackupWorkflow.from_checkpoint(
            checkpoint,
            storage_manager=library.storage,
        )
        state = workflow.progress()
        repository.save_checkpoint(int(workflow_id), state)
        while not state.status.terminal:
            state = workflow.run_next()
            repository.save_checkpoint(int(workflow_id), state)

        registered: dict[str, Any] | None = None
        result = workflow.run_to_completion()
        if result.status is WorkflowStatus.COMPLETE:
            registration = BackupArtifactRegistry(
                library.db,
                storage_manager=library.storage,
            ).register_artifact(
                int(workflow_id),
                result,
                link_sources=True,
            )
            registered_plain = _plain(registration)
            if isinstance(registered_plain, dict):
                registered = cast(dict[str, Any], registered_plain)
        else:
            repository.record_result(int(workflow_id), result)

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
    """
    Query configured identification plugins and return metadata fields, OPF bytes, and captured logs.

    Each result is converted eagerly; one conversion failure aborts the response.
    A fresh unset cancellation event is supplied internally, not exposed to callers.
    Timeout is rounded to an integer and clamped to at least one second before
    delegation; this is not a separate wall-clock deadline enforced by the wrapper.
    Cached-cover flags indicate advertised availability, not downloaded cover bytes.

    Example:
        >>> result = run_metadata_identify_job(title="Example novel", allowed_plugins=[])  # doctest: +SKIP


    :param title: Optional book-title search hint passed to the identification pipeline.
    :param authors: Optional ordered author-name hints passed through unchanged.
    :param identifiers: Optional identifier mapping, shallow-copied or replaced with an empty dictionary.
    :param timeout: Requested plugin timeout in seconds, rounded and clamped before dispatch.
    :param allowed_plugins: Exact permitted plugin names, or ``None`` for all configured identification plugins.
    :return: Result list with unpacked metadata/OPF bytes/cache flags, its count, and the collected log dump.
    """

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
    """
    Download the cover selected by configured source priorities and return its bytes and metadata.

    The cover pipeline chooses among candidates; this worker does not rank them
    again or save an image file. Bytes pass through, text is UTF-8 encoded, and
    other content is passed to ``bytes``. Those conversions do not validate image
    contents. Timeout is float-coerced but not clamped or enforced separately here.

    Example:
        >>> result = run_metadata_cover_job(title="Example novel", timeout=10.0)  # doctest: +SKIP


    :param title: Optional title hint for configured cover sources.
    :param authors: Optional author-name hints forwarded to cover lookup.
    :param identifiers: Optional identifiers shallow-copied into the lookup call.
    :param timeout: Cover-pipeline timeout in seconds, converted to a float without range checks.
    :return: Found flag, optional source/dimensions/format/content dictionary, and captured log dump.
    """

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
