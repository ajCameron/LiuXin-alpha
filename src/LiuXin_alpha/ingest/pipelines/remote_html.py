"""
Register discovered remote addresses as catalogue files and optional Store links.

Discovery owns URL validation and crawl completeness. This pipeline classifies
suffixes and writes metadata without fetching ebook bytes, checking hashes, or
removing files absent from a crawl. Each successful write may survive a later
file, callback, or discovery failure; no all-run transaction is introduced here.
"""

from __future__ import annotations

import mimetypes
import pathlib
import time

from datetime import datetime
from typing import Callable, Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse

from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.ingest.models import RemoteHtmlRegistrationReport


ProgressCallback = Callable[[str, RemoteHtmlRegistrationReport, dict[str, object]], None]
_HTML_LIKE_EXTENSIONS = {"htm", "html", "htmlz", "xhtm", "xhtml"}


def _now_ep_ms() -> int:
    """
    Read Unix wall time in truncated milliseconds for registration timestamps.

    Example:
        >>> isinstance(_now_ep_ms(), int)
        True


    :return: Current epoch milliseconds, without a monotonicity guarantee.
    """
    return int(time.time() * 1000)


def _coerce_datetime_ep_ms(value: object) -> Optional[int]:
    """
    Convert nonempty ISO datetime text to epoch milliseconds when parsing succeeds.

    Replace every uppercase Z with +00:00 before parsing. Naive timestamps use
    the process's local timezone. Only fromisoformat failures are caught;
    string conversion and timestamp conversion can still raise.

    Example:
        >>> _coerce_datetime_ep_ms('1970-01-01T00:00:01Z')
        1000
        >>> _coerce_datetime_ep_ms('unknown') is None
        True


    :param value: Value stringified and stripped, or None for absent metadata.
    :return: Truncated epoch milliseconds, or None for absent/unparseable text.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    return int(dt.timestamp() * 1000.0)


def _normalize_ebook_extensions(ebook_extensions: Optional[Iterable[str]]) -> set[str]:
    """
    Collect lowercase suffixes with leading dots removed from nonblank entries.

    Whitespace is tested but not stripped from retained entries. A bare string
    is iterated character by character; dotted-only entries can produce ''.

    Example:
        >>> _normalize_ebook_extensions(['.EPUB', 'mobi', '']) == {'epub', 'mobi'}
        True


    :param ebook_extensions: Suffix iterable, or None to use BOOK_EXTENSIONS.
    :return: Deduplicated normalized suffix strings used for exact membership.
    """
    if ebook_extensions is None:
        ebook_extensions = BOOK_EXTENSIONS
    return {str(x).lower().lstrip(".") for x in ebook_extensions if str(x).strip()}


def _storage_key_from_store_url(*, store_url: str, file_url: str) -> str:
    """
    Remove a matching textual root prefix to derive a registration key.

    This is lexical stripping, not URL parsing or scope validation. After the
    slash-boundary attempt, any matching root prefix is removed, even from a
    sibling path. Unmatched targets remain absolute except when the root is empty.

    Example:
        >>> _storage_key_from_store_url(store_url='https://example.test/books/', file_url='https://example.test/books/a.epub')
        'a.epub'


    :param store_url: Root text stripped of outer whitespace, with special colon handling.
    :param file_url: Candidate text stripped of outer whitespace before prefix removal.
    :return: Derived key or unmatched target; no normalization or containment check.
    """
    root = str(store_url).strip()
    target = str(file_url).strip()
    if not root:
        return target.lstrip("/")
    if root.endswith(":"):
        if target.startswith(root):
            return target[len(root) :].lstrip("/")
        return target
    rooted = root.rstrip("/") + "/"
    if target.startswith(rooted):
        return target[len(rooted) :]
    if target.startswith(root):
        return target[len(root) :].lstrip("/")
    return target


def _guess_remote_url_extension(candidate_url: str) -> str:
    """
    Choose the last nonempty suffix found in the decoded path and query values.

    Query values override path suffixes in parse_qs key/value iteration order.
    Values are unquoted again after query parsing. This observation heuristic
    differs from registration, which uses the raw storage key's POSIX suffix.

    Example:
        >>> _guess_remote_url_extension('https://example.test/download.php?name=book.EPUB')
        'epub'


    :param candidate_url: URL text, with falsey values treated as empty.
    :return: Lowercase suffix without its dot, or '' when no suffix is found.
    """
    parsed = urlparse(str(candidate_url or "").strip())
    suffixes: list[str] = []

    path_text = unquote(parsed.path or "")
    path_ext = pathlib.PurePosixPath(path_text).suffix.lower().lstrip(".")
    if path_ext:
        suffixes.append(path_ext)

    query_values = parse_qs(parsed.query or "", keep_blank_values=True)
    for values in query_values.values():
        for raw_value in values:
            value_text = unquote(str(raw_value or ""))
            value_ext = pathlib.PurePosixPath(value_text).suffix.lower().lstrip(".")
            if value_ext:
                suffixes.append(value_ext)

    if not suffixes:
        return ""
    return suffixes[-1]


def _extract_preferred_hash(hashes: object) -> str | None:
    """
    Select a truthy hash value by preferred key, then dictionary insertion order.

    Prefer sha256, sha1, md5, then crc32. Keys are case-sensitive; values are
    stringified without checking their algorithm, length, or hexadecimal syntax.

    Example:
        >>> _extract_preferred_hash({'md5': 'older', 'sha256': 'preferred'})
        'preferred'


    :param hashes: Dictionary of supplied hash claims; other mappings are ignored.
    :return: First preferred/fallback truthy value as text, or None.
    """
    if not isinstance(hashes, dict):
        return None
    preferred = ("sha256", "sha1", "md5", "crc32")
    for key in preferred:
        value = hashes.get(key)
        if value:
            return str(value)
    for value in hashes.values():
        if value:
            return str(value)
    return None


def _table_columns(db, table_name: str) -> set[str]:
    """
    Materialize the database's column headings as a set.

    Example:
        >>> columns = _table_columns(database, 'files')  # doctest: +SKIP


    :param db: Database adapter exposing get_column_headings.
    :param table_name: Table passed unchanged to the adapter.
    :return: New heading set; adapter failures propagate.
    """
    return set(db.get_column_headings(table_name))


def _ensure_schema_support(db) -> tuple[set[str], set[str], set[str], set[str]]:
    """
    Require minimal Store/file identity columns and inspect optional link columns.

    Require stores.store_root_uri plus files.file_store_id/file_storage_key.
    Do not migrate tables or validate every optional link column used later;
    an incomplete link schema may still fail during the initial link lookup.

    Example:
        >>> tables, stores, files, links = _ensure_schema_support(database)  # doctest: +SKIP


    :param db: Database exposing table and column metadata.
    :return: Table names, Store columns, file columns, and optional association columns.
    :raises InputIntegrityError: A required table or minimal identity column is missing.
    """
    tables = set(db.get_tables())
    required_tables = {"stores", "files"}
    missing_tables = sorted(required_tables - tables)
    if missing_tables:
        raise InputIntegrityError(
            "Database schema missing required tables for unmanaged ingestion: {}".format(", ".join(missing_tables))
        )

    store_columns = _table_columns(db, "stores")
    file_columns = _table_columns(db, "files")
    link_columns = _table_columns(db, "file_store_links") if "file_store_links" in tables else set()

    required_store_columns = {"store_root_uri"}
    required_file_columns = {"file_store_id", "file_storage_key"}
    missing_store_cols = sorted(required_store_columns - store_columns)
    missing_file_cols = sorted(required_file_columns - file_columns)
    if missing_store_cols or missing_file_cols:
        err_chunks = []
        if missing_store_cols:
            err_chunks.append("stores missing columns: {}".format(", ".join(missing_store_cols)))
        if missing_file_cols:
            err_chunks.append("files missing columns: {}".format(", ".join(missing_file_cols)))
        raise InputIntegrityError("; ".join(err_chunks))

    return tables, store_columns, file_columns, link_columns


def _build_remote_file_payload(
    *,
    file_url: str,
    storage_key: str,
    stat_blob: dict[str, object] | None,
    store_id: int,
    now_epk: int,
    source_label: str,
    capture_hashes: bool,
) -> dict[str, object]:
    """
    Build an unfiltered file metadata payload from a key and optional stat claims.

    Use POSIX path parsing on the raw key, without URL decoding. Missing or
    unconvertible size becomes zero; negative converted sizes are retained.
    MIME comes from the name, not content inspection. A selected hash is placed
    in file_hash_sha256 and marks integrity 'ok' even if its key was another
    algorithm; neither the digest nor bytes are verified here. The registration
    caller supplies no stat data and disables hash capture.

    Example:
        >>> payload = _build_remote_file_payload(file_url='https://example.test/a.epub', storage_key='a.epub', stat_blob=None, store_id=7, now_epk=1000, source_label='crawl', capture_hashes=False)
        >>> payload['file_size_bytes'], payload['file_integrity_status']
        (0, 'unchecked')


    :param file_url: Original address retained as provenance without redaction.
    :param storage_key: Key used for filename/stem/suffix extraction and row identity.
    :param stat_blob: Optional Size, ModTime, and Hashes claims; None means absent data.
    :param store_id: Store row identity assigned to the file.
    :param now_epk: Caller-supplied epoch milliseconds for generated timestamps.
    :param source_label: Provenance label retained verbatim.
    :param capture_hashes: Whether to select a supplied hash claim.
    :return: Metadata dictionary including unsupported/None fields for later filtering.
    """
    path_obj = pathlib.PurePosixPath(storage_key or pathlib.PurePosixPath(file_url).name)
    name = path_obj.name
    ext = path_obj.suffix.lower().lstrip(".")

    size_raw = (stat_blob or {}).get("Size")
    try:
        size_bytes = int(size_raw) if size_raw is not None else 0
    except Exception:
        size_bytes = 0

    mtime_epk = _coerce_datetime_ep_ms((stat_blob or {}).get("ModTime"))
    mime_type, _ = mimetypes.guess_type(name)

    chosen_hash = None
    if capture_hashes:
        chosen_hash = _extract_preferred_hash((stat_blob or {}).get("Hashes"))

    return {
        "file_store_id": store_id,
        "file_storage_key": storage_key,
        "file_name": name,
        "file_base_name": path_obj.stem,
        "file_extension": ext,
        "file_mime_type": mime_type,
        "file_role": "primary",
        "file_media_category": "ebook",
        "file_size_bytes": size_bytes,
        "file_hash_sha256": chosen_hash,
        "file_integrity_status": "ok" if chosen_hash else "unchecked",
        "file_last_seen_timestamp_ep_k": now_epk,
        "file_last_integrity_check_timestamp_ep_k": now_epk if chosen_hash else None,
        "file_acquired_timestamp_ep_k": now_epk,
        "file_source": source_label,
        "file_original_name": name,
        "file_original_path": file_url,
        "file_processed": 0,
        "file_modified_timestamp_ep_k": now_epk,
        "file_source_created_datestamp_ep_k": None,
        "file_source_modified_datestamp_ep_k": mtime_epk,
    }


def _emit_progress(
    progress_callback: Optional[ProgressCallback],
    *,
    event: str,
    report: RemoteHtmlRegistrationReport,
    details: Optional[dict[str, object]] = None,
) -> None:
    """
    Deliver a synchronous progress event while swallowing ordinary consumer errors.

    Pass the original mutable report and a shallow copy of details. Consumer
    mutations to the report survive even if the consumer subsequently raises.
    BaseException cancellation propagates; details and diagnostics are not scrubbed.

    Example:
        >>> report = RemoteHtmlRegistrationReport(7, 'https://example.test/', 'Books')
        >>> _emit_progress(None, event='scan', report=report)


    :param progress_callback: Event consumer, or None to do nothing.
    :param event: Event name stringified inside the guarded callback invocation.
    :param report: Shared registration report observed at the time of delivery.
    :param details: Optional mapping copied before delivery, defaulting to an empty dict.
    :return: None after delivery, an absent consumer, or a caught Exception.
    """
    if progress_callback is None:
        return
    try:
        progress_callback(str(event), report, dict(details or {}))
    except Exception:
        return


def _insert_file_row(db, *, payload: dict[str, object], file_columns: set[str]) -> Row:
    """
    Insert supported non-None payload fields into a new file row.

    Example:
        >>> row = _insert_file_row(database, payload=payload, file_columns=columns)  # doctest: +SKIP


    :param db: Database bound to the newly created Row.
    :param payload: Proposed metadata; fields are not validated by this helper.
    :param file_columns: Schema headings used to omit unsupported fields.
    :return: Created Row; persistence errors propagate.
    """
    row_dict = {key: value for key, value in payload.items() if key in file_columns and value is not None}
    return Row.from_idless_row_dict(db, row_dict=row_dict, table="files")


def _update_file_row(row: Row, *, payload: dict[str, object], file_columns: set[str]) -> bool:
    """
    Sync supported non-None fields only when a nonvolatile value has changed.

    Ignore acquired/last-seen/integrity-check/modified timestamps when deciding
    whether to write. Once another value differs, update those timestamps too,
    including acquired time. None never clears old metadata. Assignment/sync
    failures propagate without restoring the in-memory row here.

    Example:
        >>> changed = _update_file_row(row, payload=payload, file_columns=columns)  # doctest: +SKIP


    :param row: Existing mutable file Row selected by Store and storage key.
    :param payload: Replacement candidates, including possible timestamp fields.
    :param file_columns: Schema headings eligible for comparison and assignment.
    :return: True after sync, or False when no supported nonvolatile value differs.
    """
    volatile_columns = {
        "file_acquired_timestamp_ep_k",
        "file_last_seen_timestamp_ep_k",
        "file_last_integrity_check_timestamp_ep_k",
        "file_modified_timestamp_ep_k",
    }

    changed = False
    for key, value in payload.items():
        if value is None:
            continue
        if key not in file_columns:
            continue
        if key in volatile_columns:
            continue
        if row[key] != value:
            changed = True
            break

    if not changed:
        return False

    for key, value in payload.items():
        if value is None:
            continue
        if key not in file_columns:
            continue
        if row[key] != value:
            row[key] = value
    row.sync()
    return True


def _ensure_file_store_link(
    db,
    *,
    file_id: int,
    store_id: int,
    tables: set[str],
    link_columns: set[str],
    linked_file_ids: set[int],
) -> bool:
    """
    Insert an absent file/Store association when the schema supports its identity fields.

    linked_file_ids must describe the selected Store. Existing links are not
    updated; lookup and insertion are not protected against concurrent writers.
    Add the file ID to the supplied set only after Row creation succeeds.

    Example:
        >>> linked = _ensure_file_store_link(database, file_id=3, store_id=7, tables=tables, link_columns=columns, linked_file_ids=seen)  # doctest: +SKIP


    :param db: Database used to create the association Row.
    :param file_id: File row identity to associate.
    :param store_id: Store row identity receiving the association.
    :param tables: Available table names.
    :param link_columns: Available link headings, filtering optional priority/type fields.
    :param linked_file_ids: Mutable per-Store set of existing and newly linked file IDs.
    :return: True after insertion, or False for unsupported/already-known associations.
    """
    if "file_store_links" not in tables:
        return False
    required = {"file_store_link_file_id", "file_store_link_store_id"}
    if not required.issubset(link_columns):
        return False
    if file_id in linked_file_ids:
        return False

    payload = {
        "file_store_link_file_id": file_id,
        "file_store_link_store_id": store_id,
        "file_store_link_priority": 0,
        "file_store_link_type": "primary",
    }
    row_dict = {key: value for key, value in payload.items() if key in link_columns and value is not None}
    Row.from_idless_row_dict(db, row_dict=row_dict, table="file_store_links")
    linked_file_ids.add(file_id)
    return True


def ingest_html_discovery_store_files(
    db,
    *,
    store_row: Row,
    store_url: str,
    store_name_value: str,
    discovery_source,
    mode: str,
    ebook_extensions: Optional[Iterable[str]],
    source_label: str,
    attach_store_links: bool,
    refresh_storage_manager: bool,
    incremental_db_writes: bool,
    progress_callback: Optional[ProgressCallback],
) -> RemoteHtmlRegistrationReport:
    """
    Register accepted discovery addresses using suffix selection and per-file writes.

    Validate minimal schema, load existing rows by Store/key, and call discovery
    with force=False without startup. The last existing duplicate key wins.
    Incremental mode trusts discovery to invoke its accepted-URL callback and
    ignores the returned list; deferred mode processes that list after success.
    Neither mode wraps writes in a transaction or removes undiscovered rows.

    File/link Exceptions become report errors while later candidates continue;
    a link failure may follow a counted successful file write. Setup/discovery
    errors propagate without a finished report or done event. Ordinary progress
    callback errors are ignored. Consumers receive the live mutable report.
    A native crawler may itself handle fetch errors and return an empty list;
    an error-free report therefore does not prove a complete remote inventory.

    Optional manager bootstrap uses clear_existing=True. Its return value is
    ignored, but an Exception is appended to report.errors. Earlier metadata
    writes survive a failed refresh. No ebook bytes or remote stat are fetched
    here, and retained addresses/error messages are not generically redacted.

    Example:
        >>> report = ingest_html_discovery_store_files(database, store_row=store_row, store_url=source.url, store_name_value='Remote books', discovery_source=source, mode='native_html', ebook_extensions={'epub'}, source_label='crawl', attach_store_links=True, refresh_storage_manager=False, incremental_db_writes=True, progress_callback=None)  # doctest: +SKIP


    :param db: Caller-owned database for file/link lookups, writes, and optional refresh.
    :param store_row: Existing Store Row supplying its row_id or store_id field.
    :param store_url: Root used for lexical key derivation and report/event metadata.
    :param store_name_value: Display value stringified into the report.
    :param discovery_source: Object implementing discover_urls with the callback keywords.
    :param mode: Diagnostic label passed to events; it does not select a crawler.
    :param ebook_extensions: Accepted key suffixes, or None for BOOK_EXTENSIONS.
    :param source_label: Provenance text written to supported file_source columns.
    :param attach_store_links: Whether to inspect and insert optional Store associations.
    :param refresh_storage_manager: Attempt bootstrap after discovery if the attribute exists.
    :param incremental_db_writes: Write from discovery callbacks instead of its returned list.
    :param progress_callback: Optional synchronous event/report/details consumer.
    :return: Mutable counters/errors with a finish timestamp after normal finalization.
    :raises InputIntegrityError: The minimal Store/file schema is absent.
    """
    tables, _, file_columns, link_columns = _ensure_schema_support(db)
    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = RemoteHtmlRegistrationReport(
        store_row_id=store_id,
        store_root_uri=str(store_url),
        store_name=str(store_name_value),
    )
    _emit_progress(
        progress_callback,
        event="start",
        report=report,
        details={"mode": mode, "store_id": store_id, "store_root_uri": str(store_url)},
    )

    existing_rows = db.search("files", "file_store_id", store_id)
    existing_by_key: dict[str, Row] = {}
    for row in existing_rows:
        key = row["file_storage_key"]
        if key is not None:
            existing_by_key[str(key)] = row

    linked_file_ids: set[int] = set()
    if attach_store_links and "file_store_links" in tables and "file_store_link_store_id" in link_columns:
        for link_row in db.search("file_store_links", "file_store_link_store_id", store_id):
            file_id = link_row["file_store_link_file_id"]
            if file_id is not None:
                linked_file_ids.add(int(file_id))

    ebook_exts = _normalize_ebook_extensions(ebook_extensions)

    def _on_crawl_log_line(raw_line: str) -> None:
        """
        Forward a nonblank stripped crawler diagnostic with the enclosing mode label.

        Example:
            >>> _on_crawl_log_line('queued book.epub')  # doctest: +SKIP


        :param raw_line: Diagnostic coerced to text, treating falsey values as empty.
        :return: None after optional best-effort crawl-log delivery.
        """
        line = str(raw_line or "").strip()
        if not line:
            return
        _emit_progress(
            progress_callback,
            event="crawl-log",
            report=report,
            details={"line": line, "mode": mode},
        )

    def _on_crawl_observation(details: dict[str, object]) -> None:
        """
        Count a crawler observation and publish its inferred suffix classification.

        Ignore empty URLs. Book-like counts include rejected URLs and can overlap
        HTML counts. The callback does no deduplication; observation and actual
        registration suffix rules differ. Missing/blank reasons become 'unknown'.

        Example:
            >>> _on_crawl_observation({'url': 'https://example.test/a.epub', 'accepted': True})  # doctest: +SKIP


        :param details: Crawler mapping with url and optional accepted/reason fields.
        :return: None after counter mutation and best-effort observation delivery.
        """
        candidate_url = str(details.get("url", "") or "").strip()
        if not candidate_url:
            return
        accepted = bool(details.get("accepted", False))
        reason = str(details.get("reason", "") or "").strip() or "unknown"
        extension = _guess_remote_url_extension(candidate_url)
        html_like = extension in _HTML_LIKE_EXTENSIONS
        book_like = extension in ebook_exts

        report.crawler_urls_observed += 1
        if html_like:
            report.crawler_html_seen += 1
            if not accepted:
                report.crawler_html_rejected += 1
        if book_like:
            report.crawler_book_like_found += 1
        if not accepted:
            report.crawler_rejection_counts[reason] = int(report.crawler_rejection_counts.get(reason, 0)) + 1

        _emit_progress(
            progress_callback,
            event="crawl-observation",
            report=report,
            details={
                "url": candidate_url,
                "accepted": accepted,
                "reason": reason,
                "extension": extension,
                "html_like": html_like,
                "book_like": book_like,
            },
        )

    def _process_discovered_url(crawled_url: str) -> None:
        """
        Count and classify one address, then upsert file metadata and an optional link.

        Use the raw derived key's suffix, without decoding query/path text or
        checking URL scope. Duplicate candidates still increment counters and
        revisit the same row. Ordinary per-file failures append unsanitized error
        text without undoing earlier assignments, writes, or counter increments.

        Example:
            >>> _process_discovered_url('https://example.test/books/a.epub')  # doctest: +SKIP


        :param crawled_url: Accepted address stringified/stripped, or '<unknown>' if blank.
        :return: None after a skip, successful scan, or reported per-file Exception.
        """
        marker = str(crawled_url or "").strip() or "<unknown>"
        report.scanned_files += 1
        try:
            storage_key = _storage_key_from_store_url(store_url=store_url, file_url=marker)
            ext = pathlib.PurePosixPath(storage_key).suffix.lower().lstrip(".")
            if ext not in ebook_exts:
                report.skipped_non_ebook_files += 1
                _emit_progress(
                    progress_callback,
                    event="scan",
                    report=report,
                    details={"path": storage_key, "is_ebook": False},
                )
                return

            report.ebook_candidates += 1
            payload = _build_remote_file_payload(
                file_url=marker,
                storage_key=storage_key,
                stat_blob=None,
                store_id=store_id,
                now_epk=_now_ep_ms(),
                source_label=source_label,
                capture_hashes=False,
            )

            existing = existing_by_key.get(storage_key)
            if existing is None:
                row = _insert_file_row(db, payload=payload, file_columns=file_columns)
                existing_by_key[storage_key] = row
                report.inserted_files += 1
            else:
                if _update_file_row(existing, payload=payload, file_columns=file_columns):
                    report.updated_files += 1
                else:
                    report.unchanged_files += 1
                row = existing

            if attach_store_links and row.row_id is not None:
                linked = _ensure_file_store_link(
                    db,
                    file_id=int(row.row_id),
                    store_id=store_id,
                    tables=tables,
                    link_columns=link_columns,
                    linked_file_ids=linked_file_ids,
                )
                if linked:
                    report.linked_files += 1

        except Exception as exc:
            report.errors.append("{} :: {}".format(marker, repr(exc)))
            _emit_progress(
                progress_callback,
                event="error",
                report=report,
                details={"path": marker, "error": repr(exc)},
            )
        else:
            _emit_progress(
                progress_callback,
                event="scan",
                report=report,
                details={"path": storage_key, "is_ebook": True},
            )

    if incremental_db_writes:
        discovery_source.discover_urls(
            force=False,
            log_line_callback=_on_crawl_log_line,
            discovered_url_callback=_process_discovered_url,
            observed_url_callback=_on_crawl_observation,
        )
    else:
        crawled_urls = discovery_source.discover_urls(
            force=False,
            log_line_callback=_on_crawl_log_line,
            observed_url_callback=_on_crawl_observation,
        )
        for crawled_url in crawled_urls:
            _process_discovered_url(crawled_url)

    if refresh_storage_manager and hasattr(db, "bootstrap_storage_manager"):
        try:
            db.bootstrap_storage_manager(clear_existing=True)
        except Exception as exc:
            report.errors.append("storage_manager_bootstrap_failed :: {!r}".format(exc))

    report.finished_timestamp_ep_k = _now_ep_ms()
    _emit_progress(
        progress_callback,
        event="done",
        report=report,
        details={"mode": mode, "store_id": store_id, "store_root_uri": str(store_url)},
    )
    return report


__all__ = ["ingest_html_discovery_store_files"]
