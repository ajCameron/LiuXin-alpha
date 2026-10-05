#!/usr/bin/env python

"""
Merge metadata across book formats, run import plugins and expose serial or process job adapters.

The module keeps malformed-input, optional dependency and resource ownership
behavior explicit for registry callers.

Example:
    Exercise worker with pytest::

        python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py
"""
from __future__ import annotations

import io
import os
import shutil
from collections.abc import Iterable

from LiuXin_alpha.customize.ui import run_plugins_on_import
from LiuXin_alpha.file_formats.opf import metadata_to_opf
from LiuXin_alpha.file_formats.opf.opf2 import OPFCreator
from LiuXin_alpha.metadata.file_sources import InvalidMetadataExtractor, get_metadata as get_file_metadata
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.storage.local.filenames import samefile
from LiuXin_alpha.utils.text.icu import lower as icu_lower

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


_METADATA_PRIORITIES = [
    "html",
    "htm",
    "xhtml",
    "xhtm",
    "rtf",
    "fb2",
    "fbz",
    "pdf",
    "prc",
    "odt",
    "epub",
    "lit",
    "lrx",
    "lrf",
    "mobi",
    "azw",
    "azw3",
    "azw1",
    "rb",
    "imp",
    "snb",
    "topaz",
    "txt",
    "txtz",
]
METADATA_PRIORITIES = {ext: idx for idx, ext in enumerate(_METADATA_PRIORITIES)}
_DEFAULT_PRIORITY = -1


def _values(raw):
    """
    Perform the format-specific values operation used by this metadata source.

    Example:
        Exercise  values with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param raw: Raw value or payload to normalize, parse or serialize.
    :return: Parsed, normalized or serialized value described above.
    """
    if raw is None:
        return []
    if isinstance(raw, dict):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _is_unknown_authors(mi) -> bool:
    """
    Return whether the supplied state satisfies the is unknown authors condition.

    Example:
        Exercise  is unknown authors with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :return: True when the described condition is satisfied; otherwise False.
    """
    authors = [str(x).strip() for x in _values(getattr(mi, "authors", None)) if str(x).strip()]
    if not authors:
        return True
    return len(authors) == 1 and authors[0].lower() == "unknown"


def _flatten_paths(paths) -> list[str]:
    """
    Flatten path values and one nested iterable level into filesystem strings.

    Example:
        Exercise  flatten paths with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param paths: Ordered input values processed by this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    out: list[str] = []
    for path in paths:
        if isinstance(path, (str, os.PathLike)):
            out.append(os.fspath(path))
            continue
        if isinstance(path, Iterable):
            for item in path:
                if isinstance(item, (str, os.PathLike)):
                    out.append(os.fspath(item))
    return out


def _path_priority(path: str) -> int:
    """
    Return the configured metadata merge priority for a format path.

    Example:
        Exercise  path priority with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param path: Caller-supplied path, path-like object or stream described by this
        operation.
    :return: Parsed, normalized or serialized value described above.
    """
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    return METADATA_PRIORITIES.get(ext, _DEFAULT_PRIORITY)


def _extract_cover_payload(mi) -> bytes | None:
    """
    Return raw cover bytes from supported metadata cover_data representations.

    Example:
        Exercise  extract cover payload with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :return: Parsed, normalized or serialized value described above.
    """
    cover_data = getattr(mi, "cover_data", None)
    if isinstance(cover_data, tuple) and len(cover_data) == 2:
        payload = cover_data[1]
        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload)
    if isinstance(cover_data, dict) and cover_data:
        first = next(iter(cover_data.keys()))
        if isinstance(first, tuple) and len(first) == 2 and isinstance(first[1], (bytes, bytearray)):
            return bytes(first[1])
    return None


def _metadata_to_opf_bytes(mi, tdir: str) -> bytes:
    """
    Serialize metadata to OPF bytes, falling back to OPFCreator at compatibility edges.

    Example:
        Exercise  metadata to opf bytes with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param tdir: Directory used for temporary or generated output files.
    :return: Parsed, normalized or serialized value described above.
    """
    try:
        raw = metadata_to_opf(mi, default_lang="und")
        if isinstance(raw, str):
            return raw.encode("utf-8", "replace")
        return bytes(raw)
    except Exception as err:
        default_log.log_exception(
            "metadata_to_opf failed in worker; falling back to OPFCreator.",
            err,
            "DEBUG",
            ("title", getattr(mi, "title", None)),
        )
        opf = OPFCreator(tdir, mi)
        opf.create_manifest([])
        opf.create_spine([])
        out = io.BytesIO()
        opf.render(out)
        return out.getvalue()


def metadata_from_formats(paths):
    """
    Read readable format paths in priority order and merge them into one metadata object.

    Example:
        Exercise metadata from formats with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param paths: Ordered input values processed by this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    flattened = [path for path in _flatten_paths(paths) if os.access(path, os.R_OK)]
    if not flattened:
        return calibreMetaInformation("Unknown", ["Unknown"])

    flattened.sort(key=_path_priority)
    exts = [os.path.splitext(path)[1].lower().lstrip(".") for path in flattened]

    # Respect OPF sidecar if present and usable.
    if "opf" in exts:
        opf_path = flattened[exts.index("opf")]
        try:
            opf_md = get_file_metadata(opf_path, force_type="opf")
            if opf_md is not None and getattr(opf_md, "title", None):
                return opf_md
        except Exception as err:
            default_log.log_exception(
                "Failed reading OPF metadata sidecar.",
                err,
                "DEBUG",
                ("path", opf_path),
            )

    mi = calibreMetaInformation("Unknown", ["Unknown"])
    for path in flattened:
        ext = os.path.splitext(path)[1].lower().lstrip(".")
        if not ext:
            continue
        try:
            new_mi = get_file_metadata(path, force_type=ext)
        except (InvalidMetadataExtractor, ValueError):
            continue
        except Exception as err:
            default_log.log_exception(
                "Failed reading metadata from format in worker.",
                err,
                "DEBUG",
                ("path", path),
                ("ext", ext),
            )
            continue
        if new_mi is None:
            continue
        try:
            mi.smart_update(new_mi)
        except Exception:
            title = getattr(new_mi, "title", None)
            if title:
                mi.title = title
            if _is_unknown_authors(mi):
                authors = _values(getattr(new_mi, "authors", None))
                if authors:
                    mi.authors = authors
        if getattr(mi, "application_id", None):
            break

    if not getattr(mi, "title", None):
        mi.title = os.path.splitext(os.path.basename(flattened[0]))[0] or "Unknown"
    if _is_unknown_authors(mi):
        mi.authors = ["Unknown"]
    return mi


def serialize_metadata_for(paths, tdir, group_id):
    """
    Return merged metadata and OPF bytes, writing extracted cover bytes to the worker directory.

    Example:
        Exercise serialize metadata for with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param paths: Ordered input values processed by this operation.
    :param tdir: Directory used for temporary or generated output files.
    :param group_id: Worker, metadata or control value described by the operation.
    :return: Parsed, normalized or serialized value described above.
    """
    mi = metadata_from_formats(paths)
    mi.cover = None
    cdata = _extract_cover_payload(mi)
    mi.cover_data = (None, None)
    if not getattr(mi, "application_id", None):
        mi.application_id = "__calibre_dummy__"

    opf = _metadata_to_opf_bytes(mi, tdir)
    has_cover = False
    if cdata:
        with open(os.path.join(tdir, f"{group_id}.cdata"), "wb") as f:
            f.write(cdata)
            has_cover = True
    return mi, opf, has_cover


def read_metadata_bulk(get_opf, get_cover, paths):
    """
    Return only the requested OPF and cover payloads for a group of format paths.

    Example:
        Exercise read metadata bulk with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param get_opf: Policy flag controlling the behavior described above.
    :param get_cover: Request cover discovery or cover payload extraction when true.
    :param paths: Ordered input values processed by this operation.
    :return: Parsed, normalized or serialized value described above.
    """
    mi = metadata_from_formats(paths)
    mi.cover = None
    cdata = _extract_cover_payload(mi)
    mi.cover_data = (None, None)
    if not getattr(mi, "application_id", None):
        mi.application_id = "__calibre_dummy__"
    ans = {"opf": None, "cdata": None}
    if get_opf:
        ans["opf"] = _metadata_to_opf_bytes(mi, os.getcwd())
    if get_cover:
        ans["cdata"] = cdata
    return ans


def run_import_plugins(paths, group_id, tdir):
    """
    Run import plugins for flattened paths and return the resulting readable paths.

    Example:
        Exercise run import plugins with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param paths: Ordered input values processed by this operation.
    :param group_id: Worker, metadata or control value described by the operation.
    :param tdir: Directory used for temporary or generated output files.
    :return: Parsed, normalized or serialized value described above.
    """
    final_paths: list[str] = []
    for path in paths:
        if isinstance(path, (str, os.PathLike)):
            do_import_plugins_one_book(os.fspath(path), tdir, group_id, final_paths)
        elif isinstance(path, Iterable):
            for true_path in path:
                if isinstance(true_path, (str, os.PathLike)):
                    do_import_plugins_one_book(os.fspath(true_path), tdir, group_id, final_paths)
    return final_paths


def do_import_plugins_one_book(path, tdir, group_id, final_paths):
    """
    Run import processing for one book and preserve its source basename when relocating output.

    Example:
        Exercise do import plugins one book with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param path: Caller-supplied path, path-like object or stream described by this
        operation.
    :param tdir: Directory used for temporary or generated output files.
    :param group_id: Worker, metadata or control value described by the operation.
    :param final_paths: Worker, metadata or control value described by the operation.
    :return: None.
    """
    if not os.access(path, os.R_OK):
        return

    try:
        nfp = run_plugins_on_import(path)
    except Exception as err:
        nfp = None
        default_log.log_exception(
            "Import plugin failed for path in metadata worker.",
            err,
            "DEBUG",
            ("path", path),
        )

    if nfp and os.access(nfp, os.R_OK):
        try:
            same = samefile(nfp, path)
        except Exception:
            same = os.path.abspath(nfp) == os.path.abspath(path)
        if not same:
            # Preserve source basename so downstream filename-based metadata still works.
            name = os.path.splitext(os.path.basename(path))[0]
            ext = os.path.splitext(nfp)[1]
            path = os.path.join(tdir, str(group_id), name + ext)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            try:
                os.replace(nfp, path)
            except OSError:
                shutil.copyfile(nfp, path)
    final_paths.append(path)


def has_book(mi, data_for_has_book):
    """
    Return whether normalized title text is present in the supplied duplicate-title collection.

    Example:
        Exercise has book with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param mi: Metadata object supplying or receiving the supported fields.
    :param data_for_has_book: Worker, metadata or control value described by the
        operation.
    :return: True when the described condition is satisfied; otherwise False.
    """
    return bool(getattr(mi, "title", None)) and icu_lower(str(mi.title).strip()) in data_for_has_book


def read_metadata(paths, group_id, tdir, common_data=None):
    """
    Run imports, serialize merged metadata and report optional duplicate-title state.

    Example:
        Exercise read metadata with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param paths: Ordered input values processed by this operation.
    :param group_id: Worker, metadata or control value described by the operation.
    :param tdir: Directory used for temporary or generated output files.
    :param common_data: Worker, metadata or control value described by the operation.
    :return: Parsed, normalized or serialized value described above.
    """
    paths = run_import_plugins(paths, group_id, tdir)
    mi, opf, has_cover = serialize_metadata_for(paths, tdir, group_id)
    duplicate_info = None
    if isinstance(common_data, (set, frozenset)):
        duplicate_info = has_book(mi, common_data)
    return paths, opf, has_cover, duplicate_info


def _run_in_job(function_name, args, *, timeout=300, backend=None, no_output=True, heartbeat=None, abort=None):
    """
    Dispatch a named worker function through the configured IPC backend and return its result.

    Example:
        Exercise  run in job with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param function_name: Worker, metadata or control value described by the operation.
    :param args: Ordered input values processed by this operation.
    :param timeout: Maximum worker execution time in seconds.
    :param backend: Worker, metadata or control value described by the operation.
    :param no_output: Policy flag controlling the behavior described above.
    :param heartbeat: Worker, metadata or control value described by the operation.
    :param abort: Worker, metadata or control value described by the operation.
    :return: Parsed, normalized or serialized value described above.
    """
    from LiuXin_alpha.utils.ipc.simple_worker import fork_job

    ans = fork_job(
        __name__,
        function_name,
        args=tuple(args),
        timeout=timeout,
        no_output=no_output,
        heartbeat=heartbeat,
        abort=abort,
        backend=backend,
    )
    return ans["result"]


def read_metadata_in_job(
    paths,
    group_id,
    tdir,
    common_data=None,
    *,
    timeout=300,
    backend=None,
    no_output=True,
    heartbeat=None,
    abort=None,
):
    """
    Execute read_metadata through the configured serial or process job backend.

    Example:
        Exercise read metadata in job with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param paths: Ordered input values processed by this operation.
    :param group_id: Worker, metadata or control value described by the operation.
    :param tdir: Directory used for temporary or generated output files.
    :param common_data: Worker, metadata or control value described by the operation.
    :param timeout: Maximum worker execution time in seconds.
    :param backend: Worker, metadata or control value described by the operation.
    :param no_output: Policy flag controlling the behavior described above.
    :param heartbeat: Worker, metadata or control value described by the operation.
    :param abort: Worker, metadata or control value described by the operation.
    :return: Parsed, normalized or serialized value described above.
    """
    return _run_in_job(
        "read_metadata",
        (paths, group_id, tdir, common_data),
        timeout=timeout,
        backend=backend,
        no_output=no_output,
        heartbeat=heartbeat,
        abort=abort,
    )


def read_metadata_bulk_in_job(
    get_opf,
    get_cover,
    paths,
    *,
    timeout=300,
    backend=None,
    no_output=True,
    heartbeat=None,
    abort=None,
):
    """
    Execute read_metadata_bulk through the configured serial or process job backend.

    Example:
        Exercise read metadata bulk in job with pytest::

            python -m pytest -q tests/metadata/file_sources/test_worker_metadata_source.py


    :param get_opf: Policy flag controlling the behavior described above.
    :param get_cover: Request cover discovery or cover payload extraction when true.
    :param paths: Ordered input values processed by this operation.
    :param timeout: Maximum worker execution time in seconds.
    :param backend: Worker, metadata or control value described by the operation.
    :param no_output: Policy flag controlling the behavior described above.
    :param heartbeat: Worker, metadata or control value described by the operation.
    :param abort: Worker, metadata or control value described by the operation.
    :return: Parsed, normalized or serialized value described above.
    """
    return _run_in_job(
        "read_metadata_bulk",
        (bool(get_opf), bool(get_cover), paths),
        timeout=timeout,
        backend=backend,
        no_output=no_output,
        heartbeat=heartbeat,
        abort=abort,
    )


__all__ = [
    "METADATA_PRIORITIES",
    "metadata_from_formats",
    "serialize_metadata_for",
    "read_metadata_bulk",
    "run_import_plugins",
    "do_import_plugins_one_book",
    "has_book",
    "read_metadata",
    "read_metadata_in_job",
    "read_metadata_bulk_in_job",
]
