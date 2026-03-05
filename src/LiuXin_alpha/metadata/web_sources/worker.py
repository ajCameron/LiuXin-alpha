"""
Worker helpers for batched web metadata/cover downloads.
"""

from __future__ import annotations

import os
from collections import Counter
from io import BytesIO, StringIO
from queue import Empty, Queue
from threading import Event, Thread
from typing import Mapping

from LiuXin_alpha.file_formats.opf.opf2 import OPF, metadata_to_opf
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.metadata.web_sources.base import dump_caches, load_caches
from LiuXin_alpha.metadata.web_sources.covers import download_cover, run_download
from LiuXin_alpha.metadata.web_sources.identify import identify
from LiuXin_alpha.metadata.web_sources.prefs import msprefs
from LiuXin_alpha.utils.date import as_utc
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class GUILog:
    """
    Small in-process log object compatible with legacy worker expectations.
    """

    def __init__(self):
        self._buffer = StringIO()
        self._logger = None
        self.clear()

    def clear(self):
        self._buffer = StringIO()
        from LiuXin_alpha.metadata.web_sources.base import create_log

        self._logger = create_log(self._buffer)

    def __call__(self, *parts):
        self._logger(*parts)

    def debug(self, *parts):
        self._logger.debug(*parts)

    def info(self, *parts):
        self._logger.info(*parts)

    def warn(self, *parts):
        self._logger.warn(*parts)

    warning = warn

    def error(self, *parts):
        self._logger.error(*parts)

    def exception(self, *parts):
        self._logger.exception(*parts)

    @property
    def plain_text(self) -> str:
        return self._buffer.getvalue()

    def dump(self) -> str:
        return self.plain_text


def _iter_identify_plugins():
    try:
        from LiuXin_alpha.customize.ui import metadata_plugins
    except Exception:
        return []
    try:
        return list(metadata_plugins(["identify"]))
    except Exception:
        return []


def _metadata_from_opf_bytes(raw: bytes, tdir: str):
    if isinstance(raw, str):
        raw = raw.encode("utf-8", "replace")
    return OPF(BytesIO(raw), basedir=tdir, populate_spine=False).to_book_metadata()


def _is_equal(x, y):
    if hasattr(x, "tzinfo"):
        x = as_utc(x)
    if hasattr(y, "tzinfo"):
        y = as_utc(y)
    return x == y


def merge_result(oldmi, newmi, ensure_fields=None):
    dummy = calibreMetaInformation(_("Unknown"), [_("Unknown")])
    ignored = msprefs.get("ignore_fields", [])
    for field in ignored:
        if ":" in field or (ensure_fields and field in ensure_fields):
            continue
        setattr(newmi, field, getattr(dummy, field))

    fields = set()
    for plugin in _iter_identify_plugins():
        fields |= set(getattr(plugin, "touched_fields", ()) or ())

    for field in fields:
        if field.startswith("identifier:") or field in ("series", "series_index"):
            continue
        try:
            if not newmi.is_null(field) and _is_equal(getattr(newmi, field), getattr(oldmi, field)):
                setattr(newmi, field, getattr(dummy, field))
        except Exception:
            continue

    if (getattr(newmi, "series", None), getattr(newmi, "series_index", 1)) == (
        getattr(oldmi, "series", None),
        getattr(oldmi, "series_index", 1),
    ):
        newmi.series = None
        newmi.series_index = 1

    return newmi


def _write_bytes(path: str, payload) -> None:
    if isinstance(payload, str):
        payload = payload.encode("utf-8", "replace")
    with open(path, "wb") as handle:
        handle.write(payload)


def main(do_identify, covers, metadata: Mapping, ensure_fields, tdir):
    failed_ids = set()
    failed_covers = set()
    all_failed = True
    log = GUILog()

    for book_id, raw_mi in metadata.items():
        mi = _metadata_from_opf_bytes(raw_mi, tdir)
        title = getattr(mi, "title", None)
        authors = getattr(mi, "authors", None)
        identifiers = getattr(mi, "identifiers", None) or {}
        log.clear()

        if do_identify:
            results = []
            try:
                results = identify(log, Event(), title=title, authors=authors, identifiers=identifiers)
            except Exception:
                results = []

            if results:
                all_failed = False
                mi = merge_result(mi, results[0], ensure_fields=ensure_fields)
                identifiers = getattr(mi, "identifiers", identifiers)
                if not mi.is_null("rating"):
                    mi.rating *= 2  # set_metadata expects rating out of 10
                _write_bytes(os.path.join(tdir, f"{book_id}.mi"), metadata_to_opf(mi, default_lang="und"))
            else:
                log.error("Failed to download metadata for", title)
                failed_ids.add(book_id)

        if covers:
            cdata = download_cover(log, title=title, authors=authors, identifiers=identifiers)
            if cdata is None:
                failed_covers.add(book_id)
            else:
                _write_bytes(os.path.join(tdir, f"{book_id}.cover"), cdata[-1])
                all_failed = False

        _write_bytes(os.path.join(tdir, f"{book_id}.log"), log.plain_text)

    return failed_ids, failed_covers, all_failed


def single_identify(title, authors, identifiers):
    log = GUILog()
    results = identify(log, Event(), title=title, authors=authors, identifiers=identifiers)
    return (
        [metadata_to_opf(result) for result in results],
        [getattr(result, "has_cached_cover_url", False) for result in results],
        dump_caches(),
        log.dump(),
    )


def single_covers(title, authors, identifiers, caches, tdir):
    load_caches(caches)
    log = GUILog()
    results = Queue()
    worker = Thread(
        target=run_download,
        args=(log, results, Event()),
        kwargs=dict(title=title, authors=authors, identifiers=identifiers),
        daemon=True,
    )
    worker.start()

    counter = Counter()
    while worker.is_alive():
        try:
            plugin, width, height, fmt, data = results.get(True, 1)
        except Empty:
            continue
        else:
            name = plugin.name
            if getattr(plugin, "can_get_multiple_covers", False):
                name += "{%d}" % counter[plugin.name]
                counter[plugin.name] += 1
            filename = f"{name},,{width},,{height},,{fmt}.cover"
            _write_bytes(os.path.join(tdir, filename), data)
            os.makedirs(os.path.join(tdir, filename + ".done"), exist_ok=True)

    while True:
        try:
            plugin, width, height, fmt, data = results.get_nowait()
        except Empty:
            break
        else:
            name = plugin.name
            if getattr(plugin, "can_get_multiple_covers", False):
                name += "{%d}" % counter[plugin.name]
                counter[plugin.name] += 1
            filename = f"{name},,{width},,{height},,{fmt}.cover"
            _write_bytes(os.path.join(tdir, filename), data)
            os.makedirs(os.path.join(tdir, filename + ".done"), exist_ok=True)

    return log.dump()


__all__ = [
    "GUILog",
    "main",
    "merge_result",
    "single_covers",
    "single_identify",
]
