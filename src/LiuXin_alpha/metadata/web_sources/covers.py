"""
Cover download coordinator for metadata web-source plugins.

This module fans out cover requests to configured cover-capable plugins and
returns validated cover images.
"""

from __future__ import annotations

import time
from io import StringIO
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any

from LiuXin_alpha.metadata.web_sources.base import create_log
from LiuXin_alpha.metadata.web_sources.prefs import msprefs
from LiuXin_alpha.utils.image_tools.imghdr import identify as identify_image
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

try:
    from LiuXin_alpha.utils.image_tools.img import save_cover_data_to
except Exception:
    try:
        from LiuXin_alpha.utils.image_tools.img_fallback import save_cover_data_to
    except Exception:
        save_cover_data_to = None

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _iter_cover_plugins():
    try:
        from LiuXin_alpha.customize.ui import metadata_plugins
    except Exception as err:
        default_log.log_exception("Unable to import metadata_plugins for cover download.", err, "DEBUG")
        return []
    try:
        return [plugin for plugin in metadata_plugins(["cover"]) if plugin.is_configured()]
    except Exception as err:
        default_log.log_exception("Failed to enumerate cover metadata plugins.", err, "DEBUG")
        return []


def _as_bytes(data: Any) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, memoryview):
        return data.tobytes()
    raise TypeError(f"Cover data must be bytes-like, got {type(data)!r}")


class Worker(Thread):
    def __init__(
        self,
        plugin,
        abort: Event,
        title,
        authors,
        identifiers,
        timeout: float,
        result_queue: Queue,
        get_best_cover: bool = False,
    ):
        Thread.__init__(self, daemon=True)
        self.plugin = plugin
        self.abort = abort
        self.get_best_cover = get_best_cover
        self.buf = StringIO()
        self.log = create_log(self.buf)
        self.title = title
        self.authors = authors
        self.identifiers = identifiers
        self.timeout = timeout
        self.result_queue = result_queue
        self.time_spent = None

    def run(self):
        start_time = time.time()
        if not self.abort.is_set():
            try:
                kwargs = dict(
                    log=self.log,
                    result_queue=self.result_queue,
                    abort=self.abort,
                    title=self.title,
                    authors=self.authors,
                    identifiers=self.identifiers,
                    timeout=self.timeout,
                )
                if getattr(self.plugin, "can_get_multiple_covers", False):
                    kwargs["get_best_cover"] = self.get_best_cover
                self.plugin.download_cover(**kwargs)
            except Exception:
                self.log.exception("Failed to download cover from", self.plugin.name)
        self.time_spent = time.time() - start_time


def is_worker_alive(workers) -> bool:
    return any(worker.is_alive() for worker in workers)


def process_result(log, result):
    plugin, raw_data = result
    try:
        data = _as_bytes(raw_data)
        fmt, width, height = identify_image(data)
        if not fmt or width < 50 or height < 50:
            raise ValueError("Image too small or invalid format")

        if save_cover_data_to is not None:
            # Normalize to JPEG for stable downstream handling.
            normalized = save_cover_data_to(data, path=None, data_fmt="jpeg")
            nfmt, nwidth, nheight = identify_image(normalized)
            if nfmt and nwidth > 0 and nheight > 0:
                data = _as_bytes(normalized)
                fmt, width, height = nfmt, nwidth, nheight
    except Exception:
        if callable(log):
            log("Invalid cover from", getattr(plugin, "name", "<plugin>"))
        return None
    return (plugin, width, height, fmt, data)


def run_download(
    log,
    results: Queue,
    abort: Event,
    title=None,
    authors=None,
    identifiers={},
    timeout: float = 30,
    get_best_cover: bool = False,
):
    """
    Run asynchronous cover download and put results into `results`.

    Each result is `(plugin, width, height, fmt, bytes)`.
    """
    if title == _("Unknown"):
        title = None
    if authors == [_("Unknown")]:
        authors = None

    plugins = _iter_cover_plugins()
    raw_queue = Queue()

    workers = [
        Worker(
            plugin,
            abort=abort,
            title=title,
            authors=authors,
            identifiers=identifiers,
            timeout=timeout,
            result_queue=raw_queue,
            get_best_cover=get_best_cover,
        )
        for plugin in plugins
    ]
    for worker in workers:
        worker.start()

    first_result_at = None
    wait_time = float(msprefs.get("wait_after_first_cover_result", 60))
    found_results = {}
    start_time = time.time()

    while time.time() - start_time < 301:
        time.sleep(0.1)

        while True:
            try:
                raw = raw_queue.get_nowait()
            except Empty:
                break
            parsed = process_result(log, raw)
            if parsed is not None:
                results.put(parsed)
                found_results[parsed[0]] = parsed
                if first_result_at is None:
                    first_result_at = time.time()

        if not is_worker_alive(workers):
            break

        if first_result_at is not None and time.time() - first_result_at > wait_time:
            if callable(log):
                log("Not waiting for any more cover results")
            abort.set()
            break

        if abort.is_set():
            break

    while True:
        try:
            raw = raw_queue.get_nowait()
        except Empty:
            break
        parsed = process_result(log, raw)
        if parsed is not None:
            results.put(parsed)
            found_results[parsed[0]] = parsed

    for worker in workers:
        wlog = worker.buf.getvalue().strip()
        if callable(log):
            log("\n" + "*" * 30, worker.plugin.name, "Covers", "*" * 30)
        try:
            browser_attr = getattr(worker.plugin, "browser", None)
            browser_obj = browser_attr() if callable(browser_attr) else browser_attr
            headers = getattr(browser_obj, "addheaders", [])
        except Exception:
            headers = []
        if callable(log):
            log("Request extra headers:", headers)
        if worker.plugin in found_results:
            parsed = found_results[worker.plugin]
            if callable(log):
                log("Downloaded cover:", f"{parsed[1]}x{parsed[2]}")
        else:
            if callable(log):
                log("Failed to download valid cover")

        if worker.time_spent is None:
            if callable(log):
                log("Download aborted")
        else:
            if callable(log):
                log("Took", worker.time_spent, "seconds")
        if wlog and callable(log):
            log(wlog)
            log("\n" + "*" * 80)


def download_cover(log, title=None, authors=None, identifiers={}, timeout: float = 30):
    """
    Synchronous cover download.

    Returns `(plugin, width, height, fmt, data)` or `None`.
    """
    result_queue = Queue()
    abort = Event()
    run_download(
        log,
        result_queue,
        abort,
        title=title,
        authors=authors,
        identifiers=identifiers,
        timeout=timeout,
        get_best_cover=True,
    )

    results = []
    while True:
        try:
            results.append(result_queue.get_nowait())
        except Empty:
            break

    cover_priorities = msprefs.get("cover_priorities", {})

    def keygen(result):
        plugin, width, height, _fmt, _data = result
        area = max(1, int(width) * int(height))
        return (cover_priorities.get(plugin.name, 1), 1 / area)

    results.sort(key=keygen)
    return results[0] if results else None


__all__ = [
    "Worker",
    "download_cover",
    "is_worker_alive",
    "process_result",
    "run_download",
]
