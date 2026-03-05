from __future__ import annotations

import queue
import struct
import zlib
from threading import Event

import pytest


def _png_bytes(width: int, height: int, rgb=(255, 0, 0)) -> bytes:
    def chunk(tag: bytes, payload: bytes) -> bytes:
        return (
            struct.pack(">I", len(payload))
            + tag
            + payload
            + struct.pack(">I", zlib.crc32(tag + payload) & 0xFFFFFFFF)
        )

    signature = b"\x89PNG\r\n\x1a\n"
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    row = bytes([0]) + bytes(rgb) * width
    raw = row * height
    idat = zlib.compress(raw)
    return signature + chunk(b"IHDR", ihdr) + chunk(b"IDAT", idat) + chunk(b"IEND", b"")


class _Plugin:
    def __init__(self, name: str, payload: bytes, *, can_multi: bool = False):
        self.name = name
        self.payload = payload
        self.can_get_multiple_covers = can_multi
        self.calls = []

    @staticmethod
    def is_configured() -> bool:
        return True

    def download_cover(self, **kwargs):
        self.calls.append(kwargs)
        kwargs["result_queue"].put((self, self.payload))

    @staticmethod
    def browser():
        class _B:
            addheaders = []

        return _B()


def test_web_sources_covers_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.covers as covers

    assert covers is not None


def test_process_result_accepts_valid_cover(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources import covers

    monkeypatch.setattr(covers, "save_cover_data_to", None)
    plugin = _Plugin("P", _png_bytes(80, 90))
    result = covers.process_result(lambda *args: None, (plugin, plugin.payload))

    assert result is not None
    _, width, height, fmt, data = result
    assert (width, height) == (80, 90)
    assert fmt in {"png", "jpeg", "jpg"}
    assert isinstance(data, bytes)


def test_process_result_rejects_small_cover(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources import covers

    monkeypatch.setattr(covers, "save_cover_data_to", None)
    plugin = _Plugin("P", _png_bytes(20, 20))
    result = covers.process_result(lambda *args: None, (plugin, plugin.payload))

    assert result is None


def test_run_download_collects_results_and_passes_best_cover_flag(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources import covers

    plugin = _Plugin("P", _png_bytes(100, 110), can_multi=True)
    monkeypatch.setattr(covers, "_iter_cover_plugins", lambda: [plugin])
    monkeypatch.setattr(covers, "save_cover_data_to", None)
    monkeypatch.setitem(covers.msprefs, "wait_after_first_cover_result", 0.1)

    out = queue.Queue()
    covers.run_download(
        log=lambda *args: None,
        results=out,
        abort=Event(),
        title="Book",
        authors=["Author"],
        identifiers={"isbn": "9780306406157"},
        timeout=3,
        get_best_cover=True,
    )

    parsed = out.get_nowait()
    assert parsed[0] is plugin
    assert plugin.calls
    assert plugin.calls[0]["get_best_cover"] is True


def test_download_cover_prefers_priority_over_resolution(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources import covers

    low_priority = _Plugin("Google", _png_bytes(220, 220))
    normal_priority = _Plugin("OpenLibrary", _png_bytes(120, 120))
    monkeypatch.setattr(covers, "_iter_cover_plugins", lambda: [low_priority, normal_priority])
    monkeypatch.setattr(covers, "save_cover_data_to", None)

    result = covers.download_cover(
        log=lambda *args: None,
        title="Book",
        authors=["Author"],
        identifiers={},
        timeout=3,
    )

    assert result is not None
    assert result[0].name == "OpenLibrary"


def test_download_cover_prefers_largest_with_same_priority(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources import covers

    a = _Plugin("A", _png_bytes(80, 80))
    b = _Plugin("B", _png_bytes(200, 200))
    monkeypatch.setattr(covers, "_iter_cover_plugins", lambda: [a, b])
    monkeypatch.setattr(covers, "save_cover_data_to", None)
    monkeypatch.setitem(covers.msprefs, "cover_priorities", {})

    result = covers.download_cover(log=lambda *args: None, title="Book", authors=["Author"], identifiers={}, timeout=3)
    assert result is not None
    assert result[0].name == "B"


def test_download_cover_returns_none_when_no_valid_results(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources import covers

    plugin = _Plugin("P", b"not-an-image")
    monkeypatch.setattr(covers, "_iter_cover_plugins", lambda: [plugin])
    monkeypatch.setattr(covers, "save_cover_data_to", None)

    result = covers.download_cover(log=lambda *args: None, title="Book", authors=["Author"], identifiers={}, timeout=2)
    assert result is None
