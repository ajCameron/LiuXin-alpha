from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from LiuXin_alpha.surfaces.acquisition.api import (
    AcquisitionCompatApi,
    _coerce_payload_bytes,
    _cover_dimensions,
)
from LiuXin_alpha.surfaces.web_readonly.app import _ResolvedFileTarget, _Response


@dataclass(frozen=True)
class _WorkRow:
    work_id: int = 1


@dataclass(frozen=True)
class _ImageRow:
    image_id: int = 1


@dataclass(frozen=True)
class _FileRow:
    file_id: object = 7
    name: str = "dummy.epub"


class _DummyCore:
    def __init__(self, host: "_DummyHost") -> None:
        self.host = host

    def query(self, name: str, payload=None):
        payload = dict(payload or {})
        if name == "browse.work":
            return {
                "work": self.host.work_row
                if int(payload["work_id"]) == 1
                else None
            }
        if name == "acquisition.cover":
            if self.host.image_row is None:
                return {"covers": []}
            resolution = self._image_resolution()
            return {
                "covers": [
                    {
                        "kind": "image",
                        "id": self.host.image_row.image_id,
                        "name": "cover.png",
                        "mime_type": "image/png",
                        "resolution": resolution,
                    }
                ]
            }
        if name == "acquisition.formats":
            formats = []
            for row in self.host.file_rows:
                if row.file_id is None:
                    continue
                formats.append(
                    {
                        "kind": "legacy-file",
                        "id": row.file_id,
                        "name": row.name,
                        "extension": Path(row.name).suffix.lstrip("."),
                        "mime_type": "application/epub+zip",
                        "resolution": {
                            "delivery": "core",
                            "readable": True,
                        },
                    }
                )
            return {"formats": formats}
        if name == "acquisition.read":
            if payload["kind"] == "image":
                stored = self.host.stored_payload
                if isinstance(stored, Exception):
                    raise stored
                return {
                    "resource": self._image_resolution(),
                    "content": b"file" if stored is None else stored,
                }
            return {
                "resource": {"delivery": "core", "readable": True},
                "content": b"download",
            }
        raise AssertionError("Unexpected Core query: {}".format(name))

    def _image_resolution(self) -> dict[str, object]:
        target = self.host.image_target
        if target is not None and target.mode == "redirect":
            return {
                "delivery": "redirect",
                "readable": False,
                "location": target.location,
            }
        if self.host.stored_payload is not None or target is not None:
            return {"delivery": "core", "readable": True}
        return {"delivery": "unavailable", "readable": False}


class _DummyHost:
    def __init__(self) -> None:
        self.work_row = _WorkRow()
        self.image_row = _ImageRow()
        self.file_row = _FileRow()
        self.file_rows = [self.file_row]
        self.stored_payload: object | None = b"image-bytes"
        self.image_target: _ResolvedFileTarget | None = _ResolvedFileTarget(
            mode="local",
            location="/tmp/cover.png",
            download_name="cover.png",
        )
        self.placeholder_dimensions: list[tuple[int, int]] = []
        self.core = _DummyCore(self)

    def acquisition_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        return _Response(status=status, headers=[("Content-Type", content_type)], body=[text.encode("utf-8")])

    def acquisition_bytes_response(
        self,
        payload: bytes,
        *,
        download_name: str,
        disposition: str = "attachment",
        content_type_override: str | None = None,
    ) -> _Response:
        return _Response(
            status="200 OK",
            headers=[("Content-Type", content_type_override or "application/octet-stream"), ("X-Name", download_name), ("X-Disposition", disposition)],
            body=[payload],
        )

    def acquisition_redirect_response(self, location: str) -> _Response:
        return _Response(status="302 Found", headers=[("Location", location)], body=[b""])

    def acquisition_file_response(self, path: Path, *, download_name: str, environ, disposition: str = "attachment", content_type_override: str | None = None) -> _Response:
        del environ
        return _Response(
            status="200 OK",
            headers=[("Content-Type", content_type_override or "application/octet-stream"), ("X-Path", str(path)), ("X-Name", download_name), ("X-Disposition", disposition)],
            body=[b"file"],
        )

    def acquisition_split_book_token(self, raw_book_id: str) -> tuple[int | None, str]:
        if raw_book_id == "bad":
            return None, ""
        base, _sep, suffix = str(raw_book_id).partition("_")
        return int(base), suffix

    def acquisition_work_row(self, row_id: int):
        return self.work_row if int(row_id) == 1 else None

    def acquisition_work_image_row(self, work_row):
        return self.image_row if work_row == self.work_row else None

    def acquisition_resolve_storage_image(self, image_row):
        payload = self.stored_payload

        class _Stored:
            def as_bytes(self_nonlocal):
                if isinstance(payload, Exception):
                    raise payload
                return payload

        if image_row != self.image_row or payload is None:
            return None
        return _Stored()

    def acquisition_resolve_image_target(self, image_row):
        return self.image_target if image_row == self.image_row else None

    def acquisition_image_download_name(self, image_row) -> str:
        return "cover.png"

    def acquisition_image_content_type(self, image_row) -> str:
        return "image/png"

    def acquisition_placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        self.placeholder_dimensions.append((width, height))
        return f"<svg>{width}x{height}</svg>".encode("utf-8")

    def acquisition_related_rows_by_table(self, work_row) -> dict[str, list[object]]:
        return {"files": list(self.file_rows)}

    def acquisition_work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        return list(related_rows_by_table.get("files", []))

    def acquisition_download_name_for_file_row(self, file_row) -> str:
        return file_row.name

    def acquisition_file_id(self, file_row) -> object:
        return file_row.file_id

    def acquisition_serve_file_download(self, raw_file_id: str, environ) -> _Response:
        del environ
        return _Response(status="200 OK", headers=[("X-File-Id", str(raw_file_id))], body=[b"download"])


def _decode_response(response: _Response) -> tuple[str, dict[str, str], bytes]:
    return response.status, dict(response.headers), b"".join(response.body)


def test_acquisition_api_serves_cover_bytes() -> None:
    api = AcquisitionCompatApi(_DummyHost())
    status, headers, body = _decode_response(api.serve_compat_get("cover", "1", {}, {}))
    assert status == "200 OK"
    assert headers["Content-Type"] == "image/png"
    assert headers["X-Disposition"] == "inline"
    assert body == b"image-bytes"


def test_acquisition_api_serves_format_download() -> None:
    api = AcquisitionCompatApi(_DummyHost())
    status, headers, body = _decode_response(api.serve_compat_get("epub", "1", {}, {}))
    assert status == "200 OK"
    assert headers["X-Name"] == "dummy.epub"
    assert body == b"download"


def test_acquisition_api_rejects_invalid_book_id() -> None:
    api = AcquisitionCompatApi(_DummyHost())
    status, headers, body = _decode_response(api.serve_compat_get("epub", "bad", {}, {}))
    assert status == "400 Bad Request"
    assert headers["Content-Type"] == "text/plain"
    assert b"Invalid book id" in body


@pytest.mark.parametrize(
    ("payload", "expected"),
    (
        ("cover", b"cover"),
        (b"cover", b"cover"),
        (bytearray(b"cover"), b"cover"),
    ),
)
def test_payloads_are_coerced_to_bytes(payload: object, expected: bytes) -> None:
    assert _coerce_payload_bytes(payload) == expected


@pytest.mark.parametrize(
    ("suffix", "query", "thumb", "expected"),
    (
        ("90_120", {}, True, (90, 120)),
        ("single", {}, True, (60, 80)),
        ("bad_suffix", {}, True, (60, 80)),
        ("90_120", {"sz": ["4x5"]}, True, (4, 5)),
        ("", {"sz": ["0x-2"]}, True, (1, 1)),
        ("90_120", {"sz": ["badxsize"]}, True, (90, 120)),
        ("", {"sz": ["full"]}, True, (240, 320)),
        ("", {}, False, (240, 320)),
        ("", {"sz": ["7"]}, True, (7, 7)),
        ("", {"sz": ["invalid"]}, True, (60, 80)),
    ),
)
def test_cover_dimensions_accept_compatibility_size_forms(
    suffix: str,
    query: dict[str, list[str]],
    thumb: bool,
    expected: tuple[int, int],
) -> None:
    assert _cover_dimensions(suffix=suffix, query=query, thumb=thumb) == expected


def test_cover_requests_reject_invalid_or_missing_books() -> None:
    api = AcquisitionCompatApi(_DummyHost())

    invalid = _decode_response(api.serve_cover_or_thumb("bad", query={}, environ={}, thumb=True))
    missing = _decode_response(api.serve_cover_or_thumb("2", query={}, environ={}, thumb=True))

    assert invalid[0] == "400 Bad Request"
    assert b"Invalid book id" in invalid[2]
    assert missing[0] == "404 Not Found"
    assert b"Book row not found" in missing[2]


def test_cover_request_uses_placeholder_when_no_image_exists() -> None:
    host = _DummyHost()
    host.image_row = None
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_compat_get("thumb", "1_90_120", {}, {})
    )

    assert status == "200 OK"
    assert headers["Content-Type"] == "image/svg+xml"
    assert headers["X-Name"] == "cover.svg"
    assert host.placeholder_dimensions == [(90, 120)]
    assert body == b"<svg>90x120</svg>"


def test_failed_stored_cover_falls_back_to_redirect_target() -> None:
    host = _DummyHost()
    host.stored_payload = RuntimeError("unreadable")
    host.image_target = _ResolvedFileTarget(
        mode="redirect",
        location="https://covers.example/cover.png",
        download_name="cover.png",
    )
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_cover_or_thumb("1", query={}, environ={}, thumb=False)
    )

    assert status == "302 Found"
    assert headers["Location"] == "https://covers.example/cover.png"
    assert body == b""


def test_missing_stored_cover_uses_local_file_target() -> None:
    host = _DummyHost()
    host.stored_payload = None
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_cover_or_thumb("1", query={}, environ={"key": "value"}, thumb=False)
    )

    assert status == "200 OK"
    assert headers["X-Name"] == "cover.png"
    assert headers["X-Disposition"] == "inline"
    assert body == b"file"


def test_missing_cover_targets_fall_back_to_full_size_placeholder() -> None:
    host = _DummyHost()
    host.stored_payload = None
    host.image_target = None
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_cover_or_thumb("1", query={}, environ={}, thumb=False)
    )

    assert status == "200 OK"
    assert headers["Content-Type"] == "image/svg+xml"
    assert host.placeholder_dimensions == [(240, 320)]
    assert body == b"<svg>240x320</svg>"


def test_format_requests_report_missing_books_and_formats() -> None:
    api = AcquisitionCompatApi(_DummyHost())

    missing_book = _decode_response(api.serve_compat_get("epub", "2", {}, {}))
    missing_format = _decode_response(api.serve_compat_get("pdf", "1", {}, {}))

    assert missing_book[0] == "404 Not Found"
    assert b"Book row not found" in missing_book[2]
    assert missing_format[0] == "404 Not Found"
    assert b"No such format" in missing_format[2]


def test_format_matching_skips_rows_without_file_ids() -> None:
    host = _DummyHost()
    host.file_rows = [
        _FileRow(file_id=None, name="missing.epub"),
        _FileRow(file_id=8, name="available.EPUB"),
    ]
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_compat_get(".EPUB", "1", {}, {"request": "environment"})
    )

    assert status == "200 OK"
    assert headers["X-Name"] == "available.EPUB"
    assert body == b"download"
