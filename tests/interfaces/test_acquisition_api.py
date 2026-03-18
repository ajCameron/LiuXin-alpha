from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from LiuXin_alpha.interfaces.acquisition.api import AcquisitionCompatApi
from LiuXin_alpha.interfaces.web_readonly.app import _ResolvedFileTarget, _Response


@dataclass(frozen=True)
class _WorkRow:
    work_id: int = 1


@dataclass(frozen=True)
class _ImageRow:
    image_id: int = 1


@dataclass(frozen=True)
class _FileRow:
    file_id: int = 7
    name: str = "dummy.epub"


class _DummyHost:
    def __init__(self) -> None:
        self.work_row = _WorkRow()
        self.image_row = _ImageRow()
        self.file_row = _FileRow()

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
        class _Stored:
            def as_bytes(self_nonlocal):
                return b"image-bytes"

        return _Stored() if image_row == self.image_row else None

    def acquisition_resolve_image_target(self, image_row):
        return _ResolvedFileTarget(mode="local", location="/tmp/cover.png", download_name="cover.png") if image_row == self.image_row else None

    def acquisition_image_download_name(self, image_row) -> str:
        return "cover.png"

    def acquisition_image_content_type(self, image_row) -> str:
        return "image/png"

    def acquisition_placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        return f"<svg>{width}x{height}</svg>".encode("utf-8")

    def acquisition_related_rows_by_table(self, work_row) -> dict[str, list[object]]:
        return {"files": [self.file_row]}

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
    assert headers["X-File-Id"] == "7"
    assert body == b"download"


def test_acquisition_api_rejects_invalid_book_id() -> None:
    api = AcquisitionCompatApi(_DummyHost())
    status, headers, body = _decode_response(api.serve_compat_get("epub", "bad", {}, {}))
    assert status == "400 Bad Request"
    assert headers["Content-Type"] == "text/plain"
    assert b"Invalid book id" in body
