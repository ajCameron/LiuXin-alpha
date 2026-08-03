from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping

from LiuXin_alpha.surfaces.api import AcquisitionHostApi, SurfaceResponseAPI
from LiuXin_alpha.surfaces.core import CoreSurfaceModel


def _coerce_payload_bytes(payload: object) -> bytes:
    """Normalize presentation byte payloads retained by compatibility tests."""

    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if isinstance(payload, (bytearray, memoryview)):
        return bytes(payload)
    raise TypeError("Acquisition payload must be bytes-like or text.")


def _cover_dimensions(*, suffix: str, query: dict[str, list[str]], thumb: bool) -> tuple[int, int]:
    width, height = (60, 80)
    size = str((query.get("sz") or [None])[0] or "").strip().lower()
    if suffix:
        bits = [bit for bit in suffix.split("_") if bit]
        if len(bits) >= 2:
            try:
                width, height = int(bits[0]), int(bits[1])
            except Exception:
                pass
    if "x" in size:
        try:
            width, height = [max(1, int(one)) for one in size.split("x", 1)]
        except Exception:
            pass
    elif size == "full" or not thumb:
        width, height = (240, 320)
    elif size:
        try:
            width = height = max(1, int(size))
        except Exception:
            pass
    return width, height


@dataclass
class AcquisitionCompatApi:
    host: AcquisitionHostApi

    def __post_init__(self) -> None:
        self.model = CoreSurfaceModel(self.host.core)

    def _work(self, work_id: int) -> object | None:
        result = self.host.core.query(
            "browse.work",
            {"work_id": int(work_id)},
        )
        return result.get("work") if isinstance(result, Mapping) else None

    @staticmethod
    def _records(result: object, key: str) -> list[Mapping[str, object]]:
        raw = result.get(key, ()) if isinstance(result, Mapping) else ()
        if not isinstance(raw, list):
            return []
        return [value for value in raw if isinstance(value, Mapping)]

    def serve_cover_or_thumb(self, raw_book_id: str, *, query: dict[str, list[str]], environ, thumb: bool) -> SurfaceResponseAPI:
        row_id, suffix = self.host.acquisition_split_book_token(raw_book_id)
        if row_id is None:
            return self.host.acquisition_text_response("400 Bad Request", "Invalid book id.\n", content_type="text/plain")
        work_row = self._work(row_id)
        if work_row is None:
            return self.host.acquisition_text_response("404 Not Found", "Book row not found.\n", content_type="text/plain")

        covers_result = self.host.core.query(
            "acquisition.cover",
            {"work_id": int(row_id)},
        )
        for cover in self._records(covers_result, "covers"):
            cover_id = cover.get("id")
            resolution = cover.get("resolution", {})
            if cover_id is None or not isinstance(resolution, Mapping):
                continue
            if bool(resolution.get("readable")):
                try:
                    _resource, payload = self.model.acquisition_read(
                        "image",
                        int(cover_id),
                    )
                    return self.host.acquisition_bytes_response(
                        payload,
                        download_name=str(cover.get("name") or "cover.bin"),
                        disposition="inline",
                        content_type_override=str(
                            cover.get("mime_type")
                            or "application/octet-stream"
                        ),
                    )
                except Exception:
                    pass
            if (
                str(resolution.get("delivery") or "") == "redirect"
                and resolution.get("location")
            ):
                return self.host.acquisition_redirect_response(
                    str(resolution["location"])
                )

        width, height = _cover_dimensions(suffix=suffix, query=query, thumb=thumb)
        return self.host.acquisition_bytes_response(
            self.host.acquisition_placeholder_cover_svg(work_row, width=width, height=height),
            download_name="cover.svg",
            disposition="inline",
            content_type_override="image/svg+xml",
        )

    def serve_compat_get(self, what: str, raw_book_id: str, query: dict[str, list[str]], environ) -> SurfaceResponseAPI:
        lowered = str(what or "").strip().lower()
        if lowered in {"thumb", "cover"}:
            return self.serve_cover_or_thumb(raw_book_id, query=query, environ=environ, thumb=(lowered == "thumb"))

        row_id, _suffix = self.host.acquisition_split_book_token(raw_book_id)
        if row_id is None:
            return self.host.acquisition_text_response("400 Bad Request", "Invalid book id.\n", content_type="text/plain")
        work_row = self._work(row_id)
        if work_row is None:
            return self.host.acquisition_text_response("404 Not Found", "Book row not found.\n", content_type="text/plain")

        formats_result = self.host.core.query(
            "acquisition.formats",
            {"work_id": int(row_id)},
        )
        target_ext = lowered.lstrip(".")
        for record in self._records(formats_result, "formats"):
            if str(record.get("extension") or "").lower().lstrip(".") != target_ext:
                continue
            resource_id = record.get("id")
            kind = str(record.get("kind") or "")
            resolution = record.get("resolution", {})
            if (
                resource_id in (None, "")
                or not kind
                or not isinstance(resolution, Mapping)
            ):
                continue
            if bool(resolution.get("readable")):
                _resource, payload = self.model.acquisition_read(
                    kind,
                    int(resource_id),
                )
                return self.host.acquisition_bytes_response(
                    payload,
                    download_name=str(record.get("name") or "download.bin"),
                    content_type_override=str(
                        record.get("mime_type")
                        or "application/octet-stream"
                    ),
                )
            if (
                str(resolution.get("delivery") or "") == "redirect"
                and resolution.get("location")
            ):
                return self.host.acquisition_redirect_response(
                    str(resolution["location"])
                )
        return self.host.acquisition_text_response("404 Not Found", "No such format for this book.\n", content_type="text/plain")
