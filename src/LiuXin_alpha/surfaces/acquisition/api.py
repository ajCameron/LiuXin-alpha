from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from LiuXin_alpha.surfaces.api import AcquisitionHostApi, SurfaceResponseAPI


def _coerce_payload_bytes(payload) -> bytes:
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if isinstance(payload, bytes):
        return payload
    return bytes(payload)


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

    def serve_cover_or_thumb(self, raw_book_id: str, *, query: dict[str, list[str]], environ, thumb: bool) -> SurfaceResponseAPI:
        row_id, suffix = self.host.acquisition_split_book_token(raw_book_id)
        if row_id is None:
            return self.host.acquisition_text_response("400 Bad Request", "Invalid book id.\n", content_type="text/plain")
        work_row = self.host.acquisition_work_row(row_id)
        if work_row is None:
            return self.host.acquisition_text_response("404 Not Found", "Book row not found.\n", content_type="text/plain")

        image_row = self.host.acquisition_work_image_row(work_row)
        if image_row is not None:
            stored_file = self.host.acquisition_resolve_storage_image(image_row)
            if stored_file is not None:
                try:
                    return self.host.acquisition_bytes_response(
                        _coerce_payload_bytes(stored_file.as_bytes()),
                        download_name=self.host.acquisition_image_download_name(image_row),
                        disposition="inline",
                        content_type_override=self.host.acquisition_image_content_type(image_row),
                    )
                except Exception:
                    pass
            target = self.host.acquisition_resolve_image_target(image_row)
            if target is not None:
                if target.mode == "redirect":
                    return self.host.acquisition_redirect_response(target.location)
                return self.host.acquisition_file_response(
                    Path(target.location),
                    download_name=target.download_name,
                    environ=environ,
                    disposition="inline",
                    content_type_override=self.host.acquisition_image_content_type(image_row),
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
        work_row = self.host.acquisition_work_row(row_id)
        if work_row is None:
            return self.host.acquisition_text_response("404 Not Found", "Book row not found.\n", content_type="text/plain")

        related_rows_by_table = self.host.acquisition_related_rows_by_table(work_row)
        file_rows = self.host.acquisition_work_file_rows(related_rows_by_table)
        target_ext = lowered.lstrip(".")
        for file_row in file_rows:
            download_name = self.host.acquisition_download_name_for_file_row(file_row)
            ext = Path(download_name).suffix.lower().lstrip(".")
            if ext == target_ext:
                file_id = self.host.acquisition_file_id(file_row)
                if file_id in (None, ""):
                    continue
                return self.host.acquisition_serve_file_download(str(file_id), environ)
        return self.host.acquisition_text_response("404 Not Found", "No such format for this book.\n", content_type="text/plain")
