from __future__ import annotations

import mimetypes

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol
from urllib.parse import urljoin

from LiuXin_alpha.surfaces.web_readonly.app import _ResolvedFileTarget, _escape, _row_value, _short_text


class ImageHostApi(Protocol):
    @property
    def db(self): ...

    def _related_rows_by_table(self, row) -> dict[str, list[object]]: ...

    def _row_dict(self, table: str, row) -> dict[str, object]: ...

    def _row_primary_text(self, table: str, row) -> str: ...

    def _refresh_storage_manager(self) -> bool: ...


@dataclass
class ImageBackend:
    host: ImageHostApi

    def work_image_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        image_rows_by_id: dict[int, object] = {}

        def add_image_row(image_row) -> None:
            image_id = _row_value(image_row, "image_id")
            if image_id in (None, ""):
                return
            try:
                image_rows_by_id[int(image_id)] = image_row
            except Exception:
                return

        for image_row in related_rows_by_table.get("images", []):
            add_image_row(image_row)

        for expression_row in related_rows_by_table.get("expressions", []):
            try:
                manifestation_rows = list(self.host.db.get_interlinked_rows(target_row=expression_row, secondary_table="manifestations"))
            except Exception:
                manifestation_rows = []
            for manifestation_row in manifestation_rows:
                manifestation_id = _row_value(manifestation_row, "manifestation_id")
                if manifestation_id in (None, ""):
                    continue
                try:
                    item_rows = list(self.host.db.search("items", "item_manifestation_id", manifestation_id))
                except Exception:
                    item_rows = []
                for item_row in item_rows:
                    item_id = _row_value(item_row, "item_id")
                    if item_id in (None, ""):
                        continue
                    try:
                        discovered_image_rows = list(self.host.db.search("images", "image_item_id", item_id))
                    except Exception:
                        discovered_image_rows = []
                    for image_row in discovered_image_rows:
                        add_image_row(image_row)
        return list(image_rows_by_id.values())

    def image_download_name(self, image_row) -> str:
        row = self.host._row_dict("images", image_row)
        return str(row.get("image_name") or row.get("image_original_name") or row.get("image_storage_key") or "cover.bin")

    def image_content_type(self, image_row) -> str:
        row = self.host._row_dict("images", image_row)
        mime = str(row.get("image_mime_type") or "").strip()
        if mime:
            return mime
        guessed, _encoding = mimetypes.guess_type(self.image_download_name(image_row))
        return guessed or "application/octet-stream"

    def image_storage_lookup_metadata(self, image_row) -> dict[str, object]:
        row = self.host._row_dict("images", image_row)
        metadata: dict[str, object] = dict(row)
        metadata["image_row"] = dict(row)
        aliases = {
            "file_store_id": row.get("image_store_id"),
            "file_storage_key": row.get("image_storage_key"),
            "file_name": row.get("image_name"),
            "file_original_name": row.get("image_original_name"),
            "file_original_path": row.get("image_original_path"),
            "file_source": row.get("image_source"),
        }
        metadata.update({key: value for key, value in aliases.items() if value not in (None, "")})
        return metadata

    def resolve_storage_image(self, image_row):
        metadata = self.image_storage_lookup_metadata(image_row)
        should_refresh = bool(metadata.get("image_store_id") not in (None, ""))
        storage = getattr(self.host.db, "storage", None)
        for attempt in range(2 if should_refresh else 1):
            if storage is None and not self.host._refresh_storage_manager():
                storage = getattr(self.host.db, "storage", None)
                if storage is None:
                    continue
            else:
                storage = getattr(self.host.db, "storage", None)
            if storage is None:
                continue
            try:
                return storage.locate_file(metadata=metadata)
            except Exception:
                pass
            if attempt == 0 and should_refresh:
                self.host._refresh_storage_manager()
                storage = getattr(self.host.db, "storage", None)
        return None

    def resolve_image_target(self, image_row) -> Optional[_ResolvedFileTarget]:
        row = self.host._row_dict("images", image_row)
        image_name = self.image_download_name(image_row)
        direct_local_candidates = [row.get("image_original_path"), row.get("image_path")]
        for candidate in direct_local_candidates:
            text = str(candidate or "").strip()
            if not text:
                continue
            path = Path(text)
            if path.is_file():
                return _ResolvedFileTarget(mode="local", location=str(path), download_name=image_name)
        direct_url_candidates = [row.get("image_source"), row.get("image_original_path")]
        for candidate in direct_url_candidates:
            text = str(candidate or "").strip()
            if text.startswith(("http://", "https://")):
                return _ResolvedFileTarget(mode="redirect", location=text, download_name=image_name)
        storage_key = str(row.get("image_storage_key") or "").strip()
        store_id = row.get("image_store_id", None)
        if storage_key and store_id not in (None, ""):
            try:
                store_row = self.host.db.get_row_from_id("stores", int(store_id))
            except Exception:
                store_row = None
            if store_row is not None:
                root_uri = str(_row_value(store_row, "store_root_uri") or "").strip()
                access_protocol = str(_row_value(store_row, "store_access_protocol") or "").strip().lower()
                if root_uri.startswith(("http://", "https://")):
                    base = root_uri if root_uri.endswith("/") else root_uri + "/"
                    return _ResolvedFileTarget(mode="redirect", location=urljoin(base, storage_key), download_name=image_name)
                if root_uri.startswith("file://"):
                    local_root = Path(root_uri[7:])
                else:
                    local_root = Path(root_uri) if root_uri else None
                if local_root is not None and (access_protocol in {"", "file", "local"} or local_root.is_absolute()):
                    candidate = local_root / storage_key
                    if candidate.is_file():
                        return _ResolvedFileTarget(mode="local", location=str(candidate), download_name=image_name)
        return None

    def work_image_row(self, work_row) -> Optional[object]:
        related = self.host._related_rows_by_table(work_row)
        image_rows = self.work_image_rows(related)
        if image_rows:
            return image_rows[0]
        return None

    @staticmethod
    def thumbnail_text(text: str) -> str:
        stripped = str(text or "").strip()
        for char in stripped:
            if char.isalnum():
                return char.upper()
        return "?"

    def placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        title = self.host._row_primary_text("works", work_row)
        initial = self.thumbnail_text(title)
        font_size = max(18, min(48, int(width * 0.35)))
        subtitle = _short_text(title, width=48)
        svg = """<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>
<defs>
  <linearGradient id='g' x1='0' y1='0' x2='0' y2='1'>
    <stop offset='0%' stop-color='#d8e5ef'/>
    <stop offset='100%' stop-color='#9db4c7'/>
  </linearGradient>
</defs>
<rect width='{width}' height='{height}' rx='12' fill='url(#g)'/>
<text x='50%' y='42%' text-anchor='middle' dominant-baseline='middle' font-family='Georgia, serif' font-size='{font_size}' font-weight='700' fill='#ffffff'>{initial}</text>
<text x='50%' y='{subtitle_y}' text-anchor='middle' font-family='Georgia, serif' font-size='11' fill='#f6fbff'>{subtitle}</text>
</svg>""".format(
            width=width,
            height=height,
            font_size=font_size,
            initial=_escape(initial),
            subtitle=_escape(subtitle),
            subtitle_y=max(height - 14, int(height * 0.82)),
        )
        return svg.encode("utf-8")
