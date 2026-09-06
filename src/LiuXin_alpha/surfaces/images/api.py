"""Core-backed image, cover, and thumbnail retrieval facade."""

from __future__ import annotations

import mimetypes

from dataclasses import dataclass
from typing import Optional

from LiuXin_alpha.surfaces.api import ImageHostApi
from LiuXin_alpha.surfaces.core import CoreSurfaceModel
from LiuXin_alpha.surfaces.acquisition_types import (
    CoreStoredFile as _CoreStoredFile,
    ResolvedFileTarget as _ResolvedFileTarget,
)
from LiuXin_alpha.surfaces.presentation import (
    escape as _escape,
    row_value as _row_value,
    short_text as _short_text,
)


@dataclass
class ImageBackend:
    """Resolve and render catalogue images through a narrow surface host.

    Missing images and explicit delivery limits are normal outcomes. Discovery
    and acquisition-query failures propagate instead of hiding behind an empty
    image list or an unavailable target.
    """

    host: ImageHostApi

    def work_image_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        """Collect direct and item-linked images without returning partial reads.

        A host without a read model supports direct images only. A failed
        relationship or image query is not that limited-capability case.
        """
        image_rows_by_id: dict[int, object] = {}

        def add_image_row(image_row) -> None:
            image_id = _row_value(image_row, "image_id")
            if image_id in (None, ""):
                return
            try:
                image_rows_by_id[int(image_id)] = image_row
            except (TypeError, ValueError, OverflowError):
                return

        for image_row in related_rows_by_table.get("images", []):
            add_image_row(image_row)

        read_model = getattr(self.host, "read_model", None)
        if read_model is None:
            return list(image_rows_by_id.values())

        for expression_row in related_rows_by_table.get("expressions", []):
            manifestation_rows = read_model.interlinked_rows(expression_row, "manifestations")
            for manifestation_row in manifestation_rows:
                manifestation_id = _row_value(manifestation_row, "manifestation_id")
                if manifestation_id in (None, ""):
                    continue
                item_rows = read_model.search_rows("items", "item_manifestation_id", manifestation_id)
                for item_row in item_rows:
                    item_id = _row_value(item_row, "item_id")
                    if item_id in (None, ""):
                        continue
                    discovered_image_rows = read_model.search_rows("images", "image_item_id", item_id)
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
        """Return a byte reader for a readable image, or None for invalid/unreadable IDs.

        A failed acquisition query propagates; it does not mean unreadable.
        """
        image_id = _row_value(image_row, "image_id")
        if image_id in (None, ""):
            return None
        model = self._model()
        try:
            numeric_id = int(image_id)
        except (TypeError, ValueError, OverflowError):
            return None
        resolved = model.acquisition_resolve("image", numeric_id)
        if not bool(resolved.get("readable", False)):
            return None
        return _CoreStoredFile(
            model=model,
            kind="image",
            resource_id=int(image_id),
        )

    def resolve_image_target(self, image_row) -> Optional[_ResolvedFileTarget]:
        """Return a redirect target, or None when invalid or not offered by Core.

        Acquisition-query failures propagate rather than becoming unavailable.
        """
        image_id = _row_value(image_row, "image_id")
        if image_id in (None, ""):
            return None
        image_name = self.image_download_name(image_row)
        try:
            numeric_id = int(image_id)
        except (TypeError, ValueError, OverflowError):
            return None
        resolved = self._model().acquisition_resolve("image", numeric_id)
        if str(resolved.get("delivery") or "") == "redirect":
            return _ResolvedFileTarget(
                mode="redirect",
                location=str(resolved.get("location") or ""),
                download_name=str(resolved.get("name") or image_name),
            )
        return None

    def _model(self) -> CoreSurfaceModel:
        read_model = getattr(self.host, "read_model", None)
        model = getattr(read_model, "model", None)
        if isinstance(model, CoreSurfaceModel):
            return model
        return CoreSurfaceModel(self.host.core)

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
