"""
Discover catalogue images, resolve Core acquisition routes, and render SVG cover fallbacks through a borrowed surface host.

The backend uses shared row/text primitives rather than importing an application.
Direct images are augmented through expression/manifestation/item relationships
when a host read model exists. It preserves provider failures while distinguishing
absent/invalid IDs and explicit delivery limits. Placeholder rendering generates
SVG bytes; this module neither resizes fetched raster images nor caches thumbnails.
"""

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
    """
    Resolve and render catalogue images through a borrowed host without owning its Core lifecycle.

    Missing images and explicit delivery limits are normal outcomes. Discovery
    and acquisition-query failures propagate instead of hiding behind an empty
    image list or an unavailable target. host is retained by reference without a
    constructor-time query. An optional host.read_model is used for discovery;
    its CoreSurfaceModel is reused when available, otherwise resolution constructs
    a fresh model per call without caching it on this backend.

    Example:
        >>> ImageBackend.thumbnail_text("-- élan")
        'É'
    """

    host: ImageHostApi

    def work_image_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        """
        Collect direct images and expression/item-linked images, deduplicating by integer-converted image ID.

        Direct images are visited first. With a non-None read model, traverse each
        expression's manifestations, search their items, then search images by item
        ID. Empty/None manifestation and item IDs skip those branches; their other
        values are passed through without numeric coercion. Image IDs are converted
        to integers, skipping only missing values or TypeError/ValueError/OverflowError
        conversion failures. Later duplicate IDs replace earlier rows without moving
        their first insertion position. No independent cover-role or positive-ID
        filter is applied. Provider/iteration errors prevent a partial return.

        Example:
            >>> from types import SimpleNamespace
            >>> backend = ImageBackend(SimpleNamespace())
            >>> rows = backend.work_image_rows({"images": [{"image_id": "7"}]})
            >>> rows[0]["image_id"]
            '7'


        :param related_rows_by_table: Existing related-row groups supplying direct images and expression roots.
        :return: Deduplicated original row objects in first-ID discovery order, direct-only when no read model is available.
        """
        image_rows_by_id: dict[int, object] = {}

        def add_image_row(image_row) -> None:
            """
            Retain a row under its integer image ID, ignoring missing IDs and ordinary numeric-conversion failures.

            Row-access failures other than missing-key fallback propagate before
            conversion. Existing IDs replace their value without moving dict order.

            Example:
                >>> add_image_row({"image_id": "7"})  # doctest: +SKIP


            :param image_row: Subscriptable row whose image_id selects its deduplication key.
            :return: None after inserting/replacing the original row or skipping an unusable ID.
            """
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
        """
        Choose the first truthy image name, original name, storage key, or cover.bin fallback from host-projected metadata.

        Example:
            >>> name = backend.image_download_name(image_row)  # doctest: +SKIP


        :param image_row: Image row projected through host._row_dict with images schema context.
        :return: Stringified suggested filename without stripping, sanitization, or filesystem validation.
        """
        row = self.host._row_dict("images", image_row)
        return str(row.get("image_name") or row.get("image_original_name") or row.get("image_storage_key") or "cover.bin")

    def image_content_type(self, image_row) -> str:
        """
        Prefer stripped explicit image MIME text, otherwise guess from the suggested download name.

        Fallback obtains a fresh host projection through image_download_name.
        MIME syntax is not validated, the content is not inspected, and the
        standard library's separate encoding guess is ignored.

        Example:
            >>> media_type = backend.image_content_type(image_row)  # doctest: +SKIP


        :param image_row: Image row supplying declared MIME type and filename candidates.
        :return: Explicit/guessed media type, or application/octet-stream when no type is inferred.
        """
        row = self.host._row_dict("images", image_row)
        mime = str(row.get("image_mime_type") or "").strip()
        if mime:
            return mime
        guessed, _encoding = mimetypes.guess_type(self.image_download_name(image_row))
        return guessed or "application/octet-stream"

    def image_storage_lookup_metadata(self, image_row) -> dict[str, object]:
        """
        Copy image metadata and add legacy file-field aliases for non-None, nonempty image fields.

        The image_row entry is a separate shallow copy of the original projection.
        Store ID, storage key, name, original name/path, and source become file_*
        aliases; zero/false values are retained. Eligible aliases overwrite existing
        file-field values. All nested objects remain shared.

        Example:
            >>> metadata = backend.image_storage_lookup_metadata(image_row)  # doctest: +SKIP


        :param image_row: Image row projected by the host's visible-column policy.
        :return: New metadata dict with a nested image_row snapshot and available compatibility aliases.
        """
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
        """
        Return a bound Core byte reader when acquisition.resolve reports the image as readable.

        Missing/empty IDs return None before model selection. A model is obtained
        before numeric conversion; TypeError/ValueError/OverflowError from that
        conversion return None, while other failures propagate. A falsey/missing
        readable flag is unavailable regardless of delivery text. The ID is
        converted again when constructing the reader. No content is read here.

        Example:
            >>> stored = backend.resolve_storage_image(image_row)  # doctest: +SKIP
            >>> content = stored.read_bytes() if stored is not None else None  # doctest: +SKIP


        :param image_row: Subscriptable row supplying the image_id to resolve.
        :return: CoreStoredFile for a truthy readable receipt, otherwise None for a missing/unusable ID or explicit unreadability.
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
        """
        Return a redirect target only when Core's delivery value stringifies exactly to redirect.

        Missing/empty IDs return None before metadata access. Otherwise the download
        name is selected before integer validation, so projection errors can surface
        even for a subsequently invalid ID. Ordinary numeric-conversion failures
        return None; Core query failures propagate. Readability and redirect URL
        validity are not checked. A falsey location becomes empty text; a falsey
        receipt name uses the projected download name.

        Example:
            >>> target = backend.resolve_image_target(image_row)  # doctest: +SKIP


        :param image_row: Image row supplying its identifier and fallback download-name metadata.
        :return: ResolvedFileTarget in redirect mode, or None for an invalid ID or any other delivery mode.
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
        """
        Reuse host.read_model.model only when it is a CoreSurfaceModel, otherwise construct a fresh model from host.core.

        Model selection performs no Core query and does not store the fresh model
        for future calls. Missing/None read-model attributes take the fallback path.

        Example:
            >>> model = backend._model()  # doctest: +SKIP


        :return: Borrowed compatible model or a new model retaining the host's client.
        """
        read_model = getattr(self.host, "read_model", None)
        model = getattr(read_model, "model", None)
        if isinstance(model, CoreSurfaceModel):
            return model
        return CoreSurfaceModel(self.host.core)

    def work_image_row(self, work_row) -> Optional[object]:
        """
        Resolve the work's related groups and return the first deduplicated discovered image.

        The discovery order, not a separate cover-role ranking, determines the
        choice. Relationship/discovery failures propagate instead of becoming None.

        Example:
            >>> image = backend.work_image_row(work_row)  # doctest: +SKIP


        :param work_row: Work row passed unchanged to the host's related-row grouping hook.
        :return: First original image row, or None when successful discovery yields no images.
        """
        related = self.host._related_rows_by_table(work_row)
        image_rows = self.work_image_rows(related)
        if image_rows:
            return image_rows[0]
        return None

    @staticmethod
    def thumbnail_text(text: str) -> str:
        """
        Uppercase the first Unicode alphanumeric character in stripped stringified text, or use a question mark.

        Falsey input becomes empty text. Uppercasing can expand one character into
        several characters, such as sharp-s becoming SS; no width clamp is applied.

        Example:
            >>> ImageBackend.thumbnail_text(" -- ßeta")
            'SS'
            >>> ImageBackend.thumbnail_text("!?")
            '?'


        :param text: Title-like input converted through str(text or '').strip().
        :return: Uppercase form of the first alphanumeric character, or ? when none exists.
        """
        stripped = str(text or "").strip()
        for char in stripped:
            if char.isalnum():
                return char.upper()
        return "?"

    def placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        """
        Render an escaped title initial and abbreviated subtitle into a gradient SVG cover placeholder.

        The initial uses thumbnail_text and the subtitle uses a 48-character
        abbreviation before HTML/XML escaping. Font size is clamped to 18–48 from
        int(width * 0.35), but supplied width/height themselves are not validated
        or clamped. Subtitle y is max(height - 14, int(height * 0.82)). No source
        image, browser, or raster renderer is needed.

        Example:
            >>> svg = backend.placeholder_cover_svg(work_row, width=120, height=180)  # doctest: +SKIP


        :param work_row: Work row whose primary display text is obtained from the host.
        :param width: Requested SVG width and input to the initial's font-size calculation.
        :param height: Requested SVG height and input to subtitle positioning.
        :return: UTF-8 encoded SVG bytes with fixed gradient/layout markup and escaped title-derived text.
        """
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
