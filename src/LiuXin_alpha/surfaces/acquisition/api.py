"""
Serve compatibility format downloads, cover bytes, redirects, and generated SVG fallbacks through Core and a borrowed host.

Actual cover bytes are not resized for thumbnail requests: size hints affect only
placeholder rendering. Cover read/byte-response failures are deliberately caught
before redirect or later-cover/placeholder fallback, whereas format read failures
propagate. Initial work/discovery queries and redirect/placeholder response errors
remain visible. Request environments are retained in signatures but unused here.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping

from LiuXin_alpha.surfaces.api import AcquisitionHostApi, SurfaceResponseAPI
from LiuXin_alpha.surfaces.core import CoreSurfaceModel


def _coerce_payload_bytes(payload: object) -> bytes:
    """
    Normalize supported compatibility payload values to bytes without reading streams or iterables.

    Existing bytes are returned unchanged, text is UTF-8 encoded, and bytearray
    or memoryview contents are copied. Acquisition endpoints do not call this
    helper themselves; it remains independently exercised by compatibility tests.

    Example:
        >>> _coerce_payload_bytes("雪").hex()
        'e99baa'


    :param payload: Bytes, text, bytearray, or memoryview value to normalize.
    :return: Existing or newly encoded/copied byte payload.
    :raises TypeError: If the value has none of the supported concrete types.
    """

    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if isinstance(payload, (bytearray, memoryview)):
        return bytes(payload)
    raise TypeError("Acquisition payload must be bytes-like or text.")


def _cover_dimensions(*, suffix: str, query: dict[str, list[str]], thumb: bool) -> tuple[int, int]:
    """
    Resolve placeholder dimensions from a book-token suffix, the first sz query value, and cover/thumbnail mode.

    Start at 60x80. Two nonempty underscore suffix parts supply integer dimensions
    without clamping; additional parts are ignored. An sz containing x takes
    precedence and clamps both dimensions to at least one. Failed conversion keeps
    the preceding pair and does not continue to the full-cover branch. Otherwise
    full or a false thumb selects 240x320; a remaining scalar sz sets a clamped
    square. Conversion blocks catch Exception, but initial query/string access
    failures occur outside those blocks. No image resizing is performed.

    Example:
        >>> _cover_dimensions(suffix="90_120", query={"sz": ["4x5"]}, thumb=True)
        (4, 5)
        >>> _cover_dimensions(suffix="-2_0", query={}, thumb=True)
        (-2, 0)


    :param suffix: Underscore-separated size hint after the book identifier, normally empty or width_height.
    :param query: Parsed query mapping; only the first sz value is considered after stripping/lowercasing.
    :param thumb: Truthy for thumbnail defaults, falsey for full-cover defaults except in the x-size branch.
    :return: Selected width/height pair, potentially nonpositive when inherited from an unclamped suffix.
    """
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
    """
    Adapt Calibre acquisition requests to borrowed Core queries and host-created responses.

    Construction retains the host and creates a separate CoreSurfaceModel without
    querying Core. Response ownership and transport headers remain host policy;
    this adapter supplies no authentication, raster resizing, or runtime shutdown.

    Example:
        >>> api = AcquisitionCompatApi(host)  # doctest: +SKIP

    :ivar host: Core client access, book-token parser, placeholder renderer, and response factories.
    """

    host: AcquisitionHostApi

    def __post_init__(self) -> None:
        """
        Attach a new Core surface model over the host's client without fetching schema or content.

        Example:
            >>> api = AcquisitionCompatApi(host)  # doctest: +SKIP


        :return: None after assigning model; a missing host.core attribute propagates as an error.
        """
        self.model = CoreSurfaceModel(self.host.core)

    def _work(self, work_id: int) -> object | None:
        """
        Query browse.work with an integer ID and extract its work value only from a mapping receipt.

        Example:
            >>> work = api._work(7)  # doctest: +SKIP


        :param work_id: Identifier converted with int before the Core query, without positivity validation.
        :return: Receipt's unvalidated work value, or None for a nonmapping/missing-work result; query failures propagate.
        """
        result = self.host.core.query(
            "browse.work",
            {"work_id": int(work_id)},
        )
        return result.get("work") if isinstance(result, Mapping) else None

    @staticmethod
    def _records(result: object, key: str) -> list[Mapping[str, object]]:
        """
        Keep mapping entries from an actual list-valued receipt field, rejecting other sequence shapes.

        Example:
            >>> AcquisitionCompatApi._records({"covers": [None, {"id": 7}]}, "covers")
            [{'id': 7}]


        :param result: Optional mapping receipt containing the selected collection.
        :param key: Field whose value must be a list before filtering its elements.
        :return: New list of original mapping objects, or an empty list for missing/nonmapping/non-list input.
        """
        raw = result.get(key, ()) if isinstance(result, Mapping) else ()
        if not isinstance(raw, list):
            return []
        return [value for value in raw if isinstance(value, Mapping)]

    def serve_cover_or_thumb(self, raw_book_id: str, *, query: dict[str, list[str]], environ, thumb: bool) -> SurfaceResponseAPI:
        """
        Serve the first usable discovered cover, redirect, or SVG placeholder for a valid existing work.

        A token parser result of None yields 400; an absent/nonmapping work receipt
        yields 404. Iterate list-valued cover mappings in Core order. Readable
        covers are fetched as image resources and served inline using receipt name/
        MIME defaults. Exception from integer conversion, reading, or byte-response
        construction is suppressed before trying that cover's redirect, subsequent
        covers, or the placeholder. Redirect construction itself is outside the catch.

        Work/cover query failures propagate. Redirects require exact delivery text
        and a truthy location, with no URL validation here. Only the final placeholder
        uses dimension hints; successful stored content is not resized or re-encoded.

        Example:
            >>> response = api.serve_cover_or_thumb("7_90_120", query={}, environ={}, thumb=True)  # doctest: +SKIP


        :param raw_book_id: Host-parsed book token containing an ID and optional size suffix.
        :param query: Parsed query values used only for generated-placeholder dimensions.
        :param environ: Compatibility request context, accepted but unused by this implementation.
        :param thumb: Thumbnail/full-cover selector used only when generating a fallback image.
        :return: Host response for invalid/missing work, original cover bytes, a redirect, or inline cover.svg.
        """
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
        """
        Dispatch cover/thumb selectors or resolve the first deliverable file format matching a normalized extension.

        what is stripped/lowercased and leading dots are removed for format matching.
        Record extensions are lowercased/dot-stripped but not whitespace-stripped.
        Invalid book tokens yield 400; absent works or exhausted formats yield 404.
        Matching mappings require a nonempty ID/kind and mapping resolution.
        Readable formats take precedence over redirects and use the host's default
        byte-response disposition. Their conversion/read/response failures propagate
        immediately rather than falling back to another record or redirect.

        Example:
            >>> response = api.serve_compat_get(".EPUB", "7_main", {}, {})  # doctest: +SKIP


        :param what: cover, thumb, or a requested extension after string/whitespace/case normalization.
        :param raw_book_id: Host-parsed work token; suffix is ignored for format downloads.
        :param query: Parsed parameters forwarded for cover/thumb placeholders, unused for formats.
        :param environ: Compatibility request context forwarded for cover/thumb calls but otherwise unused.
        :return: Host-created content/redirect/error response; Core and format-delivery failures remain visible.
        """
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
