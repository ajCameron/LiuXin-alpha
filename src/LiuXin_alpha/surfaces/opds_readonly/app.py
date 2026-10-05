"""
Compose a standalone OPDS/acquisition WSGI surface over shared Core backends.

This application borrows the generic web host's response and read helpers, not
the Calibre HTML UI. It exposes navigation feeds, compatibility downloads, and
small icon/robots endpoints. Construction does not bind a socket; main owns the
Core session and stdlib WSGI server. Backend failures remain visible to callers.

Retained acquisition host hooks support older callers even where the current
AcquisitionCompatApi queries Core directly. In particular, the generic host's
enable_file_downloads setting is not an authorization gate for that adapter's
direct Core delivery path. HEAD is accepted but its response body is not removed
by this dispatcher or the inherited WSGI callable.
"""

from __future__ import annotations

import argparse
import posixpath
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote
from wsgiref.simple_server import make_server

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.acquisition.api import AcquisitionCompatApi
from LiuXin_alpha.surfaces.catalog.api import CalibreCatalogBackend, PLACEHOLDER_PNG
from LiuXin_alpha.surfaces.core import (
    add_core_client_arguments,
    open_surface_core_from_args,
)
from LiuXin_alpha.surfaces.opds.api import OpdsApi
from LiuXin_alpha.surfaces.web_readonly.app import (
    ReadOnlyWebApplication,
    ReadOnlyWebConfig,
    _Response,
    _row_value,
    add_metadata_read_source_arguments,
    metadata_read_source_help_epilog,
    metadata_read_source_config_kwargs,
)


@dataclass(frozen=True)
class OpdsReadOnlyConfig(ReadOnlyWebConfig):
    """
    Carry immutable web/Core settings plus the OPDS title and grouping threshold.

    Direct construction does not validate or clamp values. A positive grouping
    threshold groups categories whose item count strictly exceeds it; zero
    disables grouping. CLI page/group limits are clamped separately in main.

    Example:
        >>> config = OpdsReadOnlyConfig(opds_max_ungrouped_items=0)
        >>> (config.title, config.opds_max_ungrouped_items, config.default_page_size)
        ('LiuXin OPDS Read-Only', 0, 50)

    :ivar title: Service title used in OPDS feeds and CLI startup configuration.
    :ivar opds_max_ungrouped_items: Category-item threshold, interpreted by OpdsApi.
    """

    title: str = "LiuXin OPDS Read-Only"
    opds_max_ungrouped_items: int = 100


class OpdsReadOnlyApplication(ReadOnlyWebApplication):
    """
    Adapt a Core client to standalone Atom feeds and compatibility acquisitions.

    Share the base host's read model and image backend with the catalogue adapter.
    The inherited close method closes only a compatibility session created for
    a database input, not an externally supplied Core client. Calling the WSGI
    object adds X-Robots-Tag; handle_request alone returns the raw response.

    Example:
        >>> issubclass(OpdsReadOnlyApplication, ReadOnlyWebApplication)
        True
        >>> app = OpdsReadOnlyApplication(core_client)  # doctest: +SKIP

    :ivar catalog: Shared catalogue projection borrowing this host's read/image owners.
    :ivar opds_api: Feed builder and protocol router bound to this host.
    :ivar acquisition_api: Core-backed compatibility cover/format adapter.
    """

    def __init__(self, core: CoreClientAPI, *, config: Optional[OpdsReadOnlyConfig] = None) -> None:
        """
        Initialize the shared web host and bind catalogue, OPDS, and acquisition owners.

        The base constructor also accepts legacy database inputs through its Core
        compatibility bridge. Any coercion/cache initialization error propagates;
        no HTTP server starts and no application-level rollback is added here.

        Example:
            >>> app = OpdsReadOnlyApplication(core_client, config=OpdsReadOnlyConfig(title="Shelf"))  # doctest: +SKIP


        :param core: Borrowed Core client, or a legacy database supported by the base adapter.
        :param config: Frozen surface settings; None selects the OPDS-specific defaults.
        :return: None after all adapters are bound to the initialized host.
        """
        super().__init__(core, config=config or OpdsReadOnlyConfig())
        self.catalog = CalibreCatalogBackend(self, read_model=self.read_model, images=self.images)
        self.opds_api = OpdsApi(self)
        self.acquisition_api = AcquisitionCompatApi(self)

    def handle_request(self, environ) -> _Response:
        """
        Dispatch normalized GET/HEAD paths to OPDS, acquisition, and static endpoints.

        Normalize dot segments before percent-decoding individual route components.
        Query parsing drops blank values. Root and stanza redirect to /opds; HTML
        browse routes are not exposed. Extra icon/get path components are ignored;
        legacy/get requires at least five nonempty components although it uses
        only the format and book ID. HEAD follows GET and retains its body.
        Unsupported methods return 405, unknown paths 404, and backend errors escape.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> app.handle_request({"PATH_INFO": "/"}).status
            '302 Found'
            >>> app.handle_request({"REQUEST_METHOD": "POST"}).status
            '405 Method Not Allowed'


        :param environ: WSGI mapping; absent/falsey method, path, and query use GET, /, and empty text.
        :return: Unstarted response; the inherited WSGI callable later emits its headers/body.
        """
        method = str(environ.get("REQUEST_METHOD", "GET") or "GET").upper()
        if method not in {"GET", "HEAD"}:
            return self._text_response("405 Method Not Allowed", "Method not allowed.\n", content_type="text/plain")

        path = posixpath.normpath(str(environ.get("PATH_INFO", "/") or "/"))
        if not path.startswith("/"):
            path = "/" + path
        query = parse_qs(str(environ.get("QUERY_STRING", "") or ""), keep_blank_values=False)

        if path == "/":
            return self._redirect_response("/opds")
        if path == "/robots.txt":
            return self._text_response("200 OK", "User-agent: *\nAllow: /\n", content_type="text/plain")
        if path in {"/favicon.png", "/apple-touch-icon.png"}:
            return self._bytes_response(
                PLACEHOLDER_PNG,
                download_name=path.lstrip("/"),
                disposition="inline",
                content_type_override="image/png",
            )
        if path.startswith("/icon/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) >= 2:
                return self._serve_icon(parts[1], query)
        if path == "/stanza":
            return self._redirect_response("/opds")
        if path == "/opds" or path.startswith("/opds/"):
            return self._serve_opds(path, query)
        if path.startswith("/get/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) >= 3 and parts[0] == "get":
                return self._serve_compat_get(parts[1], parts[2], query, environ)
        if path.startswith("/legacy/get/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) >= 5 and parts[0] == "legacy" and parts[1] == "get":
                return self._serve_compat_get(parts[2], parts[3], query, environ)
        return self._text_response("404 Not Found", "Unknown OPDS route.\n", content_type="text/plain")

    def _xml_response(self, xml_text: str, *, status: str = "200 OK") -> _Response:
        """
        Encode trusted XML text as one UTF-8 Atom response chunk without validation.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> app._xml_response("<feed/>").body
            [b'<feed/>']


        :param xml_text: Complete XML document; no escaping, parsing, or declaration is added.
        :param status: WSGI status text retained unchanged.
        :return: Response containing only an Atom Content-Type header and encoded body.
        """
        return _Response(
            status=status,
            headers=[("Content-Type", "application/atom+xml; charset=utf-8")],
            body=[xml_text.encode("utf-8")],
        )

    def _serve_icon(self, which: str, query: dict[str, list[str]]) -> _Response:
        """
        Return the fixed built-in PNG regardless of requested icon name or size.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> b"".join(app._serve_icon("missing", {}).body) == PLACEHOLDER_PNG
            True


        :param which: Ignored icon selector, retained for route compatibility.
        :param query: Ignored query values; no resizing is performed.
        :return: Inline 200 byte response named icon.png with image/png media type.
        """
        del which, query
        return self._bytes_response(PLACEHOLDER_PNG, download_name="icon.png", disposition="inline", content_type_override="image/png")

    def opds_xml_response(self, xml_text: str, *, status: str = "200 OK") -> _Response:
        """
        Expose the UTF-8 Atom response builder to the shared OPDS adapter.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> app.opds_xml_response("<feed/>", status="201 Created").status
            '201 Created'


        :param xml_text: Trusted XML document passed unchanged to _xml_response.
        :param status: WSGI status text forwarded unchanged.
        :return: Atom response without additional headers or XML validation.
        """
        return self._xml_response(xml_text, status=status)

    def opds_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        """
        Supply the OPDS adapter with a plain UTF-8 text/error response.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> app.opds_text_response("404 Not Found", "Missing", content_type="text/plain").body
            [b'Missing']


        :param status: WSGI status text returned without interpretation.
        :param text: Body text encoded as UTF-8, without escaping.
        :param content_type: Media type to which the base builder appends charset=utf-8.
        :return: The generic host's single-chunk text response.
        """
        return self._text_response(status, text, content_type=content_type)

    def acquisition_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        """
        Supply the acquisition adapter with the generic host's text/error response.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> app.acquisition_text_response("400 Bad Request", "Bad ID", content_type="text/plain").status
            '400 Bad Request'


        :param status: WSGI status string forwarded unchanged.
        :param text: Unescaped body text to encode as UTF-8.
        :param content_type: Media type with the base builder's UTF-8 charset suffix added.
        :return: Generic text response, without catching builder failures.
        """
        return self._text_response(status, text, content_type=content_type)

    def acquisition_bytes_response(
        self,
        payload: bytes,
        *,
        download_name: str,
        disposition: str = "attachment",
        content_type_override: Optional[str] = None,
    ) -> _Response:
        """
        Wrap acquired bytes in a 200 response without reading a file or checking policy.

        The base builder computes byte length and MIME type from the override or
        filename. It strips double quotes from the disposition filename, not all
        possible header control characters; callers must supply trusted metadata.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> response = app.acquisition_bytes_response(b"book", download_name="book.epub")
            >>> dict(response.headers)["Content-Length"]
            '4'


        :param payload: Already acquired byte payload, retained as one response chunk.
        :param download_name: Filename used for MIME guessing and Content-Disposition.
        :param disposition: Disposition text, normally attachment or inline.
        :param content_type_override: Truthy explicit media type, or None for filename-based guessing.
        :return: Generic byte response; enable_file_downloads is not consulted here.
        """
        return self._bytes_response(
            payload,
            download_name=download_name,
            disposition=disposition,
            content_type_override=content_type_override,
        )

    def acquisition_redirect_response(self, location: str) -> _Response:
        """
        Return an empty 302 response for a Core-provided acquisition location.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> app.acquisition_redirect_response("/asset").headers
            [('Location', '/asset')]


        :param location: Location string forwarded without URL or header-value validation.
        :return: Generic redirect response with one empty byte chunk.
        """
        return self._redirect_response(location)

    def acquisition_file_response(
        self,
        path: Path,
        *,
        download_name: str,
        environ,
        disposition: str = "attachment",
        content_type_override: Optional[str] = None,
    ) -> _Response:
        """
        Open and stream a local file through the generic host's WSGI response builder.

        Retained for host compatibility; the current acquisition adapter reads
        through Core instead. The returned response owns an open file and exposes
        its closer. This hook adds neither path confinement nor a download-policy
        check; opening, statting, and wrapper-construction failures propagate.

        Example:
            >>> response = app.acquisition_file_response(path, download_name="book.epub", environ={})  # doctest: +SKIP
            >>> response.close()  # doctest: +SKIP


        :param path: Local Path to open in binary mode.
        :param download_name: Name used for MIME guessing and the disposition header.
        :param environ: WSGI mapping optionally providing a wsgi.file_wrapper callable.
        :param disposition: Content-Disposition mode forwarded unchanged.
        :param content_type_override: Truthy explicit media type, or None to guess from the name.
        :return: Streaming response whose close callback must run after consumption.
        """
        return self._file_response(
            path,
            download_name=download_name,
            environ=environ,
            disposition=disposition,
            content_type_override=content_type_override,
        )

    def acquisition_split_book_token(self, raw_book_id: str) -> tuple[Optional[int], str]:
        """
        Delegate integer work-ID and underscore-suffix parsing to the catalogue.

        Example:
            >>> parts = app.acquisition_split_book_token("7_90_120")  # doctest: +SKIP
            (7, '90_120')


        :param raw_book_id: Compatibility book token; parsing preserves the suffix after the first underscore.
        :return: Parsed integer ID or None, paired with the suffix text.
        """
        return self.catalog.split_compat_book_token(raw_book_id)

    def acquisition_work_row(self, row_id: int):
        """
        Fetch a work through the shared read model for legacy host callers.

        The current AcquisitionCompatApi uses its own Core browse query instead.

        Example:
            >>> row = app.acquisition_work_row(7)  # doctest: +SKIP


        :param row_id: Work identifier converted to int before lookup.
        :return: Shared work row or None for successful absence; conversion/query failures propagate.
        """
        return self.read_model.row_by_id("works", int(row_id))

    def acquisition_work_image_row(self, work_row):
        """
        Delegate first-image discovery for legacy acquisition host callers.

        Example:
            >>> image_row = app.acquisition_work_image_row(work_row)  # doctest: +SKIP


        :param work_row: Work whose related groups are queried by the image backend.
        :return: First discovered image row or None, preserving backend failure behavior.
        """
        return self.images.work_image_row(work_row)

    def acquisition_resolve_storage_image(self, image_row):
        """
        Delegate creation of a Core byte reader for an image reported readable.

        No image bytes are read by this compatibility hook itself.

        Example:
            >>> stored = app.acquisition_resolve_storage_image(image_row)  # doctest: +SKIP


        :param image_row: Image metadata supplying the resource ID to resolve.
        :return: Bound CoreStoredFile or None for an unusable ID or explicit unreadability.
        """
        return self.images.resolve_storage_image(image_row)

    def acquisition_resolve_image_target(self, image_row):
        """
        Delegate image redirect-target selection without creating a local file target.

        Example:
            >>> target = app.acquisition_resolve_image_target(image_row)  # doctest: +SKIP


        :param image_row: Image metadata supplying its ID and fallback download name.
        :return: Redirect target or None under the image backend's resolution policy.
        """
        return self.images.resolve_image_target(image_row)

    def acquisition_image_download_name(self, image_row) -> str:
        """
        Delegate image filename selection with the shared cover.bin fallback.

        Example:
            >>> name = app.acquisition_image_download_name(image_row)  # doctest: +SKIP


        :param image_row: Image metadata projected by the image backend through this host.
        :return: First usable name/original name/storage key, stringified without sanitization.
        """
        return self.images.image_download_name(image_row)

    def acquisition_image_content_type(self, image_row) -> str:
        """
        Delegate explicit-or-guessed image media type selection without reading bytes.

        Example:
            >>> content_type = app.acquisition_image_content_type(image_row)  # doctest: +SKIP


        :param image_row: Metadata supplying MIME text and fallback filename candidates.
        :return: Declared or filename-guessed type, falling back to application/octet-stream.
        """
        return self.images.image_content_type(image_row)

    def acquisition_placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        """
        Render the shared title-based SVG placeholder at the supplied dimensions.

        Example:
            >>> svg = app.acquisition_placeholder_cover_svg(work_row, width=60, height=80)  # doctest: +SKIP


        :param work_row: Work whose display title supplies the escaped initial and subtitle.
        :param width: SVG width passed unchanged; the image backend does not clamp it.
        :param height: SVG height passed unchanged; it also controls subtitle placement.
        :return: UTF-8 SVG bytes, not a resized or rasterized source cover.
        """
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)

    def acquisition_related_rows_by_table(self, work_row) -> dict[str, list[object]]:
        """
        Expose the generic related-row grouping hook to legacy acquisition callers.

        Example:
            >>> related = app.acquisition_related_rows_by_table(work_row)  # doctest: +SKIP


        :param work_row: Work forwarded unchanged to the generic web relationship helper.
        :return: Related-row groups keyed by table, without using the narrower OPDS subset.
        """
        return self._related_rows_by_table(work_row)

    def acquisition_work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        """
        Delegate direct and WEMI-linked file discovery to the catalogue read model.

        Example:
            >>> files = app.acquisition_work_file_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Existing related groups containing files and/or expressions.
        :return: Discovered, ID-deduplicated file rows without extra filtering or copying here.
        """
        return self.catalog.work_file_rows(related_rows_by_table)

    def acquisition_download_name_for_file_row(self, file_row) -> str:
        """
        Expose the generic host's file-name precedence to compatibility callers.

        Example:
            >>> name = app.acquisition_download_name_for_file_row(file_row)  # doctest: +SKIP


        :param file_row: File metadata projected through the host's visible-column policy.
        :return: Name, original name, storage key, or download.bin, without sanitization.
        """
        return self._download_name_for_file_row(file_row)

    def acquisition_file_id(self, file_row) -> object:
        """
        Read the raw file_id value without integer conversion or schema validation.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> app.acquisition_file_id({"file_id": "007"})
            '007'


        :param file_row: Row-like object passed to the shared row_value helper.
        :return: Original file_id value, or None when the helper cannot obtain it.
        """
        return _row_value(file_row, "file_id")

    def acquisition_serve_file_download(self, raw_file_id: str, environ) -> _Response:
        """
        Retain the generic file-ID download route for legacy acquisition callers.

        This is distinct from the current compatibility format adapter's direct
        Core path. The base route retains its own missing/unsupported/read-failure
        responses and download-setting checks; this wrapper adds no error handling.

        Example:
            >>> response = app.acquisition_serve_file_download("7", {})  # doctest: +SKIP


        :param raw_file_id: File identifier text parsed by the base download handler.
        :param environ: WSGI mapping forwarded for any eventual local-file streaming.
        :return: Base handler response, which may contain bytes, a stream, redirect, or error.
        """
        return self._serve_file_download(raw_file_id, environ)

    def opds_search_work_rows(self, query_text: str) -> list[object]:
        """
        Search through the generic host and retain only entries labelled works.

        Entry order and row identity are preserved. A works-labelled entry without
        a row key raises rather than silently disappearing; query errors propagate.

        Example:
            >>> rows = app.opds_search_work_rows("Alpha")  # doctest: +SKIP


        :param query_text: Search text forwarded unchanged with the works table filter.
        :return: New list of original row objects from works-labelled search entries.
        """
        return [entry["row"] for entry in self._global_search_entries(query_text, table_filter="works") if str(entry.get("table")) == "works"]

    def opds_work_rows(self, *, sorted_by: str) -> list[object]:
        """
        Borrow the catalogue's ordered work enumeration without repagination.

        Example:
            >>> rows = app.opds_work_rows(sorted_by="recent")  # doctest: +SKIP


        :param sorted_by: Ordering token; the shared model recognizes exact recent specially.
        :return: Catalogue work list unchanged, with lookup failures still visible.
        """
        return self.catalog.work_rows(sorted_by=sorted_by)

    def opds_category_rows(self, category: str) -> list[dict[str, object]]:
        """
        Delegate category-item enumeration and compatibility browse-URL augmentation.

        The catalogue may mutate its returned row dicts to add URLs. Those links
        describe compatibility navigation; this standalone router does not expose
        the Calibre HTML browse pages themselves.

        Example:
            >>> items = app.opds_category_rows("authors")  # doctest: +SKIP


        :param category: Category selector passed through the catalogue's normalization policy.
        :return: Catalogue item dicts with label/count/identity and applicable browse links.
        """
        return self.catalog.category_rows(category)

    def opds_category_display_name(self, category: str) -> str:
        """
        Delegate fixed English category labels and the title-cased unknown fallback.

        Example:
            >>> name = app.opds_category_display_name("authors")  # doctest: +SKIP


        :param category: Plain category value passed unchanged to the catalogue label helper.
        :return: Display label, not a translated or schema-validated category name.
        """
        return self.catalog.category_display_name(category)

    def opds_rows_for_category_item(self, category: str, item_token: str) -> list[object]:
        """
        Select linked works for exact authors, tags, or series category tokens.

        Author tables are tried in preference order until the first nonempty
        result, not merged. Tags use the read model's chosen table or tags fallback.
        Unlike the catalogue's broader helper, this method does not normalize
        category aliases or handle all-books categories. Read failures propagate.

        Example:
            >>> app = object.__new__(OpdsReadOnlyApplication)
            >>> app.opds_rows_for_category_item("author", "7")
            []


        :param category: Already normalized category text supplied by the OPDS router.
        :param item_token: Entity ID token forwarded unchanged to linked-work lookup.
        :return: Selected work list, or an empty list for an unsupported category/no match.
        """
        if category == "authors":
            rows: list[object] = []
            for table in self.catalog.author_tables():
                rows = self.catalog.works_for_linked_entity(table, item_token)
                if rows:
                    break
            return rows
        if category == "tags":
            return self.catalog.works_for_linked_entity(self.catalog.read_model.tag_category_table() or "tags", item_token)
        if category == "series":
            return self.catalog.works_for_linked_entity("series", item_token)
        return []

    def _opds_related_rows_by_table(self, row) -> dict[str, list[object]]:
        """
        Read only direct expression, file, selected-tag, and series groups for a work.

        Skip absent tables and successful empty groups. This helper does not build
        the full generic relationship graph or traverse descendants itself; later
        metadata projection handles any necessary WEMI asset discovery. Table and
        relation query failures are not converted to empty groups.

        Example:
            >>> related = app._opds_related_rows_by_table(work_row)  # doctest: +SKIP


        :param row: Work row passed unchanged to each present table's relation query.
        :return: Insertion-ordered table-to-row-list dict containing only nonempty results.
        """
        related: dict[str, list[object]] = {}
        # OPDS entries only need a small subset of linked data. Avoid building the
        # full generic related-entity graph for each work row.
        for linked_table in ("expressions", "files", self.catalog.read_model.tag_category_table() or "tags", "series"):
            if not self._table_exists(linked_table):
                continue
            linked_rows = self.read_model.interlinked_rows(row, linked_table)
            if linked_rows:
                related[linked_table] = linked_rows
        return related

    def opds_work_metadata_payload(self, row) -> dict[str, object]:
        """
        Project feed metadata using the limited OPDS relationship groups.

        Call the catalogue's read model directly, bypassing the catalogue wrapper's
        category_urls augmentation. Author/asset resolution and failures remain
        the shared metadata projector's responsibility.

        Example:
            >>> metadata = app.opds_work_metadata_payload(work_row)  # doctest: +SKIP


        :param row: Work whose direct OPDS-related rows and metadata should be read.
        :return: Shared work metadata dict without extra category URL augmentation here.
        """
        return self.catalog.read_model.work_metadata_payload(
            row,
            related_rows_by_table=self._opds_related_rows_by_table(row),
        )

    def _serve_opds(self, path: str, query: dict[str, list[str]]) -> _Response:
        """
        Forward a normalized feed path and parsed query to the shared OPDS router.

        Example:
            >>> response = app._serve_opds("/opds", {})  # doctest: +SKIP


        :param path: Normalized request path including the /opds prefix.
        :param query: Parsed query values retained as lists.
        :return: Adapter feed/error response unchanged; adapter exceptions propagate.
        """
        return self.opds_api.serve(path, query)

    def _serve_compat_get(self, what: str, raw_book_id: str, query: dict[str, list[str]], environ) -> _Response:
        """
        Forward a cover, thumbnail, or format request to Core-backed acquisition.

        This path does not invoke the legacy file-ID download hook or consult the
        host's enable_file_downloads flag. Cover fallback and format error behavior
        are owned by AcquisitionCompatApi, not changed by this wrapper.

        Example:
            >>> response = app._serve_compat_get("thumb", "7", {}, {})  # doctest: +SKIP


        :param what: Cover/thumb selector or ebook extension interpreted by the adapter.
        :param raw_book_id: Compatibility work ID with an optional underscore suffix.
        :param query: Parsed query values, including any cover size request.
        :param environ: Original WSGI mapping passed through even though current Core delivery ignores it.
        :return: Adapter response unchanged, with its existing exceptions left visible.
        """
        return self.acquisition_api.serve_compat_get(what, raw_book_id, query, environ)


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build Core/profile, metadata-source, bind, paging, and acquisition CLI options.

    Parsing does not open a database or server. Integer options are not clamped
    until main constructs the configuration. The CLI default page size is 25,
    unlike direct OpdsReadOnlyConfig construction's inherited default of 50.

    Example:
        >>> args = build_arg_parser().parse_args(["--page-size", "0"])
        >>> (args.page_size, args.port, args.opds_max_ungrouped_items)
        (0, 8080, 100)


    :return: Fresh argparse parser; argument errors/help follow standard SystemExit behavior.
    """
    parser = argparse.ArgumentParser(
        description="Run the LiuXin OPDS read-only interface.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=metadata_read_source_help_epilog("PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.opds_readonly"),
    )
    add_core_client_arguments(parser)
    parser.add_argument("--db-type", default="sqlite", help="Database driver type. Default: sqlite")
    add_metadata_read_source_arguments(parser)
    parser.add_argument("--host", default=OpdsReadOnlyConfig.host, help="Bind host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=OpdsReadOnlyConfig.port, help="Bind port. Default: 8080")
    parser.add_argument("--page-size", type=int, default=25, help="Default page size.")
    parser.add_argument("--max-page-size", type=int, default=200, help="Maximum page size.")
    parser.add_argument(
        "--opds-max-ungrouped-items",
        type=int,
        default=OpdsReadOnlyConfig.opds_max_ungrouped_items,
        help="Maximum OPDS category size before category-group feeds are used.",
    )
    parser.add_argument("--title", default=OpdsReadOnlyConfig.title, help="Service title.")
    parser.add_argument("--no-file-downloads", action="store_true", help="Disable file download / redirect links.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """
    Parse settings, own a Core session, and serve standalone OPDS until shutdown.

    Clamp page limits to at least one and grouping threshold to at least zero.
    Open storage-enabled, maintenance-disabled Core access, then construct the
    application and print the requested URL before attempting to bind the server.
    A requested port zero is printed as zero, not the subsequently assigned port.
    Context managers release server/session resources on unwinding; startup,
    serving, and KeyboardInterrupt failures are not caught here.

    The no-file-downloads option is copied into the shared configuration, but
    the current compatibility acquisition adapter does not enforce that flag
    for its direct Core path. It must not be treated as authorization.

    Example:
        >>> main(["--database", "catalog.sqlite", "--port", "8080"])  # doctest: +SKIP


    :param argv: Explicit argument tokens, or None to read process arguments via argparse.
    :return: Zero only after serve_forever and both context managers return normally.
    :raises SystemExit: For parser help or invalid command-line syntax.
    """
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = OpdsReadOnlyConfig(
        title=str(args.title),
        host=str(args.host),
        port=int(args.port),
        default_page_size=max(1, int(args.page_size)),
        max_page_size=max(1, int(args.max_page_size)),
        opds_max_ungrouped_items=max(0, int(args.opds_max_ungrouped_items)),
        enable_file_downloads=not bool(args.no_file_downloads),
        **metadata_read_source_config_kwargs(args),
    )
    cache_type = (
        str(args.cache_type)
        if str(args.metadata_read_source) == "cache"
        else None
    )
    with open_surface_core_from_args(
        args,
        cache_type=cache_type,
        cache_allow_database_fallback=not bool(args.no_cache_db_fallback),
        enable_storage_manager=True,
        enable_maintenance=False,
    ) as core_session:
        app = OpdsReadOnlyApplication(core_session.client, config=config)
        url = "http://{}:{}/opds".format(config.host, config.port)
        sys.stdout.write("Serving OPDS read-only interface on {}\n".format(url))
        sys.stdout.flush()
        with make_server(config.host, config.port, app) as server:
            server.serve_forever()
    return 0


__all__ = [
    "OpdsReadOnlyApplication",
    "OpdsReadOnlyConfig",
    "build_arg_parser",
    "main",
]
