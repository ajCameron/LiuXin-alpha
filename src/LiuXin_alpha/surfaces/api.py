"""
Declare structural host ports and payload aliases shared by image, catalogue, acquisition, and OPDS surfaces.

Ports expose Core clients, presentation hooks, and response construction rather
than concrete database/storage implementations. Protocol methods describe caller
requirements and retain ellipsis bodies; these are not runnable default methods
or runtime-checkable validators. The payload aliases are ordinary mutable dict
types, not validated schemas, immutable records, or guaranteed JSON encodings.
"""

from __future__ import annotations

from typing import Callable, Iterable, Optional, Protocol, TypeAlias

from LiuXin_alpha.core import CoreClientAPI


SurfaceCategoryItem: TypeAlias = dict[str, object]
SurfaceEntitySummary: TypeAlias = dict[str, object]
SurfaceFilePayload: TypeAlias = dict[str, object]
SurfaceRelatedPayload: TypeAlias = dict[str, list[SurfaceEntitySummary]]
SurfaceSearchEntry: TypeAlias = dict[str, object]
SurfaceWorkMetadataPayload: TypeAlias = dict[str, object]


class SurfaceResponseAPI(Protocol):
    """
    Describe an HTTP response with a status line, ordered header pairs, and an iterable of byte chunks.

    ``close`` is an optional zero-argument cleanup callback; a consumer must arrange
    any necessary cleanup around body consumption. The protocol does not require
    a re-iterable body, validate headers, or perform transmission or cleanup itself.

    Example:
        >>> try:  # doctest: +SKIP
        ...     body = b"".join(response.body)
        ... finally:
        ...     if response.close is not None:
        ...         response.close()
    """

    status: str
    headers: list[tuple[str, str]]
    body: Iterable[bytes]
    close: Optional[Callable[[], None]]


class ResolvedFileTargetAPI(Protocol):
    """
    Describe a delivery mode, location, and suggested download name without requiring a concrete target class.

    These fields guide the serving adapter; their presence alone does not prove
    local readability, validate a redirect URL, or authorize a download.

    Example:
        >>> location = target.location if target.mode == "redirect" else None  # doctest: +SKIP
    """

    mode: str
    location: str
    download_name: str


class ImageHostApi(Protocol):
    """
    Supply a Core client and row-presentation hooks to the reusable image backend.

    Implementations own relationship lookup, visible-column projection, label
    policy, and storage-refresh reporting; the backend need not import a web app.

    Example:
        >>> title = host._row_primary_text("works", work)  # doctest: +SKIP
    """

    @property
    def core(self) -> CoreClientAPI:
        """
        Expose the client's named Core operations used for image resolution and acquisition.

        Example:
            >>> client = host.core  # doctest: +SKIP


        :return: Borrowed Core client; accessing this port does not transfer shutdown ownership.
        """
        ...

    def _related_rows_by_table(self, row: object) -> dict[str, list[object]]:
        """
        Group a row's related records by table for image and cover selection.

        Example:
            >>> related = host._related_rows_by_table(work)  # doctest: +SKIP


        :param row: Source row understood by the host's relationship adapter.
        :return: Table-name mapping to related-row lists in host-selected order.
        """
        ...

    def _row_dict(self, table: str, row: object) -> dict[str, object]:
        """
        Project a row into the column-value mapping used by image metadata helpers.

        Example:
            >>> values = host._row_dict("images", image_row)  # doctest: +SKIP


        :param table: Schema context determining which columns the host exposes.
        :param row: Row object to project using the host's lookup policy.
        :return: Display-facing column mapping, not a guarantee that all stored columns are included.
        """
        ...

    def _row_primary_text(self, table: str, row: object) -> str:
        """
        Select a row's primary display text for uses such as placeholder-cover titles.

        Example:
            >>> title = host._row_primary_text("works", work)  # doctest: +SKIP


        :param table: Schema context for preferred display fields and fallback labels.
        :param row: Row whose presentation title should be selected.
        :return: Host-selected text; callers still apply their output format's escaping.
        """
        ...

    def _refresh_storage_manager(self) -> bool:
        """
        Request the host's storage refresh and expose its boolean outcome.

        Example:
            >>> refreshed = host._refresh_storage_manager()  # doctest: +SKIP


        :return: Host-interpreted refresh success flag, not independent proof of resource readability.
        """
        ...


class ReadModelHostApi(Protocol):
    """
    Provide Core access, configuration, and row/relationship presentation hooks for catalogue projections.

    The shared read model performs Core queries while delegating application-facing
    labels, routes, and capability presentation through this port. Dict results
    can contain live row/target objects; they are not necessarily serialized JSON.

    Example:
        >>> href = host._row_href("works", work)  # doctest: +SKIP
    """

    @property
    def core(self) -> CoreClientAPI:
        """
        Expose the host's Core client for named catalogue and metadata operations.

        Example:
            >>> client = host.core  # doctest: +SKIP


        :return: Borrowed direct or remote client, with lifecycle ownership retained by its composer.
        """
        ...

    @property
    def config(self) -> object:
        """
        Expose application configuration consulted by catalogue/presentation consumers.

        This broad port leaves concrete fields to those consumers and does not
        validate them or copy configuration state.

        Example:
            >>> config = host.config  # doctest: +SKIP


        :return: Host configuration object, such as its title, paging, and download-policy settings.
        """
        ...

    def _table_exists(self, table: str) -> bool:
        """
        Report whether the host's schema inventory includes a requested table.

        Example:
            >>> available = host._table_exists("works")  # doctest: +SKIP


        :param table: Table name to inspect through the host's schema policy.
        :return: Table-presence flag, distinct from a count of stored rows.
        """
        ...

    def _id_column(self, table: str) -> Optional[str]:
        """
        Select the identifier column used for row identity and route construction.

        Example:
            >>> column = host._id_column("works")  # doctest: +SKIP


        :param table: Table whose identifying column is requested.
        :return: Host-selected ID column name, or None when no usable column is available.
        """
        ...

    def _row_primary_text(self, table: str, row: object) -> str:
        """
        Choose the main human-facing text for a catalogue row, with host-defined fallback.

        Example:
            >>> title = host._row_primary_text("works", work)  # doctest: +SKIP


        :param table: Schema context for preferred display columns.
        :param row: Row supplying display values and any identity fallback.
        :return: Plain presentation text, not output-context escaping or a stable database identifier.
        """
        ...

    def _row_label(self, table: str, row: object) -> str:
        """
        Build a compact row label from identity and selected display values according to host policy.

        Example:
            >>> label = host._row_label("tags", tag)  # doctest: +SKIP


        :param table: Table supplying schema/display context and a possible fallback label.
        :param row: Row whose identifying and descriptive values should be displayed.
        :return: Plain human-facing label, which need not be unique or HTML-escaped.
        """
        ...

    def _row_dict(self, table: str, row: object) -> dict[str, object]:
        """
        Project host-visible row columns into a presentation mapping.

        Example:
            >>> values = host._row_dict("works", work)  # doctest: +SKIP


        :param table: Table whose visible-column policy controls the projection.
        :param row: Row supplying values through the host's access adapter.
        :return: Column-value dict; the port does not promise every physical column or deep copying.
        """
        ...

    def _row_href(self, table: str, row: object) -> Optional[str]:
        """
        Construct the host's navigation link for an addressable row.

        Example:
            >>> href = host._row_href("works", work)  # doctest: +SKIP


        :param table: Route/schema context for the row.
        :param row: Row from which the host obtains a usable identifier.
        :return: Host-specific link text, or None when the row cannot be linked.
        """
        ...

    def _related_rows_by_table(self, row: object) -> dict[str, list[object]]:
        """
        Provide related-row groups when catalogue projection delegates relationship enumeration to its host.

        Example:
            >>> related = host._related_rows_by_table(work)  # doctest: +SKIP


        :param row: Source row understood by the host's relationship lookup.
        :return: Related table names mapped to ordered lists of row objects.
        """
        ...

    def _download_name_for_file_row(self, file_row: object) -> str:
        """
        Choose a suggested download filename from file metadata and host fallback policy.

        Example:
            >>> name = host._download_name_for_file_row(file_row)  # doctest: +SKIP


        :param file_row: File record carrying available filename or storage-key metadata.
        :return: Suggested name, not a resolved local path or proof that downloading is enabled.
        """
        ...

    def _refresh_storage_manager(self) -> bool:
        """
        Request storage refresh using the host's refresh and failure-reporting policy.

        Example:
            >>> refreshed = host._refresh_storage_manager()  # doctest: +SKIP


        :return: Host-interpreted success flag, not a guarantee that every store is accessible.
        """
        ...

    def _work_credit_entries(self, row: object) -> list[dict[str, object]]:
        """
        Project a work's contributors and credit roles into host-ordered entry mappings.

        Example:
            >>> credits = host._work_credit_entries(work)  # doctest: +SKIP


        :param row: Work row whose related contributors should be described.
        :return: Credit entry dicts carrying row/role/order metadata expected by catalogue renderers.
        """
        ...

    def _file_capabilities(self, file_row: object) -> dict[str, object]:
        """
        Describe file delivery and preview capabilities using the host's resolver and configuration.

        Example:
            >>> capabilities = host._file_capabilities(file_row)  # doctest: +SKIP


        :param file_row: File row whose available target or stored-resource reader is inspected.
        :return: Presentation mapping such as downloadable, preview_kind, delivery, and target/reader values.
        """
        ...

    def _stringify_detail_value(self, value: object) -> str:
        """
        Convert a metadata value into detail-view text under the host's formatting policy.

        Example:
            >>> text = host._stringify_detail_value({"language": "ja"})  # doctest: +SKIP


        :param value: Scalar, container, or missing value to present.
        :return: Display text before output-context escaping; this port is not a serialization contract.
        """
        ...


class CalibreCatalogHostApi(ReadModelHostApi, Protocol):
    """
    Extend catalogue presentation hooks with the host's cross-table search-entry projection.

    Search results retain row objects and presentation/ranking metadata so Calibre
    compatibility adapters can select work records without a concrete web host.

    Example:
        >>> entries = host._global_search_entries("雪", table_filter="works")  # doctest: +SKIP
    """

    def _global_search_entries(self, query_text: str, *, table_filter: str = "") -> list[SurfaceSearchEntry]:
        """
        Search host-visible catalogue tables and return row-bearing presentation entries.

        Example:
            >>> entries = host._global_search_entries("雪", table_filter="works")  # doctest: +SKIP


        :param query_text: Search text interpreted by the host's query policy.
        :param table_filter: Optional table selection; empty text requests the host's normal cross-table scope.
        :return: Ordered search-entry dicts containing table/row identity and host-provided match metadata.
        """
        ...


class AcquisitionHostApi(Protocol):
    """
    Supply named Core access and HTTP response/cover hooks to acquisition compatibility routes.

    Routes select resources through Core while the host owns response headers,
    token interpretation, and placeholder SVG generation. This protocol performs
    no request handling, authorization, or resource lifecycle management itself.

    Example:
        >>> response = host.acquisition_redirect_response("https://example.invalid/book")  # doctest: +SKIP
    """

    @property
    def core(self) -> CoreClientAPI:
        """
        Expose the borrowed Core client used for work lookup, format/cover selection, and content reads.

        Example:
            >>> client = host.core  # doctest: +SKIP


        :return: Direct or remote client whose lifecycle remains owned by the host's composition boundary.
        """
        ...

    def acquisition_text_response(self, status: str, text: str, *, content_type: str) -> SurfaceResponseAPI:
        """
        Construct a text response for acquisition status/error messages using host encoding and headers.

        Example:
            >>> response = host.acquisition_text_response("404 Not Found", "No resource.", content_type="text/plain")  # doctest: +SKIP


        :param status: HTTP status line including code and reason phrase.
        :param text: Response text to encode through the host's text-response constructor.
        :param content_type: Requested media type for the text payload.
        :return: Response object exposing status, headers, byte body, and optional cleanup.
        """
        ...

    def acquisition_bytes_response(
        self,
        payload: bytes,
        *,
        download_name: str,
        disposition: str = "attachment",
        content_type_override: Optional[str] = None,
    ) -> SurfaceResponseAPI:
        """
        Construct an acquisition response for a materialized byte payload and suggested filename.

        Header formatting, filename handling, and media-type fallback belong to
        the host rather than this structural port.

        Example:
            >>> response = host.acquisition_bytes_response(content, download_name="book.epub")  # doctest: +SKIP


        :param payload: Bytes to deliver as the response body.
        :param download_name: Suggested filename used by the host's response-header policy.
        :param disposition: Content-disposition mode, normally attachment or inline.
        :param content_type_override: Optional explicit media type instead of the host's inferred/default type.
        :return: Host-constructed response wrapping the supplied content for delivery.
        """
        ...

    def acquisition_redirect_response(self, location: str) -> SurfaceResponseAPI:
        """
        Construct a redirect response to a resolved acquisition location without reading that destination here.

        Example:
            >>> response = host.acquisition_redirect_response("https://example.invalid/book")  # doctest: +SKIP


        :param location: Redirect target passed to the host's response constructor.
        :return: Host-selected redirect response; URL validation and status policy belong to the implementation.
        """
        ...

    def acquisition_split_book_token(self, raw_book_id: str) -> tuple[Optional[int], str]:
        """
        Split a compatibility book token into an optional work identifier and trailing cover-size suffix.

        Example:
            >>> work_id, suffix = host.acquisition_split_book_token("7_60_80")  # doctest: +SKIP


        :param raw_book_id: Route token interpreted by the host's Calibre-compatible parser.
        :return: Parsed identifier or None for an invalid token, paired with its host-parsed suffix text.
        """
        ...

    def acquisition_placeholder_cover_svg(self, work_row: object, *, width: int, height: int) -> bytes:
        """
        Generate an SVG fallback cover from a work's display metadata and requested dimensions.

        Example:
            >>> svg = host.acquisition_placeholder_cover_svg(work, width=60, height=80)  # doctest: +SKIP


        :param work_row: Work row supplying placeholder title and other host-selected display information.
        :param width: Requested SVG width in the host's image-coordinate convention.
        :param height: Requested SVG height in the host's image-coordinate convention.
        :return: Encoded SVG bytes, not a fetched cover image or an HTTP response.
        """
        ...


class OpdsHostApi(Protocol):
    """
    Provide configuration, catalogue projections, and response constructors to OPDS navigation/acquisition feeds.

    The OPDS adapter owns feed routing and pagination while the host supplies
    work/category data and serialization-ready metadata under its catalogue policy.

    Example:
        >>> rows = host.opds_work_rows(sorted_by="recent")  # doctest: +SKIP
    """

    @property
    def config(self) -> object:
        """
        Expose catalogue title and paging settings used by the OPDS adapter.

        Consumers use title, default_page_size, max_page_size, and optionally
        opds_max_ungrouped_items; this broad annotation does not validate them.

        Example:
            >>> config = host.config  # doctest: +SKIP


        :return: Host configuration object consulted during feed construction and page-size selection.
        """
        ...

    def opds_xml_response(self, xml_text: str, *, status: str = "200 OK") -> SurfaceResponseAPI:
        """
        Wrap generated feed XML in the host's OPDS/XML response representation.

        Example:
            >>> response = host.opds_xml_response(feed_xml)  # doctest: +SKIP


        :param xml_text: Already generated XML document text to encode and deliver.
        :param status: HTTP status line, defaulting to a successful 200 response.
        :return: Host response with byte body and XML media-type/header policy.
        """
        ...

    def opds_text_response(self, status: str, text: str, *, content_type: str) -> SurfaceResponseAPI:
        """
        Construct a non-feed text response for OPDS route errors or other status messages.

        Example:
            >>> response = host.opds_text_response("404 Not Found", "Unknown route.", content_type="text/plain")  # doctest: +SKIP


        :param status: HTTP status line including code and reason phrase.
        :param text: Message encoded by the host's text-response constructor.
        :param content_type: Requested response media type.
        :return: Host response with a byte body and the requested status/media-type policy.
        """
        ...

    def opds_search_work_rows(self, query_text: str) -> list[object]:
        """
        Select work rows matching an OPDS search request for later feed pagination.

        Example:
            >>> rows = host.opds_search_work_rows("雪")  # doctest: +SKIP


        :param query_text: Search text interpreted by the host's catalogue search policy.
        :return: Matching work rows in host-selected order, before the OPDS adapter slices its page.
        """
        ...

    def opds_work_rows(self, *, sorted_by: str) -> list[object]:
        """
        Enumerate work rows in the ordering requested by an OPDS catalogue route.

        Example:
            >>> rows = host.opds_work_rows(sorted_by="title")  # doctest: +SKIP


        :param sorted_by: Catalogue ordering token; OPDS routes request title or recent.
        :return: Host-ordered work list for subsequent OPDS page slicing.
        """
        ...

    def opds_category_rows(self, category: str) -> list[SurfaceCategoryItem]:
        """
        Project one category's navigation items for OPDS grouping and pagination.

        Example:
            >>> authors = host.opds_category_rows("authors")  # doctest: +SKIP


        :param category: Catalogue category token such as authors, tags, or series.
        :return: Category-item dicts with host-provided identifiers, labels, and count/navigation metadata.
        """
        ...

    def opds_category_display_name(self, category: str) -> str:
        """
        Map a category token to the human-facing label used in OPDS feed headings.

        Example:
            >>> label = host.opds_category_display_name("authors")  # doctest: +SKIP


        :param category: Internal category token understood by the catalogue host.
        :return: Display label before XML escaping by the feed renderer.
        """
        ...

    def opds_rows_for_category_item(self, category: str, item_token: str) -> list[object]:
        """
        Find work rows associated with a category item selected by an OPDS route.

        Example:
            >>> works = host.opds_rows_for_category_item("tags", "7")  # doctest: +SKIP


        :param category: Category identifying the host's relationship lookup family.
        :param item_token: Item identity text interpreted within that category.
        :return: Associated work rows, with absent/unsupported-item handling owned by the host.
        """
        ...

    def opds_work_metadata_payload(self, row: object) -> SurfaceWorkMetadataPayload:
        """
        Build the metadata projection consumed when rendering one work's OPDS entry.

        Example:
            >>> metadata = host.opds_work_metadata_payload(work)  # doctest: +SKIP


        :param row: Work row whose identifiers, title, credits, and available formats should be projected.
        :return: Host-produced work metadata dict interpreted by the OPDS entry renderer.
        """
        ...


__all__ = [
    "AcquisitionHostApi",
    "CalibreCatalogHostApi",
    "ImageHostApi",
    "OpdsHostApi",
    "ReadModelHostApi",
    "ResolvedFileTargetAPI",
    "SurfaceCategoryItem",
    "SurfaceEntitySummary",
    "SurfaceFilePayload",
    "SurfaceRelatedPayload",
    "SurfaceResponseAPI",
    "SurfaceSearchEntry",
    "SurfaceWorkMetadataPayload",
]
