"""
Expose Core-backed catalogue, category, search, and file metadata through JSON WSGI.

The application extends the generic web host's read model and legacy-file
delivery, but does not mount its HTML table routes. Projected HTML/image URLs
remain metadata hints and are not all served by this dispatcher. GET and HEAD
share response bodies; lookup and projection failures normally propagate rather
than becoming missing-record JSON. Selected invalid IDs and absent rows have
explicit 400/404 responses.

Read-only is an interface description, not an authentication or content-safety
boundary. Author routes accept a table component without an author-table allowlist,
and inherited HTML/SVG previews are not sanitized. Importing this module defines
the application and CLI without starting Core or binding a listener.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import sys

from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, quote, unquote
from wsgiref.simple_server import make_server

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.catalog.api import CalibreCatalogBackend
from LiuXin_alpha.surfaces.core import (
    add_core_client_arguments,
    open_surface_core_from_args,
)
from LiuXin_alpha.surfaces.web_readonly.app import (
    ReadOnlyWebApplication,
    ReadOnlyWebConfig,
    _Response,
    _build_query_string,
    _coerce_int,
    _row_value,
    add_metadata_read_source_arguments,
    metadata_read_source_help_epilog,
    metadata_read_source_config_kwargs,
)


@dataclass(frozen=True)
class ApiReadOnlyConfig(ReadOnlyWebConfig):
    """
    Specialize generic web configuration with the JSON service title and port.

    Inherited paging, visibility, cache, and download settings retain their
    generic-web meanings. Values are immutable but not validated on construction;
    the command-line runner performs its own page-size clamps.

    Example:
        >>> config = ApiReadOnlyConfig(enable_file_downloads=False)
        >>> (config.title, config.port, config.enable_file_downloads)
        ('LiuXin API Read-Only', 8083, False)


    :ivar title: Service title returned by the API index.
    :ivar port: Requested listener port, defaulting to 8083.
    """

    title: str = "LiuXin API Read-Only"
    port: int = 8083


class ApiReadOnlyApplication(ReadOnlyWebApplication):
    """
    Adapt shared metadata projections into API routes and acquisition links.

    The generic host owns Core coercion, metadata/image helpers, WSGI cleanup,
    and file delivery. This subclass adds a catalogue backend and JSON route
    handling without starting a server. It inherits session ownership and close
    behavior; a borrowed Core client remains caller-owned.

    Example:
        >>> app = ApiReadOnlyApplication(core_client)  # doctest: +SKIP
        >>> response = app.handle_request({"PATH_INFO": "/api/works"})  # doctest: +SKIP
        >>> app.close()  # doctest: +SKIP
    """

    def __init__(self, core: CoreClientAPI, *, config: Optional[ApiReadOnlyConfig] = None) -> None:
        """
        Initialize generic Core-backed helpers and bind catalogue projection.

        Although annotated for a Core client, the base host retains its legacy
        database-coercion path. Construction exceptions propagate, and no
        listener is opened here.

        Example:
            >>> app = ApiReadOnlyApplication(core_client, config=ApiReadOnlyConfig(port=8090))  # doctest: +SKIP


        :param core: Borrowed Core client, or base-host-compatible legacy database input.
        :param config: Configuration to retain, or None for API-specific defaults.
        :return: None; initialize the generic host and its catalogue collaborator.
        """
        super().__init__(core, config=config or ApiReadOnlyConfig())
        self.catalog = CalibreCatalogBackend(self, read_model=self.read_model)

    def handle_request(self, environ) -> _Response:
        """
        Dispatch GET/HEAD API requests and inherited file-delivery routes.

        Dot segments are normalized before individual parts are unquoted, and
        blank query values are dropped. Works/author/tag/series dispatch uses
        string prefixes, so a same-length path such as /api/works-extra reaches
        the works collection handler. Other routes use their explicit shapes.
        HEAD retains GET bodies. Unsupported methods yield JSON 405 and unknown
        paths JSON 404; robots.txt and acquisition responses need not be JSON.
        Backend errors are not caught at this boundary.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> app.handle_request({"REQUEST_METHOD": "POST"}).status
            '405 Method Not Allowed'
            >>> app.handle_request({"PATH_INFO": "/tables/works"}).status
            '404 Not Found'


        :param environ: WSGI mapping providing method, path, query text, and file-wrapper hooks.
        :return: Response carrier for JSON, robots text, file bytes, or redirects.
        """
        method = str(environ.get("REQUEST_METHOD", "GET") or "GET").upper()
        if method not in {"GET", "HEAD"}:
            return self._json_response({"error": "method_not_allowed", "message": "Method not allowed."}, status="405 Method Not Allowed")

        path = posixpath.normpath(str(environ.get("PATH_INFO", "/") or "/"))
        if not path.startswith("/"):
            path = "/" + path
        query = parse_qs(str(environ.get("QUERY_STRING", "") or ""), keep_blank_values=False)

        if path in {"/", "/api"}:
            return self._json_response(self._index_payload())
        if path == "/robots.txt":
            return self._text_response("200 OK", "User-agent: *\nAllow: /\n", content_type="text/plain")
        if path.startswith("/api/works"):
            return self._serve_api_works(path, query)
        if path == "/api/categories":
            return self._json_response({"items": self._category_summary_payload()})
        if path.startswith("/api/authors"):
            return self._serve_api_category(path, query, kind="authors")
        if path.startswith("/api/tags"):
            return self._serve_api_category(path, query, kind="tags")
        if path.startswith("/api/series"):
            return self._serve_api_category(path, query, kind="series")
        if path == "/api/search":
            return self._json_response(self._search_payload(query))
        if path.startswith("/api/files/"):
            return self._serve_api_file(path)
        if path.startswith("/files/") and path.endswith("/download"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 3 and parts[0] == "files" and parts[2] == "download":
                return self._serve_file_download(parts[1], environ)
        if path.startswith("/files/") and path.endswith("/preview"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 3 and parts[0] == "files" and parts[2] == "preview":
                return self._serve_file_preview(parts[1], environ)
        return self._json_response({"error": "not_found", "message": "Unknown API route."}, status="404 Not Found")

    def _json_response(self, payload: object, *, status: str = "200 OK") -> _Response:
        """
        Serialize a payload into one UTF-8 JSON chunk with sorted object keys.

        Unicode remains literal, and standard json.dumps nonfinite-float handling
        is retained rather than strict-JSON rejection. Serialization failures
        propagate; no custom encoder, length header, or body cleanup is installed.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> b"".join(app._json_response({"z": 1, "a": "café"}).body).decode("utf-8")
            '{"a": "café", "z": 1}'


        :param payload: JSON-serializable value, not restricted to a mapping.
        :param status: HTTP status line retained without validation.
        :return: Buffered response labelled application/json with UTF-8 charset.
        :raises TypeError: A value cannot be encoded by the standard JSON encoder.
        """
        return _Response(
            status=status,
            headers=[("Content-Type", "application/json; charset=utf-8")],
            body=[json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")],
        )

    def _index_payload(self) -> dict[str, object]:
        """
        Describe service endpoints and read current work/category/file counts.

        Category counts use the read model's materialized browse operations;
        file count is zero only when the files table is confirmed absent.
        Reads are independent rather than an atomic snapshot. Unlike generic
        HTML home counts, capability/count failures are not suppressed here.

        Example:
            >>> payload = app._index_payload()  # doctest: +SKIP


        :return: Service/title/endpoints/counts mapping, including file-route templates.
        """
        return {
            "service": "api_readonly",
            "title": self.config.title,
            "endpoints": {
                "self": "/api",
                "categories": "/api/categories",
                "works": "/api/works",
                "authors": "/api/authors",
                "tags": "/api/tags",
                "series": "/api/series",
                "search": "/api/search?q=...",
                "file": "/api/files/<id>",
                "file_download": "/files/<id>/download",
                "file_preview": "/files/<id>/preview",
            },
            "counts": {
                "works": self.read_model.browse_count("titles"),
                "authors": self.read_model.browse_count("authors"),
                "tags": self.read_model.browse_count("tags"),
                "series": self.read_model.browse_count("series"),
                "files": self.read_model.table_record_count("files") if self._table_exists("files") else 0,
            },
        }

    def _pagination_payload(
        self,
        *,
        base_path: str,
        total: int,
        limit: int,
        offset: int,
        query_values: Optional[dict[str, object]] = None,
    ) -> dict[str, object]:
        """
        Build pagination metadata and links without validating or clamping the page.

        Copy the query mapping and replace limit/offset. Self always includes
        those keys; Previous floors its offset at zero and Next exists when
        offset + limit is below total. Oversized offsets and nonpositive limits
        are retained, so callers should pass already-coerced paging values.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> values = {"sort": "recent"}
            >>> page = app._pagination_payload(base_path="/api/works", total=3, limit=2, offset=0, query_values=values)
            >>> (page["previous"], page["next"], values)
            (None, '/api/works?sort=recent&limit=2&offset=2', {'sort': 'recent'})


        :param base_path: Link path without an existing query; a question mark is appended.
        :param total: Full matching count, integer-coerced in metadata but otherwise unchecked.
        :param limit: Requested page length used for URL values and navigation arithmetic.
        :param offset: Zero-based start retained even beyond the total.
        :param query_values: Optional scalar filters copied into all navigation URLs.
        :return: Total/limit/offset/self/previous/next mapping with None for absent links.
        """
        values = dict(query_values or {})
        values["limit"] = limit
        values["offset"] = offset
        result = {
            "total": int(total),
            "limit": int(limit),
            "offset": int(offset),
            "self": base_path + ("?" + _build_query_string(values) if values else ""),
            "previous": None,
            "next": None,
        }
        if offset > 0:
            prev_values = dict(values)
            prev_values["offset"] = max(0, offset - limit)
            result["previous"] = base_path + "?" + _build_query_string(prev_values)
        if offset + limit < total:
            next_values = dict(values)
            next_values["offset"] = offset + limit
            result["next"] = base_path + "?" + _build_query_string(next_values)
        return result

    def _category_summary_payload(self) -> list[dict[str, object]]:
        """
        Project shared navigation categories into API-facing summary mappings.

        Allbooks and newest link to title/recent work collections; other category
        tokens are percent-quoted into an API path. Required fields are coerced,
        and provider ordering is retained without changing provider entries.

        Example:
            >>> categories = app._category_summary_payload()  # doctest: +SKIP


        :return: New name/count/is_category/category/api_url entries; projection errors propagate.
        """
        items: list[dict[str, object]] = []
        for entry in self.read_model.category_summary_payload():
            category = str(entry["category"])
            if category == "allbooks":
                api_url = "/api/works?sort=title"
            elif category == "newest":
                api_url = "/api/works?sort=recent"
            else:
                api_url = "/api/{}".format(quote(category, safe=""))
            items.append(
                {
                    "name": str(entry["name"]),
                    "count": int(entry["count"]),
                    "is_category": bool(entry["is_category"]),
                    "category": category,
                    "api_url": api_url,
                }
            )
        return items

    def _api_entity_url(self, table: str, row) -> str:
        """
        Read a schema-selected entity ID and format its API or fallback URL.

        Missing identity is not rejected: the downstream formatter can produce
        a path containing None. No record-existence or route-availability check
        is performed beyond the schema read used to select an ID column.

        Example:
            >>> url = app._api_entity_url("works", work)  # doctest: +SKIP


        :param table: Schema context and exact route-family selector.
        :param row: Row-like object from which the selected ID value is read.
        :return: Root-relative entity link, even when the identity is unusable.
        """
        row_id = _row_value(row, self._id_column(table) or "")
        return self._api_entity_url_from_id(table, row_id)

    def _api_entity_url_from_id(self, table: str, row_id: object) -> str:
        """
        Map known entity tables to API routes, percent-quoting stringified IDs.

        Contributor routes retain their table component; labels and tags both
        map to the tag family without retaining their source-table distinction.
        Unknown tables use generic HTML table URLs, which this dispatcher does
        not serve. IDs, including None, are not validated or looked up here.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> app._api_entity_url_from_id("agents", "a/b")
            '/api/authors/agents/a%2Fb'
            >>> app._api_entity_url_from_id("labels", None)
            '/api/tags/None'


        :param table: Exact schema table token selecting a known family or HTML fallback.
        :param row_id: Identity stringified and quoted as a single path component.
        :return: Root-relative API or generic-table link without availability guarantees.
        """
        safe_id = quote(str(row_id), safe="")
        if table == "works":
            return "/api/works/{}".format(safe_id)
        if table in {"agents", "human_agents", "org_agents"}:
            return "/api/authors/{}/{}".format(quote(table, safe=""), safe_id)
        if table in {"labels", "tags"}:
            return "/api/tags/{}".format(safe_id)
        if table == "series":
            return "/api/series/{}".format(safe_id)
        if table == "files":
            return "/api/files/{}".format(safe_id)
        return "/tables/{}/{}".format(quote(table, safe=""), safe_id)

    def _entity_summary_payload(self, table: str, row) -> dict[str, object]:
        """
        Shallow-copy an entity summary and add a URL from its projected table and ID.

        The provider's returned identity, not the original table argument, drives
        the added link. Nested values remain shared; required-field errors propagate.

        Example:
            >>> payload = app._entity_summary_payload("agents", author)  # doctest: +SKIP


        :param table: Schema/display context passed to the shared summary builder.
        :param row: Entity row to project without an additional lookup here.
        :return: New outer summary mapping containing api_url alongside existing fields.
        """
        payload = dict(self.read_model.entity_summary_payload(table, row))
        payload["api_url"] = self._api_entity_url_from_id(str(payload["table"]), payload["id"])
        return payload

    def _related_payload(self, row) -> dict[str, list[dict[str, object]]]:
        """
        Add API links in place to each shared related-entity summary.

        Both the outer mapping and nested entries are retained from the backend;
        this adapter is not a defensive copy. A later malformed entry can raise
        after earlier entries were already augmented.

        Example:
            >>> from unittest.mock import Mock
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> original = {"labels": [{"table": "labels", "id": 3}]}
            >>> app.read_model = Mock(related_payload=Mock(return_value=original))
            >>> app._related_payload(object()) is original
            True
            >>> original["labels"][0]["api_url"]
            '/api/tags/3'


        :param row: Entity whose related summaries should be requested from the backend.
        :return: Original related mapping with api_url inserted into each entry.
        """
        payload = self.read_model.related_payload(row)
        for items in payload.values():
            for entry in items:
                entry["api_url"] = self._api_entity_url_from_id(str(entry["table"]), entry["id"])
        return payload

    def _work_summary_payload(self, row) -> dict[str, object]:
        """
        Select catalogue metadata fields for one work collection entry.

        Authors, series, tags, formats, and image/link hints are retained from
        the shared metadata projection rather than independently validated.
        The API ID is interpolated directly, unlike the general entity URL
        formatter; nested values are shared with the metadata result.

        Example:
            >>> summary = app._work_summary_payload(work)  # doctest: +SKIP


        :param row: Work row used by the shared metadata builder.
        :return: New work summary with identity, descriptive fields, and API/HTML URLs.
        """
        metadata = self.read_model.work_metadata_payload(row)
        return {
            "id": metadata["id"],
            "title": metadata["title"],
            "authors": metadata["authors"],
            "series": metadata["series"],
            "tags": metadata["tags"],
            "summary": metadata["summary"],
            "formats": metadata["formats"],
            "thumbnail": metadata["thumbnail"],
            "cover": metadata["cover"],
            "api_url": "/api/works/{}".format(metadata["id"]),
            "html_url": metadata["url"],
        }

    def _work_detail_payload(self, row) -> dict[str, object]:
        """
        Augment a shared work-detail payload's credits, files, and related entries in place.

        Credit entities and related entries use the general entity URL formatter;
        file IDs are directly interpolated. The nested work metadata itself is
        not augmented here. The original outer mapping and nested objects remain
        shared, and partial mutation can precede a malformed-entry exception.

        Example:
            >>> detail = app._work_detail_payload(work)  # doctest: +SKIP


        :param row: Work row passed to the shared detail projection.
        :return: The backend's original work/credits/files/related mapping with added links.
        """
        payload = self.read_model.work_detail_payload(row)
        for entry in payload["credits"]:
            entity = entry["entity"]
            entity["api_url"] = self._api_entity_url_from_id(str(entity["table"]), entity["id"])
        for entry in payload["files"]:
            entry["api_url"] = "/api/files/{}".format(entry["id"])
        for items in payload["related"].values():
            for entry in items:
                entry["api_url"] = self._api_entity_url_from_id(str(entry["table"]), entry["id"])
        return payload

    def _file_summary_payload(self, file_row) -> dict[str, object]:
        """
        Shallow-copy a shared file summary and append its metadata API link.

        The ID is interpolated directly, not percent-quoted; this helper does
        not revalidate acquisition or copy nested metadata values.

        Example:
            >>> summary = app._file_summary_payload(file_row)  # doctest: +SKIP


        :param file_row: Legacy file row passed to shared naming/capability projection.
        :return: New outer summary mapping with api_url and retained nested values.
        """
        payload = dict(self.read_model.file_summary_payload(file_row))
        payload["api_url"] = "/api/files/{}".format(payload["id"])
        return payload

    def _file_detail_payload(self, file_row) -> dict[str, object]:
        """
        Copy the outer file detail while augmenting its shared related entries in place.

        The file API URL interpolates its ID directly. Related URLs use the
        general quoted formatter. Copying the outer dict does not isolate nested
        mutations or roll them back if a later entry is malformed.

        Example:
            >>> detail = app._file_detail_payload(file_row)  # doctest: +SKIP


        :param file_row: Legacy file row passed to the shared detail builder.
        :return: New outer file-detail mapping with api_url and mutated nested related summaries.
        """
        payload = dict(self.read_model.file_detail_payload(file_row))
        payload["api_url"] = "/api/files/{}".format(payload["id"])
        for items in payload["related"].values():
            for entry in items:
                entry["api_url"] = self._api_entity_url_from_id(str(entry["table"]), entry["id"])
        return payload

    def _serve_api_works(self, path: str, query: dict[str, list[str]]) -> _Response:
        """
        Serve a work collection or integer-ID detail selected by decoded path length.

        Two nonempty parts select a collection; three select detail, without
        rechecking the first two names after prefix dispatch. Collection sorting
        normalizes exact recent and otherwise uses title. First paging values
        are coerced to a positive bounded limit and nonnegative offset. Detail
        conversion failures are 400, missing rows 404, and other path lengths
        404; backend lookup/projection errors remain exceptions.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> app._serve_api_works("/api/works/not-an-id", {}).status
            '400 Bad Request'


        :param path: Normalized route whose nonempty components are individually unquoted.
        :param query: Multi-value query mapping for sort, limit, and offset on collections.
        :return: JSON work page, detail, or explicit 400/404 error response.
        """
        parts = [unquote(part) for part in path.split("/") if part]
        if len(parts) == 2:
            sort = str((query.get("sort") or ["title"])[0] or "title").strip().lower()
            sort = "recent" if sort == "recent" else "title"
            limit = _coerce_int((query.get("limit") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
            offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
            visible, total = self.read_model.work_page(
                sorted_by=sort,
                limit=limit,
                offset=offset,
            )
            return self._json_response(
                {
                    "kind": "works",
                    "sort": sort,
                    "items": [self._work_summary_payload(row) for row in visible],
                    "pagination": self._pagination_payload(base_path="/api/works", total=total, limit=limit, offset=offset, query_values={"sort": sort}),
                }
            )
        if len(parts) == 3:
            try:
                row_id = int(str(parts[2]).strip())
            except Exception:
                return self._json_response({"error": "bad_work_id", "message": "Invalid work id."}, status="400 Bad Request")
            row = self.read_model.row_by_id("works", row_id)
            if row is None:
                return self._json_response({"error": "missing_work", "message": "Work not found."}, status="404 Not Found")
            return self._json_response(self._work_detail_payload(row))
        return self._json_response({"error": "not_found", "message": "Unknown works route."}, status="404 Not Found")

    def _category_item_payload(self, kind: str, item: dict[str, object]) -> dict[str, object]:
        """
        Project a category row summary into detail and linked-work navigation.

        Exact authors retains table in both URLs; exact tags uses tag URLs;
        every other kind uses series URLs without validation. IDs remain raw
        in metadata but are percent-quoted in links. Falsey counts become zero
        before integer conversion, and falsey HTML URLs become empty strings.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> item = {"table": "labels", "id": 3, "label": "Fantasy"}
            >>> app._category_item_payload("tags", item)["works_url"]
            '/api/tags/3/works'


        :param kind: Expected authors/tags/series selector used for route-family choice.
        :param item: Required table/id/label mapping with optional count and URL fields.
        :return: New id/table/name/count/api_url/works_url/html_url mapping.
        """
        table = str(item["table"])
        row_id = item["id"]
        if kind == "authors":
            works_url = "/api/authors/{}/{}/works".format(quote(table, safe=""), quote(str(row_id), safe=""))
            api_url = "/api/authors/{}/{}".format(quote(table, safe=""), quote(str(row_id), safe=""))
        elif kind == "tags":
            works_url = "/api/tags/{}/works".format(quote(str(row_id), safe=""))
            api_url = "/api/tags/{}".format(quote(str(row_id), safe=""))
        else:
            works_url = "/api/series/{}/works".format(quote(str(row_id), safe=""))
            api_url = "/api/series/{}".format(quote(str(row_id), safe=""))
        return {
            "id": row_id,
            "table": table,
            "name": str(item["label"]),
            "count": int(item.get("count") or 0),
            "api_url": api_url,
            "works_url": works_url,
            "html_url": item.get("url") or "",
        }

    def _category_collection_payload(self, kind: str, *, limit: int, offset: int) -> dict[str, object]:
        """
        Request a name-ascending category page and adapt its entries for the API.

        Shared category logic owns enumeration and sorting. This adapter neither
        validates kind nor clamps paging; route callers provide coerced values.

        Example:
            >>> payload = app._category_collection_payload("authors", limit=20, offset=0)  # doctest: +SKIP


        :param kind: Category token forwarded to the backend and interpolated into the page URL.
        :param limit: Requested visible entry count, forwarded as the backend num argument.
        :param offset: Zero-based slice start retained in pagination metadata.
        :return: Kind, projected items, and pagination using the provider's full count.
        """
        payload = self.read_model.category_items_payload(kind, num=limit, offset=offset, sort="name", sort_order="asc")
        visible = list(payload["items"])
        return {
            "kind": kind,
            "items": [self._category_item_payload(kind, item) for item in visible],
            "pagination": self._pagination_payload(base_path="/api/{}".format(kind), total=int(payload["total_num"]), limit=limit, offset=offset),
        }

    def _category_detail_payload(self, *, kind: str, table: str, row_id: str) -> dict[str, object] | None:
        """
        Resolve one category entity and count its fully materialized linked works.

        Common integer-conversion failures and absent rows return None. The
        numeric ID drives row lookup, but the original ID spelling is forwarded
        to linked-work discovery and percent-quoted into its URL. Lookup and
        relationship failures are not translated into a missing result.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> app._category_detail_payload(kind="tags", table="labels", row_id="bad") is None
            True


        :param kind: Category label and linked-work route selector; authors retains table.
        :param table: Exact entity table, accepted without an author-table allowlist.
        :param row_id: Text identity converted via int(str(...)) for the initial lookup.
        :return: Kind/entity/works_count/works_url mapping, or None for invalid/absent identity.
        """
        try:
            numeric_id = int(str(row_id))
        except (TypeError, ValueError, OverflowError):
            return None
        row = self.read_model.row_by_id(table, numeric_id)
        if row is None:
            return None
        works = self.read_model.works_for_linked_entity(table, row_id)
        return {
            "kind": kind,
            "entity": self._entity_summary_payload(table, row),
            "works_count": len(works),
            "works_url": (
                "/api/authors/{}/{}/works".format(quote(table, safe=""), quote(str(row_id), safe=""))
                if kind == "authors"
                else "/api/{}/{}/works".format(quote(kind, safe=""), quote(str(row_id), safe=""))
            ),
        }

    def _works_for_category_payload(self, kind: str, rows: list[object], *, path: str, limit: int, offset: int) -> dict[str, object]:
        """
        Slice already-discovered linked works and project the visible summaries.

        Input order is retained without sorting or deduplication. Paging uses
        unchecked Python slicing, including negative semantics for direct calls.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> app._works_for_category_payload("tags", [], path="/api/tags/3/works", limit=10, offset=0)["items"]
            []


        :param kind: Category token echoed in the result rather than normalized.
        :param rows: Complete linked-work list whose length supplies the total count.
        :param path: Page URL path reused verbatim in pagination links.
        :param limit: Requested number of rows in the local slice.
        :param offset: Slice start and echoed pagination offset.
        :return: Kind/items/pagination mapping, with summaries only for the visible slice.
        """
        visible = rows[offset : offset + limit]
        return {
            "kind": kind,
            "items": [self._work_summary_payload(row) for row in visible],
            "pagination": self._pagination_payload(base_path=path, total=len(rows), limit=limit, offset=offset),
        }

    def _serve_api_category(self, path: str, query: dict[str, list[str]], *, kind: str) -> _Response:
        """
        Serve category lists, entity details, or paged linked works by route shape.

        Tags/series use ID as the third part; authors uses table and ID as the
        third/fourth parts. Author tables are forwarded without an allowlist.
        Tag routes choose the shared tags/labels source, falling back to tags
        only when no table is selected. Details map invalid/absent identities to
        404; linked-work routes delegate directly and can return successful empty
        lists for invalid or missing entities. Other backend failures propagate.

        Paging is coerced even for detail paths. This handler checks lengths
        and the final works marker, not the prefix names already used by dispatch.

        Example:
            >>> response = app._serve_api_category("/api/authors/agents/1/works", {"limit": ["10"]}, kind="authors")  # doctest: +SKIP


        :param path: Normalized route with percent-encoded components decoded here.
        :param query: Multi-value limit/offset mapping, using only each first value.
        :param kind: Expected authors, tags, or series family selected by dispatch.
        :return: JSON collection/detail/linked-work response, or explicit missing/unknown 404.
        """
        parts = [unquote(part) for part in path.split("/") if part]
        limit = _coerce_int((query.get("limit") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
        offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
        if kind in {"tags", "series"}:
            if len(parts) == 2:
                return self._json_response(self._category_collection_payload(kind, limit=limit, offset=offset))
            if len(parts) == 3:
                table = (self.read_model.tag_category_table() or "tags") if kind == "tags" else "series"
                payload = self._category_detail_payload(kind=kind, table=table, row_id=parts[2])
                if payload is None:
                    return self._json_response({"error": "missing_category_row", "message": "Category row not found."}, status="404 Not Found")
                return self._json_response(payload)
            if len(parts) == 4 and parts[3] == "works":
                table = (self.read_model.tag_category_table() or "tags") if kind == "tags" else "series"
                rows = self.read_model.works_for_linked_entity(table, parts[2])
                return self._json_response(self._works_for_category_payload(kind, rows, path=path, limit=limit, offset=offset))
        if kind == "authors":
            if len(parts) == 2:
                return self._json_response(self._category_collection_payload(kind, limit=limit, offset=offset))
            if len(parts) == 4:
                payload = self._category_detail_payload(kind=kind, table=str(parts[2]), row_id=parts[3])
                if payload is None:
                    return self._json_response({"error": "missing_author_row", "message": "Author row not found."}, status="404 Not Found")
                return self._json_response(payload)
            if len(parts) == 5 and parts[4] == "works":
                table = str(parts[2])
                rows = self.read_model.works_for_linked_entity(table, parts[3])
                return self._json_response(self._works_for_category_payload(kind, rows, path=path, limit=limit, offset=offset))
        return self._json_response({"error": "not_found", "message": "Unknown category route."}, status="404 Not Found")

    def _search_payload(self, query: dict[str, list[str]]) -> dict[str, object]:
        """
        Request ranked search results, add API links in place, and build page metadata.

        A truthy q list takes precedence over global_q even if its first item is
        blank; route parsing normally drops blank values beforehand. Selected
        query/table text is stripped. Limit and offset are coerced, while the
        backend owns table selection and ranking. Results and group_counts are
        retained from its payload, and result entries are mutated with api_url.

        Example:
            >>> payload = app._search_payload({"q": ["ocean"], "table": ["works"], "limit": ["10"]})  # doctest: +SKIP


        :param query: Multi-value q/global_q/table/limit/offset mapping using first values.
        :return: Query/filter/results/group_counts/pagination mapping sharing backend entries.
        """
        q = str((query.get("q") or query.get("global_q") or [""])[0] or "").strip()
        limit = _coerce_int((query.get("limit") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
        offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
        table_filter = str((query.get("table") or [""])[0] or "").strip()
        payload = self.read_model.search_results_payload(query_text=q, table_filter=table_filter, limit=limit, offset=offset)
        for entry in payload["results"]:
            entry["api_url"] = self._api_entity_url_from_id(str(entry["table"]), entry["id"])
        return {
            "query": q,
            "table_filter": table_filter,
            "results": payload["results"],
            "group_counts": payload["group_counts"],
            "pagination": self._pagination_payload(base_path="/api/search", total=payload["total"], limit=limit, offset=offset, query_values={"q": q, "table": table_filter}),
        }

    def _serve_api_file(self, path: str) -> _Response:
        """
        Return file metadata for a three-part route containing an integer ID.

        Wrong component count returns 404, conversion failure 400, and an absent
        file row 404. Lookup and projection failures propagate. This route reports
        capabilities and links; it does not deliver the file's bytes.

        Example:
            >>> app = object.__new__(ApiReadOnlyApplication)
            >>> app._serve_api_file("/api/files/not-an-id").status
            '400 Bad Request'


        :param path: Route whose nonempty components are unquoted before parsing its ID.
        :return: JSON file-detail response or explicit route/identity error response.
        """
        parts = [unquote(part) for part in path.split("/") if part]
        if len(parts) != 3:
            return self._json_response({"error": "not_found", "message": "Unknown file route."}, status="404 Not Found")
        try:
            row_id = int(str(parts[2]).strip())
        except Exception:
            return self._json_response({"error": "bad_file_id", "message": "Invalid file id."}, status="400 Bad Request")
        row = self.read_model.row_by_id("files", row_id)
        if row is None:
            return self._json_response({"error": "missing_file", "message": "File not found."}, status="404 Not Found")
        return self._json_response(self._file_detail_payload(row))


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build the JSON API parser with shared Core/cache and service-specific options.

    Page values are integer-parsed here but clamped in main. The parser does
    not initialize Core, load a cache, or bind a listener. Unlike the generic
    web parser, it does not expose the database-path display flag.

    Example:
        >>> args = build_arg_parser().parse_args(["--database", "library.sqlite", "--page-size", "0"])
        >>> (args.port, args.page_size, args.metadata_read_source)
        (8083, 0, 'database')


    :return: Fresh argparse parser with listener, paging, title, and download controls.
    """
    parser = argparse.ArgumentParser(
        description="Run the LiuXin read-only JSON API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=metadata_read_source_help_epilog("PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.api_readonly"),
    )
    add_core_client_arguments(parser)
    parser.add_argument("--db-type", default="sqlite", help="Database driver type. Default: sqlite")
    add_metadata_read_source_arguments(parser)
    parser.add_argument("--host", default=ApiReadOnlyConfig.host, help="Bind host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=ApiReadOnlyConfig.port, help="Bind port. Default: 8083")
    parser.add_argument("--page-size", type=int, default=ApiReadOnlyConfig.default_page_size, help="Default page size.")
    parser.add_argument("--max-page-size", type=int, default=ApiReadOnlyConfig.max_page_size, help="Maximum page size.")
    parser.add_argument("--title", default=ApiReadOnlyConfig.title, help="Service title.")
    parser.add_argument("--no-file-downloads", action="store_true", help="Disable file download / redirect links.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """
    Compose Core and serve the blocking stdlib JSON API until normal termination.

    Default/max page sizes are independently clamped to at least one. Exact
    cache selection forwards its type and fallback flag; local composition
    enables storage and disables maintenance. Core and server are context-managed.
    The printed /api URL precedes binding and shows the requested port, including
    zero rather than the assigned ephemeral port. Runtime failures propagate.

    Example:
        >>> main(["--database", "library.sqlite", "--host", "127.0.0.1", "--port", "8083"])  # doctest: +SKIP


    :param argv: Argument tokens without program name, or None for process arguments.
    :return: Zero after serve_forever returns normally and both contexts exit.
    :raises SystemExit: Argparse handles help or rejects invalid arguments.
    """
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = ApiReadOnlyConfig(
        title=str(args.title),
        host=str(args.host),
        port=int(args.port),
        default_page_size=max(1, int(args.page_size)),
        max_page_size=max(1, int(args.max_page_size)),
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
        app = ApiReadOnlyApplication(core_session.client, config=config)
        url = "http://{}:{}/api".format(config.host, config.port)
        sys.stdout.write("Serving read-only JSON API on {}\n".format(url))
        sys.stdout.flush()
        with make_server(config.host, config.port, app) as server:
            server.serve_forever()
    return 0


__all__ = [
    "ApiReadOnlyApplication",
    "ApiReadOnlyConfig",
    "build_arg_parser",
    "main",
]
