"""Machine-facing read-only JSON API for the LiuXin library."""

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
    title: str = "LiuXin API Read-Only"
    port: int = 8083


class ApiReadOnlyApplication(ReadOnlyWebApplication):
    """Stable JSON read model over the shared browse/catalog helpers."""

    def __init__(self, core: CoreClientAPI, *, config: Optional[ApiReadOnlyConfig] = None) -> None:
        super().__init__(core, config=config or ApiReadOnlyConfig())
        self.catalog = CalibreCatalogBackend(self, read_model=self.read_model)

    def handle_request(self, environ) -> _Response:
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
        return _Response(
            status=status,
            headers=[("Content-Type", "application/json; charset=utf-8")],
            body=[json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")],
        )

    def _index_payload(self) -> dict[str, object]:
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
        row_id = _row_value(row, self._id_column(table) or "")
        return self._api_entity_url_from_id(table, row_id)

    def _api_entity_url_from_id(self, table: str, row_id: object) -> str:
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
        payload = dict(self.read_model.entity_summary_payload(table, row))
        payload["api_url"] = self._api_entity_url_from_id(str(payload["table"]), payload["id"])
        return payload

    def _related_payload(self, row) -> dict[str, list[dict[str, object]]]:
        payload = self.read_model.related_payload(row)
        for items in payload.values():
            for entry in items:
                entry["api_url"] = self._api_entity_url_from_id(str(entry["table"]), entry["id"])
        return payload

    def _work_summary_payload(self, row) -> dict[str, object]:
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
        payload = dict(self.read_model.file_summary_payload(file_row))
        payload["api_url"] = "/api/files/{}".format(payload["id"])
        return payload

    def _file_detail_payload(self, file_row) -> dict[str, object]:
        payload = dict(self.read_model.file_detail_payload(file_row))
        payload["api_url"] = "/api/files/{}".format(payload["id"])
        for items in payload["related"].values():
            for entry in items:
                entry["api_url"] = self._api_entity_url_from_id(str(entry["table"]), entry["id"])
        return payload

    def _serve_api_works(self, path: str, query: dict[str, list[str]]) -> _Response:
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
        payload = self.read_model.category_items_payload(kind, num=limit, offset=offset, sort="name", sort_order="asc")
        visible = list(payload["items"])
        return {
            "kind": kind,
            "items": [self._category_item_payload(kind, item) for item in visible],
            "pagination": self._pagination_payload(base_path="/api/{}".format(kind), total=int(payload["total_num"]), limit=limit, offset=offset),
        }

    def _category_detail_payload(self, *, kind: str, table: str, row_id: str) -> dict[str, object]:
        row = self.read_model.row_by_id(table, int(str(row_id)))
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
        visible = rows[offset : offset + limit]
        return {
            "kind": kind,
            "items": [self._work_summary_payload(row) for row in visible],
            "pagination": self._pagination_payload(base_path=path, total=len(rows), limit=limit, offset=offset),
        }

    def _serve_api_category(self, path: str, query: dict[str, list[str]], *, kind: str) -> _Response:
        parts = [unquote(part) for part in path.split("/") if part]
        limit = _coerce_int((query.get("limit") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
        offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
        if kind in {"tags", "series"}:
            if len(parts) == 2:
                return self._json_response(self._category_collection_payload(kind, limit=limit, offset=offset))
            if len(parts) == 3:
                table = (self.read_model.tag_category_table() or "tags") if kind == "tags" else "series"
                try:
                    return self._json_response(self._category_detail_payload(kind=kind, table=table, row_id=parts[2]))
                except Exception:
                    return self._json_response({"error": "missing_category_row", "message": "Category row not found."}, status="404 Not Found")
            if len(parts) == 4 and parts[3] == "works":
                table = (self.read_model.tag_category_table() or "tags") if kind == "tags" else "series"
                rows = self.read_model.works_for_linked_entity(table, parts[2])
                return self._json_response(self._works_for_category_payload(kind, rows, path=path, limit=limit, offset=offset))
        if kind == "authors":
            if len(parts) == 2:
                return self._json_response(self._category_collection_payload(kind, limit=limit, offset=offset))
            if len(parts) == 4:
                try:
                    return self._json_response(self._category_detail_payload(kind=kind, table=str(parts[2]), row_id=parts[3]))
                except Exception:
                    return self._json_response({"error": "missing_author_row", "message": "Author row not found."}, status="404 Not Found")
            if len(parts) == 5 and parts[4] == "works":
                table = str(parts[2])
                rows = self.read_model.works_for_linked_entity(table, parts[3])
                return self._json_response(self._works_for_category_payload(kind, rows, path=path, limit=limit, offset=offset))
        return self._json_response({"error": "not_found", "message": "Unknown category route."}, status="404 Not Found")

    def _search_payload(self, query: dict[str, list[str]]) -> dict[str, object]:
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
