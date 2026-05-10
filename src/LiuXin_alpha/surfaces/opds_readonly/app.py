"""Standalone OPDS read-only interface built from shared protocol backends."""

from __future__ import annotations

import argparse
import posixpath
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote
from wsgiref.simple_server import make_server

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.surfaces.acquisition.api import AcquisitionCompatApi
from LiuXin_alpha.surfaces.catalog.api import CalibreCatalogBackend, PLACEHOLDER_PNG
from LiuXin_alpha.surfaces.opds.api import OpdsApi
from LiuXin_alpha.surfaces.web_readonly.app import (
    ReadOnlyWebApplication,
    ReadOnlyWebConfig,
    _Response,
    _open_database,
    _row_value,
    add_metadata_read_source_arguments,
    metadata_read_source_help_epilog,
    metadata_read_source_config_kwargs,
)


@dataclass(frozen=True)
class OpdsReadOnlyConfig(ReadOnlyWebConfig):
    title: str = "LiuXin OPDS Read-Only"
    opds_max_ungrouped_items: int = 100


class OpdsReadOnlyApplication(ReadOnlyWebApplication):
    """Narrow OPDS/acquisition surface separated from HTML browse UI."""

    def __init__(self, db: Database, *, config: Optional[OpdsReadOnlyConfig] = None) -> None:
        super().__init__(db, config=config or OpdsReadOnlyConfig())
        self.catalog = CalibreCatalogBackend(self, read_model=self.read_model, images=self.images)
        self.opds_api = OpdsApi(self)
        self.acquisition_api = AcquisitionCompatApi(self)

    def handle_request(self, environ) -> _Response:
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
        return _Response(
            status=status,
            headers=[("Content-Type", "application/atom+xml; charset=utf-8")],
            body=[xml_text.encode("utf-8")],
        )

    def _serve_icon(self, which: str, query: dict[str, list[str]]) -> _Response:
        del which, query
        return self._bytes_response(PLACEHOLDER_PNG, download_name="icon.png", disposition="inline", content_type_override="image/png")

    def opds_xml_response(self, xml_text: str, *, status: str = "200 OK") -> _Response:
        return self._xml_response(xml_text, status=status)

    def opds_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        return self._text_response(status, text, content_type=content_type)

    def acquisition_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        return self._text_response(status, text, content_type=content_type)

    def acquisition_bytes_response(
        self,
        payload: bytes,
        *,
        download_name: str,
        disposition: str = "attachment",
        content_type_override: Optional[str] = None,
    ) -> _Response:
        return self._bytes_response(
            payload,
            download_name=download_name,
            disposition=disposition,
            content_type_override=content_type_override,
        )

    def acquisition_redirect_response(self, location: str) -> _Response:
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
        return self._file_response(
            path,
            download_name=download_name,
            environ=environ,
            disposition=disposition,
            content_type_override=content_type_override,
        )

    def acquisition_split_book_token(self, raw_book_id: str) -> tuple[Optional[int], str]:
        return self.catalog.split_compat_book_token(raw_book_id)

    def acquisition_work_row(self, row_id: int):
        return self.read_model.row_by_id("works", int(row_id))

    def acquisition_work_image_row(self, work_row):
        return self.images.work_image_row(work_row)

    def acquisition_resolve_storage_image(self, image_row):
        return self.images.resolve_storage_image(image_row)

    def acquisition_resolve_image_target(self, image_row):
        return self.images.resolve_image_target(image_row)

    def acquisition_image_download_name(self, image_row) -> str:
        return self.images.image_download_name(image_row)

    def acquisition_image_content_type(self, image_row) -> str:
        return self.images.image_content_type(image_row)

    def acquisition_placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)

    def acquisition_related_rows_by_table(self, work_row) -> dict[str, list[object]]:
        return self._related_rows_by_table(work_row)

    def acquisition_work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        return self.catalog.work_file_rows(related_rows_by_table)

    def acquisition_download_name_for_file_row(self, file_row) -> str:
        return self._download_name_for_file_row(file_row)

    def acquisition_file_id(self, file_row) -> object:
        return _row_value(file_row, "file_id")

    def acquisition_serve_file_download(self, raw_file_id: str, environ) -> _Response:
        return self._serve_file_download(raw_file_id, environ)

    def opds_search_work_rows(self, query_text: str) -> list[object]:
        return [entry["row"] for entry in self._global_search_entries(query_text, table_filter="works") if str(entry.get("table")) == "works"]

    def opds_work_rows(self, *, sorted_by: str) -> list[object]:
        return self.catalog.work_rows(sorted_by=sorted_by)

    def opds_category_rows(self, category: str) -> list[dict[str, object]]:
        return self.catalog.category_rows(category)

    def opds_category_display_name(self, category: str) -> str:
        return self.catalog.category_display_name(category)

    def opds_rows_for_category_item(self, category: str, item_token: str) -> list[object]:
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
        related: dict[str, list[object]] = {}
        # OPDS entries only need a small subset of linked data. Avoid building the
        # full generic related-entity graph for each work row.
        for linked_table in ("expressions", "files", self.catalog.read_model.tag_category_table() or "tags", "series"):
            if not self._table_exists(linked_table):
                continue
            try:
                linked_rows = self.read_model.interlinked_rows(row, linked_table)
            except Exception:
                continue
            if linked_rows:
                related[linked_table] = linked_rows
        return related

    def opds_work_metadata_payload(self, row) -> dict[str, object]:
        return self.catalog.read_model.work_metadata_payload(
            row,
            related_rows_by_table=self._opds_related_rows_by_table(row),
        )

    def _serve_opds(self, path: str, query: dict[str, list[str]]) -> _Response:
        return self.opds_api.serve(path, query)

    def _serve_compat_get(self, what: str, raw_book_id: str, query: dict[str, list[str]], environ) -> _Response:
        return self.acquisition_api.serve_compat_get(what, raw_book_id, query, environ)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the LiuXin OPDS read-only interface.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=metadata_read_source_help_epilog("PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.opds_readonly"),
    )
    parser.add_argument("--database", required=True, help="Path to the LiuXin database.")
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
    with _open_database(database_path=str(args.database), db_type=str(args.db_type)) as db:
        app = OpdsReadOnlyApplication(db, config=config)
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
