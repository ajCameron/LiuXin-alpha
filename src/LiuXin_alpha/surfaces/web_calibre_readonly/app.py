"""Calibre-inspired read-only web interface built on top of web_readonly."""

from __future__ import annotations

import argparse
import json
import posixpath
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, quote, unquote
from wsgiref.simple_server import make_server

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.surfaces.acquisition.api import AcquisitionCompatApi
from LiuXin_alpha.surfaces.catalog.api import CalibreCatalogBackend, PLACEHOLDER_PNG
from LiuXin_alpha.surfaces.opds.api import (
    OpdsApi,
    decode_compat_token,
    encode_compat_token,
    normalized_category_key,
)
from LiuXin_alpha.surfaces.web_readonly.app import (
    ReadOnlyWebApplication,
    ReadOnlyWebConfig,
    _ResolvedFileTarget,
    _Response,
    _build_query_string,
    _coerce_int,
    _escape,
    _open_database,
    _row_value,
    _short_text,
    add_metadata_read_source_arguments,
    metadata_read_source_help_epilog,
    metadata_read_source_config_kwargs,
)

_RESET_CSS = """
html, body {
    margin: 0;
    padding: 0;
    border: 0;
    outline: 0;
    vertical-align: baseline;
    background: transparent;
}

div, span, object, iframe,
h1, h2, h3, h4, h5, h6, p, blockquote, pre,
abbr, address, cite, code,
del, dfn, em, img, ins, kbd, q, samp,
small, strong, sub, sup, var,
b, i,
dl, dt, dd, ol, ul, li,
fieldset, form, label, legend,
table, caption, tbody, tfoot, thead, tr, th, td,
article, aside, canvas, details, figcaption, figure,
footer, header, hgroup, menu, nav, section, summary,
time, mark, audio, video {
    margin: 0;
    padding: 0;
    border: 0;
    outline: 0;
    font-size: 100%;
    vertical-align: baseline;
    background: transparent;
}

body {
    line-height: 1.2;
}

a {
    margin: 0;
    padding: 0;
    font-size: 100%;
    vertical-align: baseline;
    background: transparent;
    text-decoration: none;
    color: currentColor;
}

a:visited {
    color: currentColor;
}

table {
    border-collapse: collapse;
    border-spacing: 0;
}

input, select {
    vertical-align: middle;
}
"""

_MOBILE_CSS = """
:root {
    --paper: #f6f2e8;
    --card: #ffffff;
    --line: #d7ccb2;
    --ink: #2a241c;
    --muted: #6d6456;
    --accent: #4a6a8a;
    --accent-soft: #dde7f1;
    --button: #e4ddd0;
    --button-border: #978b75;
}

body {
    font-family: Georgia, "Palatino Linotype", "Book Antiqua", Palatino, serif;
    color: var(--ink);
    background: linear-gradient(180deg, #efe7d7 0%, var(--paper) 100%);
}

.shell {
    max-width: 1040px;
    margin: 0 auto;
    padding: 1rem;
}

.site-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 1rem;
    margin-bottom: 1rem;
}

#logo {
    min-width: 0;
}

#logo h1 {
    font-size: 2rem;
    line-height: 1;
    letter-spacing: 0.02em;
    margin-bottom: 0.2rem;
}

#logo p {
    color: var(--muted);
}

.top-nav {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-top: 0.4rem;
}

.top-nav a {
    color: var(--accent);
}

#search_box {
    border: 1px solid var(--line);
    border-radius: 0.65rem;
    padding: 0.9rem;
    background: rgba(255, 255, 255, 0.82);
    min-width: min(22rem, 100%);
    box-shadow: 0 10px 24px rgba(61, 47, 29, 0.08);
}

#search_box form {
    display: grid;
    gap: 0.55rem;
}

#search_box input,
#search_box select {
    border: 1px solid var(--line);
    border-radius: 0.45rem;
    padding: 0.55rem 0.65rem;
    font: inherit;
    background: #fff;
}

.navigation {
    padding-bottom: 1rem;
    clear: both;
}

.navigation table.buttons {
    width: 100%;
}

.navigation .button {
    width: 50%;
    padding: 0.15rem;
}

.button a,
.button:visited a,
.button button {
    display: block;
    padding: 0.75rem;
    font-size: 1rem;
    border: 1px solid var(--button-border);
    color: var(--ink);
    text-decoration: none;
    background: linear-gradient(180deg, #f7f2e8 0%, var(--button) 100%);
    border-radius: 0.45rem;
    text-align: center;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.8);
}

.button:hover a,
.button button:hover {
    background: linear-gradient(180deg, #fff9ef 0%, #ebe3d6 100%);
}

.section-title {
    font-size: 1.2rem;
    margin: 0.1rem 0 0.35rem 0;
}

.section-meta,
.meta,
.second-line,
.result-snippet,
.detail-note {
    color: var(--muted);
}

.panel {
    background: rgba(255, 255, 255, 0.84);
    border: 1px solid var(--line);
    border-radius: 0.75rem;
    padding: 1rem;
    box-shadow: 0 10px 28px rgba(61, 47, 29, 0.08);
    margin-bottom: 1rem;
}

#listing {
    width: 100%;
    border-collapse: collapse;
}

#listing td {
    padding: 0.5rem 0.35rem;
    vertical-align: middle;
    border-top: 1px solid rgba(151, 139, 117, 0.22);
}

#listing tr:first-child td {
    border-top: none;
}

#listing td.thumbnail {
    height: 64px;
    width: 64px;
}

.thumb {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 52px;
    height: 52px;
    border-radius: 0.65rem;
    background: linear-gradient(180deg, #c9d9e8 0%, #94aec5 100%);
    color: #fff;
    font-size: 1.1rem;
    font-weight: 700;
    text-transform: uppercase;
    overflow: hidden;
}

.thumb img,
.thumb .thumb-fallback {
    width: 100%;
    height: 100%;
    object-fit: cover;
}

#listing tr:nth-child(even) {
    background: rgba(116, 95, 63, 0.06);
}

#listing .button {
    width: 1%;
    white-space: nowrap;
}

#listing .button a {
    display: inline-block;
    min-width: 4.4rem;
}

.data-container {
    display: inline-block;
    vertical-align: middle;
    min-width: 0;
}

.first-line {
    display: block;
    font-size: 1.08rem;
    font-weight: 700;
    color: var(--ink);
}

.second-line {
    margin-top: 0.35rem;
    display: block;
}

.action-row,
.actions,
.pager {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-top: 0.75rem;
}

.actions a,
.pager a,
.inline-pill,
.search-result-count,
.format-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    padding: 0.4rem 0.7rem;
    border-radius: 999px;
    background: var(--accent-soft);
    color: #26435f;
}

.book-title {
    font-size: 2rem;
    line-height: 1.1;
    margin-bottom: 0.45rem;
}

.book-subtitle {
    margin-top: 0.25rem;
}

.book-grid {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 1rem;
}

.book-hero {
    display: grid;
    grid-template-columns: 152px 1fr;
    gap: 1rem;
    align-items: start;
}

.book-cover-wrap {
    display: flex;
    justify-content: center;
}

.book-cover {
    width: 152px;
    max-width: 100%;
    border-radius: 0.75rem;
    border: 1px solid var(--line);
    background: rgba(255,255,255,0.7);
    box-shadow: 0 10px 28px rgba(61, 47, 29, 0.08);
}

.detail-table,
.meta-table {
    width: 100%;
}

.detail-table td,
.meta-table td,
.meta-table th {
    padding: 0.45rem 0.5rem;
    border-top: 1px solid rgba(151, 139, 117, 0.22);
    vertical-align: top;
}

.detail-table tr:first-child td,
.meta-table tr:first-child td,
.meta-table tr:first-child th {
    border-top: none;
}

.detail-table td:first-child,
.meta-table th {
    width: 11rem;
    font-weight: 700;
    color: var(--muted);
    text-align: left;
}

.pill-list {
    display: flex;
    flex-wrap: wrap;
    gap: 0.45rem;
    margin-top: 0.65rem;
}

.related-card-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 0.75rem;
    margin-top: 0.75rem;
}

.related-card {
    border: 1px solid var(--line);
    border-radius: 0.65rem;
    padding: 0.75rem;
    background: rgba(255,255,255,0.7);
}

.related-card strong {
    display: block;
    margin-bottom: 0.25rem;
}

.table-wrap {
    overflow-x: auto;
    overflow-y: hidden;
}

.search-result-card + .search-result-card {
    margin-top: 0.75rem;
}

.search-result-card {
    border: 1px solid var(--line);
    border-radius: 0.65rem;
    padding: 0.8rem;
    background: rgba(255,255,255,0.75);
}

mark {
    background: #f2d58b;
    color: inherit;
    padding: 0 0.12rem;
    border-radius: 0.15rem;
}

code {
    font-family: "Iosevka Fixed", "Cascadia Mono", "SFMono-Regular", Consolas, monospace;
    font-size: 0.92em;
    overflow-wrap: anywhere;
    word-break: break-word;
}

.empty {
    color: var(--muted);
}

.footer-note {
    margin-top: 1rem;
    color: var(--muted);
    font-size: 0.95rem;
}

@media (max-width: 900px) {
    .site-header {
        flex-direction: column;
    }

    #search_box {
        width: 100%;
        min-width: 0;
    }

    .book-grid {
        grid-template-columns: 1fr;
    }

    .book-hero {
        grid-template-columns: 1fr;
    }
}

@media (max-width: 720px) {
    .shell {
        padding: 0.75rem;
    }

    .navigation .button {
        display: block;
        width: 100%;
    }

    #listing td.thumbnail {
        width: 52px;
    }

    .thumb {
        width: 44px;
        height: 44px;
        border-radius: 0.55rem;
    }

    .book-title {
        font-size: 1.6rem;
    }
}
"""

@dataclass(frozen=True)
class CalibreReadOnlyWebConfig(ReadOnlyWebConfig):
    title: str = "LiuXin Calibre-Style Read-Only Web"
    opds_max_ungrouped_items: int = 100


class CalibreReadOnlyWebApplication(ReadOnlyWebApplication):
    """Calibre mobile/content-server inspired read-only web interface."""

    def __init__(
        self,
        db: Database,
        *,
        config: Optional[CalibreReadOnlyWebConfig] = None,
        read_source: Any = None,
    ) -> None:
        super().__init__(
            db,
            config=config or CalibreReadOnlyWebConfig(),
            read_source=read_source,
        )
        self.catalog = CalibreCatalogBackend(self, read_model=self.read_model, images=self.images)
        self.acquisition_api = AcquisitionCompatApi(self)
        self.opds_api = OpdsApi(self)

    def handle_request(self, environ) -> _Response:
        method = str(environ.get("REQUEST_METHOD", "GET") or "GET").upper()
        if method not in {"GET", "HEAD"}:
            return self._text_response("405 Method Not Allowed", "Method not allowed.\n", content_type="text/plain")

        path = posixpath.normpath(str(environ.get("PATH_INFO", "/") or "/"))
        if not path.startswith("/"):
            path = "/" + path
        query = parse_qs(str(environ.get("QUERY_STRING", "") or ""), keep_blank_values=False)

        if path == "/robots.txt":
            return self._text_response("200 OK", "User-agent: *\nAllow: /\n", content_type="text/plain")
        if path == "/ajax-setup":
            return self._json_response(self._ajax_setup_payload())
        if path.startswith("/static/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) >= 2:
                return self._serve_static_asset("/".join(parts[1:]))
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
        if path == "/opds" or path.startswith("/opds/"):
            return self._serve_opds(path, query)
        if path.startswith("/ajax/"):
            return self._serve_ajax(path, query)
        if path.startswith("/interface-data"):
            return self._serve_interface_data(path, query)
        if path == "/":
            return self._html_response(self._render_home_page())
        if path == "/mobile":
            if any(key in query for key in ("search", "num", "start", "sort", "order")):
                return self._html_response(self._render_mobile_catalog_page(query))
            return self._html_response(self._render_home_page())
        if path.startswith("/browse/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 3 and parts[1] == "book":
                return self._redirect_response("/book/{}".format(quote(parts[2], safe="")))
            if len(parts) == 2:
                return self._html_response(self._render_browse_page(parts[1], query))
        if path.startswith("/get/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) >= 3 and parts[0] == "get":
                return self._serve_compat_get(parts[1], parts[2], query, environ)
        if path.startswith("/legacy/get/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) >= 5 and parts[0] == "legacy" and parts[1] == "get":
                return self._serve_compat_get(parts[2], parts[3], query, environ)
        if path.startswith("/stanza"):
            return self._redirect_response("/opds")
        if path.startswith("/book/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 2:
                return self._html_response(self._render_book_page(parts[1]))
        if path.startswith("/author/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 3:
                return self._html_response(self._render_linked_works_page(parts[1], parts[2], kind="authors"))
        if path.startswith("/series/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 2:
                return self._html_response(self._render_linked_works_page("series", parts[1], kind="series"))
        if path.startswith("/tag/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 2:
                return self._html_response(self._render_linked_works_page(self._tag_category_table(), parts[1], kind="tags"))
        return super().handle_request(environ)

    def _render_layout(self, *, title: str, body_html: str) -> str:
        if self.config.expose_database_path:
            db_hint = "<p class='meta'>database: <code>{}</code></p>".format(
                _escape(self.db.metadata.get("database_path", ""))
            )
        else:
            db_hint = ""
        return """<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>{page_title}</title>
  <meta name='robots' content='noindex'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <style>
{reset_css}
{mobile_css}
  </style>
</head>
<body>
  <div class='shell'>
    <header class='site-header'>
      <div id='logo'>
        <h1>{site_title}</h1>
        <p>Calibre-shaped public browse surface over the LiuXin library.</p>
        <nav class='top-nav'>
          <a href='/'>Home</a>
          <a href='/browse/titles'>Titles</a>
          <a href='/browse/authors'>Authors</a>
          <a href='/browse/tags'>Tags</a>
          <a href='/browse/series'>Series</a>
          <a href='/browse/recent'>Recent</a>
          <a href='/search'>Search</a>
        </nav>
        {db_hint}
      </div>
      {search_box}
    </header>
    {body}
  </div>
</body>
</html>
""".format(
            page_title=_escape("{} | {}".format(title, self.config.title)),
            site_title=_escape(self.config.title),
            body=body_html,
            db_hint=db_hint,
            search_box=self._render_quick_search_box(),
            reset_css=_RESET_CSS,
            mobile_css=_MOBILE_CSS,
        )

    def _json_response(self, payload: object, *, status: str = "200 OK") -> _Response:
        return _Response(
            status=status,
            headers=[("Content-Type", "application/json; charset=utf-8")],
            body=[json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")],
        )

    def _xml_response(self, xml_text: str, *, status: str = "200 OK") -> _Response:
        return _Response(
            status=status,
            headers=[("Content-Type", "application/atom+xml; charset=utf-8")],
            body=[xml_text.encode("utf-8")],
        )

    def _render_quick_search_box(self) -> str:
        return """
<section id='search_box'>
  <form method='get' action='/search'>
    <label for='global_q'><strong>Search library</strong></label>
    <input id='global_q' name='global_q' type='text' placeholder='title, author, tag, series...'>
    <input type='hidden' name='global_limit' value='20'>
    <div class='button'><button type='submit'>Search</button></div>
  </form>
</section>
"""

    @staticmethod
    def _encode_compat_token(value: object) -> str:
        return encode_compat_token(value)

    @staticmethod
    def _decode_compat_token(raw: str) -> str:
        return decode_compat_token(raw)

    @classmethod
    def _normalized_category_key(cls, raw: object) -> str:
        return normalized_category_key(raw)

    def _category_icon_name(self, category: str) -> str:
        return self.catalog.category_icon_name(category)

    def _category_display_name(self, category: str) -> str:
        return self.catalog.category_display_name(category)

    def _author_tables(self) -> list[str]:
        return self.catalog.author_tables()

    def _browse_count(self, kind: str) -> int:
        return self.catalog.browse_count(kind)

    def _tag_category_table(self) -> str:
        return self.catalog.read_model.tag_category_table() or "tags"

    def _nav_buttons(self) -> str:
        buttons = [
            ("/browse/titles", "Titles", self._browse_count("titles")),
            ("/browse/authors", "Authors", self._browse_count("authors")),
            ("/browse/tags", "Tags", self._browse_count("tags")),
            ("/browse/series", "Series", self._browse_count("series")),
            ("/browse/recent", "Recent", self._browse_count("recent")),
            ("/search", "Search", 0),
        ]
        rows: list[str] = []
        for index in range(0, len(buttons), 2):
            cells = []
            for href, label, count in buttons[index : index + 2]:
                text = label if count <= 0 else "{} ({})".format(label, count)
                cells.append("<td class='button'><a href='{href}'>{text}</a></td>".format(href=_escape(href), text=_escape(text)))
            if len(cells) == 1:
                cells.append("<td class='button'></td>")
            rows.append("<tr>{}</tr>".format("".join(cells)))
        return "<div class='navigation'><table class='buttons'>{}</table></div>".format("".join(rows))

    def _render_home_page(self) -> str:
        recent_rows = self.catalog.work_rows(sorted_by="recent")[:8]
        recent_listing = self._render_work_listing(recent_rows, empty_message="No works available yet.")
        body = """
{nav}
<section class='panel'>
  <h2 class='section-title'>Library overview</h2>
  <p class='section-meta'>A Calibre-style browse surface over LiuXin's read-only database and file access layer.</p>
</section>
<section class='panel'>
  <div class='action-row'>
    <span class='search-result-count'>works {works}</span>
    <span class='search-result-count'>authors {authors}</span>
    <span class='search-result-count'>tags {tags}</span>
    <span class='search-result-count'>series {series}</span>
  </div>
</section>
<section class='panel'>
  <h2 class='section-title'>Recent titles</h2>
  <p class='section-meta'>Newest work rows by record id. Use the category pages for full browse.</p>
  {recent_listing}
  <div class='actions'><a href='/browse/recent'>See all recent titles</a></div>
</section>
<p class='footer-note'>Downloads and previews still use the same safe read-only delivery rules as the base web interface.</p>
""".format(
            nav=self._nav_buttons(),
            works=self._browse_count("titles"),
            authors=self._browse_count("authors"),
            tags=self._browse_count("tags"),
            series=self._browse_count("series"),
            recent_listing=recent_listing,
        )
        return self._render_layout(title="Home", body_html=body)

    def _row_href(self, table: str, row) -> Optional[str]:
        id_column = self._id_column(table)
        if not id_column:
            return None
        row_id = _row_value(row, id_column)
        if row_id in (None, ""):
            return None
        safe_id = quote(str(row_id), safe="")
        if table == "works":
            return "/book/{}".format(safe_id)
        if table in {"agents", "human_agents", "org_agents"}:
            return "/author/{}/{}".format(quote(table, safe=""), safe_id)
        if table == "series":
            return "/series/{}".format(safe_id)
        if table in {"labels", "tags"}:
            return "/tag/{}".format(safe_id)
        return super()._row_href(table, row)

    def _split_compat_book_token(self, raw_book_id: str) -> tuple[Optional[int], str]:
        return self.catalog.split_compat_book_token(raw_book_id)

    def _browse_entries(self, kind: str) -> list[dict[str, object]]:
        return [{"table": str(entry["table"]), "row": entry["row"]} for entry in self.catalog.category_rows(kind)]

    def _category_summary_payload(self) -> list[dict[str, object]]:
        return self.catalog.category_summary_payload()

    def _thumbnail_text(self, text: str) -> str:
        return self.catalog.thumbnail_text(text)

    def _work_subtitle(self, row) -> str:
        return self.catalog.work_subtitle(row)

    def _work_sort_value(self, row, *, sort_key: str) -> object:
        return self.catalog.work_sort_value(row, sort_key=sort_key)

    def _work_metadata_payload(self, row) -> dict[str, object]:
        return self.catalog.work_metadata_payload(row)

    def _work_rows_payload(self, rows: list[object]) -> list[dict[str, object]]:
        return self.catalog.work_rows_payload(rows)

    def _ajax_setup_payload(self) -> dict[str, object]:
        return self.catalog.ajax_setup_payload()

    def _category_route_target(self, category: str, item_id: object) -> str:
        return self.catalog.category_route_target(category, item_id)

    def _category_items_payload(
        self,
        category: str,
        *,
        num: int,
        offset: int,
        sort: str,
        sort_order: str,
    ) -> dict[str, object]:
        return self.catalog.category_items_payload(category, num=num, offset=offset, sort=sort, sort_order=sort_order)

    def _search_result_payload(
        self,
        *,
        query_text: str,
        rows: list[object],
        num: int,
        offset: int,
        sort: str,
        sort_order: str,
        base_url: str,
    ) -> dict[str, object]:
        return self.catalog.search_result_payload(
            query_text=query_text,
            rows=rows,
            num=num,
            offset=offset,
            sort=sort,
            sort_order=sort_order,
            base_url=base_url,
        )

    def _books_metadata_payload(self, rows: list[object]) -> dict[str, dict[str, object]]:
        return self.catalog.books_metadata_payload(rows)

    def _basic_interface_data_payload(self) -> dict[str, object]:
        return self.catalog.basic_interface_data_payload()

    def _tag_browser_payload(self) -> dict[str, object]:
        return self.catalog.tag_browser_payload()

    def _serve_static_asset(self, what: str) -> _Response:
        asset = str(what or "").strip().lower()
        if asset == "mobile.css":
            return self._text_response("200 OK", _MOBILE_CSS, content_type="text/css")
        if asset == "reset.css":
            return self._text_response("200 OK", _RESET_CSS, content_type="text/css")
        if asset == "empty.html":
            return self._text_response("200 OK", "<!DOCTYPE html><html><body></body></html>\n", content_type="text/html")
        if asset == "calibre.png":
            return self._bytes_response(PLACEHOLDER_PNG, download_name="calibre.png", disposition="inline", content_type_override="image/png")
        return self._text_response("404 Not Found", "Static asset not found.\n", content_type="text/plain")

    def _serve_icon(self, which: str, query: dict[str, list[str]]) -> _Response:
        del which, query
        return self._bytes_response(PLACEHOLDER_PNG, download_name="icon.png", disposition="inline", content_type_override="image/png")

    def _serve_ajax(self, path: str, query: dict[str, list[str]]) -> _Response:
        parts = [unquote(part) for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "ajax" and parts[1] == "library-info":
            return self._json_response({"library_map": {"main": {"title": self.config.title}}, "default_library": "main"})
        if len(parts) >= 2 and parts[0] == "ajax" and parts[1] == "categories":
            return self._json_response(self._category_summary_payload())
        if len(parts) >= 3 and parts[0] == "ajax" and parts[1] == "category":
            category = self._normalized_category_key(parts[2])
            num = _coerce_int((query.get("num") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
            offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
            sort = str((query.get("sort") or ["name"])[0] or "name")
            sort_order = str((query.get("sort_order") or ["asc"])[0] or "asc")
            if category in {"allbooks", "newest"}:
                rows = self.catalog.work_rows_for_category_item(category, "0")
                return self._json_response(
                    self._search_result_payload(
                        query_text="",
                        rows=rows,
                        num=num,
                        offset=offset,
                        sort="date" if category == "newest" else "title",
                        sort_order=sort_order,
                        base_url="/ajax/books_in/{}/{}/main".format(
                            self._encode_compat_token(category),
                            self._encode_compat_token("0"),
                        ),
                    )
                )
            return self._json_response(self._category_items_payload(category, num=num, offset=offset, sort=sort, sort_order=sort_order))
        if len(parts) >= 4 and parts[0] == "ajax" and parts[1] == "books_in":
            category = self._normalized_category_key(parts[2])
            item_token = self._decode_compat_token(parts[3])
            num = _coerce_int((query.get("num") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
            offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
            sort = str((query.get("sort") or ["title"])[0] or "title")
            sort_order = str((query.get("sort_order") or ["asc"])[0] or "asc")
            rows = self.catalog.work_rows_for_category_item(category, item_token)
            return self._json_response(
                self._search_result_payload(
                    query_text="",
                    rows=rows,
                    num=num,
                    offset=offset,
                    sort=sort,
                    sort_order=sort_order,
                    base_url="/ajax/books_in/{}/{}/main".format(
                        self._encode_compat_token(category),
                        self._encode_compat_token(str(item_token)),
                    ),
                )
            )
        if len(parts) >= 2 and parts[0] == "ajax" and parts[1] == "books":
            ids_raw = str((query.get("ids") or [""])[0] or "").strip()
            if ids_raw:
                rows = self.catalog.work_rows_for_ids(ids_raw)
            else:
                rows = self.catalog.work_rows(sorted_by="recent")
            return self._json_response(self._books_metadata_payload(rows))
        if len(parts) >= 3 and parts[0] == "ajax" and parts[1] == "book":
            work_id, _suffix = self._split_compat_book_token(parts[2])
            if work_id is None:
                return self._text_response("400 Bad Request", "Invalid book id.\n", content_type="text/plain")
            row = self.catalog.work_row_from_token(parts[2])
            if row is None:
                return self._text_response("404 Not Found", "Book row not found.\n", content_type="text/plain")
            return self._json_response(self._work_metadata_payload(row))
        if len(parts) >= 2 and parts[0] == "ajax" and parts[1] == "search":
            query_text = str((query.get("query") or [""])[0] or "").strip()
            rows = self.catalog.search_work_rows(query_text)
            num = _coerce_int((query.get("num") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
            offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
            sort = str((query.get("sort") or ["title"])[0] or "title")
            sort_order = str((query.get("sort_order") or ["asc"])[0] or "asc")
            return self._json_response(
                self._search_result_payload(
                    query_text=query_text,
                    rows=rows,
                    num=num,
                    offset=offset,
                    sort=sort,
                    sort_order=sort_order,
                    base_url="/ajax/search/main",
                )
            )
        return self._text_response("404 Not Found", "Unknown ajax route.\n", content_type="text/plain")

    def _serve_interface_data(self, path: str, query: dict[str, list[str]]) -> _Response:
        parts = [unquote(part) for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "interface-data" and parts[1] == "init":
            num = _coerce_int((query.get("num") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
            searchq = str((query.get("search") or [""])[0] or "").strip()
            sort = str((query.get("sort") or ["title"])[0] or "title")
            sort_order = str((query.get("sort_order") or ["asc"])[0] or "asc")
            rows = self.catalog.work_rows_for_query_or_recent(searchq)
            visible_search = self._search_result_payload(query_text=searchq, rows=rows, num=num, offset=0, sort=sort, sort_order=sort_order, base_url="/interface-data/get-books")
            metadata_rows = self.catalog.metadata_rows_for_search_result(rows, visible_search)
            payload = self._basic_interface_data_payload()
            payload.update(
                {
                    "search_result": visible_search,
                    "metadata": self._books_metadata_payload(metadata_rows),
                    "sortable_fields": [("title", "Title"), ("author", "Author"), ("series", "Series"), ("tags", "Tags")],
                    "field_metadata": {},
                    "virtual_libraries": {},
                    "bools_are_tristate": True,
                    "book_display_fields": ["title", "authors", "series", "tags"],
                    "fts_enabled": False,
                    "book_details_vertical_categories": (),
                    "fields_that_support_notes": (),
                    "categories_using_hierarchy": (),
                }
            )
            return self._json_response(payload)
        if len(parts) >= 2 and parts[0] == "interface-data" and parts[1] == "books-init":
            num = _coerce_int((query.get("num") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
            sort = str((query.get("sort") or ["title"])[0] or "title")
            sort_order = str((query.get("sort_order") or ["asc"])[0] or "asc")
            searchq = str((query.get("search") or [""])[0] or "").strip()
            rows = self.catalog.work_rows_for_query_or_recent(searchq)
            search_result = self._search_result_payload(query_text=searchq, rows=rows, num=num, offset=0, sort=sort, sort_order=sort_order, base_url="/interface-data/get-books")
            metadata_rows = self.catalog.metadata_rows_for_search_result(rows, search_result)
            return self._json_response({"library_id": "main", "search_result": search_result, "metadata": self._books_metadata_payload(metadata_rows)})
        if len(parts) >= 2 and parts[0] == "interface-data" and parts[1] == "get-books":
            ids_raw = str((query.get("ids") or [""])[0] or "").strip()
            searchq = str((query.get("search") or [""])[0] or "").strip()
            num = _coerce_int((query.get("num") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
            sort = str((query.get("sort") or ["title"])[0] or "title")
            sort_order = str((query.get("sort_order") or ["asc"])[0] or "asc")
            if ids_raw:
                rows = self.catalog.work_rows_for_ids(ids_raw)
            else:
                rows = self.catalog.work_rows_for_query_or_recent(searchq)
            search_result = self._search_result_payload(query_text=searchq, rows=rows, num=num, offset=0, sort=sort, sort_order=sort_order, base_url="/interface-data/get-books")
            metadata_rows = self.catalog.metadata_rows_for_search_result(rows, search_result)
            return self._json_response({"search_result": search_result, "metadata": self._books_metadata_payload(metadata_rows)})
        if len(parts) >= 3 and parts[0] == "interface-data" and parts[1] == "book-metadata":
            work_id, _suffix = self._split_compat_book_token(parts[2])
            if work_id is None:
                return self._text_response("400 Bad Request", "Invalid book id.\n", content_type="text/plain")
            row = self.catalog.work_row_from_token(parts[2])
            if row is None:
                return self._text_response("404 Not Found", "Book row not found.\n", content_type="text/plain")
            return self._json_response(self._work_metadata_payload(row))
        if len(parts) >= 2 and parts[0] == "interface-data" and parts[1] == "tag-browser":
            return self._json_response(self._tag_browser_payload())
        if len(parts) >= 2 and parts[0] == "interface-data" and parts[1] == "update":
            payload = self._basic_interface_data_payload()
            payload["translations_hash"] = parts[2] if len(parts) >= 3 else ""
            return self._json_response(payload)
        return self._text_response("404 Not Found", "Unknown interface-data route.\n", content_type="text/plain")

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
        return self._split_compat_book_token(raw_book_id)

    def acquisition_work_row(self, row_id: int):
        return self.read_model.row_by_id("works", int(row_id))

    def acquisition_work_image_row(self, work_row):
        return self.images.work_image_row(work_row)

    def acquisition_resolve_storage_image(self, image_row):
        return self.images.resolve_storage_image(image_row)

    def acquisition_resolve_image_target(self, image_row) -> Optional[_ResolvedFileTarget]:
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
        return self._work_file_rows(related_rows_by_table)

    def acquisition_download_name_for_file_row(self, file_row) -> str:
        return self._download_name_for_file_row(file_row)

    def acquisition_file_id(self, file_row) -> object:
        return _row_value(file_row, "file_id")

    def acquisition_serve_file_download(self, raw_file_id: str, environ) -> _Response:
        return self._serve_file_download(raw_file_id, environ)

    def opds_search_work_rows(self, query_text: str) -> list[object]:
        return self.catalog.search_work_rows(query_text)

    def opds_work_rows(self, *, sorted_by: str) -> list[object]:
        return self.catalog.work_rows(sorted_by=sorted_by)

    def opds_category_rows(self, category: str) -> list[dict[str, object]]:
        return self.catalog.category_rows(category)

    def opds_category_display_name(self, category: str) -> str:
        return self._category_display_name(category)

    def opds_rows_for_category_item(self, category: str, item_token: str) -> list[object]:
        return self.catalog.work_rows_for_category_item(category, item_token)

    def _opds_related_rows_by_table(self, row) -> dict[str, list[object]]:
        related: dict[str, list[object]] = {}
        for linked_table in ("expressions", "files", self._tag_category_table(), "series"):
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

    def _entity_subtitle(self, table: str, row) -> str:
        related = self._related_rows_by_table(row)
        works = related.get("works", [])
        parts: list[str] = []
        if works:
            parts.append("{} linked titles".format(len(works)))
        if table in {"agents", "human_agents", "org_agents"}:
            agent_type = _short_text(_row_value(row, "agent_type"), width=48).strip()
            if agent_type:
                parts.append(agent_type)
        return " · ".join(parts)

    def _render_listing_rows(self, entries: list[dict[str, object]]) -> str:
        rows_html: list[str] = []
        for entry in entries:
            table = str(entry["table"])
            row = entry["row"]
            href = self._row_href(table, row) or "#"
            primary = self._row_primary_text(table, row)
            if table == "works":
                thumb = self._work_thumbnail_html(row)
            else:
                thumb = "<span class='thumb-fallback'>{}</span>".format(_escape(self._thumbnail_text(primary)))
            if table == "works":
                subtitle = self._work_subtitle(row)
                action_label = "Open"
            else:
                subtitle = self._entity_subtitle(table, row)
                action_label = "View"
            rows_html.append(
                """
<tr>
  <td class='thumbnail'><a class='thumb' href='{href}'>{thumb}</a></td>
  <td>
    <div class='data-container'>
      <a class='first-line' href='{href}'>{primary}</a>
      {subtitle}
    </div>
  </td>
  <td class='button'><a href='{href}'>{action_label}</a></td>
</tr>
""".format(
                    href=_escape(href),
                    thumb=thumb,
                    primary=_escape(primary),
                    subtitle=("<span class='second-line'>{}</span>".format(_escape(subtitle)) if subtitle else ""),
                    action_label=_escape(action_label),
                )
            )
        return "".join(rows_html)

    def _render_work_listing(self, rows: list[object], *, empty_message: str) -> str:
        entries = [{"table": "works", "row": row} for row in rows]
        return self._render_browse_listing(entries, empty_message=empty_message)

    def _render_browse_listing(self, entries: list[dict[str, object]], *, empty_message: str) -> str:
        if not entries:
            return "<p class='meta'>{}</p>".format(_escape(empty_message))
        return "<table id='listing'><tbody>{}</tbody></table>".format(self._render_listing_rows(entries))

    def _render_browse_page(self, kind: str, query: dict[str, list[str]]) -> str:
        titles = {
            "titles": "Titles",
            "authors": "Authors",
            "tags": "Tags",
            "series": "Series",
            "recent": "Recent",
        }
        if kind not in titles:
            return self._render_layout(
                title="Missing browse category",
                body_html="<section class='panel'><h2 class='section-title'>Unknown category</h2></section>",
            )
        limit = _coerce_int((query.get("limit") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
        offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
        all_entries = self._browse_entries(kind)
        visible = all_entries[offset : offset + limit]
        pager = self._render_pager(
            path="/browse/{}".format(quote(kind, safe="")),
            query_values={"limit": limit},
            offset=offset,
            limit=limit,
            total=len(all_entries),
            offset_key="offset",
        )
        listing = self._render_browse_listing(visible, empty_message="No entries found in this category.")
        body = """
{nav}
<section class='panel'>
  <h2 class='section-title'>{title}</h2>
  <p class='section-meta'>count={count}</p>
  {pager}
  {listing}
</section>
""".format(nav=self._nav_buttons(), title=_escape(titles[kind]), count=len(all_entries), pager=pager, listing=listing)
        return self._render_layout(title=titles[kind], body_html=body)

    def _render_mobile_catalog_page(self, query: dict[str, list[str]]) -> str:
        search = str((query.get("search") or [""])[0] or "").strip()
        num = _coerce_int((query.get("num") or [None])[0], default=min(self.config.default_page_size, 25), minimum=1, maximum=self.config.max_page_size)
        start = _coerce_int((query.get("start") or [None])[0], default=1, minimum=1)
        sort_key = str((query.get("sort") or ["date"])[0] or "date").strip().lower()
        order = str((query.get("order") or ["descending"])[0] or "descending").strip().lower()
        ascending = order == "ascending"

        if search:
            rows = self.catalog.search_work_rows(search)
        else:
            rows = self.catalog.work_rows(sorted_by="recent")

        rows = sorted(rows, key=lambda row: self._work_sort_value(row, sort_key=sort_key), reverse=not ascending)
        total = len(rows)
        offset = max(0, start - 1)
        visible = rows[offset : offset + num]

        nav = self._render_mobile_navigation(
            search=search,
            sort_key=sort_key,
            order=order,
            num=num,
            start=start,
            total=total,
        )
        form = self._render_mobile_search_form(search=search, sort_key=sort_key, order=order, num=num)
        listing = self._render_work_listing(visible, empty_message="No books matched this mobile search.")
        body = """
<section class='panel'>
  {form}
</section>
{nav}
<section class='panel'>
  {listing}
</section>
{nav}
""".format(form=form, nav=nav, listing=listing)
        return self._render_layout(title="Mobile catalog", body_html=body)

    def _render_mobile_search_form(self, *, search: str, sort_key: str, order: str, num: int) -> str:
        options_num = []
        for option in (5, 10, 25, 100):
            selected = " selected" if int(option) == int(num) else ""
            options_num.append("<option value='{value}'{selected}>{value}</option>".format(value=option, selected=selected))
        options_sort = []
        for option in ("date", "author", "title", "rating", "size", "tags", "series"):
            selected = " selected" if option == sort_key else ""
            options_sort.append("<option value='{value}'{selected}>{label}</option>".format(value=_escape(option), selected=selected, label=_escape(option)))
        options_order = []
        for option in ("ascending", "descending"):
            selected = " selected" if option == order else ""
            options_order.append("<option value='{value}'{selected}>{label}</option>".format(value=_escape(option), selected=selected, label=_escape(option)))
        return """
<form method='get' action='/mobile'>
  <label for='mobile_num'>Show</label>
  <select id='mobile_num' name='num'>{options_num}</select>
  books matching
  <input id='mobile_search' name='search' type='text' value='{search}'>
  sorted by
  <select name='sort'>{options_sort}</select>
  <select name='order'>{options_order}</select>
  <div class='actions'><button type='submit'>Search</button></div>
</form>
""".format(
            options_num="".join(options_num),
            search=_escape(search),
            options_sort="".join(options_sort),
            options_order="".join(options_order),
        )

    def _render_mobile_navigation(self, *, search: str, sort_key: str, order: str, num: int, start: int, total: int) -> str:
        if total <= 0:
            return "<div class='navigation'><span class='meta'>Books 0 to 0 of 0</span></div>"
        end = min(start + num - 1, total)
        base = {"search": search, "sort": sort_key, "order": order, "num": num}
        left_links: list[str] = []
        right_links: list[str] = []
        if start > 1:
            first_q = dict(base)
            first_q["start"] = 1
            prev_q = dict(base)
            prev_q["start"] = max(1, start - num)
            left_links.append("<a href='/mobile?{}'>First</a>".format(_escape(_build_query_string(first_q))))
            left_links.append("<a href='/mobile?{}'>Previous</a>".format(_escape(_build_query_string(prev_q))))
        if end < total:
            next_q = dict(base)
            next_q["start"] = start + num
            last_q = dict(base)
            last_q["start"] = max(1, total - num + 1)
            right_links.append("<a href='/mobile?{}'>Next</a>".format(_escape(_build_query_string(next_q))))
            right_links.append("<a href='/mobile?{}'>Last</a>".format(_escape(_build_query_string(last_q))))
        return """
<div class='navigation'>
  <span class='meta' style='display:block; text-align:center;'>Books {start} to {end} of {total}</span>
  <table class='buttons'><tr>
    <td class='button' style='text-align:left'>{left}</td>
    <td class='button' style='text-align:right'>{right}</td>
  </tr></table>
</div>
""".format(
            start=start,
            end=end,
            total=total,
            left=" ".join(left_links),
            right=" ".join(right_links),
        )

    def _book_format_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[str]:
        rows_html: list[str] = []
        file_rows = sorted(
            self._work_file_rows(related_rows_by_table),
            key=lambda row: self._download_name_for_file_row(row).lower(),
        )
        for row in file_rows:
            file_id = _row_value(row, "file_id")
            if file_id in (None, ""):
                continue
            capabilities = self._file_capabilities(row)
            download_name = self._download_name_for_file_row(row)
            suffix = Path(download_name).suffix.lower().lstrip(".") or "file"
            actions: list[str] = []
            if capabilities.get("downloadable"):
                actions.append("<a href='/files/{}/download'>Download</a>".format(_escape(file_id)))
            if capabilities.get("preview_kind"):
                actions.append("<a href='/files/{}/preview'>Preview</a>".format(_escape(file_id)))
            location_bits = []
            store_name = _short_text(_row_value(row, "file_store_name"), width=48).strip()
            if store_name:
                location_bits.append(store_name)
            source = _short_text(_row_value(row, "file_source"), width=64).strip()
            if source:
                location_bits.append(source)
            rows_html.append(
                "<tr><th>{fmt}</th><td><div class='first-line'>{name}</div>{meta}<div class='actions'>{actions}</div></td></tr>".format(
                    fmt=_escape(suffix.upper()),
                    name=_escape(download_name),
                    meta=("<div class='second-line'>{}</div>".format(_escape(" · ".join(location_bits))) if location_bits else ""),
                    actions=" ".join(actions) if actions else "<span class='empty'>unavailable</span>",
                )
            )
        return rows_html

    def _work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        return self.catalog.work_file_rows(related_rows_by_table)

    def _work_image_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        return self.images.work_image_rows(related_rows_by_table)

    def _image_download_name(self, image_row) -> str:
        return self.images.image_download_name(image_row)

    def _image_content_type(self, image_row) -> str:
        return self.images.image_content_type(image_row)

    def _image_storage_lookup_metadata(self, image_row) -> dict[str, object]:
        return self.images.image_storage_lookup_metadata(image_row)

    def _resolve_storage_image(self, image_row):
        return self.images.resolve_storage_image(image_row)

    def _resolve_image_target(self, image_row):
        return self.images.resolve_image_target(image_row)

    def _work_image_row(self, work_row) -> Optional[object]:
        return self.images.work_image_row(work_row)

    def _placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)

    def _work_thumbnail_html(self, work_row) -> str:
        work_id = _row_value(work_row, "work_id")
        if work_id in (None, ""):
            return "<span class='thumb-fallback'>{}</span>".format(_escape(self._thumbnail_text(self._row_primary_text("works", work_row))))
        return "<img src='/get/thumb/{}/main?sz=60x80' alt='cover'>".format(_escape(work_id))

    def _serve_compat_get(self, what: str, raw_book_id: str, query: dict[str, list[str]], environ) -> _Response:
        return self.acquisition_api.serve_compat_get(what, raw_book_id, query, environ)

    def _render_book_page(self, raw_row_id: str) -> str:
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception:
            return self._render_layout(
                title="Bad book id",
                body_html="<section class='panel'><h2 class='section-title'>Invalid book id</h2></section>",
            )
        row = self.read_model.row_by_id("works", row_id)
        if row is None:
            return self._render_layout(
                title="Missing book",
                body_html="<section class='panel'><h2 class='section-title'>Book not found</h2></section>",
            )

        row_data = self._row_dict("works", row)
        related_rows_by_table = self._related_rows_by_table(row)
        title = self._stringify_detail_value(
            row_data.get("work_title") or row_data.get("work_canonical_title") or row_data.get("work_sort_title") or row_id
        )
        credit_entries = self._work_credit_entries(row)
        byline = ", ".join(self._row_primary_text(str(entry["table"]), entry["row"]) for entry in credit_entries[:4])
        series_rows = related_rows_by_table.get("series", [])
        tag_table, label_rows = self.catalog.read_model.work_tag_rows(related_rows_by_table)
        note_rows = related_rows_by_table.get("synopses") or related_rows_by_table.get("comments") or related_rows_by_table.get("notes") or []

        series_pills = []
        for series_row in series_rows:
            href = self._row_href("series", series_row)
            if href:
                series_pills.append("<a class='inline-pill' href='{href}'>{label}</a>".format(
                    href=_escape(href),
                    label=_escape(self._row_primary_text("series", series_row)),
                ))
        tag_pills = []
        for label_row in label_rows[:12]:
            href = self._row_href(tag_table or "tags", label_row)
            if href:
                tag_pills.append("<a class='inline-pill' href='{href}'>{label}</a>".format(
                    href=_escape(href),
                    label=_escape(self._row_primary_text(tag_table or "tags", label_row)),
                ))

        note_html = ""
        if note_rows:
            note_html = """
<section class='panel'>
  <h3 class='section-title'>Notes</h3>
  <p class='detail-note'>{note}</p>
</section>
""".format(note=_escape(_short_text(self._row_primary_text(str(getattr(note_rows[0], 'table', 'notes')), note_rows[0]), width=600)))

        format_rows = self._book_format_rows(related_rows_by_table)
        metadata_rows = self._render_detail_table_rows(
            row_data,
            [
                column
                for column in self._visible_columns("works")
                if column in {"work_id", "work_canonical_title", "work_sort_title", "work_status", "work_type", "work_source"}
            ],
            code_values=False,
            include_empty=False,
        )

        body = """
<section class='panel'>
  <div class='book-hero'>
    <div class='book-cover-wrap'><img class='book-cover' src='/get/cover/{row_id}/main' alt='cover'></div>
    <div>
      <h2 class='book-title'>{title}</h2>
      {byline}
      <div class='actions'>
        <a href='/browse/titles'>Back to titles</a>
        <a href='/search?global_q={title_query}'>Search similar</a>
      </div>
      {series_pills}
      {tag_pills}
    </div>
  </div>
</section>
{note_html}
<section class='book-grid'>
  <section class='panel'>
    <h3 class='section-title'>Available formats</h3>
    {formats}
  </section>
  <section class='panel'>
    <h3 class='section-title'>Record</h3>
    <table class='detail-table'><tbody>{metadata_rows}</tbody></table>
  </section>
</section>
{credits}
{related}
""".format(
            row_id=_escape(row_id),
            title=_escape(title),
            title_query=quote(title, safe=""),
            byline=("<p class='book-subtitle'>by {}</p>".format(_escape(byline)) if byline else ""),
            series_pills=("<div class='pill-list'>{}</div>".format("".join(series_pills)) if series_pills else ""),
            tag_pills=("<div class='pill-list'>{}</div>".format("".join(tag_pills)) if tag_pills else ""),
            note_html=note_html,
            formats=(
                "<table class='meta-table'><tbody>{}</tbody></table>".format("".join(format_rows))
                if format_rows
                else "<p class='meta'>No directly linked files yet.</p>"
            ),
            metadata_rows=metadata_rows or "<tr><td>work_id</td><td>{}</td></tr>".format(_escape(row_id)),
            credits=self._render_work_credits_section(row),
            related=self._render_related_sections(
                row,
                related_rows_by_table=related_rows_by_table,
                exclude_tables={"agents", "human_agents", "org_agents", "tags", "labels", "series", "files", "notes", "comments", "synopses"},
            ),
        )
        return self._render_layout(title=title, body_html=body)

    def _render_row_page(self, table: str, raw_row_id: str) -> str:
        if table == "works":
            return self._render_book_page(raw_row_id)
        return super()._render_row_page(table, raw_row_id)

    def _render_linked_works_page(self, table: str, raw_row_id: str, *, kind: str) -> str:
        if not self._table_exists(table):
            return self._render_layout(
                title="Missing category row",
                body_html="<section class='panel'><h2 class='section-title'>Unknown record</h2></section>",
            )
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception:
            return self._render_layout(
                title="Bad row id",
                body_html="<section class='panel'><h2 class='section-title'>Invalid row id</h2></section>",
            )
        row = self.read_model.row_by_id(table, row_id)
        if row is None:
            return self._render_layout(
                title="Missing row",
                body_html="<section class='panel'><h2 class='section-title'>Row not found</h2></section>",
            )
        related = self._related_rows_by_table(row)
        works = related.get("works", [])
        titles = {
            "authors": "Author",
            "series": "Series",
            "tags": "Tag",
        }
        body = """
<section class='panel'>
  <h2 class='section-title'>{title_kind}: {label}</h2>
  <p class='section-meta'>linked titles={count}</p>
  <div class='actions'><a href='/browse/{kind}'>Back to {kind}</a></div>
</section>
<section class='panel'>
  {listing}
</section>
""".format(
            title_kind=_escape(titles.get(kind, "Entry")),
            label=_escape(self._row_primary_text(table, row)),
            count=len(works),
            kind=_escape(kind),
            listing=self._render_work_listing(works, empty_message="No linked works found."),
        )
        return self._render_layout(title=self._row_primary_text(table, row), body_html=body)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the LiuXin Calibre-style read-only web interface.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=metadata_read_source_help_epilog("PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_calibre_readonly"),
    )
    parser.add_argument("--database", required=True, help="Path to the LiuXin database.")
    parser.add_argument("--db-type", default="sqlite", help="Database driver type. Default: sqlite")
    add_metadata_read_source_arguments(parser)
    parser.add_argument("--host", default=CalibreReadOnlyWebConfig.host, help="Bind host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=CalibreReadOnlyWebConfig.port, help="Bind port. Default: 8080")
    parser.add_argument("--page-size", type=int, default=CalibreReadOnlyWebConfig.default_page_size, help="Default page size.")
    parser.add_argument("--max-page-size", type=int, default=CalibreReadOnlyWebConfig.max_page_size, help="Maximum page size.")
    parser.add_argument("--opds-max-ungrouped-items", type=int, default=CalibreReadOnlyWebConfig.opds_max_ungrouped_items, help="Maximum OPDS category size before category-group feeds are used.")
    parser.add_argument("--title", default=CalibreReadOnlyWebConfig.title, help="Site title.")
    parser.add_argument("--expose-database-path", action="store_true", help="Show the backing database path in the UI.")
    parser.add_argument("--no-file-downloads", action="store_true", help="Disable file download/redirect links.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = CalibreReadOnlyWebConfig(
        title=str(args.title),
        host=str(args.host),
        port=int(args.port),
        default_page_size=max(1, int(args.page_size)),
        max_page_size=max(1, int(args.max_page_size)),
        opds_max_ungrouped_items=max(0, int(args.opds_max_ungrouped_items)),
        expose_database_path=bool(args.expose_database_path),
        enable_file_downloads=not bool(args.no_file_downloads),
        **metadata_read_source_config_kwargs(args),
    )
    with _open_database(database_path=str(args.database), db_type=str(args.db_type)) as db:
        app = CalibreReadOnlyWebApplication(
            db,
            config=config,
        )
        url = "http://{}:{}/".format(config.host, config.port)
        sys.stdout.write("Serving Calibre-style read-only web UI on {}\n".format(url))
        sys.stdout.flush()
        with make_server(config.host, config.port, app) as server:
            server.serve_forever()
    return 0


__all__ = [
    "CalibreReadOnlyWebApplication",
    "CalibreReadOnlyWebConfig",
    "build_arg_parser",
    "main",
]
