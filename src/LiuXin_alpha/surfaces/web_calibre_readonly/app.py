"""
Compose Calibre-style HTML, JSON, OPDS, and acquisition routes over shared Core reads.

The application extends the generic read-only web host, retaining its search,
table, and file routes as a fallback. Catalogue, image, and protocol owners supply
data while this module builds compatibility responses and HTML with embedded CSS.
It is a partial compatibility surface, not an upstream Calibre implementation.

Read/query failures remain visible. Human-facing missing-record pages are HTML
render results and can be returned with status 200; JSON endpoints have explicit
400/404 cases. HEAD follows GET without removing response bodies. The shared
compatibility acquisition adapter's direct Core path does not enforce the host's
enable_file_downloads flag; UI text and configuration must not be mistaken for
an authorization boundary. main owns server/Core lifetimes, not construction.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import sys

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, quote, unquote
from wsgiref.simple_server import make_server

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.acquisition.api import AcquisitionCompatApi
from LiuXin_alpha.surfaces.catalog.api import CalibreCatalogBackend, PLACEHOLDER_PNG
from LiuXin_alpha.surfaces.core import (
    CoreSurfaceModel,
    add_core_client_arguments,
    open_surface_core_from_args,
)
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
    """
    Carry immutable shared web settings with a Calibre-style title and OPDS threshold.

    Direct construction performs no validation. OpdsApi groups categories only
    when a positive threshold is exceeded; zero disables grouping. HTTP parsing
    and main apply their own bounds rather than changing these stored values.

    Example:
        >>> config = CalibreReadOnlyWebConfig(opds_max_ungrouped_items=0)
        >>> (config.default_page_size, config.opds_max_ungrouped_items)
        (50, 0)


    :ivar title: Site/feed title, also used in the single compatibility library's metadata.
    :ivar opds_max_ungrouped_items: Category-item threshold interpreted by the OPDS adapter.
    """

    title: str = "LiuXin Calibre-Style Read-Only Web"
    opds_max_ungrouped_items: int = 100


class CalibreReadOnlyWebApplication(ReadOnlyWebApplication):
    """
    Serve a small Calibre-shaped UI and compatibility APIs using shared read owners.

    Catalogue composition shares the base host's read model and image backend.
    The inherited WSGI callable emits responses and adds X-Robots-Tag. Inherited
    close releases a database-compatibility Core session when one was created,
    not an externally supplied Core client. No listener is opened by construction.

    Example:
        >>> issubclass(CalibreReadOnlyWebApplication, ReadOnlyWebApplication)
        True
        >>> app = CalibreReadOnlyWebApplication(core_client)  # doctest: +SKIP


    :ivar catalog: Compatibility catalogue projection borrowing the shared read/image owners.
    :ivar acquisition_api: Core-backed cover, thumbnail, and format delivery adapter.
    :ivar opds_api: Shared feed builder and protocol router bound to this host.
    """

    def __init__(
        self,
        core: CoreClientAPI,
        *,
        config: Optional[CalibreReadOnlyWebConfig] = None,
        model: CoreSurfaceModel | None = None,
    ) -> None:
        """
        Initialize the generic web host, then bind catalogue and protocol adapters.

        Legacy database inputs use the base host's Core compatibility bridge.
        An explicit model is forwarded without verifying it belongs to core;
        callers must keep those collaborators consistent. Initialization failures
        propagate without wrapper-level rollback or server startup.

        Example:
            >>> app = CalibreReadOnlyWebApplication(core_client, config=CalibreReadOnlyWebConfig(title="Shelf"))  # doctest: +SKIP


        :param core: Borrowed Core client, or a database accepted by the base compatibility bridge.
        :param config: Surface settings; None selects Calibre-specific defaults.
        :param model: Optional shared Core model, otherwise constructed by the base host.
        :return: None after the three adapters reference the initialized application.
        """
        super().__init__(
            core,
            config=config or CalibreReadOnlyWebConfig(),
            model=model,
        )
        self.catalog = CalibreCatalogBackend(self, read_model=self.read_model, images=self.images)
        self.acquisition_api = AcquisitionCompatApi(self)
        self.opds_api = OpdsApi(self)

    def handle_request(self, environ) -> _Response:
        """
        Dispatch GET/HEAD compatibility routes before falling back to the generic web host.

        Normalize dot segments before percent-decoding route components and drop
        blank query values. Bare /mobile renders the home page unless a recognized
        query key remains. Any /stanza-prefixed path redirects to /opds. Icon/get
        routes accept extra components; legacy/get needs at least five. Unhandled
        paths fall back using the original environ, while /interface-data-prefixed
        paths stay with that handler even if its route is unknown.

        HEAD retains the GET body. Unsupported methods return 405. HTML missing
        book/category pages are wrapped as 200 responses; protocol handlers choose
        their own error statuses. Backend/rendering errors are not caught here.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app.handle_request({"PATH_INFO": "/stanza-old"}).status
            '302 Found'
            >>> app.handle_request({"REQUEST_METHOD": "POST"}).status
            '405 Method Not Allowed'


        :param environ: WSGI mapping; absent/falsey method, path, and query default to GET, /, and empty text.
        :return: Unstarted response whose headers/body are later emitted by the WSGI callable.
        """
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
        """
        Wrap trusted body markup in the shared Calibre-style shell and inline CSS.

        Escape page/site titles and the optional database path, not body_html.
        When path exposure is enabled, query database.info on every render; a
        nonmapping receipt/metadata yields an empty hint, while query errors escape.
        Always include fixed navigation, search form, and noindex metadata.

        Example:
            >>> from types import SimpleNamespace
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app.config = SimpleNamespace(title="A & B", expose_database_path=False)
            >>> page = app._render_layout(title="<Shelf>", body_html="<main>Books</main>")
            >>> "&lt;Shelf&gt; | A &amp; B" in page and "<main>Books</main>" in page
            True


        :param title: Page-specific title combined with the configured site title.
        :param body_html: Internally rendered HTML fragment inserted verbatim.
        :return: Complete HTML document as text, without choosing an HTTP status.
        """
        if self.config.expose_database_path:
            info = self.core.query("database.info")
            metadata = (
                info.get("metadata", {})
                if isinstance(info, Mapping)
                else {}
            )
            db_hint = "<p class='meta'>database: <code>{}</code></p>".format(
                _escape(
                    metadata.get("database_path", "")
                    if isinstance(metadata, Mapping)
                    else ""
                )
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
        """
        Serialize JSON with sorted keys and literal Unicode into one UTF-8 response chunk.

        Retain json.dumps defaults such as allowing nonfinite floats; no schema
        validation or custom object encoder is supplied. Serialization errors escape.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> b"".join(app._json_response({"z": 1, "a": "雪"}).body).decode("utf-8")
            '{"a": "雪", "z": 1}'


        :param payload: JSON-serializable value; unsupported objects are not coerced.
        :param status: WSGI status text retained unchanged.
        :return: JSON response with a UTF-8 Content-Type header and no explicit length header.
        """
        return _Response(
            status=status,
            headers=[("Content-Type", "application/json; charset=utf-8")],
            body=[json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")],
        )

    def _xml_response(self, xml_text: str, *, status: str = "200 OK") -> _Response:
        """
        Encode trusted XML text as an Atom response without parsing or escaping it.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app._xml_response("<feed/>").body
            [b'<feed/>']


        :param xml_text: Complete XML text to encode as UTF-8.
        :param status: WSGI status string forwarded unchanged.
        :return: Single-chunk response with application/atom+xml and a UTF-8 charset.
        """
        return _Response(
            status=status,
            headers=[("Content-Type", "application/atom+xml; charset=utf-8")],
            body=[xml_text.encode("utf-8")],
        )

    def _render_quick_search_box(self) -> str:
        """
        Return the fixed global-search form with a hidden twenty-result limit.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> "name='global_limit' value='20'" in app._render_quick_search_box()
            True


        :return: Trusted HTML fragment targeting /search, without reading application state.
        """
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
        """
        Delegate falsey-to-empty, string-to-UTF-8-hex compatibility token encoding.

        Example:
            >>> CalibreReadOnlyWebApplication._encode_compat_token("A")
            '41'


        :param value: Value stringified by the shared encoder; falsey values become empty text.
        :return: Lowercase hexadecimal token, without whitespace stripping.
        """
        return encode_compat_token(value)

    @staticmethod
    def _decode_compat_token(raw: str) -> str:
        """
        Delegate permissive hex decoding with stripped original-text fallback.

        Plain text that happens to be valid UTF-8 hex is also decoded. Hyphens
        are removed only for the hex candidate, not from fallback text.

        Example:
            >>> CalibreReadOnlyWebApplication._decode_compat_token(" 41 ")
            'A'


        :param raw: Plain or encoded token passed unchanged to the shared decoder.
        :return: Decoded UTF-8 text, or stripped original text when decoding is unsuitable.
        """
        return decode_compat_token(raw)

    @classmethod
    def _normalized_category_key(cls, raw: object) -> str:
        """
        Delegate token decoding, stripping, lowercasing, and singular category aliases.

        Unknown names remain usable strings; this is not category validation.

        Example:
            >>> CalibreReadOnlyWebApplication._normalized_category_key("author")
            'authors'


        :param raw: Plain/encoded selector interpreted by normalized_category_key.
        :return: Normalized key, including unknown keys unchanged beyond normalization.
        """
        return normalized_category_key(raw)

    def _category_icon_name(self, category: str) -> str:
        """
        Delegate fixed icon-name selection without category alias or token decoding.

        Example:
            >>> icon = app._category_icon_name("authors")  # doctest: +SKIP


        :param category: Selector stripped/lowercased by the catalogue icon helper.
        :return: Conventional PNG filename, with blank.png for unknown categories.
        """
        return self.catalog.category_icon_name(category)

    def _category_display_name(self, category: str) -> str:
        """
        Delegate fixed English category labels and title-cased unknown-name fallback.

        Example:
            >>> label = app._category_display_name("authors")  # doctest: +SKIP


        :param category: Plain category value forwarded without extra decoding here.
        :return: Catalogue display label, not a localized or schema-validated name.
        """
        return self.catalog.category_display_name(category)

    def _author_tables(self) -> list[str]:
        """
        Borrow the catalogue's preferred agent-table list and its agents fallback.

        Example:
            >>> tables = app._author_tables()  # doctest: +SKIP


        :return: Preferred author-source table names, with backend read failures preserved.
        """
        return self.catalog.author_tables()

    def _browse_count(self, kind: str) -> int:
        """
        Delegate an exact browse-count selector without normalizing aliases.

        Example:
            >>> count = app._browse_count("titles")  # doctest: +SKIP


        :param kind: Exact category/count token passed unchanged to the catalogue.
        :return: Shared count, including zero for unsupported selectors and visible read errors.
        """
        return self.catalog.browse_count(kind)

    def _tag_category_table(self) -> str:
        """
        Use the catalogue read model's selected tag source, defaulting to tags.

        Example:
            >>> table = app._tag_category_table()  # doctest: +SKIP


        :return: Truthy selected table or tags; the fallback is not rechecked for existence.
        """
        return self.catalog.read_model.tag_category_table() or "tags"

    def _nav_buttons(self) -> str:
        """
        Render six fixed navigation buttons in pairs after reading five browse counts.

        Append a parenthesized count only when positive. Zero/negative values
        leave the label bare; Search has no count query. Read errors propagate.

        Example:
            >>> navigation = app._nav_buttons()  # doctest: +SKIP


        :return: Trusted HTML for a two-column navigation table with escaped links/labels.
        """
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
        """
        Render overview counts and the first eight rows from the recent-work list.

        The full work list is obtained before slicing. Navigation and overview
        counts are separate reads, not one consistent snapshot. The retained
        footer's delivery claim does not add authorization to compatibility routes.

        Example:
            >>> page = app._render_home_page()  # doctest: +SKIP


        :return: Complete HTML home page; catalogue/rendering failures are not suppressed.
        """
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
        """
        Prefer book, author, series, and tag routes over generic row-detail URLs.

        Discover the table's ID column first; absent columns or None/empty IDs
        return None. Zero is retained. Quote IDs and author table names as path
        components; labels/tags share a route without a table discriminator.

        Example:
            >>> href = app._row_href("works", work_row)  # doctest: +SKIP


        :param table: Exact table name selecting compatibility routing or base-host fallback.
        :param row: Row-like value supplying the discovered identifier field.
        :return: Root-relative quoted URL or None when no usable identifier exists.
        """
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
        """
        Delegate integer work-ID parsing and retention of the first-underscore suffix.

        Example:
            >>> parts = app._split_compat_book_token("7_main")  # doctest: +SKIP


        :param raw_book_id: Compatibility ID text passed through the catalogue's falsey/strip policy.
        :return: Integer ID or None, paired with the untouched remainder after the first underscore.
        """
        return self.catalog.split_compat_book_token(raw_book_id)

    def _browse_entries(self, kind: str) -> list[dict[str, object]]:
        """
        Reduce catalogue category items to table/row entries for HTML rendering.

        Preserve provider order and row identity; discard other item metadata.
        Missing table/row keys and catalogue failures propagate.

        Example:
            >>> entries = app._browse_entries("authors")  # doctest: +SKIP


        :param kind: Category selector interpreted by the catalogue.
        :return: New dicts with stringified table names and original row objects.
        """
        return [{"table": str(entry["table"]), "row": entry["row"]} for entry in self.catalog.category_rows(kind)]

    def _category_summary_payload(self) -> list[dict[str, object]]:
        """
        Borrow the catalogue's counted compatibility category navigation summary.

        Example:
            >>> summary = app._category_summary_payload()  # doctest: +SKIP


        :return: Category dicts with encoded AJAX routes, icon paths, flags, and counts.
        """
        return self.catalog.category_summary_payload()

    def _thumbnail_text(self, text: str) -> str:
        """
        Delegate the uppercase first-alphanumeric thumbnail initial or question mark.

        Unicode uppercase expansion is retained, so one input character may
        produce multiple output characters.

        Example:
            >>> initial = app._thumbnail_text(" -- ßeta")  # doctest: +SKIP


        :param text: Title-like value passed to the shared thumbnail-text policy.
        :return: Uppercase initial, potentially expanded, or ? when no character qualifies.
        """
        return self.catalog.thumbnail_text(text)

    def _work_subtitle(self, row) -> str:
        """
        Delegate compact credit, series, and tag subtitle assembly without HTML escaping.

        Example:
            >>> subtitle = app._work_subtitle(work_row)  # doctest: +SKIP


        :param row: Work whose shared metadata/relationship reads supply the subtitle.
        :return: Plain-text catalogue subtitle, with read errors propagated.
        """
        return self.catalog.work_subtitle(row)

    def _work_sort_value(self, row, *, sort_key: str) -> object:
        """
        Delegate a work's normalized field/relationship or numeric-ID sorting key.

        Example:
            >>> value = app._work_sort_value(work_row, sort_key="title")  # doctest: +SKIP


        :param row: Work row passed unchanged to the shared sort-key projector.
        :param sort_key: Selector normalized and interpreted by the catalogue read model.
        :return: Shared string or integer key without wrapper-side coercion.
        """
        return self.catalog.work_sort_value(row, sort_key=sort_key)

    def _work_metadata_payload(self, row) -> dict[str, object]:
        """
        Delegate work metadata projection with in-place category-URL augmentation.

        The catalogue adds encoded author/tag/series links through additional
        reads, without promising a snapshot or copying the returned metadata dict.

        Example:
            >>> metadata = app._work_metadata_payload(work_row)  # doctest: +SKIP


        :param row: Work used for shared metadata and category-link discovery.
        :return: Catalogue metadata dict with category_urls added or replaced.
        """
        return self.catalog.work_metadata_payload(row)

    def _work_rows_payload(self, rows: list[object]) -> list[dict[str, object]]:
        """
        Project work rows in order without the catalogue's category_urls augmentation.

        This bulk helper differs from _books_metadata_payload, which augments
        each work and keys results by ID.

        Example:
            >>> metadata = app._work_rows_payload(works)  # doctest: +SKIP


        :param rows: Work sequence forwarded without deduplication or reordering.
        :return: List of shared read-model metadata dicts without added category links here.
        """
        return self.catalog.work_rows_payload(rows)

    def _ajax_setup_payload(self) -> dict[str, object]:
        """
        Borrow fixed main-library setup metadata and root-relative compatibility routes.

        Example:
            >>> setup = app._ajax_setup_payload()  # doctest: +SKIP


        :return: Setup dict using the configured title without reading catalogue contents.
        """
        return self.catalog.ajax_setup_payload()

    def _category_route_target(self, category: str, item_id: object) -> str:
        """
        Delegate an HTML category-item URL, including preferred-table author resolution.

        Author IDs may cause row lookups; tags and series do not require existence
        checks. The shared helper quotes components and supplies browse fallbacks.

        Example:
            >>> url = app._category_route_target("tags", "7")  # doctest: +SKIP


        :param category: Plain or encoded category normalized by the catalogue.
        :param item_id: Raw item identifier, integer-converted only for author lookup.
        :return: Root-relative HTML route; backend failures propagate.
        """
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
        """
        Delegate category pagination and Calibre AJAX item/link projection.

        Example:
            >>> page = app._category_items_payload("tags", num=20, offset=0, sort="name", sort_order="asc")  # doctest: +SKIP


        :param category: Selector normalized by the catalogue before shared paging.
        :param num: Requested slice length, forwarded without additional bounds here.
        :param offset: Requested offset passed unchanged to the shared paging policy.
        :param sort: Category sort selector interpreted by the read model.
        :param sort_order: Direction selector interpreted by the read model.
        :return: AJAX category page with counts, encoded links, and compatibility item fields.
        """
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
        """
        Delegate sorted work-ID pagination and the compatibility search receipt.

        This wraps rows already selected by the caller; it does not execute
        query_text as a search. The catalogue also performs a separate title count.

        Example:
            >>> page = app._search_result_payload(query_text="snow", rows=works, num=20, offset=0, sort="title", sort_order="asc", base_url="/ajax/search/main")  # doctest: +SKIP


        :param query_text: Text echoed in the result, not searched by this helper.
        :param rows: Candidate works to sort and slice through the shared read model.
        :param num: Requested page length without extra wrapper validation.
        :param offset: Requested zero-based slice offset.
        :param sort: Work sort-key selector interpreted by the shared projector.
        :param sort_order: Direction selector interpreted by the shared projector.
        :param base_url: Caller-provided base route echoed without URL validation.
        :return: Search receipt containing book IDs/page metadata, not full row objects.
        """
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
        """
        Delegate augmented metadata indexed by stringified work ID, with later duplicates winning.

        Example:
            >>> books = app._books_metadata_payload(works)  # doctest: +SKIP


        :param rows: Work rows processed in input order, including duplicates.
        :return: String-ID mapping of category-link-augmented metadata dicts.
        """
        return self.catalog.books_metadata_payload(rows)

    def _basic_interface_data_payload(self) -> dict[str, object]:
        """
        Borrow main-library interface settings with the configured title and page length.

        Example:
            >>> settings = app._basic_interface_data_payload()  # doctest: +SKIP


        :return: Compatibility interface dict without additional validation or catalogue reads.
        """
        return self.catalog.basic_interface_data_payload()

    def _tag_browser_payload(self) -> dict[str, object]:
        """
        Delegate the fixed author/tag/series tree with positional compatibility node IDs.

        Editability flags describe client presentation, not mutation authorization.

        Example:
            >>> tree = app._tag_browser_payload()  # doctest: +SKIP


        :return: Root/item-map payload with all three categories, including empty ones.
        """
        return self.catalog.tag_browser_payload()

    def _serve_static_asset(self, what: str) -> _Response:
        """
        Serve the four built-in CSS/HTML/PNG assets without reading a filesystem path.

        Strip and lowercase the falsey-to-empty selector. Only mobile.css,
        reset.css, empty.html, and calibre.png are recognized; nested paths and
        other names return 404. CSS/HTML uses the generic UTF-8 text builder.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app._serve_static_asset(" MOBILE.CSS ").status
            '200 OK'
            >>> app._serve_static_asset("../mobile.css").status
            '404 Not Found'


        :param what: Relative asset selector, not an arbitrary path to open.
        :return: Static response or a plain-text 404 for an unsupported name.
        """
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
        """
        Return the built-in PNG regardless of icon selector or requested dimensions.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> b"".join(app._serve_icon("unknown", {}).body) == PLACEHOLDER_PNG
            True


        :param which: Ignored icon name retained for route compatibility.
        :param query: Ignored query values; no resize or alternate image selection occurs.
        :return: Inline 200 PNG response named icon.png.
        """
        del which, query
        return self._bytes_response(PLACEHOLDER_PNG, download_name="icon.png", disposition="inline", content_type_override="image/png")

    def _serve_ajax(self, path: str, query: dict[str, list[str]]) -> _Response:
        """
        Serve library/category/search/book JSON through the shared catalogue adapter.

        Decode path components and accept extra components without validating a
        library ID; all receipts describe main. Category and item tokens use the
        permissive compatibility decoder. Page requests use the first query value,
        bounded num and nonnegative offset; allbooks/newest category routes force
        title/date sorting. books without nonblank IDs returns every recent work,
        not a bounded page. Search trims its query and executes catalogue search.

        A malformed book ID returns 400, absent book 404, and unknown route 404.
        Data/query/serialization errors propagate rather than becoming empty JSON.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app._serve_ajax("/ajax/unknown", {}).status
            '404 Not Found'


        :param path: AJAX path with percent-encoded components and optional unused suffixes.
        :param query: Parsed query mapping with list-valued fields; only first values are used.
        :return: JSON success response or explicit plain-text routing/identity error.
        """
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
        """
        Serve initial interface state, first-page metadata, tag trees, and update metadata.

        init/books-init/get-books always request offset zero and ignore any
        supplied offset. Nonblank IDs take precedence over search in get-books,
        though search text is still echoed. Metadata is selected from the input
        rows by result-ID membership, not reordered to page rank. init adds fixed
        capability placeholders; update only echoes an optional translations hash
        into fresh interface settings and does not mutate or refresh the catalogue.

        Extra route components are tolerated. Book-token errors return 400/404;
        unknown routes return 404. Backend and serialization failures propagate.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app._serve_interface_data("/interface-data-no-match", {}).status
            '404 Not Found'


        :param path: Interface-data route whose individual components are percent-decoded here.
        :param query: List-valued query fields; the first value controls each supported option.
        :return: Compatibility JSON response or a plain-text route/identity error.
        """
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
        """
        Expose the UTF-8 Atom response builder to the shared OPDS adapter.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app.opds_xml_response("<feed/>").body
            [b'<feed/>']


        :param xml_text: Trusted XML text, passed without parsing or additional escaping.
        :param status: WSGI status text forwarded unchanged.
        :return: The XML builder's single-chunk response.
        """
        return self._xml_response(xml_text, status=status)

    def opds_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        """
        Supply OPDS errors through the generic UTF-8 text response builder.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app.opds_text_response("404 Not Found", "Missing", content_type="text/plain").body
            [b'Missing']


        :param status: WSGI status text retained without interpretation.
        :param text: Unescaped body text encoded as UTF-8.
        :param content_type: Media type to which the base builder appends charset=utf-8.
        :return: Generic text response with builder errors left visible.
        """
        return self._text_response(status, text, content_type=content_type)

    def acquisition_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        """
        Supply acquisition errors through the generic UTF-8 text response builder.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app.acquisition_text_response("400 Bad Request", "Bad ID", content_type="text/plain").status
            '400 Bad Request'


        :param status: WSGI status forwarded unchanged.
        :param text: Body text to encode without escaping.
        :param content_type: Media type with the base builder's UTF-8 charset appended.
        :return: Generic text response, without intercepting failures.
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
        Wrap acquired bytes as a 200 response without reading files or checking download policy.

        The base builder supplies length, media type, and disposition. It strips
        double quotes from the filename, not every possible header control
        character; callers remain responsible for trusted header metadata.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> response = app.acquisition_bytes_response(b"book", download_name="book.epub")
            >>> dict(response.headers)["Content-Length"]
            '4'


        :param payload: Already acquired bytes retained as one response chunk.
        :param download_name: Name used for MIME guessing and Content-Disposition.
        :param disposition: Disposition text, normally attachment or inline.
        :param content_type_override: Truthy explicit media type, or None to guess from the name.
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
        Return an empty 302 acquisition redirect without validating its location.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app.acquisition_redirect_response("/asset").headers
            [('Location', '/asset')]


        :param location: Location string passed unchanged to the generic redirect builder.
        :return: Redirect response containing one empty byte chunk.
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
        Retain the generic local-file streaming hook for compatibility callers.

        The current acquisition adapter reads through Core instead. This hook
        opens the supplied path and returns a response with a file closer; it adds
        no path confinement or download-policy check. Open/stat/wrapper errors
        propagate, and the consumer must close a successfully returned response.

        Example:
            >>> response = app.acquisition_file_response(path, download_name="book.epub", environ={})  # doctest: +SKIP
            >>> response.close()  # doctest: +SKIP


        :param path: Local Path opened in binary mode by the base builder.
        :param download_name: Filename used for media-type guessing and disposition.
        :param environ: WSGI mapping optionally containing a wsgi.file_wrapper callable.
        :param disposition: Content-Disposition mode forwarded unchanged.
        :param content_type_override: Truthy explicit media type, or None for filename guessing.
        :return: Streaming response owning an open file and exposing its closer.
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
        Expose the local compatibility book-token hook to the acquisition adapter.

        Example:
            >>> parts = app.acquisition_split_book_token("7_60_80")  # doctest: +SKIP


        :param raw_book_id: Work ID with an optional underscore suffix, passed unchanged.
        :return: Parsed integer ID or None, paired with the retained suffix.
        """
        return self._split_compat_book_token(raw_book_id)

    def acquisition_work_row(self, row_id: int):
        """
        Fetch a work through the shared read model for legacy acquisition callers.

        The current acquisition adapter uses its own Core browse query instead.

        Example:
            >>> work = app.acquisition_work_row(7)  # doctest: +SKIP


        :param row_id: Work identifier converted to int before lookup.
        :return: Work row or None for successful absence; conversion/query failures propagate.
        """
        return self.read_model.row_by_id("works", int(row_id))

    def acquisition_work_image_row(self, work_row):
        """
        Delegate the first discovered work image for compatibility host callers.

        Example:
            >>> image = app.acquisition_work_image_row(work_row)  # doctest: +SKIP


        :param work_row: Work used for shared relationship and image discovery.
        :return: First original image row or None, with discovery errors preserved.
        """
        return self.images.work_image_row(work_row)

    def acquisition_resolve_storage_image(self, image_row):
        """
        Delegate creation of a Core image reader without reading its bytes yet.

        Example:
            >>> reader = app.acquisition_resolve_storage_image(image_row)  # doctest: +SKIP


        :param image_row: Image metadata supplying the resource ID to resolve.
        :return: Bound CoreStoredFile, or None for an unusable ID or explicit unreadability.
        """
        return self.images.resolve_storage_image(image_row)

    def acquisition_resolve_image_target(self, image_row) -> Optional[_ResolvedFileTarget]:
        """
        Delegate redirect-only image target selection for compatibility callers.

        Example:
            >>> target = app.acquisition_resolve_image_target(image_row)  # doctest: +SKIP


        :param image_row: Image metadata supplying ID and fallback filename.
        :return: Redirect target or None under the image backend's policy, not a local path target.
        """
        return self.images.resolve_image_target(image_row)

    def acquisition_image_download_name(self, image_row) -> str:
        """
        Delegate image filename precedence without adding sanitization.

        Example:
            >>> name = app.acquisition_image_download_name(image_row)  # doctest: +SKIP


        :param image_row: Image metadata projected through the shared image backend.
        :return: Name, original name, storage key, or cover.bin, stringified by the backend.
        """
        return self.images.image_download_name(image_row)

    def acquisition_image_content_type(self, image_row) -> str:
        """
        Delegate declared-or-guessed image media type without inspecting content.

        Example:
            >>> media_type = app.acquisition_image_content_type(image_row)  # doctest: +SKIP


        :param image_row: Metadata supplying explicit MIME text or a filename to inspect.
        :return: Declared/guessed type or application/octet-stream when no type is available.
        """
        return self.images.image_content_type(image_row)

    def acquisition_placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        """
        Delegate escaped title-based SVG cover rendering at the supplied dimensions.

        Example:
            >>> svg = app.acquisition_placeholder_cover_svg(work_row, width=60, height=80)  # doctest: +SKIP


        :param work_row: Work whose display title supplies the initial and subtitle.
        :param width: Requested SVG width forwarded without clamping.
        :param height: Requested SVG height forwarded without clamping.
        :return: UTF-8 placeholder bytes, not a resized source cover.
        """
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)

    def acquisition_related_rows_by_table(self, work_row) -> dict[str, list[object]]:
        """
        Expose generic related-row groups to legacy acquisition host callers.

        Example:
            >>> related = app.acquisition_related_rows_by_table(work_row)  # doctest: +SKIP


        :param work_row: Work passed unchanged to the base host's relationship helper.
        :return: Full related-table mapping, not the narrower OPDS metadata subset.
        """
        return self._related_rows_by_table(work_row)

    def acquisition_work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        """
        Expose the local file-discovery hook to legacy acquisition callers.

        Example:
            >>> files = app.acquisition_work_file_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Existing related groups forwarded to _work_file_rows.
        :return: Discovered direct/WEMI file rows under the shared catalogue policy.
        """
        return self._work_file_rows(related_rows_by_table)

    def acquisition_download_name_for_file_row(self, file_row) -> str:
        """
        Expose generic file-name precedence without validating or sanitizing the result.

        Example:
            >>> name = app.acquisition_download_name_for_file_row(file_row)  # doctest: +SKIP


        :param file_row: File metadata projected by the base host.
        :return: Name, original name, storage key, or download.bin under the base policy.
        """
        return self._download_name_for_file_row(file_row)

    def acquisition_file_id(self, file_row) -> object:
        """
        Return the row's raw file_id without numeric conversion or existence checks.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app.acquisition_file_id({"file_id": "007"})
            '007'


        :param file_row: Row-like input queried through the shared row_value helper.
        :return: Original ID value or None when the helper cannot obtain it.
        """
        return _row_value(file_row, "file_id")

    def acquisition_serve_file_download(self, raw_file_id: str, environ) -> _Response:
        """
        Retain the generic file-ID download route for legacy acquisition callers.

        This is not the compatibility format adapter's direct Core path. The
        base route retains its own download-setting and failure-response policy.

        Example:
            >>> response = app.acquisition_serve_file_download("7", {})  # doctest: +SKIP


        :param raw_file_id: Identifier text parsed by the base download handler.
        :param environ: WSGI context forwarded for any eventual local streaming.
        :return: Base handler bytes, stream, redirect, or error response unchanged.
        """
        return self._serve_file_download(raw_file_id, environ)

    def opds_search_work_rows(self, query_text: str) -> list[object]:
        """
        Delegate catalogue work search to supply candidate rows for OPDS feeds.

        Example:
            >>> works = app.opds_search_work_rows("snow")  # doctest: +SKIP


        :param query_text: Search text interpreted by the catalogue's trimming and works-filter policy.
        :return: Matching work rows, preserving provider order and visible query failures.
        """
        return self.catalog.search_work_rows(query_text)

    def opds_work_rows(self, *, sorted_by: str) -> list[object]:
        """
        Borrow catalogue work ordering without applying an OPDS page slice here.

        Example:
            >>> works = app.opds_work_rows(sorted_by="recent")  # doctest: +SKIP


        :param sorted_by: Shared ordering selector, recognizing exact recent specially.
        :return: Catalogue work list unchanged, with its read failures preserved.
        """
        return self.catalog.work_rows(sorted_by=sorted_by)

    def opds_category_rows(self, category: str) -> list[dict[str, object]]:
        """
        Delegate category item enumeration and compatibility browse-URL augmentation.

        Example:
            >>> items = app.opds_category_rows("authors")  # doctest: +SKIP


        :param category: Selector normalized by the shared catalogue.
        :return: Item dicts whose URL fields may have been modified by catalogue projection.
        """
        return self.catalog.category_rows(category)

    def opds_category_display_name(self, category: str) -> str:
        """
        Expose the local category-label hook to the shared OPDS adapter.

        Example:
            >>> name = app.opds_category_display_name("authors")  # doctest: +SKIP


        :param category: Plain category value forwarded unchanged.
        :return: Shared fixed English label or title-cased unknown-name fallback.
        """
        return self._category_display_name(category)

    def opds_rows_for_category_item(self, category: str, item_token: str) -> list[object]:
        """
        Delegate normalized category-item work selection, including whole-work categories.

        Unlike standalone OPDS's exact-token hook, the catalogue normalizes aliases
        and supports title/recent lists. Author tables use first-nonempty results,
        not a union. The item token is forwarded without an extra hex decode here.

        Example:
            >>> works = app.opds_rows_for_category_item("authors", "7")  # doctest: +SKIP


        :param category: Plain or encoded selector normalized by the catalogue.
        :param item_token: Item identity supplied by the OPDS token parser.
        :return: Selected work rows or an empty list under the catalogue's absence policy.
        """
        return self.catalog.work_rows_for_category_item(category, item_token)

    def _opds_related_rows_by_table(self, row) -> dict[str, list[object]]:
        """
        Read direct expression/file/selected-tag/series groups needed for feed metadata.

        Skip absent tables and successful empty results without traversing the
        full generic graph. Later metadata projection owns descendant asset reads.
        Existence/relationship query failures propagate instead of yielding empties.

        Example:
            >>> related = app._opds_related_rows_by_table(work_row)  # doctest: +SKIP


        :param row: Work row passed unchanged to each present table's relationship query.
        :return: Table-to-row-list mapping containing only nonempty groups in query order.
        """
        related: dict[str, list[object]] = {}
        for linked_table in ("expressions", "files", self._tag_category_table(), "series"):
            if not self._table_exists(linked_table):
                continue
            linked_rows = self.read_model.interlinked_rows(row, linked_table)
            if linked_rows:
                related[linked_table] = linked_rows
        return related

    def opds_work_metadata_payload(self, row) -> dict[str, object]:
        """
        Project feed metadata from limited related groups without category-URL augmentation.

        Call the catalogue's read model directly, not _work_metadata_payload.
        Author/asset resolution remains the shared projector's responsibility.

        Example:
            >>> metadata = app.opds_work_metadata_payload(work_row)  # doctest: +SKIP


        :param row: Work used for the limited relationship lookup and metadata projection.
        :return: Shared work metadata dict, without adding category_urls at this layer.
        """
        return self.catalog.read_model.work_metadata_payload(
            row,
            related_rows_by_table=self._opds_related_rows_by_table(row),
        )

    def _serve_opds(self, path: str, query: dict[str, list[str]]) -> _Response:
        """
        Forward a normalized feed route to the shared OPDS protocol adapter.

        Example:
            >>> response = app._serve_opds("/opds", {})  # doctest: +SKIP


        :param path: Normalized path including its /opds prefix.
        :param query: Parsed list-valued query mapping passed unchanged.
        :return: Adapter feed/error response, with its exceptions left visible.
        """
        return self.opds_api.serve(path, query)

    def _entity_subtitle(self, table: str, row) -> str:
        """
        Describe linked-work count and, for agent tables, an abbreviated agent type.

        Read generic related groups even for non-agent rows. Omit zero counts and
        blank types; the result is plain text for the renderer to escape later.

        Example:
            >>> subtitle = app._entity_subtitle("agents", agent_row)  # doctest: +SKIP


        :param table: Exact table name; agents/human_agents/org_agents enable type text.
        :param row: Entity row used for relationship reads and optional agent_type lookup.
        :return: Nonempty pieces joined by a middle dot, or empty text when none apply.
        """
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
        """
        Render ordered table/row entries as linked thumbnail, title, subtitle, and action cells.

        Work rows use cover markup and an Open action; others use an escaped
        initial, entity subtitle, and View. A missing row URL becomes #. Scalar
        links/text are escaped; the internal thumbnail fragment is inserted raw.
        Row metadata/relationship failures abort rendering rather than skipping rows.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app._render_listing_rows([])
            ''


        :param entries: Ordered mappings requiring table and row keys; other keys are ignored.
        :return: Concatenated HTML tr fragments without a surrounding table element.
        """
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
        """
        Adapt a work sequence to the generic browse-listing renderer without reordering.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app._render_work_listing([], empty_message="<none>")
            "<p class='meta'>&lt;none&gt;</p>"


        :param rows: Work objects wrapped in fresh table/row mappings, retaining row identity.
        :param empty_message: Plain text escaped if the sequence is empty.
        :return: Listing table HTML or an empty-state paragraph.
        """
        entries = [{"table": "works", "row": row} for row in rows]
        return self._render_browse_listing(entries, empty_message=empty_message)

    def _render_browse_listing(self, entries: list[dict[str, object]], *, empty_message: str) -> str:
        """
        Wrap rendered entries in the listing table, or escape an empty-state message.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app._render_browse_listing([], empty_message="No books")
            "<p class='meta'>No books</p>"


        :param entries: Ordered table/row mappings consumed by _render_listing_rows.
        :param empty_message: Plain text used only when entries is falsey.
        :return: Table markup for nonempty entries, otherwise a paragraph fragment.
        """
        if not entries:
            return "<p class='meta'>{}</p>".format(_escape(empty_message))
        return "<table id='listing'><tbody>{}</tbody></table>".format(self._render_listing_rows(entries))

    def _render_browse_page(self, kind: str, query: dict[str, list[str]]) -> str:
        """
        Render one exact browse category with full enumeration before offset/limit slicing.

        Recognize titles/authors/tags/series/recent only; unknown names produce a
        missing-category HTML page, not an HTTP error response. Parse the first
        limit/offset values with configured bounds, render a generic pager, and
        query navigation counts separately. An offset beyond the end yields an
        empty listing rather than moving the slice back to a valid page.

        Example:
            >>> page = app._render_browse_page("authors", {"limit": ["20"]})  # doctest: +SKIP


        :param kind: Exact lowercase browse token; this method does not decode category aliases.
        :param query: List-valued query fields supporting limit and offset.
        :return: Complete HTML page; the caller determines its HTTP status.
        """
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
        """
        Search or enumerate works, sort the complete result, and render a one-based mobile slice.

        Trim search text and normalize sort/order. Only exact ascending sorts
        forward; other order text sorts in reverse even if no form option matches.
        The default page size is at most 25, bounded by configured limits. Clamp
        start only to at least one, not to result length. Read/sort failures escape.
        Navigation appears above and below the listing and preserves query state.

        Example:
            >>> page = app._render_mobile_catalog_page({"search": ["snow"], "start": ["1"]})  # doctest: +SKIP


        :param query: First-valued search, num, start, sort, and order request fields.
        :return: Complete HTML mobile catalogue, including an empty message for an empty slice.
        """
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
        """
        Render the fixed mobile search controls with exact matching options selected.

        Page-size options are 5/10/25/100 irrespective of configured limits. An
        unmatched size/sort/order selects no option explicitly. Only num is
        int-converted here; search text is escaped for its attribute value.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> form = app._render_mobile_search_form(search="A & B", sort_key="title", order="ascending", num=10)
            >>> "value='A &amp; B'" in form and "value='10' selected" in form
            True


        :param search: Current plain search text inserted into the form after escaping.
        :param sort_key: Exact option token to select from the fixed seven sort choices.
        :param order: Exact ascending/descending option token to select.
        :param num: Current page size, int-converted for option comparison.
        :return: Trusted GET form HTML targeting /mobile without a retained start field.
        """
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
        """
        Render one-based mobile range text and first/previous/next/last query links.

        A nonpositive total returns the fixed zero range. Otherwise end is capped
        at total but start is not clamped, so oversized starts can display an
        inverted range. Last starts at total-num+1, not at a page-aligned boundary.
        Query construction omits empty search text and escapes the resulting URLs.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> nav = app._render_mobile_navigation(search="", sort_key="title", order="ascending", num=10, start=11, total=2)
            >>> "Books 11 to 2 of 2" in nav
            True


        :param search: Search text preserved in navigation when nonempty.
        :param sort_key: Sort token preserved without normalization here.
        :param order: Direction text preserved without normalization here.
        :param num: Page step/length expected to be positive by the caller; not validated here.
        :param start: One-based first item position, used as supplied.
        :param total: Total candidate-row count before page slicing.
        :return: Navigation HTML with conditional links and a displayed range.
        """
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
        """
        Render discovered files in case-insensitive filename order with capability-based actions.

        Omit rows with None/empty IDs after sorting. Derive uppercase format labels
        from filenames, not media parsing; an absent suffix becomes FILE. Retain
        unavailable rows with an explanatory marker. Store/source text is shortened
        before escaping. File IDs in action paths are HTML-escaped, not percent-quoted.
        Capability/name/discovery errors remain visible.

        Example:
            >>> rows_html = app._book_format_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Existing related groups used for direct/WEMI file discovery.
        :return: Ordered HTML tr fragments with format, filename, location hints, and actions.
        """
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
        """
        Delegate direct/WEMI file discovery and ID deduplication to the catalogue.

        Example:
            >>> files = app._work_file_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Direct-file/expression groups passed unchanged.
        :return: Discovered original file rows without wrapper-side filtering or copying.
        """
        return self.catalog.work_file_rows(related_rows_by_table)

    def _work_image_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        """
        Delegate direct/WEMI image discovery to the retained shared image backend.

        Example:
            >>> images = app._work_image_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Direct-image/expression groups passed unchanged.
        :return: Discovered original image rows with backend ordering and deduplication preserved.
        """
        return self.images.work_image_rows(related_rows_by_table)

    def _image_download_name(self, image_row) -> str:
        """
        Delegate image-name precedence and the cover.bin fallback without sanitization.

        Example:
            >>> name = app._image_download_name(image_row)  # doctest: +SKIP


        :param image_row: Image metadata projected by the shared backend through this host.
        :return: First truthy name/original name/storage key or cover.bin as text.
        """
        return self.images.image_download_name(image_row)

    def _image_content_type(self, image_row) -> str:
        """
        Delegate stripped explicit image MIME text or filename-based type guessing.

        Example:
            >>> media_type = app._image_content_type(image_row)  # doctest: +SKIP


        :param image_row: Image metadata supplying declared type and filename candidates.
        :return: Declared/guessed media type or application/octet-stream, without content validation.
        """
        return self.images.image_content_type(image_row)

    def _image_storage_lookup_metadata(self, image_row) -> dict[str, object]:
        """
        Delegate image metadata projection with legacy file-field aliases.

        The backend creates a shallow outer dict and nested image_row snapshot;
        eligible image aliases replace corresponding file fields.

        Example:
            >>> metadata = app._image_storage_lookup_metadata(image_row)  # doctest: +SKIP


        :param image_row: Image row passed unchanged to the shared image backend.
        :return: Metadata dict with available image-to-file aliases and shared nested values.
        """
        return self.images.image_storage_lookup_metadata(image_row)

    def _resolve_storage_image(self, image_row):
        """
        Delegate readable-image resolution into a deferred Core byte reader.

        Example:
            >>> reader = app._resolve_storage_image(image_row)  # doctest: +SKIP


        :param image_row: Image row supplying the resource identifier.
        :return: Bound reader or None for an unusable/unreadable image; bytes are not fetched here.
        """
        return self.images.resolve_storage_image(image_row)

    def _resolve_image_target(self, image_row):
        """
        Delegate redirect-only image target selection without URL validation.

        Example:
            >>> target = app._resolve_image_target(image_row)  # doctest: +SKIP


        :param image_row: Image metadata supplying identity and a fallback download name.
        :return: Redirect target or None according to the backend's ID/delivery policy.
        """
        return self.images.resolve_image_target(image_row)

    def _work_image_row(self, work_row) -> Optional[object]:
        """
        Delegate selection of the first discovered work image without separate cover ranking.

        Example:
            >>> image = app._work_image_row(work_row)  # doctest: +SKIP


        :param work_row: Work whose related groups and image descendants should be inspected.
        :return: First image row or None for successful empty discovery; failures propagate.
        """
        return self.images.work_image_row(work_row)

    def _placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        """
        Delegate an escaped title-based SVG fallback at unmodified dimensions.

        Example:
            >>> svg = app._placeholder_cover_svg(work_row, width=60, height=80)  # doctest: +SKIP


        :param work_row: Work whose display title supplies the placeholder content.
        :param width: SVG width, not validated or clamped by this hook.
        :param height: SVG height, also used by the backend to position the subtitle.
        :return: UTF-8 SVG bytes, without inspecting or resizing an image file.
        """
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)

    def _work_thumbnail_html(self, work_row) -> str:
        """
        Link a work's thumbnail route, or render its title initial when its ID is absent.

        None/empty IDs use the fallback; zero is retained. The nonempty identifier
        is HTML-escaped but not percent-quoted or numerically validated. No image
        discovery, availability query, or download-policy check occurs here.

        Example:
            >>> app = object.__new__(CalibreReadOnlyWebApplication)
            >>> app._work_thumbnail_html({"work_id": 7})
            "<img src='/get/thumb/7/main?sz=60x80' alt='cover'>"


        :param work_row: Row-like value supplying work_id and, for fallback, a display title.
        :return: Trusted img markup requesting 60x80 sizing or an escaped initial span.
        """
        work_id = _row_value(work_row, "work_id")
        if work_id in (None, ""):
            return "<span class='thumb-fallback'>{}</span>".format(_escape(self._thumbnail_text(self._row_primary_text("works", work_row))))
        return "<img src='/get/thumb/{}/main?sz=60x80' alt='cover'>".format(_escape(work_id))

    def _serve_compat_get(self, what: str, raw_book_id: str, query: dict[str, list[str]], environ) -> _Response:
        """
        Forward cover, thumbnail, or ebook-format delivery to Core-backed acquisition.

        This adapter path does not use the legacy file-ID download hook or enforce
        enable_file_downloads. Cover fallback and format error behavior remain
        AcquisitionCompatApi's responsibility; the wrapper catches nothing.

        Example:
            >>> response = app._serve_compat_get("epub", "7", {}, {})  # doctest: +SKIP


        :param what: Cover/thumb selector or format extension interpreted by the adapter.
        :param raw_book_id: Compatibility work token with an optional underscore suffix.
        :param query: Parsed list-valued query, including any cover sizing request.
        :param environ: WSGI context passed through; current direct Core delivery ignores it.
        :return: Adapter bytes, redirect, placeholder, or error response unchanged.
        """
        return self.acquisition_api.serve_compat_get(what, raw_book_id, query, environ)

    def _render_book_page(self, raw_row_id: str) -> str:
        """
        Render a work's title, credits, facets, formats, selected fields, and related records.

        Parse stripped integer text rather than a compatibility underscore token.
        Conversion failure or successful row absence produces explanatory HTML;
        the route still wraps it as status 200. Other query/render errors escape.

        Byline shows at most four credits, tag pills the first twelve tags, and
        notes the first row from the first nonempty synopses/comments/notes group,
        shortened to 600 characters. Series pills are unbounded. Always emit a
        cover URL without checking cover availability or download policy. Dynamic
        scalar text is escaped; internally generated sections are joined as HTML.

        Example:
            >>> page = app._render_book_page("7")  # doctest: +SKIP


        :param raw_row_id: Work ID text converted through str(...).strip() and int.
        :return: Complete book or explanatory error-page HTML, not an HTTP response.
        """
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
        """
        Use the Calibre book page for exact works and the generic detail renderer otherwise.

        Example:
            >>> page = app._render_row_page("works", "7")  # doctest: +SKIP


        :param table: Exact table name selecting work-specific versus base-host rendering.
        :param raw_row_id: Identifier text passed unchanged to the selected renderer.
        :return: Complete HTML page, retaining the renderer's absence/error behavior.
        """
        if table == "works":
            return self._render_book_page(raw_row_id)
        return super()._render_row_page(table, raw_row_id)

    def _render_linked_works_page(self, table: str, raw_row_id: str, *, kind: str) -> str:
        """
        Render every directly grouped work for a selected category entity without pagination.

        Test table existence before integer ID parsing. Missing table, invalid ID,
        or absent row yields an explanatory HTML page rather than an HTTP status;
        the route wraps that result as 200. Successful pages use related['works']
        order and a display/back-link label derived from kind. Read failures escape.

        Example:
            >>> page = app._render_linked_works_page("agents", "7", kind="authors")  # doctest: +SKIP


        :param table: Entity table whose existence, row, and related groups are queried.
        :param raw_row_id: Entity ID converted from stripped string text to int.
        :param kind: Presentation category, normally authors/series/tags; not an authorization check.
        :return: Complete linked-work listing or explanatory error-page HTML.
        """
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
    """
    Build Core/profile, metadata-source, bind, paging, and UI compatibility options.

    Parser construction opens neither a catalogue nor a listener. Numeric limits
    are accepted as integers here and clamped only when main builds its config.

    Example:
        >>> args = build_arg_parser().parse_args(["--page-size", "0"])
        >>> (args.page_size, args.max_page_size, args.opds_max_ungrouped_items)
        (0, 200, 100)


    :return: Fresh argparse parser with shared Core/cache options and startup examples.
    """
    parser = argparse.ArgumentParser(
        description="Run the LiuXin Calibre-style read-only web interface.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=metadata_read_source_help_epilog("PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_calibre_readonly"),
    )
    add_core_client_arguments(parser)
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
    """
    Own a configured Core session and stdlib WSGI server for the Calibre-style UI.

    Clamp both page sizes to at least one and grouping threshold to at least zero.
    Open storage-enabled, maintenance-disabled Core access; build the application;
    print and flush the requested URL before binding. Port zero is printed as
    zero, not the port later assigned by the OS. Context managers release resources
    on unwinding, but startup, serving, and interrupt exceptions are not caught.

    The no-file-downloads option reaches the shared config, not an authorization
    layer: the current compatibility acquisition adapter's direct Core path does
    not enforce it. The path-exposure flag can reveal database metadata in HTML.

    Example:
        >>> main(["--database", "catalog.sqlite", "--port", "8081"])  # doctest: +SKIP


    :param argv: Explicit argument tokens, or None for argparse's process-argument default.
    :return: Zero only after serve_forever and both context managers complete normally.
    :raises SystemExit: For parser help or invalid command-line syntax.
    """
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
        app = CalibreReadOnlyWebApplication(
            core_session.client,
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
