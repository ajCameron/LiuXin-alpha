"""
Browse Core metadata and deliver legacy-file acquisitions through stdlib WSGI.

This generic host supplies table, relationship, search, and rendering helpers
used by other web surfaces. HTTP routes accept GET and HEAD, but retain response
bodies for HEAD. Metadata query failures generally propagate instead of becoming
empty results; acquisition handlers translate selected delivery failures.

Read-only describes the browsing interface, not an authorization or content
sanitization boundary. Column-name hiding is heuristic, preferred summaries can
read other fields, and HTML/SVG previews may contain active content. Deployment
must supply its own access controls and content isolation. Importing this module
does not bind a listener; ``main`` composes Core and runs the server.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import posixpath
import re
import sys
import unicodedata

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional
from urllib.parse import parse_qs, quote, unquote, urljoin
from wsgiref.simple_server import make_server
from wsgiref.util import FileWrapper

from LiuXin_alpha.core import CoreClientAPI

# Preserve historical helper/type imports while their implementations live in
# independent shared modules. Reusable backends import those owners directly.
from LiuXin_alpha.surfaces.acquisition_types import (
    CoreStoredFile as _CoreStoredFile,
    ResolvedFileTarget as _ResolvedFileTarget,
)
from LiuXin_alpha.surfaces.core import (
    CoreDatabaseView,
    CoreSurfaceModel,
    add_core_client_arguments,
    coerce_surface_core,
    open_surface_core_from_args,
)
from LiuXin_alpha.surfaces.presentation import (
    coerce_int as _coerce_int,
    escape as _escape,
    row_value as _row_value,
    short_text as _short_text,
)


def _search_terms(value: object) -> list[str]:
    """
    Split a falsey-as-empty query on whitespace, retaining case and duplicates.

    Quotes have no special meaning; this is not a phrase-query parser.

    Example:
        >>> _search_terms("  Red  red Red ")
        ['Red', 'red', 'Red']


    :param value: Query-like value converted to text after falsey substitution.
    :return: Nonempty tokens in input order, or an empty list.
    """
    text = str(value or "").strip()
    if not text:
        return []
    return [one for one in re.split(r"\s+", text) if one]


def _normalized_search_text(value: object) -> str:
    """
    Apply Unicode compatibility normalization and case folding for comparison.

    Surrounding whitespace is retained; falsey values become empty text.

    Example:
        >>> _normalized_search_text(" Ａ Straße ")
        ' a strasse '


    :param value: Search or candidate value to stringify and normalize.
    :return: NFKC-normalized, case-folded text without trimming.
    """
    return unicodedata.normalize("NFKC", str(value or "")).casefold()


def _build_query_string(values: dict[str, object]) -> str:
    """
    Encode ordered scalar query pairs, omitting None and empty-string values.

    Values are stringified, not expanded as sequences. Spaces use percent
    encoding; zero and False remain present. No leading question mark is added.

    Example:
        >>> _build_query_string({"q": "a b", "offset": 0, "unused": None})
        'q=a%20b&offset=0'


    :param values: Insertion-ordered names and scalar values to encode.
    :return: Ampersand-separated encoded pairs, possibly empty.
    """
    parts: list[str] = []
    for key, value in values.items():
        if value is None:
            continue
        text = str(value)
        if text == "":
            continue
        parts.append("{}={}".format(quote(str(key), safe=""), quote(text, safe="")))
    return "&".join(parts)


def _closing_iterable(iterable: Iterable[bytes], closer: Callable[[], None]) -> Iterable[bytes]:
    """
    Attach a cleanup callback to iteration finalization and explicit close.

    Cleanup is not made idempotent: exhausting an iteration and then calling
    ``close`` invokes the callback twice. Callback failures propagate and may
    replace an iteration failure. The wrapped iterable's own close method is
    not called unless the supplied callback does so.

    Example:
        >>> from unittest.mock import Mock
        >>> closer = Mock()
        >>> body = _closing_iterable([b"content"], closer)
        >>> list(body)
        [b'content']
        >>> body.close()
        >>> closer.call_count
        2


    :param iterable: Byte chunks to yield without coercion or buffering.
    :param closer: Resource cleanup callback, potentially called repeatedly.
    :return: Iterable exposing a close method for a WSGI server to invoke.
    """
    class _ClosingIterable:
        """
        Bind one source iterable and its cleanup callback in the enclosing scope.

        Each iteration creates a generator with a finally block. Cleanup before
        any iteration starts requires an explicit call to ``close``.

        Example:
            >>> body = _closing_iterable([], lambda: None)
            >>> list(body)
            []
        """
        def __iter__(self_inner) -> Iterator[bytes]:
            """
            Yield source chunks and run cleanup when this generator finalizes.

            Example:
                >>> list(_closing_iterable([b"a", b"b"], lambda: None))
                [b'a', b'b']


            :return: Generator whose finally block invokes the captured closer.
            """
            try:
                for chunk in iterable:
                    yield chunk
            finally:
                closer()

        def close(self_inner) -> None:
            """
            Invoke the captured closer, even if iteration already invoked it.

            Example:
                >>> _closing_iterable([], lambda: None).close()


            :return: None; callback exceptions are not suppressed.
            """
            closer()

    return _ClosingIterable()


@dataclass(frozen=True)
class ReadOnlyWebConfig:
    """
    Hold immutable, unvalidated composition and presentation settings.

    Cache settings affect newly composed compatibility sessions, not a borrowed
    Core client's configuration. Hidden-column rules are display heuristics,
    not a complete privacy boundary; preferred summary fields bypass them.

    Example:
        >>> config = ReadOnlyWebConfig(enable_file_downloads=False)
        >>> (config.host, config.default_page_size, config.enable_file_downloads)
        ('127.0.0.1', 50, False)


    :ivar title: Site title used in escaped HTML headings and page titles.
    :ivar host: Listener address used by the command-line runner.
    :ivar port: Requested listener port; zero asks the server for an ephemeral port.
    :ivar default_page_size: Default metadata page length before request coercion.
    :ivar max_page_size: Upper request-page bound, not a full-enumeration bound.
    :ivar expose_database_path: Whether layouts may query and display the DB path.
    :ivar enable_file_downloads: Gate for this host's legacy-file resolvers.
    :ivar metadata_read_source: Composition selector; exact cache enables caching.
    :ivar metadata_cache_type: Cache implementation name for composition.
    :ivar metadata_cache_allow_database_fallback: Permit cache-to-database fallback.
    :ivar hidden_column_tokens: Substrings tested against lowercased column names.
    :ivar hidden_column_suffixes: Suffixes tested against lowercased column names.
    """

    title: str = "LiuXin Read-Only Web"
    host: str = "127.0.0.1"
    port: int = 8080
    default_page_size: int = 50
    max_page_size: int = 200
    expose_database_path: bool = False
    enable_file_downloads: bool = True
    metadata_read_source: str = "database"
    metadata_cache_type: str = "schema_backed"
    metadata_cache_allow_database_fallback: bool = True
    hidden_column_tokens: tuple[str, ...] = ("credential", "password", "secret", "token", "policy_json")
    hidden_column_suffixes: tuple[str, ...] = ("_scratch",)


@dataclass
class _Response:
    """
    Carry a WSGI status, ordered headers, body iterable, and optional cleanup.

    Construction performs no validation, encoding, emission, or resource cleanup.
    The application wrapper is responsible for attaching the close callback.

    Example:
        >>> response = _Response("200 OK", [], [b"ok"])
        >>> (response.status, list(response.body), response.close)
        ('200 OK', [b'ok'], None)


    :ivar status: HTTP status line passed to start_response.
    :ivar headers: Mutable list of header pairs, retaining duplicates and order.
    :ivar body: Iterable expected to yield bytes without further encoding.
    :ivar close: Optional resource-release callback for the response lifetime.
    """
    status: str
    headers: list[tuple[str, str]]
    body: Iterable[bytes]
    close: Optional[Callable[[], None]] = None


class ReadOnlyWebApplication:
    """
    Compose Core-backed browsing, HTML rendering, and legacy-file delivery.

    Construction binds collaborators without starting a server. A borrowed Core
    client remains caller-owned; a database compatibility input can create an
    owned session released by ``close``. Table browsing is not restricted to
    the smaller set used for default public search. Active-content previews and
    external redirects require deployment-specific trust controls.

    Example:
        >>> app = ReadOnlyWebApplication(core_client)  # doctest: +SKIP
        >>> response = app.handle_request({"PATH_INFO": "/"})  # doctest: +SKIP
        >>> app.close()  # doctest: +SKIP
    """

    _RELATED_TABLE_ORDER = (
        "tags",
        "labels",
        "genres",
        "subjects",
        "languages",
        "series",
        "agents",
        "human_agents",
        "org_agents",
        "notes",
        "comments",
        "synopses",
        "annotations",
        "files",
        "folders",
        "items",
        "expressions",
        "manifestations",
    )

    def __init__(
        self,
        core: CoreClientAPI | Any,
        *,
        config: Optional[ReadOnlyWebConfig] = None,
        model: CoreSurfaceModel | None = None,
        read_source: Any | None = None,
    ) -> None:
        """
        Adapt the supplied Core or database and bind shared surface backends.

        An injected model is accepted without checking that it uses the same
        Core client. Only exact ``metadata_read_source == "cache"`` selects a
        cache type during coercion. Construction failures propagate; this method
        does not provide a rollback wrapper around partially built collaborators.

        Example:
            >>> app = ReadOnlyWebApplication(core_client, config=ReadOnlyWebConfig())  # doctest: +SKIP


        :param core: Borrowed Core client or legacy database compatibility input.
        :param config: Settings to retain, or None for the default configuration.
        :param model: Optional prebuilt metadata/acquisition query adapter.
        :param read_source: Optional legacy read source passed to Core coercion.
        :return: None; bind configuration, Core, database view, images, and reads.
        """
        self.config = config or ReadOnlyWebConfig()
        core, compatibility_session = coerce_surface_core(
            core,
            read_source=read_source,
            cache_type=(
                self.config.metadata_cache_type
                if self.config.metadata_read_source == "cache"
                else None
            ),
            cache_allow_database_fallback=(
                self.config.metadata_cache_allow_database_fallback
            ),
        )
        self._compatibility_core_session = compatibility_session
        self.core = core
        self.model = model or CoreSurfaceModel(core)
        self.db = CoreDatabaseView(core, model=self.model)
        from LiuXin_alpha.surfaces.images import ImageBackend
        from LiuXin_alpha.surfaces.read_model import ReadModelBackend

        self.images = ImageBackend(self)
        self.read_model = ReadModelBackend(
            self,
            images=self.images,
            model=self.model,
        )

    def close(self) -> None:
        """
        Close an owned compatibility session without closing a borrowed client.

        The session reference is retained; repeat-call behavior belongs to that
        session's close implementation, and exceptions propagate.

        Example:
            >>> app.close()  # doctest: +SKIP


        :return: None after delegating cleanup when a compatibility session exists.
        """
        if self._compatibility_core_session is not None:
            self._compatibility_core_session.close()

    def __call__(self, environ, start_response):
        """
        Dispatch a WSGI request, append robot guidance, and attach body cleanup.

        Headers are copied before appending X-Robots-Tag. HEAD bodies are not
        suppressed. A start_response failure propagates before the cleanup
        wrapper is created; its optional write callable is not used.

        Example:
            >>> body = app(environ, start_response)  # doctest: +SKIP


        :param environ: WSGI environment consumed by dispatch and delivery.
        :param start_response: WSGI callback accepting the status and header list.
        :return: Original body iterable or an iterable wrapping its close callback.
        """
        response = self.handle_request(environ)
        headers = list(response.headers)
        headers.append(("X-Robots-Tag", "noai, noimageai"))
        start_response(response.status, headers)
        if response.close is not None:
            return _closing_iterable(response.body, response.close)
        return response.body

    def refresh_metadata_read_source(self) -> bool:
        """
        Delegate explicit post-write metadata refresh to the shared policy.

        This compatibility hook can refresh mutable read state even though
        the HTTP browsing routes do not expose metadata-edit operations.

        Example:
            >>> refreshed = app.refresh_metadata_read_source()  # doctest: +SKIP


        :return: Shared refresh helper's boolean outcome; failures follow its policy.
        """
        from LiuXin_alpha.surfaces.write_refresh import refresh_metadata_read_source_after_write

        return refresh_metadata_read_source_after_write(self)

    def handle_request(self, environ) -> _Response:
        """
        Route GET/HEAD requests to browsing, search, or legacy-file delivery.

        Dot segments are normalized before individual path parts are unquoted;
        blank query values are discarded. Unsupported methods return 405 and
        unmatched paths return 404. Invalid/missing table or row pages are HTML
        explanations still wrapped in 200. HEAD follows GET, including its body.
        Backend failures are not caught by this dispatcher.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app.handle_request({"REQUEST_METHOD": "POST"}).status
            '405 Method Not Allowed'


        :param environ: Mapping with optional request method, path, and query text.
        :return: Response carrier; body consumption and cleanup happen separately.
        """
        method = str(environ.get("REQUEST_METHOD", "GET") or "GET").upper()
        if method not in {"GET", "HEAD"}:
            return self._text_response("405 Method Not Allowed", "Method not allowed.\n", content_type="text/plain")

        path = posixpath.normpath(str(environ.get("PATH_INFO", "/") or "/"))
        if not path.startswith("/"):
            path = "/" + path
        query = parse_qs(str(environ.get("QUERY_STRING", "") or ""), keep_blank_values=False)

        if path == "/":
            return self._html_response(self._render_home_page())
        if path == "/search":
            return self._html_response(self._render_search_page(query))
        if path.startswith("/tables/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 2:
                return self._html_response(self._render_table_page(parts[1], query))
            if len(parts) == 3:
                return self._html_response(self._render_row_page(parts[1], parts[2]))
        if path.startswith("/files/") and path.endswith("/download"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 3 and parts[0] == "files" and parts[2] == "download":
                return self._serve_file_download(parts[1], environ)
        if path.startswith("/files/") and path.endswith("/preview"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 3 and parts[0] == "files" and parts[2] == "preview":
                return self._serve_file_preview(parts[1], environ)
        return self._html_response(
            self._render_layout(
                title="Not Found",
                body_html="<h1>Not found</h1><p>The requested page does not exist.</p>",
            ),
            status="404 Not Found",
        )

    def _text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        """
        Encode text as one UTF-8 chunk with an appended charset declaration.

        No HTML escaping or Content-Length calculation is performed.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> list(app._text_response("200 OK", "ok", content_type="text/plain").body)
            [b'ok']


        :param status: HTTP status line to retain unchanged.
        :param text: Unicode body to encode.
        :param content_type: Media type to which a UTF-8 charset is appended.
        :return: Response with a single Content-Type header and one body chunk.
        """
        return _Response(
            status=status,
            headers=[("Content-Type", "{}; charset=utf-8".format(content_type))],
            body=[text.encode("utf-8")],
        )

    def _html_response(self, html_text: str, *, status: str = "200 OK") -> _Response:
        """
        Encode already-rendered HTML without escaping or sanitizing it.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._html_response("<p>ok</p>").status
            '200 OK'


        :param html_text: Trusted HTML document or fragment to encode as UTF-8.
        :param status: HTTP status line, defaulting to success.
        :return: HTML response containing one byte chunk and no length header.
        """
        return _Response(
            status=status,
            headers=[("Content-Type", "text/html; charset=utf-8")],
            body=[html_text.encode("utf-8")],
        )

    def _redirect_response(self, location: str) -> _Response:
        """
        Construct a 302 redirect without validating the destination or headers.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._redirect_response("/tables/works").headers
            [('Location', '/tables/works')]


        :param location: Destination stringified into the Location header.
        :return: Redirect response whose body contains one empty byte chunk.
        """
        return _Response(
            status="302 Found",
            headers=[("Location", str(location))],
            body=[b""],
        )

    def _file_response(
        self,
        path: Path,
        *,
        download_name: str,
        environ,
        disposition: str = "attachment",
        content_type_override: Optional[str] = None,
    ) -> _Response:
        """
        Open a local path and expose it through the available WSGI file wrapper.

        The block size is 64 KiB; length comes from a separate path stat after
        opening. A truthy media override wins over filename guessing. The
        successful response carries file-handle cleanup, but failures during
        setup are not explicitly closed here. No confinement, range handling,
        HEAD suppression, or download-policy check occurs in this helper.
        Removing filename quotes is not general header sanitization.

        Example:
            >>> response = app._file_response(path, download_name="book.epub", environ={})  # doctest: +SKIP
            >>> response.close()  # doctest: +SKIP


        :param path: Existing readable filesystem path to open in binary mode.
        :param download_name: Suggested filename and fallback MIME-guess input.
        :param environ: WSGI mapping optionally supplying wsgi.file_wrapper.
        :param disposition: Content-Disposition token, normally attachment or inline.
        :param content_type_override: Truthy explicit media type, or automatic guess.
        :return: Streaming 200 response owning an open file handle until cleanup.
        :raises OSError: Opening or statting the path fails.
        """
        file_handle = path.open("rb")
        guessed_type, _encoding = mimetypes.guess_type(download_name)
        content_type = content_type_override or guessed_type or "application/octet-stream"
        content_length = path.stat().st_size
        wrapper = environ.get("wsgi.file_wrapper")
        if wrapper is None:
            body: Iterable[bytes] = FileWrapper(file_handle, blksize=64 * 1024)
        else:
            body = wrapper(file_handle, 64 * 1024)
        return _Response(
            status="200 OK",
            headers=[
                ("Content-Type", content_type),
                ("Content-Length", str(content_length)),
                ("Content-Disposition", '{disposition}; filename="{name}"'.format(
                    disposition=str(disposition),
                    name=download_name.replace('"', ""),
                )),
            ],
            body=body,
            close=file_handle.close,
        )

    def _bytes_response(
        self,
        payload: bytes,
        *,
        download_name: str,
        disposition: str = "attachment",
        content_type_override: Optional[str] = None,
    ) -> _Response:
        """
        Wrap an already-buffered payload with length and disposition headers.

        Payload bytes are retained without copying or coercion. A truthy MIME
        override wins over filename guessing and application/octet-stream.
        Only double quotes are removed from the suggested filename; this is
        neither header sanitization nor an acquisition-policy check.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> response = app._bytes_response(b"abc", download_name="note.txt")
            >>> dict(response.headers)["Content-Length"]
            '3'


        :param payload: Complete bytes object to place in a one-element body list.
        :param download_name: Suggested filename and fallback MIME-guess input.
        :param disposition: Content-Disposition token, normally attachment or inline.
        :param content_type_override: Truthy media type to use instead of guessing.
        :return: Buffered 200 response with no close callback.
        """
        guessed_type, _encoding = mimetypes.guess_type(download_name)
        content_type = content_type_override or guessed_type or "application/octet-stream"
        return _Response(
            status="200 OK",
            headers=[
                ("Content-Type", content_type),
                ("Content-Length", str(len(payload))),
                ("Content-Disposition", '{disposition}; filename="{name}"'.format(
                    disposition=str(disposition),
                    name=download_name.replace('"', ""),
                )),
            ],
            body=[payload],
        )

    def _all_tables(self) -> list[str]:
        """
        Materialize model table names without filtering or sorting them.

        Example:
            >>> tables = app._all_tables()  # doctest: +SKIP


        :return: New list in provider order; schema-read failures propagate.
        """
        return list(self.model.table_names())

    @staticmethod
    def _table_category(table: str) -> str:
        """
        Classify a stripped, lowercased table name for navigation grouping.

        Explicit main/helper names take precedence over relationship suffixes.
        Unknown and empty names default to helper; classification is not access
        control and does not check whether a table actually exists.

        Example:
            >>> [ReadOnlyWebApplication._table_category(name) for name in ("WORKS", "work_intralinks", "work_links", "custom")]
            ['main', 'intralink', 'interlink', 'helper']


        :param table: Table identifier to classify after string conversion.
        :return: Main, helper, interlink, or intralink category token in lowercase.
        """
        name = str(table).strip().lower()
        if not name:
            return "helper"

        main_tables = {
            "agents",
            "annotations",
            "comments",
            "devices",
            "expressions",
            "files",
            "folders",
            "genres",
            "human_agents",
            "images",
            "items",
            "labels",
            "languages",
            "manifestations",
            "notes",
            "org_agents",
            "ratings",
            "series",
            "stores",
            "subjects",
            "synopses",
            "tags",
            "works",
        }
        helper_tables = {
            "compressed_files",
            "conversion_options",
            "custom_columns",
            "database_metadata",
            "database_version",
            "feeds",
            "file_workflow",
            "hashes",
            "last_read_positions",
            "library_id",
            "metadata_dirtied_books",
            "new_books",
            "preferences",
            "workflow_states",
            "workflow_steps",
            "works_plugin_data",
            "expressions_plugin_data",
            "manifestations_plugin_data",
            "items_plugin_data",
        }
        interlink_tables = {
            "entity_identifiers",
            "file_derivations",
            "file_workflow_events",
            "item_identifiers",
            "item_workflow",
            "item_workflow_events",
            "org_agent_relations",
            "transform_run_inputs",
            "transform_run_outputs",
        }

        if name in main_tables:
            return "main"
        if name in helper_tables:
            return "helper"
        if name.endswith(("_intralinks", "_intralink")):
            return "intralink"
        if name in interlink_tables:
            return "interlink"
        if name.endswith(("_links", "_link", "_relations", "_relation", "_identifiers", "_derivations")):
            return "interlink"
        if name.endswith(("_workflow", "_workflow_events", "_plugin_data", "_states", "_steps")):
            return "helper"
        return "helper"

    def _grouped_tables(self) -> dict[str, list[str]]:
        """
        Place every model table into one of four navigation groups.

        Original names and provider order are preserved within each group;
        even empty groups remain present.

        Example:
            >>> groups = app._grouped_tables()  # doctest: +SKIP


        :return: Main/helper/interlink/intralink lists, including operational tables.
        """
        groups = {"main": [], "helper": [], "interlink": [], "intralink": []}
        for table in self._all_tables():
            groups.setdefault(self._table_category(table), []).append(table)
        return groups

    def _table_exists(self, table: str) -> bool:
        """
        Check exact string membership in a freshly enumerated schema.

        Example:
            >>> exists = app._table_exists("works")  # doctest: +SKIP


        :param table: Identifier stringified without stripping or case normalization.
        :return: Whether the model exposes that exact name; read failures propagate.
        """
        return str(table) in set(self._all_tables())

    def _id_column(self, table: str) -> Optional[str]:
        """
        Ask for the model's ID column only when its column list is nonempty.

        Column enumeration and ID lookup are separate reads, not a snapshot.

        Example:
            >>> column = app._id_column("works")  # doctest: +SKIP


        :param table: Table identifier passed unchanged to schema queries.
        :return: Provider-selected ID column, or None for an empty column list.
        """
        columns = list(self.model.columns(table))
        if not columns:
            return None
        return self.model.id_column(table)

    def _visible_columns(self, table: str) -> list[str]:
        """
        Filter schema columns through configured suffix and substring exclusions.

        Only column names are lowercased for matching; custom exclusion tokens
        should therefore be lowercase. This display heuristic neither inspects
        cell contents nor constrains all preferred-summary reads.

        Example:
            >>> columns = app._visible_columns("stores")  # doctest: +SKIP


        :param table: Table whose model-provided columns should be screened.
        :return: Original stringified names in provider order, excluding matches.
        """
        result: list[str] = []
        for column in self.model.columns(table):
            name = str(column)
            lowered = name.lower()
            if any(lowered.endswith(suffix) for suffix in self.config.hidden_column_suffixes):
                continue
            if any(token in lowered for token in self.config.hidden_column_tokens):
                continue
            result.append(name)
        return result

    def _table_display_columns(self, table: str) -> list[str]:
        """
        Select up to eight visible columns for compact browse tables.

        Prefer a visible ID, then configured-in-code descriptive keyword order,
        then remaining schema order. A column is included only once.

        Example:
            >>> columns = app._table_display_columns("works")  # doctest: +SKIP


        :param table: Schema context for visibility, identity, and display ordering.
        :return: At most eight visible column names, or an empty list.
        """
        columns = self._visible_columns(table)
        if not columns:
            return []
        id_column = self._id_column(table)
        preferred_tokens = (
            "name",
            "title",
            "label",
            "kind",
            "type",
            "status",
            "role",
            "medium",
            "base_name",
            "extension",
            "storage_key",
            "source",
            "tag",
        )
        ordered: list[str] = []
        if id_column and id_column in columns:
            ordered.append(id_column)
        for token in preferred_tokens:
            for column in columns:
                if column in ordered:
                    continue
                if token in column.lower():
                    ordered.append(column)
        for column in columns:
            if column not in ordered:
                ordered.append(column)
        return ordered[:8]

    @staticmethod
    def _pretty_table_name(table: str) -> str:
        """
        Replace underscores with spaces and uppercase only the first character.

        Example:
            >>> ReadOnlyWebApplication._pretty_table_name("human_agents")
            'Human agents'
            >>> ReadOnlyWebApplication._pretty_table_name("  ")
            'Related'


        :param table: Name stringified and stripped after underscore replacement.
        :return: Navigation label, or Related when the resulting text is empty.
        """
        text = str(table).replace("_", " ").strip()
        if not text:
            return "Related"
        return text[0].upper() + text[1:]

    def _preferred_summary_fields(self, table: str) -> tuple[str, ...]:
        """
        Return conventional descriptive fields for an exact known table name.

        These candidates are not checked against the schema or hidden-column
        policy. Callers needing visible-only fields must intersect separately.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._preferred_summary_fields("works")[0]
            'work_title'
            >>> app._preferred_summary_fields("unknown")
            ()


        :param table: Exact table token; no case normalization is applied.
        :return: Ordered field-name tuple, or empty tuple for an unknown table.
        """
        mapping = {
            "works": ("work_title", "work_canonical_title", "work_sort_title"),
            "stores": ("store_name", "store_kind", "store_root_uri"),
            "labels": ("label_text", "label", "label_text_norm"),
            "tags": ("tag", "tag_phash", "label_text", "label"),
            "notes": ("note", "note_text", "note_body"),
            "comments": ("comment", "comment_text", "comment_body", "note"),
            "synopses": ("synopsis", "synopsis_text", "note", "note_text"),
            "folders": ("folder_name", "folder_relpath"),
            "files": ("file_name", "file_original_name", "file_storage_key"),
            "agents": ("agent_canonical_name", "agent_name", "agent_sort_name"),
            "human_agents": ("agent_canonical_name", "agent_name", "agent_sort_name"),
            "org_agents": ("agent_canonical_name", "agent_name", "agent_sort_name"),
            "series": ("series", "series_sort", "series_name_norm"),
            "genres": ("genre", "genre_text", "genre_name"),
            "subjects": ("subject", "subject_text", "subject_name"),
            "languages": ("language", "language_name", "language_code"),
            "expressions": ("expression_label", "expression_title_override", "expression_type"),
            "manifestations": ("manifestation_label", "manifestation_title", "manifestation_type"),
            "items": ("item_source_name", "item_inventory_code", "item_location"),
        }
        return mapping.get(table, ())

    def _row_summary_parts(self, table: str, row, *, limit: int = 3) -> list[str]:
        """
        Collect distinct nonblank display values, preferring conventional fields.

        Preferred fields bypass the visibility filter. Fallback visible columns
        are ordered by descriptive keyword then name, excluding the ID. Values
        are shortened to width 72 and stripped before case-sensitive deduplication.
        The limit is checked after appending, so a nonpositive limit may yield one
        value rather than none.

        Example:
            >>> parts = app._row_summary_parts("works", work, limit=2)  # doctest: +SKIP


        :param table: Schema and preferred-field context for the row.
        :param row: Row-like object read through the shared row-value accessor.
        :param limit: Desired part count; expected positive and not validated here.
        :return: Ordered short text parts, possibly empty, without HTML escaping.
        """
        parts: list[str] = []
        seen: set[str] = set()
        id_column = self._id_column(table)

        for column in self._preferred_summary_fields(table):
            text = _short_text(_row_value(row, column), width=72).strip()
            if not text or text in seen:
                continue
            parts.append(text)
            seen.add(text)
            if len(parts) >= limit:
                return parts

        keyword_priority = ("name", "title", "tag", "label", "note", "text", "path", "uri", "kind", "type", "location", "code")
        ordered_columns = sorted(
            self._visible_columns(table),
            key=lambda key: (
                min((idx for idx, token in enumerate(keyword_priority) if token in str(key).lower()), default=len(keyword_priority)),
                str(key),
            ),
        )
        for column in ordered_columns:
            if id_column and column == id_column:
                continue
            text = _short_text(_row_value(row, column), width=72).strip()
            if not text or text in seen:
                continue
            parts.append(text)
            seen.add(text)
            if len(parts) >= limit:
                break
        return parts

    def _row_primary_text(self, table: str, row) -> str:
        """
        Choose one summary value, falling back to the composite row label.

        Example:
            >>> title = app._row_primary_text("works", work)  # doctest: +SKIP


        :param table: Schema/display context used by both label strategies.
        :param row: Row-like value whose descriptive fields should be read.
        :return: Unescaped primary text or fallback label, not a fetched full record.
        """
        parts = self._row_summary_parts(table, row, limit=1)
        if parts:
            return parts[0]
        return self._row_label(table, row)

    @staticmethod
    def _stringify_detail_value(value: object) -> str:
        """
        Convert a cell to display text, shortening common container representations.

        Dict/list/tuple/set reprs use width 400; other non-None objects are
        stringified without a length cap. This is not JSON serialization.

        Example:
            >>> ReadOnlyWebApplication._stringify_detail_value([1, 2])
            '[1, 2]'
            >>> ReadOnlyWebApplication._stringify_detail_value(None)
            ''


        :param value: Raw metadata cell, potentially None or a container.
        :return: Unescaped display text, empty only for None or empty stringification.
        """
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple, set)):
            return _short_text(repr(value), width=400)
        return str(value)

    @staticmethod
    def _detail_value_kind(column: str) -> str:
        """
        Infer a presentation kind from column-name substrings and suffixes.

        JSON takes precedence over millisecond timestamps, then URI, then path;
        no schema type or actual cell content is examined.

        Example:
            >>> [ReadOnlyWebApplication._detail_value_kind(name) for name in ("source_json", "created_timestamp_ep_k", "root_uri", "storage_key", "title")]
            ['json', 'timestamp_ms', 'uri', 'path', 'text']


        :param column: Falsey-as-empty column identifier, compared lowercase.
        :return: json, timestamp_ms, uri, path, or text presentation token.
        """
        lowered = str(column or "").lower()
        if "json" in lowered:
            return "json"
        if lowered.endswith("_timestamp_ep_k") or (lowered.endswith("_ep_k") and "datestamp" in lowered):
            return "timestamp_ms"
        if any(token in lowered for token in ("uri", "url")):
            return "uri"
        if any(token in lowered for token in ("path", "location", "storage_key")):
            return "path"
        return "text"

    @staticmethod
    def _pretty_json_text(value: object) -> str:
        """
        Pretty-print parseable JSON text, retaining original text on parse failure.

        Successful output uses sorted keys, two-space indentation, and unescaped
        Unicode. Standard json nonfinite-number behavior is unchanged. Invalid
        JSON retains surrounding whitespace; blank or falsey input returns empty.

        Example:
            >>> ReadOnlyWebApplication._pretty_json_text('  "café"  ')
            '"café"'
            >>> ReadOnlyWebApplication._pretty_json_text(' invalid ')
            ' invalid '


        :param value: Value interpreted through str(value or empty string), not dumped directly.
        :return: Reformatted JSON, original invalid text, or an empty string.
        """
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            parsed = json.loads(text)
        except Exception:
            return str(value or "")
        return json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)

    @staticmethod
    def _format_epoch_ms_value(value: object) -> tuple[str, str] | None:
        """
        Interpret numeric text as epoch milliseconds and format it at UTC minute precision.

        Conversion passes through float and int, so fractions are truncated and
        large values may lose precision. Numeric-conversion and datetime-range
        failures return None; errors during initial string conversion do not.

        Example:
            >>> ReadOnlyWebApplication._format_epoch_ms_value(" 0 ")
            ('1970-01-01 00:00 UTC', '0')
            >>> ReadOnlyWebApplication._format_epoch_ms_value("invalid") is None
            True


        :param value: Millisecond timestamp-like value; None and blank mean absent.
        :return: Human UTC label and stripped original text, or None if unsupported.
        """
        if value in (None, ""):
            return None
        raw = str(value).strip()
        if not raw:
            return None
        try:
            epoch_ms = int(float(raw))
        except Exception:
            return None
        try:
            pretty = datetime.fromtimestamp(epoch_ms / 1000.0, UTC).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            return None
        return pretty, raw

    def _render_detail_value_html(self, *, column: str, value: object, code_values: bool) -> str:
        """
        Render an escaped detail cell using its name-derived presentation kind.

        Empty text becomes an em dash. JSON gets a preformatted block, valid
        timestamps a UTC label plus raw value, and paths/URIs code styling without
        links. JSON formatting uses the original value, not the container repr cap.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._render_detail_value_html(column="title", value="<x>", code_values=False)
            "<span class='field-value'>&lt;x&gt;</span>"


        :param column: Column name selecting JSON, timestamp, path, or plain display.
        :param value: Metadata cell to stringify and escape.
        :param code_values: Use code markup for otherwise ordinary nonempty values.
        :return: Trusted HTML fragment containing escaped metadata text.
        """
        value_text = self._stringify_detail_value(value)
        if value_text == "":
            return "<span class='empty'>&mdash;</span>"
        kind = self._detail_value_kind(column)
        if kind == "json":
            pretty_json = self._pretty_json_text(value)
            return "<pre class='field-value field-value-block'><code>{}</code></pre>".format(_escape(pretty_json))
        if kind == "timestamp_ms":
            formatted = self._format_epoch_ms_value(value)
            if formatted is not None:
                pretty, raw = formatted
                return "<div class='field-stack'><span class='field-value'>{}</span><div class='meta'><code>{}</code></div></div>".format(
                    _escape(pretty),
                    _escape(raw),
                )
        if kind in {"uri", "path"} or code_values:
            return "<code>{}</code>".format(_escape(value_text))
        return "<span class='field-value'>{}</span>".format(_escape(value_text))

    def _render_browse_value_html(self, *, column: str, value: object) -> str:
        """
        Render an escaped compact browse cell with kind-specific shortening.

        JSON uses width 140, paths/URIs 120, and ordinary text 72. Timestamps
        retain an unshortened raw value alongside their formatted UTC label.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._render_browse_value_html(column="title", value="A & B")
            'A &amp; B'


        :param column: Column identifier used to infer presentation kind.
        :param value: Raw cell value, with empty text rendered as an em dash.
        :return: Escaped text or trusted markup wrapping escaped text.
        """
        value_text = self._stringify_detail_value(value)
        if value_text == "":
            return "<span class='empty'>&mdash;</span>"
        kind = self._detail_value_kind(column)
        if kind == "json":
            pretty_json = _short_text(self._pretty_json_text(value), width=140)
            return "<code>{}</code>".format(_escape(pretty_json))
        if kind == "timestamp_ms":
            formatted = self._format_epoch_ms_value(value)
            if formatted is not None:
                pretty, raw = formatted
                return "<div class='field-stack'><span class='field-value'>{}</span><div class='meta'><code>{}</code></div></div>".format(
                    _escape(_short_text(pretty, width=72)),
                    _escape(raw),
                )
        if kind in {"uri", "path"}:
            return "<code>{}</code>".format(_escape(_short_text(value_text, width=120)))
        return _escape(_short_text(value_text, width=72))

    def _row_dict(self, table: str, row) -> dict[str, object]:
        """
        Project all visible columns into a new dictionary without copying values.

        Example:
            >>> visible = app._row_dict("files", file_row)  # doctest: +SKIP


        :param table: Schema context defining visible keys and their order.
        :param row: Row-like value read through the shared accessor.
        :return: Shallow visible-column projection, including absent values as None.
        """
        return {column: _row_value(row, column) for column in self._visible_columns(table)}

    def _row_href(self, table: str, row) -> Optional[str]:
        """
        Build a table-detail URL when the row has a nonblank schema-selected ID.

        Both components are percent-quoted. IDs need not be integers here and
        zero is retained; existence is not checked.

        Example:
            >>> href = app._row_href("works", work)  # doctest: +SKIP


        :param table: Table identifier used for ID lookup and the URL component.
        :param row: Row-like object supplying the selected ID value.
        :return: Root-relative detail URL, or None for missing ID column/value.
        """
        id_column = self._id_column(table)
        if not id_column:
            return None
        row_id = _row_value(row, id_column)
        if row_id in (None, ""):
            return None
        return "/tables/{}/{}".format(quote(table, safe=""), quote(str(row_id), safe=""))

    def _row_label(self, table: str, row) -> str:
        """
        Join a visible ID and up to three total short display parts with separators.

        The ID is prefixed with #. Non-ID fields follow compact display-column
        order and are shortened to width 72; only None and empty strings are
        skipped before shortening. This label is not HTML-escaped.

        Example:
            >>> label = app._row_label("works", work)  # doctest: +SKIP


        :param table: Schema/display context and fallback label if no parts exist.
        :param row: Row-like object from which visible values are projected.
        :return: Composite label, or the supplied table name when no parts exist.
        """
        row_data = self._row_dict(table, row)
        id_column = self._id_column(table)
        label_parts: list[str] = []
        if id_column and row_data.get(id_column) not in (None, ""):
            label_parts.append("#{}".format(row_data.get(id_column)))
        for column in self._table_display_columns(table):
            if column == id_column:
                continue
            value = row_data.get(column)
            if value in (None, ""):
                continue
            label_parts.append(_short_text(value, width=72))
            if len(label_parts) >= 3:
                break
        return " | ".join(label_parts) if label_parts else table

    def _public_search_tables(self) -> list[str]:
        """
        Choose existing preferred search tables, otherwise all main-category tables.

        Finding even one preferred table excludes other main tables from this
        default selection. The fallback performs a second schema enumeration.
        Explicit table browsing is not constrained by this search preference.

        Example:
            >>> searchable = app._public_search_tables()  # doctest: +SKIP


        :return: Preferred-order names, or provider-order main names if none preferred exist.
        """
        preferred = ("works", "agents", "human_agents", "org_agents", "series", "tags", "labels", "genres", "subjects", "files", "stores")
        available = set(self._all_tables())
        ordered = [table for table in preferred if table in available]
        if ordered:
            return ordered
        return [table for table in self._all_tables() if self._table_category(table) == "main"]

    def _search_candidate_columns(self, table: str) -> list[str]:
        """
        Select up to ten visible fields for per-row global-search scoring.

        Prefer conventional summary fields only when visible, then descriptive
        keyword matches in schema order, then remaining columns without duplicates.

        Example:
            >>> candidates = app._search_candidate_columns("works")  # doctest: +SKIP


        :param table: Schema context for visibility and summary-field preferences.
        :return: Ordered visible candidate names, capped at ten.
        """
        columns = self._visible_columns(table)
        if not columns:
            return []

        ordered: list[str] = []
        for column in self._preferred_summary_fields(table):
            if column in columns and column not in ordered:
                ordered.append(column)

        keyword_tokens = ("name", "title", "tag", "label", "note", "text", "canonical", "sort", "path", "uri", "source")
        for column in columns:
            lowered = str(column).lower()
            if column in ordered:
                continue
            if any(token in lowered for token in keyword_tokens):
                ordered.append(column)

        for column in columns:
            if column not in ordered:
                ordered.append(column)
        return ordered[:10]

    @staticmethod
    def _table_search_bonus(table: str) -> int:
        """
        Return the fixed relevance bias for an exact recognized table token.

        Example:
            >>> [ReadOnlyWebApplication._table_search_bonus(t) for t in ("works", "files", "unknown")]
            [40, 8, 0]


        :param table: Identifier stringified without stripping or case normalization.
        :return: Nonnegative score bonus, zero for an unrecognized name.
        """
        bonus = {
            "works": 40,
            "agents": 30,
            "human_agents": 28,
            "org_agents": 28,
            "series": 24,
            "tags": 20,
            "labels": 18,
            "genres": 16,
            "subjects": 16,
            "files": 8,
            "stores": 6,
        }
        return int(bonus.get(str(table), 0))

    @staticmethod
    def _highlight_text(text: object, terms: list[str]) -> str:
        """
        Escape text and wrap nonoverlapping literal, case-insensitive matches in mark tags.

        Distinct nonempty terms are tried longest first with regex IGNORECASE.
        Unlike ranking, highlighting does not NFKC-normalize or case-fold text.
        Regex metacharacters in terms are treated literally.

        Example:
            >>> ReadOnlyWebApplication._highlight_text("<A+B> & a", ["a+b"])
            '&lt;<mark>A+B</mark>&gt; &amp; a'


        :param text: Falsey-as-empty source to stringify and escape.
        :param terms: Literal search strings; empty strings are ignored.
        :return: Escaped HTML with generated mark elements, or plain escaped text.
        """
        source = str(text or "")
        filtered_terms = [term for term in terms if term]
        if not source or not filtered_terms:
            return _escape(source)
        pattern = re.compile("|".join(re.escape(term) for term in sorted(set(filtered_terms), key=len, reverse=True)), re.IGNORECASE)
        chunks: list[str] = []
        last = 0
        for match in pattern.finditer(source):
            chunks.append(_escape(source[last : match.start()]))
            chunks.append("<mark>{}</mark>".format(_escape(match.group(0))))
            last = match.end()
        chunks.append(_escape(source[last:]))
        return "".join(chunks)

    @staticmethod
    def _extract_snippet(text: object, terms: list[str], *, width: int = 120) -> str:
        """
        Select a text window near the earliest lowercase term match.

        The window begins up to max(10, width // 4) characters before the match.
        Added ellipses can exceed width by six characters. Missing terms use the
        shared short-text fallback; width is not validated. Lowercase matching
        does not implement the Unicode normalization used by ranking.

        Example:
            >>> ReadOnlyWebApplication._extract_snippet("A small book", ["book"], width=20)
            'A small book'


        :param text: Falsey-as-empty source text to crop without HTML escaping.
        :param terms: Literal strings searched case-insensitively, ignoring empties.
        :param width: Desired source-window length in characters before ellipses.
        :return: Unescaped snippet, shortened fallback, or empty text.
        """
        source = str(text or "")
        if not source:
            return ""
        filtered_terms = [term for term in terms if term]
        if not filtered_terms:
            return _short_text(source, width=width)

        lowered = source.lower()
        positions = [lowered.find(term.lower()) for term in filtered_terms if lowered.find(term.lower()) >= 0]
        if not positions:
            return _short_text(source, width=width)
        start = max(0, min(positions) - max(10, width // 4))
        end = min(len(source), start + width)
        snippet = source[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(source):
            snippet = snippet + "..."
        return snippet

    def _global_search_entry(self, table: str, row, query_text: str) -> Optional[dict[str, object]]:
        """
        Score a row across primary text, composite label, and candidate columns.

        NFKC/case-folded exact, prefix, all-term, and partial matches receive
        additive weights plus the table bonus. Candidate-column weights decrease
        with position. The first matching column supplies the snippet source;
        display-only matches can leave match_column empty. Backend read failures
        propagate rather than making a row appear unmatched.

        Example:
            >>> entry = app._global_search_entry("works", work, "ocean")  # doctest: +SKIP


        :param table: Table used for display hooks, searchable fields, and score bias.
        :param row: Original row retained in a successful result, not copied.
        :param query_text: Query whose stripped normalized form and tokens drive matching.
        :return: Table/row/score/match_column/snippet/sort_key mapping, or None for blank/no match.
        """
        needle = _normalized_search_text(str(query_text or "").strip())
        terms = _search_terms(query_text)
        normalized_terms = [_normalized_search_text(term) for term in terms]
        if not needle:
            return None

        primary_text = self._row_primary_text(table, row)
        summary_text = self._row_label(table, row)
        primary_lower = _normalized_search_text(primary_text)
        summary_lower = _normalized_search_text(summary_text)

        score = self._table_search_bonus(table)
        match_column = ""
        snippet_source = summary_text
        found = False

        if primary_lower == needle:
            score += 500
            found = True
        elif any(primary_lower == term for term in normalized_terms):
            score += 420
            found = True
        elif primary_lower.startswith(needle):
            score += 360
            found = True
        elif all(term in primary_lower for term in normalized_terms):
            score += 300
            found = True
        elif needle in primary_lower:
            score += 260
            found = True
        elif any(term in primary_lower for term in normalized_terms):
            score += 220
            found = True

        if summary_lower == needle:
            score += 180
            found = True
        elif all(term in summary_lower for term in normalized_terms):
            score += 120
            found = True
            snippet_source = summary_text
        elif needle in summary_lower:
            score += 100
            found = True
            snippet_source = summary_text

        for index, column in enumerate(self._search_candidate_columns(table)):
            text = self._stringify_detail_value(_row_value(row, column))
            lowered = _normalized_search_text(text)
            if not lowered:
                continue
            column_score = None
            if lowered == needle:
                column_score = 200 - index
            elif lowered.startswith(needle):
                column_score = 150 - index
            elif all(term in lowered for term in normalized_terms):
                column_score = 110 - index
            elif needle in lowered:
                column_score = 90 - index
            elif any(term in lowered for term in normalized_terms):
                column_score = 60 - index
            if column_score is None:
                continue
            score += column_score
            if not match_column:
                match_column = str(column)
                snippet_source = text
            found = True

        if not found:
            return None

        snippet = self._extract_snippet(snippet_source, terms, width=140)
        return {
            "table": str(table),
            "row": row,
            "score": int(score),
            "match_column": match_column,
            "snippet": snippet,
            "sort_key": (-int(score), str(table), self._row_label(table, row).lower()),
        }

    def _global_search_entries(self, query_text: str, *, table_filter: str = "") -> list[dict[str, object]]:
        """
        Delegate full ranked search enumeration to the shared read-model backend.

        Example:
            >>> entries = app._global_search_entries("ocean", table_filter="works")  # doctest: +SKIP


        :param query_text: Search text forwarded without local normalization.
        :param table_filter: Optional table restriction interpreted by the backend.
        :return: Backend's ordered entry list; no local pagination or error suppression.
        """
        return self.read_model.search_entries(query_text, table_filter=table_filter)

    @staticmethod
    def _group_search_entries(entries: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
        """
        Group raw search entries by their required, stringified table key.

        Example:
            >>> entry = {"table": "works", "score": 3}
            >>> ReadOnlyWebApplication._group_search_entries([entry])["works"][0] is entry
            True


        :param entries: Raw result mappings, each required to contain table.
        :return: First-seen table groups preserving input order and entry identity.
        :raises KeyError: An entry lacks the required table key.
        """
        grouped: dict[str, list[dict[str, object]]] = {}
        for entry in entries:
            grouped.setdefault(str(entry["table"]), []).append(entry)
        return grouped

    @staticmethod
    def _group_search_result_payload(entries: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
        """
        Group projected search results, using an empty key for missing/falsey tables.

        Example:
            >>> ReadOnlyWebApplication._group_search_result_payload([{}])
            {'': [{}]}


        :param entries: Projected result mappings whose table field is optional.
        :return: Insertion-ordered groups retaining original mapping objects and order.
        """
        grouped: dict[str, list[dict[str, object]]] = {}
        for entry in entries:
            grouped.setdefault(str(entry.get("table") or ""), []).append(entry)
        return grouped

    def _render_pager(
        self,
        *,
        path: str,
        query_values: dict[str, object],
        offset: int,
        limit: int,
        total: int,
        offset_key: str,
    ) -> str:
        """
        Render Previous/Next links and numeric range information for a result page.

        Query mappings are copied before changing the offset. Oversized offsets
        are not clamped and can display an inverted range. Nonpositive limits
        avoid division but do not create meaningful paging; callers should pass
        a positive limit. An empty/nonpositive total produces no markup.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> html = app._render_pager(path="/search", query_values={"q": "a"}, offset=0, limit=10, total=11, offset_key="offset")
            >>> "Next" in html and "Showing 1-10 of 11" in html
            True


        :param path: Link path without an existing query; a question mark is appended.
        :param query_values: Scalar query parameters retained in navigation URLs.
        :param offset: Zero-based start, expected nonnegative but not validated here.
        :param limit: Requested page length used for range and navigation arithmetic.
        :param total: Total matching records, not merely the visible count.
        :param offset_key: Query key to replace in copied Previous/Next parameters.
        :return: Escaped-link HTML and range text, or an empty string.
        """
        if total <= 0:
            return ""

        start = offset + 1 if total else 0
        end = min(total, offset + limit)
        page = (offset // limit) + 1 if limit > 0 else 1
        pages = max(1, ((total - 1) // limit) + 1) if limit > 0 else 1
        links: list[str] = []

        if offset > 0:
            prev_values = dict(query_values)
            prev_values[offset_key] = max(0, offset - limit)
            href = "{}?{}".format(path, _build_query_string(prev_values))
            links.append("<a href='{href}'>Previous</a>".format(href=_escape(href)))
        if end < total:
            next_values = dict(query_values)
            next_values[offset_key] = offset + limit
            href = "{}?{}".format(path, _build_query_string(next_values))
            links.append("<a href='{href}'>Next</a>".format(href=_escape(href)))

        return """
<div class='actions pager'>{links}</div>
<p class='meta'>Showing {start}-{end} of {total} results. Page {page} of {pages}.</p>
""".format(
            links="".join(links) if links else "<span class='pill'>end of results</span>",
            start=start,
            end=end,
            total=total,
            page=page,
            pages=pages,
        )

    @staticmethod
    def _pretty_credit_role(value: object) -> str:
        """
        Translate known contributor roles and prettify unknown role tokens.

        Unknown alphabetic tokens up to four characters become uppercase;
        others replace underscores/hyphens with spaces and use title case.

        Example:
            >>> [ReadOnlyWebApplication._pretty_credit_role(v) for v in ("aut", "xyz", "guest_editor", "")]
            ['Author', 'XYZ', 'Guest Editor', 'Contributors']


        :param value: Falsey-as-empty role string, stripped before classification.
        :return: Unescaped human role label, with Contributors for blank input.
        """
        text = str(value or "").strip()
        if not text:
            return "Contributors"
        lowered = text.lower()
        mapping = {
            "aut": "Author",
            "author": "Author",
            "trl": "Translator",
            "translator": "Translator",
            "primary": "Primary contributors",
            "secondary": "Secondary contributors",
            "incidental": "Incidental contributors",
            "edt": "Editor",
            "editor": "Editor",
            "ill": "Illustrator",
            "illustrator": "Illustrator",
            "artist": "Artist",
            "nrt": "Narrator",
            "narrator": "Narrator",
            "compiler": "Compiler",
        }
        if lowered in mapping:
            return mapping[lowered]
        if len(lowered) <= 4 and lowered.isalpha():
            return text.upper()
        return text.replace("_", " ").replace("-", " ").title()

    def _work_credit_entries(self, row) -> list[dict[str, object]]:
        """
        Delegate work-credit discovery and ordering to the shared read model.

        Example:
            >>> credits = app._work_credit_entries(work)  # doctest: +SKIP


        :param row: Work row whose linked contributor credits should be read.
        :return: Backend credit-entry list; errors propagate without local fallback.
        """
        return self.read_model.work_credit_entries(row)

    def _render_work_credits_section(self, row) -> str:
        """
        Read and render work credits in first-seen role groups.

        Entry order is retained, not sorted here despite the priority-order
        caption. Table/row/role fields are required; links and labels come from
        host hooks. Scalar labels are escaped, but link destinations are not
        independently validated. Every returned credit is displayed.

        Example:
            >>> html = app._render_work_credits_section(work)  # doctest: +SKIP


        :param row: Work row passed to shared credit discovery.
        :return: Contributor section HTML, or empty text when no credits exist.
        """
        entries = self._work_credit_entries(row)
        if not entries:
            return ""

        grouped: dict[str, list[dict[str, object]]] = {}
        order: list[str] = []
        for entry in entries:
            role = str(entry["role"])
            if role not in grouped:
                grouped[role] = []
                order.append(role)
            grouped[role].append(entry)

        sections: list[str] = []
        for role in order:
            cards: list[str] = []
            for entry in grouped[role]:
                linked_table = str(entry["table"])
                linked_row = entry["row"]
                href = self._row_href(linked_table, linked_row)
                label = _escape(self._row_primary_text(linked_table, linked_row))
                agent_type = _short_text(_row_value(linked_row, "agent_type"), width=48).strip()
                actions = "<a href='{}'>open</a>".format(_escape(href)) if href else ""
                subtitle_parts = []
                if agent_type:
                    subtitle_parts.append(_escape(agent_type))
                priority_value = entry.get("priority")
                if priority_value not in (None, ""):
                    subtitle_parts.append("priority {}".format(_escape(priority_value)))
                subtitle = " · ".join(subtitle_parts)
                cards.append(
                    """
<article class='related-card credit-card'>
  <strong>{label}</strong>
  {subtitle}
  <div class='actions'>{actions}</div>
</article>
""".format(
                        label=label,
                        subtitle=("<p class='meta'>{}</p>".format(subtitle) if subtitle else ""),
                        actions=actions,
                    )
                )
            sections.append(
                """
<section class='panel related-section credit-group'>
  <h3>{title}</h3>
  <p class='meta'>count={count}</p>
  <div class='related-card-grid'>{cards}</div>
</section>
""".format(title=_escape(role), count=len(grouped[role]), cards="".join(cards))
            )

        return """
<section class='panel'>
  <h2>Credits</h2>
  <p class='meta'>Contributors linked to this work, ordered by interlink priority where available.</p>
  {sections}
</section>
""".format(sections="".join(sections))

    def _render_work_credits_payload_section(self, detail_payload: dict[str, object]) -> str:
        """
        Render precomputed contributor credits without querying their row objects.

        Entries are shallow-copied into first-seen role groups; absent/falsey roles
        use Contributors. Entity-provided links, labels, types, and priorities are
        escaped for markup, not validated as trusted URL destinations. Ordering
        and completeness belong to the payload provider.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._render_work_credits_payload_section({"credits": []})
            ''


        :param detail_payload: Mapping containing optional credit mappings with entity summaries.
        :return: Credits section HTML, or empty text when the credit list is empty.
        """
        entries = list(detail_payload.get("credits", []) or [])
        if not entries:
            return ""

        grouped: dict[str, list[dict[str, object]]] = {}
        order: list[str] = []
        for entry in entries:
            role = str(entry.get("role") or "Contributors")
            if role not in grouped:
                grouped[role] = []
                order.append(role)
            grouped[role].append(dict(entry))

        sections: list[str] = []
        for role in order:
            cards: list[str] = []
            for entry in grouped[role]:
                entity = dict(entry.get("entity") or {})
                href = str(entity.get("html_url") or "")
                label = _escape(entity.get("primary") or entity.get("label") or "")
                subtitle_parts: list[str] = []
                entity_type = str(entry.get("entity_type") or "").strip()
                if entity_type:
                    subtitle_parts.append(_escape(entity_type))
                elif entity.get("table"):
                    subtitle_parts.append(_escape(self._pretty_table_name(str(entity["table"]))))
                if entry.get("priority") not in (None, ""):
                    subtitle_parts.append("priority {}".format(_escape(entry["priority"])))
                subtitle = " · ".join(subtitle_parts)
                actions = "<a href='{}'>open</a>".format(_escape(href)) if href else ""
                cards.append(
                    """
<article class='related-card credit-card'>
  <strong>{label}</strong>
  {subtitle}
  <div class='actions'>{actions}</div>
</article>
""".format(
                        label=label,
                        subtitle=("<p class='meta'>{}</p>".format(subtitle) if subtitle else ""),
                        actions=actions,
                    )
                )
            sections.append(
                """
<section class='panel related-section credit-group'>
  <h3>{title}</h3>
  <p class='meta'>count={count}</p>
  <div class='related-card-grid'>{cards}</div>
</section>
""".format(title=_escape(role), count=len(grouped[role]), cards="".join(cards))
            )

        return """
<section class='panel'>
  <h2>Credits</h2>
  <p class='meta'>Contributors linked to this work, ordered by interlink priority where available.</p>
  {sections}
</section>
""".format(sections="".join(sections))

    def _render_work_formats_section(self, detail_payload: dict[str, object]) -> str:
        """
        Render all projected work-file formats and their supplied acquisition links.

        Media, role, delivery, and nonblank size are display hints, not validated
        media facts. Download/preview links are escaped but not resolved, checked
        against the host download setting, or restricted to a URL scheme here.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._render_work_formats_section({"files": []})
            ''


        :param detail_payload: Work detail mapping with an optional files sequence.
        :return: Format-card section HTML, or empty text for no projected files.
        """
        files = list(detail_payload.get("files", []) or [])
        if not files:
            return ""
        cards: list[str] = []
        for file_payload in files:
            name = _escape(file_payload.get("name") or "file")
            subtitle_parts: list[str] = []
            for key in ("media_category", "role", "delivery"):
                value = str(file_payload.get(key) or "").strip()
                if value:
                    subtitle_parts.append(_escape(value))
            if file_payload.get("size") not in (None, ""):
                subtitle_parts.append("{} bytes".format(_escape(file_payload["size"])))
            actions: list[str] = []
            download_url = str(file_payload.get("download_url") or "")
            preview_url = str(file_payload.get("preview_url") or "")
            if download_url:
                actions.append("<a href='{}'>download</a>".format(_escape(download_url)))
            if preview_url:
                actions.append("<a href='{}'>preview</a>".format(_escape(preview_url)))
            cards.append(
                """
<article class='related-card'>
  <strong>{label}</strong>
  {subtitle}
  <div class='actions'>{actions}</div>
</article>
""".format(
                    label=name,
                    subtitle=("<p class='meta'>{}</p>".format(" · ".join(subtitle_parts)) if subtitle_parts else ""),
                    actions=(" ".join(actions) if actions else "<span class='empty'>No direct action</span>"),
                )
            )
        return """
<section class='panel'>
  <h2>Formats</h2>
  <p class='meta'>Files discovered for this work through related expressions, manifestations, items, and files.</p>
  <div class='related-card-grid'>{cards}</div>
</section>
""".format(cards="".join(cards))

    def _ordered_related_tables(self, row) -> list[str]:
        """
        Order unique related-table candidates by preferred group then name.

        A truthy row.linkable_tables supplies candidates; an empty value falls
        back to model discovery when row.table is present. Candidates are
        stringified, so None becomes the literal name None rather than being
        discarded. The current row's table and empty strings are excluded.

        Example:
            >>> tables = app._ordered_related_tables(work)  # doctest: +SKIP


        :param row: Object optionally exposing table and linkable_tables attributes.
        :return: Deterministically ordered unique names without fetching linked rows.
        """
        table = str(getattr(row, "table", "") or "")
        candidate_tables = list(
            getattr(row, "linkable_tables", None)
            or (self.model.related_tables(table) if table else ())
        )
        return sorted(
            {str(one) for one in candidate_tables if str(one) and str(one) != table},
            key=lambda name: (
                self._RELATED_TABLE_ORDER.index(name) if name in self._RELATED_TABLE_ORDER else len(self._RELATED_TABLE_ORDER),
                name,
            ),
        )

    def _render_related_pill_section(self, linked_table: str, rows: list[object]) -> str:
        """
        Render every linked row as a compact label pill, linked when an ID exists.

        Example:
            >>> html = app._render_related_pill_section("labels", labels)  # doctest: +SKIP


        :param linked_table: Schema context and humanized section title source.
        :param rows: Ordered linked rows to display without pagination or truncation.
        :return: Counted section HTML with escaped labels and root-relative row links.
        """
        pills = []
        for row in rows:
            href = self._row_href(linked_table, row)
            label = _escape(self._row_primary_text(linked_table, row))
            if href:
                pills.append("<a class='pill related-pill' href='{href}'>{label}</a>".format(href=_escape(href), label=label))
            else:
                pills.append("<span class='pill related-pill'>{}</span>".format(label))
        return """
<section class='panel related-section'>
  <h3>{title}</h3>
  <p class='meta'>count={count}</p>
  <div class='pill-list'>{items}</div>
</section>
""".format(title=_escape(self._pretty_table_name(linked_table)), count=len(rows), items="".join(pills))

    def _render_related_note_section(self, linked_table: str, rows: list[object]) -> str:
        """
        Render linked note-like rows as excerpt cards with composite labels.

        Each primary-text excerpt is shortened to width 280 after the primary
        label helper's own shortening. The list itself is not capped.

        Example:
            >>> html = app._render_related_note_section("notes", notes)  # doctest: +SKIP


        :param linked_table: Note-like table used for headings, links, and labels.
        :param rows: Linked rows retained in their supplied order.
        :return: Section HTML with escaped excerpts and linked or unlinked card labels.
        """
        cards = []
        for row in rows:
            href = self._row_href(linked_table, row)
            summary = _escape(_short_text(self._row_primary_text(linked_table, row), width=280))
            label = _escape(self._row_label(linked_table, row))
            actions = "<a href='{}'>open</a>".format(_escape(href)) if href else ""
            cards.append(
                """
<article class='related-card related-note-card'>
  <div class='related-card-meta'>{label}</div>
  <p>{summary}</p>
  <div class='actions'>{actions}</div>
</article>
""".format(label=label, summary=summary, actions=actions)
            )
        return """
<section class='panel related-section'>
  <h3>{title}</h3>
  <p class='meta'>count={count}</p>
  <div class='related-card-grid'>{items}</div>
</section>
""".format(title=_escape(self._pretty_table_name(linked_table)), count=len(rows), items="".join(cards))

    def _render_related_agent_section(self, linked_table: str, rows: list[object]) -> str:
        """
        Render all linked contributors with primary labels and optional agent types.

        Agent-type hints are shortened to width 48 and escaped; rows are neither
        grouped by credit role nor reordered in this generic related renderer.

        Example:
            >>> html = app._render_related_agent_section("agents", agents)  # doctest: +SKIP


        :param linked_table: Contributor table supplying identity and label context.
        :param rows: Linked contributor rows to display in input order.
        :return: Counted contributor-card section HTML without list pagination.
        """
        cards = []
        for row in rows:
            href = self._row_href(linked_table, row)
            label = _escape(self._row_primary_text(linked_table, row))
            agent_type = _short_text(_row_value(row, "agent_type"), width=48).strip()
            actions = "<a href='{}'>open</a>".format(_escape(href)) if href else ""
            cards.append(
                """
<article class='related-card'>
  <strong>{label}</strong>
  {subtitle}
  <div class='actions'>{actions}</div>
</article>
""".format(
                    label=label,
                    subtitle=("<p class='meta'>{}</p>".format(_escape(agent_type)) if agent_type else ""),
                    actions=actions,
                )
            )
        return """
<section class='panel related-section'>
  <h3>{title}</h3>
  <p class='meta'>count={count}</p>
  <div class='related-card-grid'>{items}</div>
</section>
""".format(title=_escape(self._pretty_table_name(linked_table)), count=len(rows), items="".join(cards))

    def _render_related_default_section(self, linked_table: str, rows: list[object]) -> str:
        """
        Render an uncapped linked-row list using escaped composite labels.

        Example:
            >>> html = app._render_related_default_section("items", items)  # doctest: +SKIP


        :param linked_table: Table used for section title, ID lookup, and labels.
        :param rows: Related rows retained in input order, linked when possible.
        :return: Counted list-section HTML, including a section for an empty list.
        """
        items = []
        for row in rows:
            href = self._row_href(linked_table, row)
            label = _escape(self._row_label(linked_table, row))
            if href:
                items.append("<li><a href='{href}'>{label}</a></li>".format(href=_escape(href), label=label))
            else:
                items.append("<li>{}</li>".format(label))
        return """
<section class='panel related-section'>
  <h3>{title}</h3>
  <p class='meta'>count={count}</p>
  <ul class='related-list'>{items}</ul>
</section>
""".format(title=_escape(self._pretty_table_name(linked_table)), count=len(rows), items="".join(items))

    def _related_rows_by_table(self, row) -> dict[str, list[object]]:
        """
        Delegate related-row grouping to the shared metadata read model.

        Example:
            >>> groups = app._related_rows_by_table(work)  # doctest: +SKIP


        :param row: Entity whose relationships should be resolved.
        :return: Backend's ordered table-to-rows mapping; read failures propagate.
        """
        return self.read_model.related_rows_by_table(row)

    def _render_related_sections(
        self,
        row,
        *,
        related_rows_by_table: Optional[dict[str, list[object]]] = None,
        exclude_tables: Optional[set[str]] = None,
    ) -> str:
        """
        Render related groups through pill, note, contributor, or generic owners.

        A falsey supplied mapping, including an explicit empty dict, triggers a
        fresh relationship query. Mapping order is preserved, excluded table
        names are exact, and empty row lists in a nonempty mapping still render
        sections. Neither rows nor groups are capped here.

        Example:
            >>> html = app._render_related_sections(work, exclude_tables={"agents"})  # doctest: +SKIP


        :param row: Entity used when relationships need to be queried.
        :param related_rows_by_table: Optional nonempty precomputed groups to reuse.
        :param exclude_tables: Table names stringified and omitted from output.
        :return: Linked-entities wrapper HTML, or empty text if no sections remain.
        """
        sections: list[str] = []
        pill_tables = {"tags", "labels", "genres", "subjects", "languages", "series"}
        note_tables = {"notes", "comments", "synopses", "annotations"}
        agent_tables = {"agents", "human_agents", "org_agents"}
        excluded = {str(one) for one in (exclude_tables or set())}

        related_rows_by_table = related_rows_by_table or self._related_rows_by_table(row)
        for linked_table, linked_rows in related_rows_by_table.items():
            if linked_table in excluded:
                continue
            if linked_table in pill_tables:
                sections.append(self._render_related_pill_section(linked_table, linked_rows))
            elif linked_table in note_tables:
                sections.append(self._render_related_note_section(linked_table, linked_rows))
            elif linked_table in agent_tables:
                sections.append(self._render_related_agent_section(linked_table, linked_rows))
            else:
                sections.append(self._render_related_default_section(linked_table, linked_rows))

        if not sections:
            return ""
        return """
<section class='panel'>
  <h2>Linked entities</h2>
  <p class='meta'>Rows connected to this record through interlink tables.</p>
  {sections}
</section>
""".format(sections="".join(sections))

    def _render_detail_table_rows(
        self,
        row_data: dict[str, object],
        columns: list[str],
        *,
        code_values: bool,
        include_empty: bool = True,
    ) -> str:
        """
        Render requested detail columns as escaped-name/value table rows.

        Column order and duplicates are retained. Missing keys act like None;
        empty suppression compares stringified text, not generic truthiness.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._render_detail_table_rows({}, ["title"], code_values=False, include_empty=False)
            ''


        :param row_data: Mapping of column names to raw display values.
        :param columns: Ordered names to render, including any intentional duplicates.
        :param code_values: Request code styling for ordinary nonempty detail values.
        :param include_empty: Keep em-dash rows for absent/empty values when true.
        :return: Concatenated tr elements without a surrounding table.
        """
        detail_rows: list[str] = []
        for column in columns:
            value_text = self._stringify_detail_value(row_data.get(column))
            if not include_empty and value_text == "":
                continue
            rendered = self._render_detail_value_html(column=column, value=row_data.get(column), code_values=code_values)
            detail_rows.append("<tr><td>{}</td><td>{}</td></tr>".format(_escape(column), rendered))
        return "".join(detail_rows)

    def _render_detail_card(
        self,
        *,
        title: str,
        row_data: dict[str, object],
        columns: list[str],
    ) -> str:
        """
        Wrap nonempty detail rows in a titled card with ordinary value styling.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app._render_detail_card(title="Identity", row_data={}, columns=["name"])
            ''


        :param title: Card heading escaped as text.
        :param row_data: Column/value mapping passed to detail-row rendering.
        :param columns: Ordered fields to include, with empty values omitted.
        :return: Card HTML, or empty text when no nonempty rows survive.
        """
        rows = self._render_detail_table_rows(row_data, columns, code_values=False, include_empty=False)
        if not rows:
            return ""
        return """
<section class='panel detail-card'>
  <h3>{title}</h3>
  <div class='table-wrap'>
    <table class='detail-table'>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>
""".format(title=_escape(title), rows=rows)

    def _render_work_detail_page(
        self,
        *,
        row,
        row_id: int,
        row_data: dict[str, object],
        actions: list[str],
        related_rows_by_table: dict[str, list[object]],
        detail_payload: dict[str, object],
    ) -> str:
        """
        Assemble a work detail fragment from row values and projected metadata.

        The hero prefers payload title, then stored title variants and identity.
        Visible fields are grouped into title, record, date, and remaining cards;
        empty cards disappear. Contributor and format sections use the payload,
        while other related sections use row groups and exclude contributor
        tables. At most six format labels become hero pills; related lists are
        not capped. Actions are trusted HTML, while metadata text is escaped.

        Example:
            >>> html = app._render_work_detail_page(row=work, row_id=1, row_data=values, actions=[], related_rows_by_table=related, detail_payload=payload)  # doctest: +SKIP


        :param row: Work row retained for relationship fallback when groups are empty.
        :param row_id: Display identity used in the hero and last-resort title.
        :param row_data: Preprojected values; conventional title fields are read directly.
        :param actions: Already-rendered action fragments concatenated without escaping.
        :param related_rows_by_table: Related row groups for hero counts and entity sections.
        :param detail_payload: Projected work, credits, formats, and file metadata.
        :return: Work-body HTML fragment, not the surrounding document layout.
        """
        metadata = dict(detail_payload.get("work") or {})
        title = self._stringify_detail_value(
            metadata.get("title") or row_data.get("work_title") or row_data.get("work_canonical_title") or row_data.get("work_sort_title") or row_id
        )
        canonical = self._stringify_detail_value(row_data.get("work_canonical_title"))
        sort_title = self._stringify_detail_value(row_data.get("work_sort_title"))
        summary = self._stringify_detail_value(metadata.get("summary"))

        hero_pills = ["<span class='pill'>work_id {}</span>".format(_escape(row_id))]
        for linked_table in ("tags", "labels", "genres", "subjects", "series", "agents", "files", "items"):
            linked_rows = related_rows_by_table.get(linked_table, [])
            if linked_rows:
                hero_pills.append(
                    "<span class='pill'>{label} {count}</span>".format(
                        label=_escape(self._pretty_table_name(linked_table)),
                        count=len(linked_rows),
                    )
                )
        formats = list(metadata.get("formats") or [])
        if formats:
            hero_pills.append("<span class='pill'>formats: {}</span>".format(_escape(", ".join(str(one) for one in formats[:6]))))

        all_columns = self._visible_columns("works")
        used: set[str] = set()

        title_columns = [column for column in ("work_title", "work_canonical_title", "work_sort_title") if column in row_data]
        used.update(title_columns)

        record_columns = [
            column
            for column in all_columns
            if column not in used
            and any(token in column.lower() for token in ("status", "type", "kind", "role", "source", "uuid"))
        ]
        used.update(record_columns)

        date_columns = [
            column
            for column in all_columns
            if column not in used
            and any(token in column.lower() for token in ("date", "time", "year", "created", "modified", "updated"))
        ]
        used.update(date_columns)

        other_columns = [
            column
            for column in all_columns
            if column not in used and column != "work_id"
        ]

        cards = [
            self._render_detail_card(title="Titles", row_data=row_data, columns=title_columns),
            self._render_detail_card(title="Record", row_data=row_data, columns=record_columns),
            self._render_detail_card(title="Dates", row_data=row_data, columns=date_columns),
            self._render_detail_card(title="Other metadata", row_data=row_data, columns=other_columns),
        ]
        cards = [card for card in cards if card]
        details_html = ""
        if cards:
            details_html = "<section class='detail-grid'>{}</section>".format("".join(cards))

        return """
<section class='panel work-hero'>
  <p class='eyebrow'>Work record</p>
  <h2 class='hero-title'>{title}</h2>
  {canonical}
  {sort_title}
  <div class='actions'>{actions}</div>
  <div class='pill-list'>{hero_pills}</div>
</section>
{details}
{related}
""".format(
            title=_escape(title),
            canonical=(
                "<p class='meta'><strong>Canonical title:</strong> {}</p>".format(_escape(canonical))
                if canonical and canonical != title
                else ""
            ),
            sort_title=(
                "<p class='meta'><strong>Sort title:</strong> {}</p>".format(_escape(sort_title))
                if sort_title and sort_title not in {title, canonical}
                else ""
            ),
            actions=" ".join(actions),
            hero_pills="".join(hero_pills),
            details=details_html,
            related=(
                ("<p class='meta'>{}</p>".format(_escape(summary)) if summary else "")
                + self._render_work_credits_payload_section(detail_payload)
                + self._render_work_formats_section(detail_payload)
            )
            + self._render_related_sections(
                row,
                related_rows_by_table=related_rows_by_table,
                exclude_tables={"agents", "human_agents", "org_agents"},
            ),
        )

    @staticmethod
    def _preview_kind_from_name(file_name: str) -> Optional[str]:
        """
        Classify a filename suffix as image, HTML, text, or unsupported.

        Matching is case-insensitive and does not inspect file bytes. Image
        includes SVG and HTML includes XHTML; neither implies passive or
        sanitized content. Unsupported suffixes return None.

        Example:
            >>> [ReadOnlyWebApplication._preview_kind_from_name(n) for n in ("a.SVG", "a.xhtml", "a.json", "a.epub")]
            ['image', 'html', 'text', None]


        :param file_name: Falsey-as-empty path-like name whose last suffix is examined.
        :return: image, html, text, or None; no safety or readability guarantee.
        """
        suffix = Path(str(file_name or "")).suffix.lower()
        if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
            return "image"
        if suffix in {".html", ".htm", ".xhtml"}:
            return "html"
        if suffix in {".txt", ".md", ".rst", ".json", ".xml", ".csv"}:
            return "text"
        return None

    def _preview_kind_for_file_row(self, file_row) -> Optional[str]:
        """
        Classify the projected download filename without reading the asset.

        Example:
            >>> kind = app._preview_kind_for_file_row(file_row)  # doctest: +SKIP


        :param file_row: Legacy file row used to select a visible download name.
        :return: Filename-based preview token or None for an unsupported suffix.
        """
        return self._preview_kind_from_name(self._download_name_for_file_row(file_row))

    def _preview_content_type(self, file_row) -> Optional[str]:
        """
        Choose preview MIME hints from the selected filename and preview kind.

        HTML/text declare UTF-8 without transcoding any bytes. Images use MIME
        guessing with an octet-stream fallback; no content inspection occurs.

        Example:
            >>> media_type = app._preview_content_type(file_row)  # doctest: +SKIP


        :param file_row: File row supplying the projected filename and suffix.
        :return: HTML/text/image media hint, or None when no preview kind is supported.
        """
        preview_kind = self._preview_kind_for_file_row(file_row)
        if preview_kind == "html":
            return "text/html; charset=utf-8"
        if preview_kind == "text":
            return "text/plain; charset=utf-8"
        if preview_kind == "image":
            guessed_type, _encoding = mimetypes.guess_type(self._download_name_for_file_row(file_row))
            return guessed_type or "application/octet-stream"
        return None

    def _file_capabilities(self, file_row) -> dict[str, object]:
        """
        Describe potential file delivery without reading its bytes.

        Target resolution takes precedence over a stored-file reader here,
        unlike delivery handlers, which try a reader first. Preview kind is
        exposed only for a resolvable delivery. A local-file target is supported
        for overrides; the built-in target resolver currently produces redirects.

        Example:
            >>> capabilities = app._file_capabilities(file_row)  # doctest: +SKIP


        :param file_row: Legacy file row used for Core acquisition resolution.
        :return: target/stored_file/downloadable/preview_kind/delivery mapping, without delivery validation.
        """
        target = self._resolve_file_target(file_row)
        stored_file = None if target is not None else self._resolve_storage_file(file_row)
        downloadable = target is not None or stored_file is not None
        preview_kind = self._preview_kind_for_file_row(file_row) if downloadable else None
        if stored_file is not None:
            delivery = "store-backed"
        elif target is None:
            delivery = ""
        elif target.mode == "redirect":
            delivery = "external redirect"
        else:
            delivery = "local file"
        return {
            "target": target,
            "stored_file": stored_file,
            "downloadable": downloadable,
            "preview_kind": preview_kind,
            "delivery": delivery,
        }

    def _render_file_detail_page(
        self,
        *,
        row,
        row_id: int,
        row_data: dict[str, object],
        actions: list[str],
        related_rows_by_table: dict[str, list[object]],
        detail_payload: dict[str, object],
    ) -> str:
        """
        Assemble a file detail fragment from projected identity and delivery hints.

        Visible fields become identity, location/access, classification, date,
        and remaining cards. Hero relationship counts come from the payload,
        while rendered relationship sections use the separate row mapping.
        Availability hints and supplied actions do not trigger byte validation.

        Example:
            >>> html = app._render_file_detail_page(row=file_row, row_id=1, row_data=values, actions=[], related_rows_by_table=related, detail_payload=payload)  # doctest: +SKIP


        :param row: File row available for relationship fallback.
        :param row_id: Identity displayed in the hero and used as a title fallback.
        :param row_data: Projected file values used for cards and filename fallbacks.
        :param actions: Trusted action HTML fragments, concatenated without escaping.
        :param related_rows_by_table: Related rows used for entity-section rendering.
        :param detail_payload: File, related-summary, delivery, name, and preview projections.
        :return: File-body HTML fragment with escaped metadata, without the outer layout.
        """
        payload = dict(detail_payload)
        file_meta = dict(payload.get("file") or {})
        title = self._stringify_detail_value(
            payload.get("name") or row_data.get("file_name") or row_data.get("file_original_name") or row_data.get("file_storage_key") or row_id
        )
        original_path = self._stringify_detail_value(row_data.get("file_original_path"))
        storage_key = self._stringify_detail_value(file_meta.get("storage_key") or row_data.get("file_storage_key"))
        source = self._stringify_detail_value(payload.get("source") or row_data.get("file_source"))
        delivery = self._stringify_detail_value(payload.get("delivery"))
        preview_kind = self._stringify_detail_value(payload.get("preview_kind"))
        downloadable = bool(payload.get("downloadable"))

        hero_pills = ["<span class='pill'>file_id {}</span>".format(_escape(row_id))]
        for label, value in (
            ("media category", payload.get("media_category") or row_data.get("file_media_category")),
            ("role", payload.get("role") or row_data.get("file_role")),
            ("extension", file_meta.get("extension") or row_data.get("file_extension")),
            ("store id", file_meta.get("store_id") or row_data.get("file_store_id")),
        ):
            text = self._stringify_detail_value(value)
            if text:
                hero_pills.append("<span class='pill'>{label}: {value}</span>".format(label=_escape(label), value=_escape(text)))
        if downloadable:
            hero_pills.append("<span class='pill'>downloadable</span>")
        if delivery:
            hero_pills.append("<span class='pill'>delivery: {}</span>".format(_escape(delivery)))
        if preview_kind:
            hero_pills.append("<span class='pill'>preview: {}</span>".format(_escape(preview_kind)))
        related_payload = dict(payload.get("related") or {})
        for linked_table in ("items", "manifestations", "works", "images", "folders", "stores"):
            linked_rows = list(related_payload.get(linked_table) or [])
            if linked_rows:
                hero_pills.append(
                    "<span class='pill'>{label} {count}</span>".format(
                        label=_escape(self._pretty_table_name(linked_table)),
                        count=len(linked_rows),
                    )
                )

        all_columns = self._visible_columns("files")
        used: set[str] = set()

        identity_columns = [
            column
            for column in (
                "file_name",
                "file_original_name",
                "file_storage_key",
                "file_extension",
                "file_original_extension",
                "file_store_id",
            )
            if column in all_columns
        ]
        used.update(identity_columns)

        location_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("path", "uri", "source", "store", "folder", "location"))
        ]
        used.update(location_columns)

        classification_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("media", "mime", "role", "kind", "type", "format"))
        ]
        used.update(classification_columns)

        date_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("date", "time", "created", "modified", "updated", "added"))
        ]
        used.update(date_columns)

        other_columns = [column for column in all_columns if column not in used and column != "file_id"]

        cards = [
            self._render_detail_card(title="Identity", row_data=row_data, columns=identity_columns),
            self._render_detail_card(title="Location and access", row_data=row_data, columns=location_columns),
            self._render_detail_card(title="Classification", row_data=row_data, columns=classification_columns),
            self._render_detail_card(title="Dates", row_data=row_data, columns=date_columns),
            self._render_detail_card(title="Other metadata", row_data=row_data, columns=other_columns),
        ]
        cards = [card for card in cards if card]
        details_html = ""
        if cards:
            details_html = "<section class='detail-grid'>{}</section>".format("".join(cards))

        return """
<section class='panel file-hero'>
  <p class='eyebrow'>File record</p>
  <h2 class='hero-title'>{title}</h2>
  {original_path}
  {storage_key}
  {source}
  <div class='actions'>{actions}</div>
  <div class='pill-list'>{hero_pills}</div>
</section>
{details}
{related}
""".format(
            title=_escape(title),
            original_path=(
                "<p class='meta'><strong>Path:</strong> {}</p>".format(_escape(original_path))
                if original_path
                else ""
            ),
            storage_key=(
                "<p class='meta'><strong>Storage key:</strong> {}</p>".format(_escape(storage_key))
                if storage_key and storage_key != title
                else ""
            ),
            source=(
                "<p class='meta'><strong>Source:</strong> {}</p>".format(_escape(source))
                if source
                else ""
            ),
            actions=" ".join(actions),
            hero_pills="".join(hero_pills),
            details=details_html,
            related=self._render_related_sections(row, related_rows_by_table=related_rows_by_table),
        )

    def _render_store_detail_page(
        self,
        *,
        row,
        row_id: int,
        row_data: dict[str, object],
        actions: list[str],
        related_rows_by_table: dict[str, list[object]],
    ) -> str:
        """
        Retain the earlier store-detail renderer shadowed by the later definition.

        This source definition builds identity, access, capability, date, and
        other visible-field cards plus linked entities. Python replaces it with
        the later method of the same name during class creation, so ordinary
        application calls do not invoke this body. It is documented separately
        without deleting or reordering the duplicate implementation.

        Example:
            >>> fragment = extracted_earlier_renderer(app, row=store, row_id=1, row_data=values, actions=[], related_rows_by_table=related)  # doctest: +SKIP


        :param row: Store row available for fallback relationship discovery.
        :param row_id: Identity displayed in the hero and used as a title fallback.
        :param row_data: Store values supplying hero hints and grouped detail cards.
        :param actions: Trusted action markup concatenated without escaping.
        :param related_rows_by_table: Related rows for hero counts and linked sections.
        :return: Store-body HTML with escaped metadata, without the document layout.
        """
        title = self._stringify_detail_value(row_data.get("store_name") or row_id)
        root_uri = self._stringify_detail_value(row_data.get("store_root_uri"))
        kind = self._stringify_detail_value(row_data.get("store_kind"))
        protocol = self._stringify_detail_value(row_data.get("store_access_protocol"))

        hero_pills = ["<span class='pill'>store_id {}</span>".format(_escape(row_id))]
        for label, value in (("kind", kind), ("protocol", protocol)):
            if value:
                hero_pills.append("<span class='pill'>{label}: {value}</span>".format(label=_escape(label), value=_escape(value)))
        for linked_table in ("files", "folders", "items", "tags", "labels", "notes", "subjects"):
            linked_rows = related_rows_by_table.get(linked_table, [])
            if linked_rows:
                hero_pills.append(
                    "<span class='pill'>{label} {count}</span>".format(
                        label=_escape(self._pretty_table_name(linked_table)),
                        count=len(linked_rows),
                    )
                )

        all_columns = self._visible_columns("stores")
        used: set[str] = set()

        identity_columns = [column for column in ("store_name", "store_kind", "store_access_protocol") if column in all_columns]
        used.update(identity_columns)

        access_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("root", "uri", "path", "mount", "access", "protocol", "location"))
        ]
        used.update(access_columns)

        capability_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("read", "write", "sync", "managed", "remote", "backend", "mode", "kind", "state", "status"))
        ]
        used.update(capability_columns)

        date_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("date", "time", "created", "modified", "updated", "added"))
        ]
        used.update(date_columns)

        other_columns = [column for column in all_columns if column not in used and column != "store_id"]

        cards = [
            self._render_detail_card(title="Identity", row_data=row_data, columns=identity_columns),
            self._render_detail_card(title="Access", row_data=row_data, columns=access_columns),
            self._render_detail_card(title="Capabilities", row_data=row_data, columns=capability_columns),
            self._render_detail_card(title="Dates", row_data=row_data, columns=date_columns),
            self._render_detail_card(title="Other metadata", row_data=row_data, columns=other_columns),
        ]
        cards = [card for card in cards if card]
        details_html = ""
        if cards:
            details_html = "<section class='detail-grid'>{}</section>".format("".join(cards))

        return """
<section class='panel store-hero'>
  <p class='eyebrow'>Store record</p>
  <h2 class='hero-title'>{title}</h2>
  {root_uri}
  {kind}
  {protocol}
  <div class='actions'>{actions}</div>
  <div class='pill-list'>{hero_pills}</div>
</section>
{details}
{related}
""".format(
            title=_escape(title),
            root_uri=(
                "<p class='meta'><strong>Root:</strong> {}</p>".format(_escape(root_uri))
                if root_uri
                else ""
            ),
            kind=(
                "<p class='meta'><strong>Kind:</strong> {}</p>".format(_escape(kind))
                if kind
                else ""
            ),
            protocol=(
                "<p class='meta'><strong>Protocol:</strong> {}</p>".format(_escape(protocol))
                if protocol
                else ""
            ),
            actions=" ".join(actions),
            hero_pills="".join(hero_pills),
            details=details_html,
            related=self._render_related_sections(row, related_rows_by_table=related_rows_by_table),
        )

    def _render_store_detail_page(
        self,
        *,
        row,
        row_id: int,
        row_data: dict[str, object],
        actions: list[str],
        related_rows_by_table: dict[str, list[object]],
    ) -> str:
        """
        Render the active store-detail fragment, overriding the earlier duplicate.

        The hero shows escaped name/root/kind/protocol hints and selected linked
        counts. Visible fields are partitioned into identity, access, capability,
        date, and remaining cards, omitting empty values. Caller-provided action
        HTML is trusted; this renderer does not verify storage health or access.

        Example:
            >>> html = app._render_store_detail_page(row=store, row_id=1, row_data=values, actions=[], related_rows_by_table=related)  # doctest: +SKIP


        :param row: Store row used if empty groups cause relationship rediscovery.
        :param row_id: Display identity and fallback title when the store name is falsey.
        :param row_data: Store values supplying hero hints and grouped detail cards.
        :param actions: Already-rendered action fragments concatenated without escaping.
        :param related_rows_by_table: Row groups for hero counts and linked sections.
        :return: Store-body HTML fragment without an outer document layout.
        """
        title = self._stringify_detail_value(row_data.get("store_name") or row_id)
        root_uri = self._stringify_detail_value(row_data.get("store_root_uri"))
        kind = self._stringify_detail_value(row_data.get("store_kind"))
        protocol = self._stringify_detail_value(row_data.get("store_access_protocol"))

        hero_pills = ["<span class='pill'>store_id {}</span>".format(_escape(row_id))]
        for label, value in (("kind", kind), ("protocol", protocol)):
            if value:
                hero_pills.append("<span class='pill'>{label}: {value}</span>".format(label=_escape(label), value=_escape(value)))
        for linked_table in ("files", "folders", "items", "tags", "labels", "notes", "subjects"):
            linked_rows = related_rows_by_table.get(linked_table, [])
            if linked_rows:
                hero_pills.append(
                    "<span class='pill'>{label} {count}</span>".format(
                        label=_escape(self._pretty_table_name(linked_table)),
                        count=len(linked_rows),
                    )
                )

        all_columns = self._visible_columns("stores")
        used: set[str] = set()

        identity_columns = [
            column
            for column in ("store_name", "store_kind", "store_access_protocol")
            if column in all_columns
        ]
        used.update(identity_columns)

        access_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("root", "uri", "path", "mount", "access", "protocol", "location"))
        ]
        used.update(access_columns)

        capability_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("read", "write", "sync", "managed", "remote", "backend", "mode", "kind", "state", "status"))
        ]
        used.update(capability_columns)

        date_columns = [
            column
            for column in all_columns
            if column not in used and any(token in column.lower() for token in ("date", "time", "created", "modified", "updated", "added"))
        ]
        used.update(date_columns)

        other_columns = [column for column in all_columns if column not in used and column != "store_id"]

        cards = [
            self._render_detail_card(title="Identity", row_data=row_data, columns=identity_columns),
            self._render_detail_card(title="Access", row_data=row_data, columns=access_columns),
            self._render_detail_card(title="Capabilities", row_data=row_data, columns=capability_columns),
            self._render_detail_card(title="Dates", row_data=row_data, columns=date_columns),
            self._render_detail_card(title="Other metadata", row_data=row_data, columns=other_columns),
        ]
        cards = [card for card in cards if card]
        details_html = ""
        if cards:
            details_html = "<section class='detail-grid'>{}</section>".format("".join(cards))

        return """
<section class='panel store-hero'>
  <p class='eyebrow'>Store record</p>
  <h2 class='hero-title'>{title}</h2>
  {root_uri}
  {kind}
  {protocol}
  <div class='actions'>{actions}</div>
  <div class='pill-list'>{hero_pills}</div>
</section>
{details}
{related}
""".format(
            title=_escape(title),
            root_uri=(
                "<p class='meta'><strong>Root:</strong> {}</p>".format(_escape(root_uri))
                if root_uri
                else ""
            ),
            kind=(
                "<p class='meta'><strong>Kind:</strong> {}</p>".format(_escape(kind))
                if kind
                else ""
            ),
            protocol=(
                "<p class='meta'><strong>Protocol:</strong> {}</p>".format(_escape(protocol))
                if protocol
                else ""
            ),
            actions=" ".join(actions),
            hero_pills="".join(hero_pills),
            details=details_html,
            related=self._render_related_sections(row, related_rows_by_table=related_rows_by_table),
        )

    def _table_page_rows(self, table: str, *, offset: int, limit: int) -> list[object]:
        """
        Materialize all table rows and take a Python slice for display.

        This is not provider-side pagination. Negative offsets/limits retain
        Python slicing semantics when callers bypass request coercion.

        Example:
            >>> rows = app._table_page_rows("works", offset=0, limit=20)  # doctest: +SKIP


        :param table: Exact table identifier forwarded to shared enumeration.
        :param offset: Slice start, normally a nonnegative request offset.
        :param limit: Slice length used to compute the exclusive end.
        :return: New visible row list, with original row objects and provider ordering.
        """
        rows = self.read_model.rows_for_table(table)
        return rows[offset : offset + limit]

    def _render_layout(self, *, title: str, body_html: str) -> str:
        """
        Wrap trusted body markup in the generic site's HTML and inline styles.

        Site/page titles are escaped. Optional database-path exposure performs
        a fresh database.info query; exceptions from that optional hint are
        suppressed and omit it. This differs from the Calibre layout override's
        failure policy. Body markup is not escaped or sanitized here.

        Example:
            >>> app = object.__new__(ReadOnlyWebApplication)
            >>> app.config = ReadOnlyWebConfig(title="A & B")
            >>> html = app._render_layout(title="Home", body_html="<p>content</p>")
            >>> "A &amp; B" in html and "<p>content</p>" in html
            True


        :param title: Page-specific title escaped into the document title.
        :param body_html: Already-rendered body fragment inserted verbatim.
        :return: Complete HTML document with navigation, styles, and optional path hint.
        """
        db_hint = ""
        if self.config.expose_database_path:
            try:
                info = self.core.query("database.info")
                metadata = (
                    info.get("metadata", {})
                    if isinstance(info, dict)
                    else {}
                )
                db_hint = "<p class='meta'>database={}</p>".format(
                    _escape(
                        metadata.get("database_path", "")
                        if isinstance(metadata, dict)
                        else ""
                    )
                )
            except Exception:
                db_hint = ""
        return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{page_title}</title>
  <style>
    :root {{
      --bg: #f5f1e7;
      --panel: #fffdf8;
      --line: #d8ceb8;
      --ink: #1f241d;
      --muted: #5a6358;
      --accent: #8a3b12;
      --accent-soft: #efe0d6;
      --code: #f0eadf;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(138,59,18,0.08), transparent 28rem),
        linear-gradient(180deg, #f9f6ef 0%, var(--bg) 100%);
    }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .shell {{ max-width: 1100px; margin: 0 auto; padding: 1.5rem; }}
    header {{
      border-bottom: 1px solid var(--line);
      padding-bottom: 1rem;
      margin-bottom: 1.25rem;
    }}
    header h1 {{ margin: 0 0 0.5rem 0; font-size: 2rem; }}
    nav {{ display: flex; gap: 1rem; flex-wrap: wrap; }}
    nav a {{ font-weight: 600; }}
    .meta {{ color: var(--muted); margin: 0.25rem 0 0 0; font-size: 0.95rem; }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 0.75rem;
      padding: 1rem;
      margin: 0 0 1rem 0;
      box-shadow: 0 1px 0 rgba(31, 36, 29, 0.04);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 0.75rem;
    }}
    .stat {{
      background: linear-gradient(180deg, #fffefb 0%, #faf5ec 100%);
      border: 1px solid var(--line);
      border-radius: 0.75rem;
      padding: 0.85rem;
    }}
    .stat strong {{ display: block; font-size: 1.15rem; }}
    table {{
      width: max-content;
      min-width: 100%;
      border-collapse: collapse;
      font-size: 0.96rem;
      margin: 0 auto;
    }}
    th, td {{
      text-align: left;
      padding: 0.55rem 1.2rem;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    th {{ font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); }}
    code {{
      background: var(--code);
      padding: 0.1rem 0.25rem;
      border-radius: 0.25rem;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    .table-wrap {{
      width: 100%;
      overflow-x: auto;
      overflow-y: hidden;
      border: 1px solid var(--line);
      border-radius: 0.65rem;
      background: linear-gradient(180deg, #fffefb 0%, #faf5ec 100%);
      margin-top: 0.75rem;
      -webkit-overflow-scrolling: touch;
    }}
    .actions {{ display: flex; gap: 0.6rem; flex-wrap: wrap; margin-top: 0.75rem; }}
    .pill {{
      display: inline-block;
      background: var(--accent-soft);
      color: var(--accent);
      border-radius: 999px;
      padding: 0.25rem 0.6rem;
      font-size: 0.85rem;
      font-weight: 600;
    }}
    form.search {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.75rem;
      align-items: end;
    }}
    label {{ display: block; font-size: 0.85rem; color: var(--muted); margin-bottom: 0.25rem; }}
    input, select, button {{
      width: 100%;
      padding: 0.6rem 0.7rem;
      border: 1px solid var(--line);
      border-radius: 0.5rem;
      background: white;
      font: inherit;
      color: var(--ink);
    }}
    button {{
      background: var(--accent);
      color: white;
      border-color: var(--accent);
      cursor: pointer;
      font-weight: 600;
    }}
    .empty {{ color: var(--muted); font-style: italic; }}
    .eyebrow {{
      margin: 0 0 0.4rem 0;
      color: var(--muted);
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .hero-title {{
      margin: 0;
      font-size: 2rem;
      line-height: 1.15;
    }}
    .work-hero {{
      background:
        radial-gradient(circle at top right, rgba(138,59,18,0.1), transparent 18rem),
        linear-gradient(180deg, #fffefb 0%, #faf5ec 100%);
    }}
    .file-hero {{
      background:
        radial-gradient(circle at top right, rgba(31,36,29,0.08), transparent 18rem),
        linear-gradient(180deg, #fffefb 0%, #f7f2e8 100%);
    }}
    .store-hero {{
      background:
        radial-gradient(circle at top right, rgba(90,99,88,0.10), transparent 18rem),
        linear-gradient(180deg, #fffefb 0%, #f3f1ea 100%);
    }}
    .detail-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 1rem;
      margin-bottom: 1rem;
    }}
    .detail-card {{
      margin: 0;
    }}
    .detail-card h3 {{
      margin: 0 0 0.5rem 0;
    }}
    .field-value {{
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    .field-value-block {{
      margin: 0;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      word-break: break-word;
      font: inherit;
    }}
    .field-stack {{
      display: grid;
      gap: 0.2rem;
    }}
    .related-section {{ margin-top: 0.85rem; }}
    .related-section h3 {{ margin: 0 0 0.35rem 0; }}
    .pill-list {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      margin-top: 0.75rem;
    }}
    .related-pill {{ text-decoration: none; }}
    .related-card-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 0.75rem;
      margin-top: 0.75rem;
    }}
    .related-card {{
      border: 1px solid var(--line);
      border-radius: 0.65rem;
      padding: 0.85rem;
      background: linear-gradient(180deg, #fffefb 0%, #faf5ec 100%);
    }}
    .related-card strong {{
      display: block;
      margin-bottom: 0.25rem;
    }}
    .related-card-meta {{
      color: var(--muted);
      font-size: 0.9rem;
      margin-bottom: 0.4rem;
    }}
    .search-snippet {{
      margin: 0.5rem 0 0 0;
      color: var(--muted);
    }}
    mark {{
      background: #f2d58b;
      color: inherit;
      padding: 0 0.12rem;
      border-radius: 0.18rem;
    }}
    .related-list {{
      margin: 0.75rem 0 0 1.2rem;
      padding: 0;
    }}
    .related-list li + li {{ margin-top: 0.4rem; }}
    .detail-table td:first-child {{
      width: 18rem;
      min-width: 12rem;
      color: var(--muted);
      font-weight: 600;
    }}
    @media (max-width: 700px) {{
      .shell {{ padding: 1rem; }}
      header h1 {{ font-size: 1.6rem; }}
      .hero-title {{ font-size: 1.55rem; }}
      table {{ font-size: 0.9rem; }}
      th, td {{ padding: 0.45rem 1rem; }}
      .detail-table td:first-child {{ width: auto; min-width: 9rem; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>{title}</h1>
      <nav>
        <a href="/">Home</a>
        <a href="/search">Search</a>
      </nav>
      {db_hint}
    </header>
    {body}
  </div>
</body>
</html>
""".format(
            page_title=_escape("{} | {}".format(title, self.config.title)),
            title=_escape(self.config.title),
            body=body_html,
            db_hint=db_hint,
        )

    def _render_home_page(self) -> str:
        """
        Render all schema groups with table links, counts, and search controls.

        Count failures carrying code read_query_unavailable display an unavailable
        hint; all other count failures propagate. Helper and relationship tables
        are included, not restricted to the preferred public-search tables.

        Example:
            >>> html = app._render_home_page()  # doctest: +SKIP


        :return: Complete home HTML document; counts are independent reads, not a snapshot.
        """
        section_titles = {
            "main": "Main tables",
            "helper": "Helper tables",
            "interlink": "Interlink tables",
            "intralink": "Intralink tables",
        }
        section_descriptions = {
            "main": "Primary library entities and public-facing metadata.",
            "helper": "Operational, cache, and supporting metadata tables.",
            "interlink": "Relationship tables connecting different entity types.",
            "intralink": "Self-link tables connecting rows within the same entity type.",
        }
        grouped = self._grouped_tables()
        sections: list[str] = []
        for category in ("main", "helper", "interlink", "intralink"):
            cards: list[str] = []
            for table in grouped.get(category, []):
                try:
                    count_text = "{} rows".format(self.read_model.table_record_count(table))
                except Exception as exc:
                    # Optional counts may be unavailable in cache-only mode.
                    # Do not confuse a known capability limit with a failed read.
                    if getattr(exc, "code", None) != "read_query_unavailable":
                        raise
                    count_text = "count unavailable"
                href = "/tables/{}".format(quote(table, safe=""))
                cards.append(
                    "<a class='stat' href='{href}'><strong>{table}</strong><span class='meta'>{count}</span></a>".format(
                        href=_escape(href),
                        table=_escape(table),
                        count=count_text,
                    )
                )
            sections.append(
                """
<section class='panel'>
  <h2>{title}</h2>
  <p class='meta'>{description}</p>
  <div class='grid'>{cards}</div>
</section>
""".format(
                    title=_escape(section_titles[category]),
                    description=_escape(section_descriptions[category]),
                    cards="".join(cards) if cards else "<p class='empty'>No tables in this category.</p>",
                )
            )
        body = [
            "".join(sections),
            self._render_search_form({}),
        ]
        return self._render_layout(title="Home", body_html="".join(body))

    def _render_search_form(self, values: dict[str, str]) -> str:
        """
        Render global-search and exact-field GET forms from schema and selections.

        Global table choices use preferred public tables; the exact form exposes
        all tables and only the currently selected table's visible columns.
        Invalid selections fall back to the first available option. Page-size
        options are a bounded fixed/default/max set, and offsets are not retained.

        Example:
            >>> html = app._render_search_form({"table": "works", "q": "ocean"})  # doctest: +SKIP


        :param values: Optional text selections for global/exact query, table, column, and limits.
        :return: Two-form HTML fragment with escaped values; schema-read failures propagate.
        """
        global_q = str(values.get("global_q", "") or "")
        search_table = str(values.get("search_table", "") or "")
        global_limit = _coerce_int(values.get("global_limit"), default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
        table = str(values.get("table", "") or "")
        column = str(values.get("column", "") or "")
        q = str(values.get("q", "") or "")
        exact_limit = _coerce_int(values.get("exact_limit"), default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
        tables = self._all_tables()
        global_tables = self._public_search_tables()
        selected_table = table if table in tables else (tables[0] if tables else "")
        columns = self._visible_columns(selected_table) if selected_table else []
        selected_column = column if column in columns else (columns[0] if columns else "")
        page_size_options = sorted({10, 20, 50, 100, self.config.default_page_size, self.config.max_page_size})
        page_size_options = [size for size in page_size_options if 1 <= size <= self.config.max_page_size]
        global_table_options = ["<option value=''>All public tables</option>"]
        global_table_options.extend(
            "<option value='{value}'{selected}>{label}</option>".format(
                value=_escape(name),
                label=_escape(name),
                selected=" selected" if name == search_table else "",
            )
            for name in global_tables
        )
        global_limit_options = "".join(
            "<option value='{value}'{selected}>{label}</option>".format(
                value=size,
                label="{} / page".format(size),
                selected=" selected" if int(size) == int(global_limit) else "",
            )
            for size in page_size_options
        )
        table_options = "".join(
            "<option value='{value}'{selected}>{label}</option>".format(
                value=_escape(name),
                label=_escape(name),
                selected=" selected" if name == selected_table else "",
            )
            for name in tables
        )
        column_options = "".join(
            "<option value='{value}'{selected}>{label}</option>".format(
                value=_escape(name),
                label=_escape(name),
                selected=" selected" if name == selected_column else "",
            )
            for name in columns
        )
        exact_limit_options = "".join(
            "<option value='{value}'{selected}>{label}</option>".format(
                value=size,
                label="{} / page".format(size),
                selected=" selected" if int(size) == int(exact_limit) else "",
            )
            for size in page_size_options
        )
        return """
<section class='panel'>
  <h2>Search</h2>
  <form class='search' method='get' action='/search'>
    <div>
      <label for='global_q'>Global search</label>
      <input id='global_q' name='global_q' type='text' value='{global_q}' placeholder='title, author, tag, series...'>
    </div>
    <div>
      <label for='search_table'>Scope</label>
      <select id='search_table' name='search_table'>{global_table_options}</select>
    </div>
    <div>
      <label for='global_limit'>Results per page</label>
      <select id='global_limit' name='global_limit'>{global_limit_options}</select>
    </div>
    <div>
      <button type='submit'>Search library</button>
    </div>
  </form>
</section>
<section class='panel'>
  <h2>Advanced exact search</h2>
  <form class='search' method='get' action='/search'>
    <div>
      <label for='table'>Table</label>
      <select id='table' name='table'>{table_options}</select>
    </div>
    <div>
      <label for='column'>Column</label>
      <select id='column' name='column'>{column_options}</select>
    </div>
    <div>
      <label for='q'>Exact match</label>
      <input id='q' name='q' type='text' value='{q}'>
    </div>
    <div>
      <label for='exact_limit'>Results per page</label>
      <select id='exact_limit' name='exact_limit'>{exact_limit_options}</select>
    </div>
    <div>
      <button type='submit'>Run exact search</button>
    </div>
  </form>
</section>
""".format(
            global_q=_escape(global_q),
            global_table_options="".join(global_table_options),
            global_limit_options=global_limit_options,
            table_options=table_options,
            column_options=column_options,
            q=_escape(q),
            exact_limit_options=exact_limit_options,
        )

    def _render_table_page(self, table: str, query: dict[str, list[str]]) -> str:
        """
        Render a counted table slice using up to eight visible display columns.

        Only first query values are used. Limit is coerced between one and the
        configured maximum; offset is coerced nonnegative but not clamped to
        the table size. Rows are fully enumerated before slicing. Unknown tables
        produce explanatory HTML, not an HTTP status change in this helper.

        Example:
            >>> html = app._render_table_page("works", {"limit": ["20"], "offset": ["0"]})  # doctest: +SKIP


        :param table: Exact schema table name to count, read, and label.
        :param query: Parsed multi-value query mapping for limit and offset.
        :return: Complete table HTML document; count/read failures remain exceptions.
        """
        if not self._table_exists(table):
            return self._render_layout(title="Missing table", body_html="<section class='panel'><h2>Unknown table</h2></section>")
        limit = _coerce_int((query.get("limit") or [None])[0], default=self.config.default_page_size, minimum=1, maximum=self.config.max_page_size)
        offset = _coerce_int((query.get("offset") or [None])[0], default=0, minimum=0)
        total = self.read_model.table_record_count(table)
        rows = self._table_page_rows(table, offset=offset, limit=limit)
        columns = self._table_display_columns(table)

        header_html = "".join("<th>{}</th>".format(_escape(column)) for column in columns)
        header_html += "<th>detail</th>"
        row_html: list[str] = []
        for row in rows:
            cells: list[str] = []
            for column in columns:
                cells.append("<td>{}</td>".format(self._render_browse_value_html(column=column, value=_row_value(row, column))))
            href = self._row_href(table, row)
            cells.append("<td>{}</td>".format("<a href='{}'>open</a>".format(_escape(href)) if href else ""))
            row_html.append("<tr>{}</tr>".format("".join(cells)))
        if not row_html:
            row_html.append("<tr><td colspan='{}' class='empty'>No rows.</td></tr>".format(len(columns) + 1))

        prev_offset = max(0, offset - limit)
        next_offset = offset + limit if (offset + limit) < total else None
        pager_links: list[str] = []
        if offset > 0:
            pager_links.append(
                "<a href='/tables/{table}?{query}'>Back</a>".format(
                    table=_escape(quote(table, safe="")),
                    query=_escape(_build_query_string({"offset": prev_offset, "limit": limit})),
                )
            )
        if next_offset is not None:
            pager_links.append(
                "<a href='/tables/{table}?{query}'>Forward</a>".format(
                    table=_escape(quote(table, safe="")),
                    query=_escape(_build_query_string({"offset": next_offset, "limit": limit})),
                )
            )

        body = """
<section class='panel'>
  <h2>Table <code>{table}</code></h2>
  <p class='meta'>rows={total} shown={shown} offset={offset} limit={limit}</p>
  <div class='actions'>{pager}</div>
  <div class='table-wrap'>
    <table>
      <thead><tr>{headers}</tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>
""".format(
            table=_escape(table),
            total=total,
            shown=len(rows),
            offset=offset,
            limit=limit,
            pager=" ".join(pager_links) if pager_links else "<span class='pill'>no more pages</span>",
            headers=header_html,
            rows="".join(row_html),
        )
        return self._render_layout(title="Table {}".format(table), body_html=body + self._render_search_form({"table": table}))

    def _render_row_page(self, table: str, raw_row_id: str) -> str:
        """
        Resolve one integer row ID and render specialized or generic detail HTML.

        Unknown tables, invalid IDs, and missing rows yield explanatory pages
        that the dispatcher wraps in 200. Works/files/stores use their specialized
        renderers; other tables use coded detail rows and linked sections. Enabled
        file actions require capability discovery. Read/resolution errors propagate.

        Example:
            >>> html = app._render_row_page("works", "1")  # doctest: +SKIP


        :param table: Exact table name checked before attempting ID conversion.
        :param raw_row_id: ID string converted with int; conversion failures become a page.
        :return: Complete detail or explanatory HTML, without selecting an HTTP status.
        """
        if not self._table_exists(table):
            return self._render_layout(title="Missing table", body_html="<section class='panel'><h2>Unknown table</h2></section>")
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception:
            return self._render_layout(
                title="Bad row id",
                body_html="<section class='panel'><h2>Invalid row id</h2><p>{}</p></section>".format(_escape(raw_row_id)),
            )
        row = self.read_model.row_by_id(table, row_id)
        if row is None:
            return self._render_layout(
                title="Missing row",
                body_html="<section class='panel'><h2>Row not found</h2><p>{}:{}</p></section>".format(_escape(table), row_id),
            )

        row_data = self._row_dict(table, row)
        actions: list[str] = ["<a href='/tables/{}'>Back to table</a>".format(_escape(quote(table, safe="")))]
        if table == "files" and self.config.enable_file_downloads:
            capabilities = self._file_capabilities(row)
            if capabilities["downloadable"]:
                actions.append("<a href='/files/{}/download'>Download file</a>".format(row_id))
            if capabilities["preview_kind"]:
                actions.append("<a href='/files/{}/preview'>Preview file</a>".format(row_id))

        related_rows_by_table = self._related_rows_by_table(row)
        if table == "works":
            detail_payload = self.read_model.work_detail_payload(row)
            body = self._render_work_detail_page(
                row=row,
                row_id=row_id,
                row_data=row_data,
                actions=actions,
                related_rows_by_table=related_rows_by_table,
                detail_payload=detail_payload,
            )
            return self._render_layout(title="{}:{}".format(table, row_id), body_html=body)
        if table == "files":
            detail_payload = self.read_model.file_detail_payload(row)
            body = self._render_file_detail_page(
                row=row,
                row_id=row_id,
                row_data=row_data,
                actions=actions,
                related_rows_by_table=related_rows_by_table,
                detail_payload=detail_payload,
            )
            return self._render_layout(title="{}:{}".format(table, row_id), body_html=body)
        if table == "stores":
            body = self._render_store_detail_page(
                row=row,
                row_id=row_id,
                row_data=row_data,
                actions=actions,
                related_rows_by_table=related_rows_by_table,
            )
            return self._render_layout(title="{}:{}".format(table, row_id), body_html=body)

        body = """
<section class='panel'>
  <h2>{label}</h2>
  <div class='actions'>{actions}</div>
  <div class='table-wrap'>
    <table class='detail-table'>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>
""".format(
            label=_escape(self._row_label(table, row)),
            actions=" ".join(actions),
            rows=self._render_detail_table_rows(row_data, self._visible_columns(table), code_values=True, include_empty=True),
        )
        return self._render_layout(
            title="{}:{}".format(table, row_id),
            body_html=body + self._render_related_sections(row, related_rows_by_table=related_rows_by_table),
        )

    def _render_search_page(self, query: dict[str, list[str]]) -> str:
        """
        Render ranked global results and optional exact-field results independently.

        First query values drive both forms. A q value without table/column acts
        as a global-query alias when global_q is absent; the form is rendered
        before this alias is applied. Global results use shared ranked payloads;
        exact results require an existing table and visible column, then slice
        all returned matches locally. Both searches may appear on one page.
        Invalid exact selections silently omit that section, not an error page.

        Example:
            >>> html = app._render_search_page({"global_q": ["ocean"], "global_limit": ["20"]})  # doctest: +SKIP


        :param query: Parsed query lists containing global/exact terms, selectors, limits, and offsets.
        :return: Complete escaped-result HTML with per-search paging; backend errors propagate.
        """
        values = {
            "global_q": (query.get("global_q") or [""])[0],
            "search_table": (query.get("search_table") or [""])[0],
            "global_limit": (query.get("global_limit") or [""])[0],
            "table": (query.get("table") or [""])[0],
            "column": (query.get("column") or [""])[0],
            "q": (query.get("q") or [""])[0],
            "exact_limit": (query.get("exact_limit") or [""])[0],
        }
        body = [self._render_search_form(values)]

        global_query = str(values["global_q"] or "")
        search_table = str(values["search_table"] or "")
        table = str(values["table"] or "")
        column = str(values["column"] or "")
        search_term = values["q"]
        global_limit = _coerce_int(
            (query.get("global_limit") or [None])[0],
            default=self.config.default_page_size,
            minimum=1,
            maximum=self.config.max_page_size,
        )
        global_offset = _coerce_int(
            (query.get("global_offset") or [None])[0],
            default=0,
            minimum=0,
        )
        exact_limit = _coerce_int(
            (query.get("exact_limit") or [None])[0],
            default=self.config.default_page_size,
            minimum=1,
            maximum=self.config.max_page_size,
        )
        exact_offset = _coerce_int(
            (query.get("exact_offset") or [None])[0],
            default=0,
            minimum=0,
        )
        if not global_query and search_term and not table and not column:
            global_query = str(search_term)

        if global_query:
            payload = self.read_model.search_results_payload(
                query_text=global_query,
                table_filter=search_table,
                limit=global_limit,
                offset=global_offset,
            )
            total_matches = int(payload.get("total") or 0)
            visible_entries = list(payload.get("results") or [])
            grouped_results = self._group_search_result_payload(visible_entries)
            sections: list[str] = []
            for result_table, entries in grouped_results.items():
                cards: list[str] = []
                for entry in entries:
                    href = str(entry.get("html_url") or "")
                    summary = self._highlight_text(entry.get("label", ""), _search_terms(global_query))
                    lead = self._highlight_text(entry.get("primary", ""), _search_terms(global_query))
                    snippet = self._highlight_text(entry.get("snippet", ""), _search_terms(global_query))
                    match_column = str(entry.get("match_column") or "").strip()
                    actions = "<a href='{}'>open</a>".format(_escape(href)) if href else ""
                    cards.append(
                        """
<article class='related-card search-result-card'>
  <div class='related-card-meta'>{table}</div>
  <strong>{lead}</strong>
  {match_column}
  <p>{summary}</p>
  {snippet}
  <div class='actions'>{actions}</div>
</article>
""".format(
                            table=_escape(self._pretty_table_name(result_table)),
                            lead=lead,
                            match_column=(
                                "<p class='meta'>matched in {}</p>".format(_escape(match_column))
                                if match_column else ""
                            ),
                            summary=summary,
                            snippet=("<p class='search-snippet'>{}</p>".format(snippet) if snippet else ""),
                            actions=actions,
                        )
                    )
                sections.append(
                    """
<section class='panel search-group'>
  <h3>{title}</h3>
  <p class='meta'>matches={count}</p>
  <div class='related-card-grid'>{cards}</div>
</section>
""".format(title=_escape(self._pretty_table_name(result_table)), count=len(entries), cards="".join(cards))
                )
            body.append(
                """
<section class='panel'>
  <h2>Library results</h2>
  <p class='meta'>query={query} matches={count} tables={tables}</p>
  {pager}
  {sections}
</section>
""".format(
                    query=_escape(global_query),
                    count=total_matches,
                    tables=len(payload.get("group_counts") or grouped_results),
                    pager=self._render_pager(
                        path="/search",
                        query_values={
                            "global_q": global_query,
                            "search_table": search_table,
                            "global_limit": global_limit,
                        },
                        offset=global_offset,
                        limit=global_limit,
                        total=total_matches,
                        offset_key="global_offset",
                    ),
                    sections="".join(sections) if sections else "<p class='empty'>No public results.</p>",
                )
            )

        if table and column and search_term and self._table_exists(table) and column in self._visible_columns(table):
            matches = self.read_model.search_rows(table, column, search_term)
            columns = self._table_display_columns(table)
            header_html = "".join("<th>{}</th>".format(_escape(one)) for one in columns)
            header_html += "<th>detail</th>"
            rows_html: list[str] = []
            visible_matches = matches[exact_offset : exact_offset + exact_limit]
            for row in visible_matches:
                cells: list[str] = []
                for one in columns:
                    cells.append("<td>{}</td>".format(self._render_browse_value_html(column=one, value=_row_value(row, one))))
                href = self._row_href(table, row)
                cells.append("<td>{}</td>".format("<a href='{}'>open</a>".format(_escape(href)) if href else ""))
                rows_html.append("<tr>{}</tr>".format("".join(cells)))
            if not rows_html:
                rows_html.append("<tr><td colspan='{}' class='empty'>No matches.</td></tr>".format(len(columns) + 1))
            body.append(
                """
<section class='panel'>
  <h2>Search results</h2>
  <p class='meta'>table={table} column={column} query={query} matches={count}</p>
  {pager}
  <div class='table-wrap'>
    <table>
      <thead><tr>{headers}</tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>
""".format(
                    table=_escape(table),
                    column=_escape(column),
                    query=_escape(search_term),
                    count=len(matches),
                    pager=self._render_pager(
                        path="/search",
                        query_values={
                            "table": table,
                            "column": column,
                            "q": search_term,
                            "exact_limit": exact_limit,
                        },
                        offset=exact_offset,
                        limit=exact_limit,
                        total=len(matches),
                        offset_key="exact_offset",
                    ),
                    headers=header_html,
                    rows="".join(rows_html),
                )
            )

        return self._render_layout(title="Search", body_html="".join(body))

    def _resolve_file_target(self, file_row) -> Optional[_ResolvedFileTarget]:
        """
        Resolve an enabled legacy-file acquisition into an external redirect target.

        Missing or nonconvertible IDs and nonredirect deliveries return None.
        Resolution errors propagate; locations are stringified without URL
        validation and may be empty. This implementation does not produce local
        filesystem targets, although downstream helpers support overridden ones.

        Example:
            >>> target = app._resolve_file_target(file_row)  # doctest: +SKIP


        :param file_row: Row containing file_id and projected naming information.
        :return: Redirect target, or None when downloads are disabled or no redirect applies.
        """
        if not self.config.enable_file_downloads:
            return None
        file_id = _row_value(file_row, "file_id")
        if file_id in (None, ""):
            return None
        file_name = self._download_name_for_file_row(file_row)
        try:
            numeric_id = int(file_id)
        except (TypeError, ValueError, OverflowError):
            return None
        resolved = self.model.acquisition_resolve("legacy-file", numeric_id)
        delivery = str(resolved.get("delivery") or "")
        if delivery == "redirect":
            return _ResolvedFileTarget(
                mode="redirect",
                location=str(resolved.get("location") or ""),
                download_name=str(resolved.get("name") or file_name),
            )
        return None

    def _download_name_for_file_row(self, file_row) -> str:
        """
        Choose a visible file name, original name, storage key, or download.bin.

        This performs no basename extraction, content validation, or header
        sanitization; hidden-column rules can change which fallback is selected.

        Example:
            >>> name = app._download_name_for_file_row(file_row)  # doctest: +SKIP


        :param file_row: Row projected through the files table's visible columns.
        :return: Stringified first truthy naming value, or download.bin.
        """
        row = self._row_dict("files", file_row)
        return str(row.get("file_name") or row.get("file_original_name") or row.get("file_storage_key") or "download.bin")

    def _storage_lookup_metadata(self, file_row) -> dict[str, object]:
        """
        Build visible file metadata with a separate shallow file_row projection.

        Example:
            >>> metadata = app._storage_lookup_metadata(file_row)  # doctest: +SKIP


        :param file_row: Legacy file row projected through visible columns.
        :return: New top-level dict with a distinct nested file_row dict sharing cell values.
        """
        row = self._row_dict("files", file_row)
        metadata: dict[str, object] = dict(row)
        metadata["file_row"] = dict(row)
        return metadata

    def _refresh_storage_manager(self) -> bool:
        """
        Request storage refresh with replacement enabled and startup-on-add disabled.

        Command exceptions become False; this does not prove no partial mutation
        occurred. Truth conversion of a returned receipt happens outside the
        exception handler and may itself raise for an unusual object.

        Example:
            >>> refreshed = app._refresh_storage_manager()  # doctest: +SKIP


        :return: Truth value of the Core command result, or False for a command failure.
        """
        try:
            result = self.core.command(
                "storage.refresh",
                {"startup_on_add": False, "clear_existing": True},
            )
        except Exception:
            return False
        return bool(result)

    def _resolve_storage_file(self, file_row):
        """
        Create a Core-backed byte reader for an enabled readable legacy-file ID.

        Common integer-conversion errors return None. Acquisition-resolution
        failures propagate; a truthy readable receipt creates an adapter without
        actually reading bytes or proving successful delivery.

        Example:
            >>> stored = app._resolve_storage_file(file_row)  # doctest: +SKIP


        :param file_row: Row supplying file_id for legacy-file acquisition resolution.
        :return: CoreStoredFile adapter, or None for disabled/missing/invalid/unreadable acquisition.
        """
        if not self.config.enable_file_downloads:
            return None
        file_id = _row_value(file_row, "file_id")
        if file_id in (None, ""):
            return None
        try:
            numeric_id = int(file_id)
        except (TypeError, ValueError, OverflowError):
            return None
        resolved = self.model.acquisition_resolve("legacy-file", numeric_id)
        if not bool(resolved.get("readable", False)):
            return None
        return _CoreStoredFile(
            model=self.model,
            kind="legacy-file",
            resource_id=int(file_id),
        )

    def _serve_file_download(self, raw_file_id: str, environ) -> _Response:
        """
        Deliver a legacy file, preferring buffered store bytes over redirect fallback.

        Invalid IDs return 400 and absent rows 404. Stored strings become UTF-8
        bytes; other nonbytes values pass through bytes(). Unsupported reads with
        no fallback return 501, missing stored content falls through to fallback,
        acquisition_unavailable errors return 501, and other read/response-build
        failures return 502. Initial lookup/resolution and final fallback errors
        are outside that handler and propagate. A supported fallback after
        NotImplementedError is resolved again before delivery.

        Disabled downloads normally yield 404 after row lookup, not 403. Stored
        payloads are fully buffered without a size cap, range handling, or HEAD
        suppression. Redirect locations and overridden local targets are trusted.

        Example:
            >>> response = app._serve_file_download("1", {})  # doctest: +SKIP


        :param raw_file_id: File identity stripped and converted to int.
        :param environ: WSGI environment used only if fallback streams a local file.
        :return: Attachment, redirect, or explicit error response according to delivery outcome.
        """
        try:
            file_id = int(str(raw_file_id).strip())
        except Exception:
            return self._text_response("400 Bad Request", "Invalid file id.\n", content_type="text/plain")
        row = self.read_model.row_by_id("files", file_id)
        if row is None:
            return self._text_response("404 Not Found", "File row not found.\n", content_type="text/plain")
        download_name = self._download_name_for_file_row(row)

        stored_file = self._resolve_storage_file(row)
        if stored_file is not None:
            try:
                payload = stored_file.read_bytes()
                if isinstance(payload, str):
                    payload = payload.encode("utf-8")
                elif not isinstance(payload, bytes):
                    payload = bytes(payload)
                return self._bytes_response(payload, download_name=download_name)
            except NotImplementedError:
                target = self._resolve_file_target(row)
                if target is None:
                    return self._text_response(
                        "501 Not Implemented",
                        "The backing store does not support direct downloads for this file.\n",
                        content_type="text/plain",
                    )
            except FileNotFoundError:
                pass
            except Exception as exc:
                if getattr(exc, "code", "") == "acquisition_unavailable":
                    return self._text_response(
                        "501 Not Implemented",
                        "The backing store does not support direct downloads for this file.\n",
                        content_type="text/plain",
                    )
                return self._text_response(
                    "502 Bad Gateway",
                    "The backing store failed while retrieving this file.\n",
                    content_type="text/plain",
                )

        target = self._resolve_file_target(row)
        if target is None:
            return self._text_response("404 Not Found", "No downloadable target is available for this file row.\n", content_type="text/plain")
        if target.mode == "redirect":
            return self._redirect_response(target.location)
        return self._file_response(Path(target.location), download_name=target.download_name, environ=environ)

    def _serve_file_preview(self, raw_file_id: str, environ) -> _Response:
        """
        Deliver supported filename-based previews with inline disposition when local.

        ID/row failures return 400/404. Preview-kind rejection returns 415 before
        checking whether downloads are enabled. Stored-byte coercion and 501/502
        translation follow download handling; missing bytes can fall back, while
        initial and final resolution failures remain exceptions. Redirects retain
        the external response's content policy rather than an inline override.

        HTML and SVG bytes are not sanitized, isolated, or checked against their
        suffix. UTF-8 MIME hints do not transcode existing byte payloads. The
        historical error text's word safe is not a content-safety guarantee.

        Example:
            >>> response = app._serve_file_preview("1", {})  # doctest: +SKIP


        :param raw_file_id: Legacy file identity stripped and converted to int.
        :param environ: WSGI wrapper configuration for an overridden local-file fallback.
        :return: Inline payload, redirect, or explicit error response without HEAD suppression.
        """
        try:
            file_id = int(str(raw_file_id).strip())
        except Exception:
            return self._text_response("400 Bad Request", "Invalid file id.\n", content_type="text/plain")
        row = self.read_model.row_by_id("files", file_id)
        if row is None:
            return self._text_response("404 Not Found", "File row not found.\n", content_type="text/plain")

        preview_kind = self._preview_kind_for_file_row(row)
        if preview_kind is None:
            return self._text_response(
                "415 Unsupported Media Type",
                "This file type does not have a safe inline preview.\n",
                content_type="text/plain",
            )

        download_name = self._download_name_for_file_row(row)
        content_type_override = self._preview_content_type(row)

        stored_file = self._resolve_storage_file(row)
        if stored_file is not None:
            try:
                payload = stored_file.read_bytes()
                if isinstance(payload, str):
                    payload = payload.encode("utf-8")
                elif not isinstance(payload, bytes):
                    payload = bytes(payload)
                return self._bytes_response(
                    payload,
                    download_name=download_name,
                    disposition="inline",
                    content_type_override=content_type_override,
                )
            except NotImplementedError:
                target = self._resolve_file_target(row)
                if target is None:
                    return self._text_response(
                        "501 Not Implemented",
                        "The backing store does not support inline preview for this file.\n",
                        content_type="text/plain",
                    )
            except FileNotFoundError:
                pass
            except Exception as exc:
                if getattr(exc, "code", "") == "acquisition_unavailable":
                    return self._text_response(
                        "501 Not Implemented",
                        "The backing store does not support inline preview for this file.\n",
                        content_type="text/plain",
                    )
                return self._text_response(
                    "502 Bad Gateway",
                    "The backing store failed while previewing this file.\n",
                    content_type="text/plain",
                )

        target = self._resolve_file_target(row)
        if target is None:
            return self._text_response("404 Not Found", "No previewable target is available for this file row.\n", content_type="text/plain")
        if target.mode == "redirect":
            return self._redirect_response(target.location)
        return self._file_response(
            Path(target.location),
            download_name=target.download_name,
            environ=environ,
            disposition="inline",
            content_type_override=content_type_override,
        )


def add_metadata_read_source_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """
    Mutate a parser with database/cache selection, cache type, and fallback flags.

    This declares options only; it does not initialize a cache. Adding them to
    a parser that already owns these option strings follows argparse's conflict
    policy rather than being idempotent.

    Example:
        >>> parser = add_metadata_read_source_arguments(argparse.ArgumentParser())
        >>> args = parser.parse_args(["--metadata-read-source", "cache", "--no-cache-db-fallback"])
        >>> (args.metadata_read_source, args.cache_type, args.no_cache_db_fallback)
        ('cache', 'schema_backed', True)


    :param parser: Existing parser to extend in place with shared read-source options.
    :return: The same parser instance for composition by other web entrypoints.
    """
    parser.add_argument(
        "--metadata-read-source",
        choices=("database", "cache"),
        default="database",
        help="Read metadata directly from the database or from a loaded storage cache.",
    )
    parser.add_argument(
        "--cache-type",
        default="schema_backed",
        help="Storage cache backend to use when --metadata-read-source=cache.",
    )
    parser.add_argument(
        "--no-cache-db-fallback",
        action="store_true",
        help="When using cache metadata reads, do not fall back to live database reads.",
    )
    return parser


def metadata_read_source_help_epilog(command: str) -> str:
    """
    Format shared CLI examples and notes about startup cache selection.

    The supplied command is interpolated verbatim, not shell-quoted. Producing
    this text performs no Core access and makes no runtime selection itself.

    Example:
        >>> "serve --database /path/to/library.sqlite" in metadata_read_source_help_epilog("serve")
        True


    :param command: Display command prefix to insert into each usage example.
    :return: Multiline explanatory text suitable for RawDescriptionHelpFormatter.
    """
    return (
        "Examples:\n"
        "  {command} --database /path/to/library.sqlite\n"
        "  {command} --database /path/to/library.sqlite --metadata-read-source cache\n"
        "  {command} --database /path/to/library.sqlite --metadata-read-source cache --cache-type schema_backed --no-cache-db-fallback\n"
        "\n"
        "Cache read-source notes:\n"
        "  cache mode loads the selected storage cache once at startup.\n"
        "  without --no-cache-db-fallback, cache misses fall back to live database reads."
    ).format(command=command)


def metadata_read_source_config_kwargs(args: argparse.Namespace) -> dict[str, object]:
    """
    Translate parser attributes into web-configuration keyword values.

    Strings are coerced but not validated or normalized. The negative fallback
    flag is inverted using truth conversion; all three attributes are required.

    Example:
        >>> args = argparse.Namespace(metadata_read_source="cache", cache_type="schema_backed", no_cache_db_fallback=True)
        >>> metadata_read_source_config_kwargs(args)["metadata_cache_allow_database_fallback"]
        False


    :param args: Parsed namespace exposing read-source, cache-type, and fallback options.
    :return: New mapping for the three metadata-related ReadOnlyWebConfig fields.
    """
    return {
        "metadata_read_source": str(args.metadata_read_source),
        "metadata_cache_type": str(args.cache_type),
        "metadata_cache_allow_database_fallback": not bool(args.no_cache_db_fallback),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build the generic web parser with Core, cache, listener, and presentation options.

    Integer options are parsed but page-size positivity is enforced later by
    main. Constructing or parsing this parser does not start a listener or Core.

    Example:
        >>> args = build_arg_parser().parse_args(["--database", "library.sqlite", "--page-size", "0"])
        >>> (args.page_size, args.port, args.metadata_read_source)
        (0, 8080, 'database')


    :return: Fresh argparse parser with shared read-source help and application defaults.
    """
    parser = argparse.ArgumentParser(
        description="Run the LiuXin read-only web interface.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=metadata_read_source_help_epilog("PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.web_readonly"),
    )
    add_core_client_arguments(parser)
    parser.add_argument("--db-type", default="sqlite", help="Database driver type. Default: sqlite")
    add_metadata_read_source_arguments(parser)
    parser.add_argument("--host", default=ReadOnlyWebConfig.host, help="Bind host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=ReadOnlyWebConfig.port, help="Bind port. Default: 8080")
    parser.add_argument("--page-size", type=int, default=ReadOnlyWebConfig.default_page_size, help="Default page size.")
    parser.add_argument("--max-page-size", type=int, default=ReadOnlyWebConfig.max_page_size, help="Maximum page size.")
    parser.add_argument("--title", default=ReadOnlyWebConfig.title, help="Site title.")
    parser.add_argument("--expose-database-path", action="store_true", help="Show the backing database path in the UI.")
    parser.add_argument("--no-file-downloads", action="store_true", help="Disable file download/redirect links.")
    return parser


def build_metadata_read_source(
    core: CoreClientAPI,
    *,
    source: str = "database",
    cache_type: str = "schema_backed",
    allow_database_fallback: bool = True,
) -> CoreSurfaceModel:
    """
    Validate a compatibility selector and wrap the supplied Core in a surface model.

    Database/db/cache/storage_cache are accepted after stripping and lowercasing;
    falsey source means database. Cache type and fallback are retained signature
    compatibility only and are discarded. Actual cache selection belongs to
    Core composition, so this helper neither loads a cache nor reconfigures a
    remote daemon or borrowed client.

    Example:
        >>> client = object()
        >>> isinstance(build_metadata_read_source(client, source="cache"), CoreSurfaceModel)
        True


    :param core: Borrowed Core client to retain in the query adapter.
    :param source: Compatibility selector validated but not used to change Core state.
    :param cache_type: Ignored compatibility argument; composition chooses the actual cache.
    :param allow_database_fallback: Ignored compatibility flag; Core owns fallback policy.
    :return: New CoreSurfaceModel wrapping the supplied client without querying it.
    :raises ValueError: The normalized selector is not a recognized database/cache alias.
    """
    normalized_source = str(source or "database").strip().lower()
    if normalized_source not in {"database", "db", "cache", "storage_cache"}:
        raise ValueError(
            "Unknown metadata read source {!r}. Expected 'database' or 'cache'.".format(
                source,
            )
        )
    # Cache selection belongs to local Core composition. Remote callers query
    # whichever read source the daemon owns.
    del cache_type, allow_database_fallback
    return CoreSurfaceModel(core)


def main(argv: Optional[list[str]] = None) -> int:
    """
    Compose Core and run the blocking stdlib read-only web server.

    Page sizes are independently clamped to at least one. Local composition
    enables storage and disables maintenance; exact cache selection forwards its
    type and fallback policy. Core and server are context-managed. The printed
    URL precedes binding and contains the requested port, including zero rather
    than the assigned ephemeral port. Interrupts and runtime failures propagate.

    Example:
        >>> main(["--database", "library.sqlite", "--host", "127.0.0.1", "--port", "8080"])  # doctest: +SKIP


    :param argv: Argument tokens without program name, or None to use process arguments.
    :return: Zero after normal server termination and both context managers exit.
    :raises SystemExit: Argparse handles help or rejects invalid command-line arguments.
    """
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = ReadOnlyWebConfig(
        title=str(args.title),
        host=str(args.host),
        port=int(args.port),
        default_page_size=max(1, int(args.page_size)),
        max_page_size=max(1, int(args.max_page_size)),
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
        app = ReadOnlyWebApplication(core_session.client, config=config)
        url = "http://{}:{}/".format(config.host, config.port)
        sys.stdout.write("Serving read-only web UI on {}\n".format(url))
        sys.stdout.flush()
        with make_server(config.host, config.port, app) as server:
            server.serve_forever()
    return 0


__all__ = [
    "ReadOnlyWebApplication",
    "ReadOnlyWebConfig",
    "add_metadata_read_source_arguments",
    "build_arg_parser",
    "build_metadata_read_source",
    "main",
    "metadata_read_source_help_epilog",
    "metadata_read_source_config_kwargs",
]
