"""Public-safe read-only web interface for browsing the LiuXin database."""

from __future__ import annotations

import argparse
import html
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
from LiuXin_alpha.surfaces.core import (
    CoreDatabaseView,
    CoreSurfaceModel,
    add_core_client_arguments,
    coerce_surface_core,
    open_surface_core_from_args,
)


def _escape(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _short_text(value: object, *, width: int = 120) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _search_terms(value: object) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [one for one in re.split(r"\s+", text) if one]


def _normalized_search_text(value: object) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).casefold()


def _coerce_int(raw: Optional[str], *, default: int, minimum: int = 0, maximum: Optional[int] = None) -> int:
    try:
        value = int(str(raw).strip())
    except Exception:
        value = int(default)
    value = max(int(minimum), int(value))
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def _row_value(row, column: str):
    try:
        return row[column]
    except Exception:
        return None


def _build_query_string(values: dict[str, object]) -> str:
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
    class _ClosingIterable:
        def __iter__(self_inner) -> Iterator[bytes]:
            try:
                for chunk in iterable:
                    yield chunk
            finally:
                closer()

        def close(self_inner) -> None:
            closer()

    return _ClosingIterable()


@dataclass(frozen=True)
class ReadOnlyWebConfig:
    """Shared database, Core, and HTTP settings for read-only web surfaces."""

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


@dataclass(frozen=True)
class _ResolvedFileTarget:
    mode: str
    location: str
    download_name: str


@dataclass(frozen=True)
class _CoreStoredFile:
    model: CoreSurfaceModel
    kind: str
    resource_id: int

    def read_bytes(self) -> bytes:
        _resource, payload = self.model.acquisition_read(
            self.kind,
            self.resource_id,
        )
        return payload


@dataclass
class _Response:
    status: str
    headers: list[tuple[str, str]]
    body: Iterable[bytes]
    close: Optional[Callable[[], None]] = None


class ReadOnlyWebApplication:
    """Small stdlib WSGI app for safe public read-only access."""

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
        if self._compatibility_core_session is not None:
            self._compatibility_core_session.close()

    def __call__(self, environ, start_response):
        response = self.handle_request(environ)
        headers = list(response.headers)
        headers.append(("X-Robots-Tag", "noai, noimageai"))
        start_response(response.status, headers)
        if response.close is not None:
            return _closing_iterable(response.body, response.close)
        return response.body

    def refresh_metadata_read_source(self) -> bool:
        from LiuXin_alpha.surfaces.write_refresh import refresh_metadata_read_source_after_write

        return refresh_metadata_read_source_after_write(self)

    def handle_request(self, environ) -> _Response:
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
        return _Response(
            status=status,
            headers=[("Content-Type", "{}; charset=utf-8".format(content_type))],
            body=[text.encode("utf-8")],
        )

    def _html_response(self, html_text: str, *, status: str = "200 OK") -> _Response:
        return _Response(
            status=status,
            headers=[("Content-Type", "text/html; charset=utf-8")],
            body=[html_text.encode("utf-8")],
        )

    def _redirect_response(self, location: str) -> _Response:
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
        return list(self.model.table_names())

    @staticmethod
    def _table_category(table: str) -> str:
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
        groups = {"main": [], "helper": [], "interlink": [], "intralink": []}
        for table in self._all_tables():
            groups.setdefault(self._table_category(table), []).append(table)
        return groups

    def _table_exists(self, table: str) -> bool:
        return str(table) in set(self._all_tables())

    def _id_column(self, table: str) -> Optional[str]:
        columns = list(self.model.columns(table))
        if not columns:
            return None
        return self.model.id_column(table)

    def _visible_columns(self, table: str) -> list[str]:
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
        text = str(table).replace("_", " ").strip()
        if not text:
            return "Related"
        return text[0].upper() + text[1:]

    def _preferred_summary_fields(self, table: str) -> tuple[str, ...]:
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
        parts = self._row_summary_parts(table, row, limit=1)
        if parts:
            return parts[0]
        return self._row_label(table, row)

    @staticmethod
    def _stringify_detail_value(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple, set)):
            return _short_text(repr(value), width=400)
        return str(value)

    @staticmethod
    def _detail_value_kind(column: str) -> str:
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
        return {column: _row_value(row, column) for column in self._visible_columns(table)}

    def _row_href(self, table: str, row) -> Optional[str]:
        id_column = self._id_column(table)
        if not id_column:
            return None
        row_id = _row_value(row, id_column)
        if row_id in (None, ""):
            return None
        return "/tables/{}/{}".format(quote(table, safe=""), quote(str(row_id), safe=""))

    def _row_label(self, table: str, row) -> str:
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
        preferred = ("works", "agents", "human_agents", "org_agents", "series", "tags", "labels", "genres", "subjects", "files", "stores")
        available = set(self._all_tables())
        ordered = [table for table in preferred if table in available]
        if ordered:
            return ordered
        return [table for table in self._all_tables() if self._table_category(table) == "main"]

    def _search_candidate_columns(self, table: str) -> list[str]:
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
        return self.read_model.search_entries(query_text, table_filter=table_filter)

    @staticmethod
    def _group_search_entries(entries: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
        grouped: dict[str, list[dict[str, object]]] = {}
        for entry in entries:
            grouped.setdefault(str(entry["table"]), []).append(entry)
        return grouped

    @staticmethod
    def _group_search_result_payload(entries: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
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
        return self.read_model.work_credit_entries(row)

    def _render_work_credits_section(self, row) -> str:
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
        table = str(getattr(row, "table", "") or "")
        try:
            candidate_tables = list(
                getattr(row, "linkable_tables", None)
                or self.model.related_tables(table)
            )
        except Exception:
            candidate_tables = []
        return sorted(
            {str(one) for one in candidate_tables if str(one) and str(one) != table},
            key=lambda name: (
                self._RELATED_TABLE_ORDER.index(name) if name in self._RELATED_TABLE_ORDER else len(self._RELATED_TABLE_ORDER),
                name,
            ),
        )

    def _render_related_pill_section(self, linked_table: str, rows: list[object]) -> str:
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
        return self.read_model.related_rows_by_table(row)

    def _render_related_sections(
        self,
        row,
        *,
        related_rows_by_table: Optional[dict[str, list[object]]] = None,
        exclude_tables: Optional[set[str]] = None,
    ) -> str:
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
        suffix = Path(str(file_name or "")).suffix.lower()
        if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
            return "image"
        if suffix in {".html", ".htm", ".xhtml"}:
            return "html"
        if suffix in {".txt", ".md", ".rst", ".json", ".xml", ".csv"}:
            return "text"
        return None

    def _preview_kind_for_file_row(self, file_row) -> Optional[str]:
        return self._preview_kind_from_name(self._download_name_for_file_row(file_row))

    def _preview_content_type(self, file_row) -> Optional[str]:
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
        rows = self.read_model.rows_for_table(table)
        return rows[offset : offset + limit]

    def _render_layout(self, *, title: str, body_html: str) -> str:
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
                    count = self.read_model.table_record_count(table)
                except Exception:
                    count = -1
                href = "/tables/{}".format(quote(table, safe=""))
                cards.append(
                    "<a class='stat' href='{href}'><strong>{table}</strong><span class='meta'>{count} rows</span></a>".format(
                        href=_escape(href),
                        table=_escape(table),
                        count="?" if count < 0 else count,
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
        if not self.config.enable_file_downloads:
            return None
        file_id = _row_value(file_row, "file_id")
        if file_id in (None, ""):
            return None
        file_name = self._download_name_for_file_row(file_row)
        try:
            resolved = self.model.acquisition_resolve(
                "legacy-file",
                int(file_id),
            )
        except Exception:
            return None
        delivery = str(resolved.get("delivery") or "")
        if delivery == "redirect":
            return _ResolvedFileTarget(
                mode="redirect",
                location=str(resolved.get("location") or ""),
                download_name=str(resolved.get("name") or file_name),
            )
        return None

    def _download_name_for_file_row(self, file_row) -> str:
        row = self._row_dict("files", file_row)
        return str(row.get("file_name") or row.get("file_original_name") or row.get("file_storage_key") or "download.bin")

    def _storage_lookup_metadata(self, file_row) -> dict[str, object]:
        row = self._row_dict("files", file_row)
        metadata: dict[str, object] = dict(row)
        metadata["file_row"] = dict(row)
        return metadata

    def _refresh_storage_manager(self) -> bool:
        try:
            result = self.core.command(
                "storage.refresh",
                {"startup_on_add": False, "clear_existing": True},
            )
        except Exception:
            return False
        return bool(result)

    def _resolve_storage_file(self, file_row):
        if not self.config.enable_file_downloads:
            return None
        file_id = _row_value(file_row, "file_id")
        if file_id in (None, ""):
            return None
        try:
            resolved = self.model.acquisition_resolve(
                "legacy-file",
                int(file_id),
            )
        except Exception:
            return None
        if not bool(resolved.get("readable", False)):
            return None
        return _CoreStoredFile(
            model=self.model,
            kind="legacy-file",
            resource_id=int(file_id),
        )

    def _serve_file_download(self, raw_file_id: str, environ) -> _Response:
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
    Add metadata read-source selection arguments to a web parser.


    :param parser:
    :return:
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
    Build help text describing metadata read-source selection.


    :param command:
    :return:
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
    Extract metadata read-source configuration from parsed arguments.


    :param args:
    :return:
    """
    return {
        "metadata_read_source": str(args.metadata_read_source),
        "metadata_cache_type": str(args.cache_type),
        "metadata_cache_allow_database_fallback": not bool(args.no_cache_db_fallback),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build the read-only web surface command-line parser.


    :return:
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
    Construct the configured metadata read source for a web surface.


    :param core:
    :param source:
    :param cache_type:
    :param allow_database_fallback:
    :return:
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
    Run the web readonly command-line entry point.


    :param argv:
    :return:
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
