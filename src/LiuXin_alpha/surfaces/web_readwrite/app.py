from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import posixpath
import sys

from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from email.parser import BytesParser
from email.policy import default as email_policy_default
from io import BytesIO
from pathlib import Path
from collections.abc import Mapping
from typing import Any, Optional
from urllib.parse import parse_qs, quote, unquote, urlencode
from wsgiref.simple_server import make_server

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.core import (
    CoreRow,
    add_core_client_arguments,
    open_surface_core_from_args,
)
from LiuXin_alpha.surfaces.web_readonly.app import (
    ReadOnlyWebApplication,
    ReadOnlyWebConfig,
    _Response,
    _build_query_string,
    _coerce_int,
    _escape,
    _row_value,
    _short_text,
)


_REQUEST_NOTICE: ContextVar[Optional[dict[str, str]]] = ContextVar("web_readwrite_request_notice", default=None)

_STORE_BACKEND_KINDS = (
    "ftp_readonly",
    "native_html_readonly",
    "on_disk_calibre_like",
    "on_disk_existing_managed_drive",
    "on_disk_existing_unmanaged_drive",
    "on_disk_flat",
    "rclone_http_readonly",
    "single_file_sqlite",
    "squashfs_build",
    "squashfs_readonly",
    "wget_html_readonly",
)


@dataclass(frozen=True)
class ReadWriteWebConfig(ReadOnlyWebConfig):
    title: str = "LiuXin Read-Write Web"
    port: int = 8084
    write_banner: str = "Experimental local-first write surface. Not hardened for public internet exposure."


@dataclass(frozen=True)
class _CoreLinkReport:
    changed: bool = True
    errors: tuple[str, ...] = ()


class ReadWriteWebApplication(ReadOnlyWebApplication):
    """Experimental HTML read/write surface over the existing database APIs."""

    _WORK_LINK_SPECS: dict[str, dict[str, Any]] = {
        "agents": {
            "title": "Manage credits",
            "intro": "Contributors linked to this work.",
            "target_label": "Contributor row id",
            "add_label": "Add credit",
            "browse_label": "Browse contributors",
            "field_order": ["agent_work_link_type", "agent_work_link_priority"],
            "field_labels": {
                "agent_work_link_type": "Role",
                "agent_work_link_priority": "Priority",
            },
            "create_title": "Create contributor and link",
            "create_intro": "Create a new contributor row and attach it to this work in one step.",
            "create_fields": ["agent_canonical_name", "agent_type", "agent_sort_name", "agent_aliases", "agent_note"],
            "create_field_labels": {
                "agent_canonical_name": "Contributor name",
                "agent_type": "Contributor type",
                "agent_sort_name": "Sort name",
                "agent_aliases": "Aliases",
                "agent_note": "Contributor note",
            },
            "create_defaults": {
                "agent_type": "person",
            },
            "create_required_fields": ["agent_canonical_name"],
            "create_submit_label": "Create contributor + link",
            "open": True,
            "item_name": "credit",
        },
        "tags": {
            "title": "Manage tags",
            "intro": "Descriptive tags linked to this work.",
            "target_label": "Tag row id",
            "add_label": "Add tag",
            "browse_label": "Browse tags",
            "field_order": ["tag_work_link_priority", "tag_work_link_source"],
            "field_labels": {
                "tag_work_link_priority": "Priority",
                "tag_work_link_source": "Source",
            },
            "create_title": "Create tag and link",
            "create_intro": "Create a new tag row and attach it to this work immediately.",
            "create_fields": ["tag", "tag_description"],
            "create_field_labels": {
                "tag": "Tag text",
                "tag_description": "Description",
            },
            "create_required_fields": ["tag"],
            "create_submit_label": "Create tag + link",
            "open": True,
            "item_name": "tag",
        },
        "labels": {
            "title": "Manage labels",
            "intro": "Operational labels linked to this work.",
            "target_label": "Label row id",
            "add_label": "Add label",
            "browse_label": "Browse labels",
            "field_order": ["label_work_link_priority"],
            "field_labels": {
                "label_work_link_priority": "Priority",
            },
            "create_title": "Create label and link",
            "create_intro": "Create a new label row and attach it to this work immediately.",
            "create_fields": ["label_text", "label_description"],
            "create_field_labels": {
                "label_text": "Label text",
                "label_description": "Description",
            },
            "create_required_fields": ["label_text"],
            "create_submit_label": "Create label + link",
            "open": True,
            "item_name": "label",
        },
        "series": {
            "title": "Manage series",
            "intro": "Series links for this work.",
            "target_label": "Series row id",
            "add_label": "Add series link",
            "browse_label": "Browse series",
            "field_order": ["series_work_link_type", "series_work_link_priority"],
            "field_labels": {
                "series_work_link_type": "Relationship type",
                "series_work_link_priority": "Priority",
            },
            "create_title": "Create series and link",
            "create_intro": "Create a new series row and attach it to this work.",
            "create_fields": ["series", "series_sort", "series_parent_id", "series_parent_position"],
            "create_field_labels": {
                "series": "Series name",
                "series_sort": "Sort name",
                "series_parent_id": "Parent series row id",
                "series_parent_position": "Parent position",
            },
            "create_required_fields": ["series"],
            "create_submit_label": "Create series + link",
            "open": True,
            "item_name": "series link",
        },
        "languages": {
            "title": "Manage languages",
            "intro": "Language links for this work.",
            "target_label": "Language row id",
            "add_label": "Add language link",
            "browse_label": "Browse languages",
            "field_order": ["language_work_link_type", "language_work_link_priority"],
            "field_labels": {
                "language_work_link_type": "Language role",
                "language_work_link_priority": "Priority",
            },
            "create_disabled_reason": "Languages are reference data and cannot be created from this page.",
            "open": True,
            "item_name": "language link",
        },
    }

    _SPECIAL_FORM_SPECS: dict[str, dict[str, Any]] = {
        "works": {
            "create_title": "Create work",
            "edit_title": "Edit work",
            "create_intro": "Start with the core identity and publication metadata, then link contributors, tags, series, and languages from the row page.",
            "edit_intro": "Update the work record, then manage credits and linked metadata from the detail page.",
            "field_labels": {
                "work_title": "Title",
                "work_canonical_title": "Canonical title",
                "work_sort_title": "Sort title",
                "work_creator_sort": "Creator sort",
                "work_type": "Work type",
                "work_medium": "Medium",
                "work_original_language_id": "Original language row id",
                "work_original_year": "Original year",
                "work_original_date": "Original date",
                "work_original_copyright_date": "Copyright date",
                "work_is_fiction": "Fiction",
                "work_audience": "Audience",
                "work_completion_status": "Completion status",
                "work_wikipedia_link": "Wikipedia link",
                "work_flags": "Flags",
                "work_discovery_note": "Discovery note",
            },
            "groups": [
                ("Identity", ["work_title", "work_canonical_title", "work_sort_title", "work_creator_sort"]),
                ("Classification", ["work_type", "work_medium", "work_is_fiction", "work_audience", "work_completion_status", "work_flags"]),
                ("Origin", ["work_original_language_id", "work_original_year", "work_original_date", "work_original_copyright_date"]),
                ("Notes", ["work_wikipedia_link", "work_discovery_note"]),
            ],
        },
        "files": {
            "create_title": "Create file",
            "edit_title": "Edit file",
            "create_intro": "Describe where the file lives, how it should be named, and how it should be treated by the library.",
            "edit_intro": "Update the storage, naming, and integrity metadata for this file row.",
            "field_labels": {
                "file_item_id": "Item row id",
                "file_store_id": "Store row id",
                "file_folder_id": "Folder row id",
                "file_storage_key": "Storage key",
                "file_name": "Filename",
                "file_base_name": "Base name",
                "file_extension": "Extension",
                "file_tag": "Tag",
                "file_auto_name": "Auto name",
                "file_use_auto_name": "Use auto name",
                "file_mime_type": "MIME type",
                "file_role": "Role",
                "file_media_category": "Media category",
                "file_class_mask": "Class mask",
                "file_visibility_mask": "Visibility mask",
                "file_critical": "Critical",
                "file_size_bytes": "Size (bytes)",
                "file_hash_sha256": "SHA-256",
                "file_hash_blake3": "BLAKE3",
                "file_phash": "Perceptual hash",
                "file_corrupt": "Corrupt",
                "file_integrity_status": "Integrity status",
                "file_last_seen_timestamp_ep_k": "Last seen timestamp",
                "file_last_integrity_check_timestamp_ep_k": "Last integrity check",
                "file_acquired_timestamp_ep_k": "Acquired timestamp",
                "file_source": "Source",
                "file_original_name": "Original name",
                "file_original_path": "Original path",
                "file_anthology": "Anthology",
                "file_parent": "Parent file row id",
                "file_conversion_settings": "Conversion settings",
                "file_processed": "Processed",
            },
            "groups": [
                ("Storage", ["file_item_id", "file_store_id", "file_folder_id", "file_storage_key"]),
                ("Naming", ["file_name", "file_base_name", "file_extension", "file_tag", "file_auto_name", "file_use_auto_name"]),
                ("Classification", ["file_mime_type", "file_role", "file_media_category", "file_class_mask", "file_visibility_mask", "file_critical", "file_processed"]),
                ("Integrity", ["file_size_bytes", "file_hash_sha256", "file_hash_blake3", "file_phash", "file_corrupt", "file_integrity_status", "file_last_seen_timestamp_ep_k", "file_last_integrity_check_timestamp_ep_k", "file_acquired_timestamp_ep_k"]),
                ("Source", ["file_source", "file_original_name", "file_original_path", "file_parent", "file_anthology", "file_conversion_settings"]),
            ],
        },
        "stores": {
            "create_title": "Create store",
            "edit_title": "Edit store",
            "create_intro": "Define the store identity, connection details, and capabilities the storage manager should assume.",
            "edit_intro": "Update the store definition and capability flags used by the storage manager.",
            "field_labels": {
                "store_name": "Store name",
                "store_kind": "Store kind",
                "store_access_protocol": "Access protocol",
                "store_root_uri": "Root URI",
                "store_auth_method": "Auth method",
                "store_credentials": "Credentials",
                "store_storage_mask": "Storage mask",
                "store_policy_json": "Policy JSON",
                "store_online_status": "Online status",
                "store_location_note": "Location note",
                "store_last_seen_online_timestamp_ep_k": "Last seen online",
                "store_last_healthcheck_ok_timestamp_ep_k": "Last healthy check",
                "store_supports_folders": "Supports folders",
                "store_supports_hierarchical_list": "Supports hierarchical list",
                "store_supports_random_read": "Supports random read",
                "store_supports_random_write": "Supports random write",
                "store_supports_append": "Supports append",
                "store_supports_atomic_rename": "Supports atomic rename",
                "store_supports_atomic_overwrite": "Supports atomic overwrite",
                "store_supports_delete": "Supports delete",
                "store_is_read_only": "Read only",
                "store_is_eventually_consistent": "Eventually consistent",
                "store_supports_checksums": "Supports checksums",
                "store_supports_immutable_objects": "Supports immutable objects",
                "store_supports_snapshots": "Supports snapshots",
                "store_supports_server_side_encryption": "Supports server-side encryption",
                "store_supports_parallel_read": "Supports parallel read",
                "store_supports_parallel_write": "Supports parallel write",
                "store_requires_mount": "Requires mount",
                "store_latency_class": "Latency class",
            },
            "groups": [
                ("Identity", ["store_name", "store_kind", "store_access_protocol", "store_root_uri", "store_location_note"]),
                ("Access", ["store_auth_method", "store_credentials", "store_storage_mask", "store_policy_json", "store_online_status", "store_last_seen_online_timestamp_ep_k", "store_last_healthcheck_ok_timestamp_ep_k"]),
                ("Capabilities", [
                    "store_supports_folders",
                    "store_supports_hierarchical_list",
                    "store_supports_random_read",
                    "store_supports_random_write",
                    "store_supports_append",
                    "store_supports_atomic_rename",
                    "store_supports_atomic_overwrite",
                    "store_supports_delete",
                    "store_supports_checksums",
                    "store_supports_immutable_objects",
                    "store_supports_snapshots",
                    "store_supports_server_side_encryption",
                    "store_supports_parallel_read",
                    "store_supports_parallel_write",
                    "store_requires_mount",
                ]),
                ("Consistency", ["store_is_read_only", "store_is_eventually_consistent", "store_latency_class"]),
            ],
        },
    }

    def __init__(
        self,
        core: CoreClientAPI,
        *,
        config: Optional[ReadWriteWebConfig] = None,
    ) -> None:
        super().__init__(core, config=config or ReadWriteWebConfig())

    def handle_request(self, environ) -> _Response:
        method = str(environ.get("REQUEST_METHOD", "GET") or "GET").upper()
        path = posixpath.normpath(str(environ.get("PATH_INFO", "/") or "/"))
        if not path.startswith("/"):
            path = "/" + path
        query = parse_qs(str(environ.get("QUERY_STRING", "") or ""), keep_blank_values=False)
        token = _REQUEST_NOTICE.set(self._notice_from_query(query))
        try:
            if method in {"GET", "HEAD"}:
                parts = [unquote(part) for part in path.split("/") if part]
                if len(parts) == 2 and parts[0] == "files" and parts[1] == "upload":
                    return self._html_response(self._render_file_upload_page())
                if len(parts) == 4 and parts[0] == "tables" and parts[3] == "upload" and parts[1] in {"works", "items"}:
                    return self._html_response(self._render_file_upload_page(context_table=parts[1], raw_row_id=parts[2]))
                if len(parts) == 3 and parts[0] == "tables" and parts[2] == "new":
                    return self._html_response(self._render_new_row_page(parts[1]))
                if len(parts) == 4 and parts[0] == "tables" and parts[3] == "edit":
                    return self._html_response(self._render_edit_row_page(parts[1], parts[2]))
                if len(parts) == 4 and parts[0] == "tables" and parts[3] == "delete":
                    return self._html_response(self._render_delete_row_page(parts[1], parts[2]))
                return super().handle_request(environ)

            if method == "POST":
                parts = [unquote(part) for part in path.split("/") if part]
                if len(parts) == 2 and parts[0] == "files" and parts[1] == "upload":
                    return self._handle_file_upload(environ)
                if len(parts) == 4 and parts[0] == "tables" and parts[3] == "upload" and parts[1] in {"works", "items"}:
                    return self._handle_file_upload(environ, context_table=parts[1], raw_row_id=parts[2])
                if len(parts) == 6 and parts[0] == "tables" and parts[3] == "links" and parts[5] == "create":
                    return self._handle_create_link_target(parts[1], parts[2], parts[4], environ)
                if len(parts) == 6 and parts[0] == "tables" and parts[3] == "links" and parts[5] == "new":
                    return self._handle_add_interlink(parts[1], parts[2], parts[4], environ)
                if len(parts) == 7 and parts[0] == "tables" and parts[3] == "links" and parts[6] == "edit":
                    return self._handle_edit_interlink(parts[1], parts[2], parts[4], parts[5], environ)
                if len(parts) == 7 and parts[0] == "tables" and parts[3] == "links" and parts[6] == "delete":
                    return self._handle_delete_interlink(parts[1], parts[2], parts[4], parts[5], environ)
                if len(parts) == 3 and parts[0] == "tables" and parts[2] == "new":
                    return self._handle_create_row(parts[1], environ)
                if len(parts) == 4 and parts[0] == "tables" and parts[3] == "edit":
                    return self._handle_edit_row(parts[1], parts[2], environ)
                if len(parts) == 4 and parts[0] == "tables" and parts[3] == "delete":
                    return self._handle_delete_row(parts[1], parts[2], environ)
                return self._text_response("405 Method Not Allowed", "Method not allowed.\n", content_type="text/plain")

            return self._text_response("405 Method Not Allowed", "Method not allowed.\n", content_type="text/plain")
        finally:
            _REQUEST_NOTICE.reset(token)

    def _render_layout(self, *, title: str, body_html: str) -> str:
        extra_css = """
    .admin-banner { border-left: 0.35rem solid var(--accent); }
    .write-form { display: grid; gap: 0.9rem; }
    .write-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 0.9rem; }
    .field { display: grid; gap: 0.35rem; }
    .field label { font-weight: 700; }
    .field input, .field textarea, .field select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 0.5rem;
      padding: 0.6rem 0.7rem;
      background: #fff;
      color: var(--ink);
      font: inherit;
    }
    .field textarea { min-height: 8rem; resize: vertical; }
    .field code { width: fit-content; }
    .field-help { color: var(--muted); font-size: 0.92rem; }
    .warning-list { margin: 0.75rem 0 0 1rem; padding: 0; }
    .warning-list li { margin: 0.35rem 0; }
    .danger { color: #8a1f1f; }
    .actions form { display: inline; }
    .notice-panel { border-left: 0.35rem solid var(--line); }
    .notice-panel.notice-success { border-left-color: #2f7d32; background: #f2fbf2; }
    .notice-panel.notice-error { border-left-color: #b3261e; background: #fff4f3; }
    .notice-panel.notice-warning { border-left-color: #9a6700; background: #fff8e8; }
    .notice-panel.notice-info { border-left-color: var(--accent); background: #f5fbff; }
    .notice-panel h2 { margin-bottom: 0.35rem; }
    .link-editor-stack { display: grid; gap: 1rem; }
    .link-editor-card { display: grid; gap: 0.8rem; }
    .link-editor-card .actions { display: flex; flex-wrap: wrap; gap: 0.6rem; }
    .link-meta { display: flex; flex-wrap: wrap; gap: 0.45rem; }
    .link-editor-form { display: grid; gap: 0.8rem; }
    .link-editor-form .write-grid { margin: 0; }
    .link-add-form { margin-top: 0.8rem; }
    .write-group-stack { display: grid; gap: 0.9rem; }
    .write-group { border: 1px solid var(--line); border-radius: 0.75rem; padding: 0.9rem; background: rgba(255,255,255,0.7); }
    .write-group h3 { margin: 0 0 0.25rem 0; }
    .write-group .meta { margin-bottom: 0.75rem; }
    .link-create-helper { margin-top: 1rem; border-top: 1px dashed var(--line); padding-top: 1rem; }
    .link-editor summary { cursor: pointer; font-weight: 700; }
    .link-editor details + details { margin-top: 0.8rem; }
    .link-chip-list { display: flex; flex-wrap: wrap; gap: 0.45rem; }
    .link-chip { display: inline-flex; align-items: center; gap: 0.35rem; border: 1px solid var(--line); border-radius: 999px; padding: 0.35rem 0.7rem; background: #fff; }
    .link-chip strong { font-size: 0.94rem; }
"""
        banner_html = """
<section class='panel admin-banner'>
  <h2>Write Interface</h2>
  <p class='meta'>{}</p>
</section>
""".format(_escape(getattr(self.config, "write_banner", "")))
        notice_html = self._render_active_notice()
        html_text = super()._render_layout(title=title, body_html=notice_html + body_html)
        html_text = html_text.replace("</style>", extra_css + "\n  </style>", 1)
        html_text = html_text.replace("</header>", "</header>\n" + banner_html, 1)
        return html_text

    @staticmethod
    def _notice_from_query(query: dict[str, list[str]]) -> Optional[dict[str, str]]:
        kind = str((query.get("notice_kind") or [""])[0]).strip().lower()
        title = str((query.get("notice_title") or [""])[0]).strip()
        message = str((query.get("notice_message") or [""])[0]).strip()
        if not kind and not title and not message:
            return None
        if kind not in {"success", "error", "warning", "info"}:
            kind = "info"
        return {"kind": kind, "title": title, "message": message}

    def _render_notice_panel(self, *, kind: str, title: str, message: str) -> str:
        normalized_kind = str(kind or "info").strip().lower() or "info"
        if normalized_kind not in {"success", "error", "warning", "info"}:
            normalized_kind = "info"
        heading = title or {
            "success": "Success",
            "error": "Write failed",
            "warning": "Warning",
            "info": "Notice",
        }[normalized_kind]
        return """
<section class='panel notice-panel notice-{kind}'>
  <h2>{title}</h2>
  <p>{message}</p>
</section>
""".format(kind=_escape(normalized_kind), title=_escape(heading), message=_escape(message or ""))

    def _render_active_notice(self) -> str:
        notice = _REQUEST_NOTICE.get()
        if not notice:
            return ""
        return self._render_notice_panel(
            kind=str(notice.get("kind") or "info"),
            title=str(notice.get("title") or ""),
            message=str(notice.get("message") or ""),
        )

    def _redirect_with_notice(
        self,
        path: str,
        *,
        kind: str,
        title: str,
        message: str,
        anchor: str = "",
    ) -> _Response:
        raw_path = str(path or "")
        existing_anchor = ""
        if "#" in raw_path:
            raw_path, existing_anchor = raw_path.split("#", 1)
        if "?" in raw_path:
            base_path, raw_query = raw_path.split("?", 1)
            params = {
                str(key): (values[0] if values else "")
                for key, values in parse_qs(raw_query, keep_blank_values=True).items()
            }
        else:
            base_path = raw_path
            params = {}
        params["notice_kind"] = str(kind)
        params["notice_title"] = str(title)
        params["notice_message"] = str(message)
        location = base_path
        if params:
            location += "?" + urlencode(params)
        final_anchor = anchor or existing_anchor
        if final_anchor:
            location += "#" + str(final_anchor)
        return self._redirect_response(location)

    def _visible_columns(self, table: str) -> list[str]:
        if not self._table_exists(table):
            return []
        columns = list(self.db.get_column_headings(table))
        return [column for column in columns if not str(column).endswith("_scratch")]

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
        sections: list[str] = [
            """
<section class='panel'>
  <h2>Admin actions</h2>
  <p class='meta'>Browse a table and use the row pages to edit or delete records. Use table pages to create new rows.</p>
</section>
"""
        ]
        for category in ("main", "helper", "interlink", "intralink"):
            cards: list[str] = []
            for table in grouped.get(category, []):
                try:
                    count = int(self.db.get_record_count(table))
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
        return self._render_layout(title="Home", body_html="".join(sections) + self._render_search_form({}))

    def _render_table_page(self, table: str, query: dict[str, list[str]]) -> str:
        html_text = super()._render_table_page(table, query)
        if self._is_writable_table(table):
            create_href = "/tables/{}/new".format(quote(table, safe=""))
            actions_html = "<a href='{}'>Create row</a> ".format(_escape(create_href))
            if str(table) == "files":
                actions_html += "<a href='/files/upload'>Upload file</a> "
            html_text = html_text.replace("<div class='actions'>", "<div class='actions'>{}".format(actions_html), 1)
        return html_text

    def _render_write_error_panel(self, error_text: str) -> str:
        if not str(error_text or "").strip():
            return ""
        return self._render_notice_panel(kind="error", title="Write failed", message=str(error_text))

    def _render_row_page(self, table: str, raw_row_id: str, *, write_error_text: str = "") -> str:
        if not self._table_exists(table):
            return self._render_layout(title="Missing table", body_html="<section class='panel'><h2>Unknown table</h2></section>")
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception:
            return self._render_layout(
                title="Bad row id",
                body_html="<section class='panel'><h2>Invalid row id</h2><p>{}</p></section>".format(_escape(raw_row_id)),
            )
        row = self.db.get_row_from_id(table, row_id)
        if row is None:
            return self._render_layout(
                title="Missing row",
                body_html="<section class='panel'><h2>Row not found</h2><p>{}:{}</p></section>".format(_escape(table), row_id),
            )

        row_data = self._row_dict(table, row)
        actions: list[str] = ["<a href='/tables/{}'>Back to table</a>".format(_escape(quote(table, safe="")))]
        if self._is_writable_table(table):
            actions.append("<a href='/tables/{}/{}/edit'>Edit row</a>".format(_escape(quote(table, safe="")), row_id))
            actions.append("<a href='/tables/{}/{}/delete'>Delete row</a>".format(_escape(quote(table, safe="")), row_id))
        if table in {"works", "items"}:
            actions.append("<a href='/tables/{}/{}/upload'>Upload file</a>".format(_escape(quote(table, safe="")), row_id))
        if table == "files" and self.config.enable_file_downloads:
            capabilities = self._file_capabilities(row)
            if capabilities["downloadable"]:
                actions.append("<a href='/files/{}/download'>Download file</a>".format(row_id))
            if capabilities["preview_kind"]:
                actions.append("<a href='/files/{}/preview'>Preview file</a>".format(row_id))

        related_rows_by_table = self._related_rows_by_table(row)
        error_html = self._render_write_error_panel(write_error_text)
        link_manager_html = self._render_link_management_section(row) if self._is_writable_table(table) else ""
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
            return self._render_layout(title="{}:{}".format(table, row_id), body_html=error_html + body + link_manager_html)
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
            return self._render_layout(title="{}:{}".format(table, row_id), body_html=error_html + body + link_manager_html)
        if table == "stores":
            body = self._render_store_detail_page(
                row=row,
                row_id=row_id,
                row_data=row_data,
                actions=actions,
                related_rows_by_table=related_rows_by_table,
            )
            return self._render_layout(title="{}:{}".format(table, row_id), body_html=error_html + body + link_manager_html)

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
            body_html=error_html + body + link_manager_html + self._render_related_sections(row, related_rows_by_table=related_rows_by_table),
        )

    def _parse_form(self, environ) -> dict[str, str]:
        body = self._read_request_body(environ, limit=1024 * 1024)
        charset = "utf-8"
        content_type = str(environ.get("CONTENT_TYPE", "") or "")
        if "charset=" in content_type:
            charset = content_type.split("charset=", 1)[1].split(";", 1)[0].strip() or "utf-8"
        parsed = parse_qs(body.decode(charset, "replace"), keep_blank_values=True)
        return {str(key): str(values[0]) if values else "" for key, values in parsed.items()}

    def _read_request_body(self, environ, *, limit: int) -> bytes:
        try:
            content_length = int(str(environ.get("CONTENT_LENGTH", "0") or "0"))
        except Exception:
            content_length = 0
        content_length = max(0, min(content_length, int(limit)))
        return environ.get("wsgi.input", BytesIO()).read(content_length) if content_length else b""

    def _parse_multipart_form(self, environ) -> tuple[dict[str, str], Optional[dict[str, object]]]:
        content_type = str(environ.get("CONTENT_TYPE", "") or "")
        body = self._read_request_body(environ, limit=64 * 1024 * 1024)
        values: dict[str, str] = {}
        upload_info: Optional[dict[str, object]] = None
        if not body:
            return values, upload_info
        parser = BytesParser(policy=email_policy_default)
        message = parser.parsebytes(
            ("Content-Type: {}\r\nMIME-Version: 1.0\r\n\r\n".format(content_type)).encode("utf-8") + body
        )
        if not message.is_multipart():
            return values, upload_info
        for part in message.iter_parts():
            if part.get_content_disposition() != "form-data":
                continue
            name = str(part.get_param("name", header="content-disposition") or "").strip()
            if not name:
                continue
            filename = part.get_filename()
            payload = part.get_payload(decode=True) or b""
            if filename is not None:
                upload_info = {
                    "field_name": name,
                    "filename": str(filename),
                    "content_type": str(part.get_content_type() or ""),
                    "bytes": bytes(payload),
                }
                continue
            charset = part.get_content_charset() or "utf-8"
            values[name] = payload.decode(charset, "replace")
        return values, upload_info

    def _is_writable_table(self, table: str) -> bool:
        return self._writability_error(table) is None

    def _writability_error(self, table: str) -> Optional[str]:
        if not self._table_exists(table):
            return "Unknown table."
        if self.db.driver_wrapper.is_view(table):
            return "Views and compatibility surfaces are read-only."
        if self._is_trigger_locked_table(table):
            return "This table is managed reference data and is read-only."
        return None

    def _is_trigger_locked_table(self, table: str) -> bool:
        # The bootstrap version table is schema-owned reference data.  Driver
        # trigger inspection is intentionally unavailable across Core/RPC.
        return str(table) == "database_version"

    def _editable_columns(self, table: str) -> list[str]:
        if not self._is_writable_table(table):
            return []
        id_column = self._id_column(table)
        try:
            datestamp_column = self.db.driver_wrapper.get_datestamp_column(table)
        except Exception:
            datestamp_column = None
        columns = []
        for column in self.db.get_column_headings(table):
            if column == id_column:
                continue
            if datestamp_column and column == datestamp_column:
                continue
            if str(column).endswith("_scratch"):
                continue
            columns.append(str(column))
        return columns

    def _preview_rows(self, table: str, *, limit: int = 80) -> list[object]:
        rows: list[object] = []
        if not self._table_exists(table):
            return rows
        try:
            iterator = self.db.get_all_rows(table)
        except Exception:
            return rows
        for row in iterator:
            rows.append(row)
            if len(rows) >= max(1, int(limit)):
                break
        return rows

    def _field_choice_options(self, *, table: str, column: str) -> list[tuple[str, str]] | None:
        lowered = str(column or "").lower()
        candidate_tables: list[str] = []
        if str(table) == "agents" and lowered == "agent_type":
            return [
                ("person", "person"),
                ("organisation", "organisation"),
                ("group", "group"),
                ("pseudonym", "pseudonym"),
            ]
        if str(table) == "stores" and lowered == "store_kind":
            return [(kind, kind) for kind in _STORE_BACKEND_KINDS]
        if str(table) == "stores" and lowered == "store_access_protocol":
            return [
                ("", "Unset"),
                ("file", "file"),
                ("sqlite", "sqlite"),
                ("http", "http"),
                ("https", "https"),
                ("rclone", "rclone"),
                ("squashfs", "squashfs"),
            ]
        if str(table) == "stores" and lowered == "store_auth_method":
            return [
                ("", "Unset"),
                ("none", "none"),
                ("filesystem_permissions", "filesystem_permissions"),
                ("basic", "basic"),
                ("token", "token"),
                ("cookie", "cookie"),
            ]
        if lowered.endswith("_type"):
            candidate_tables.append("{}__types".format(table))
            candidate_tables.append("allowed_types__{}".format(table))
        for candidate_table in candidate_tables:
            if not self._table_exists(candidate_table):
                continue
            try:
                headings = list(self.db.get_column_headings(candidate_table))
            except Exception:
                continue
            value_column = "type" if "type" in headings else (headings[0] if headings else None)
            if not value_column:
                continue
            options: list[tuple[str, str]] = []
            seen: set[str] = set()
            try:
                option_rows = list(self.db.driver_wrapper.get_all_rows(candidate_table))[:200]
            except Exception:
                option_rows = []
            for option_row in option_rows:
                value = "" if option_row[value_column] is None else str(option_row[value_column])
                if not value or value in seen:
                    continue
                seen.add(value)
                options.append((value, value))
            if options:
                return options
        return None

    def _referenced_table_for_column(self, *, table: str, column: str) -> str | None:
        if not str(column or "").lower().endswith("_id"):
            return None
        try:
            id_column = self._id_column(table)
        except Exception:
            id_column = None
        if id_column and str(column) == str(id_column):
            return None
        suffix_matches: list[str] = []
        column_text = str(column)
        for candidate_table in sorted(str(one) for one in self.db.get_tables()):
            if candidate_table == str(table) or candidate_table.endswith("__types"):
                continue
            try:
                candidate_id = self._id_column(candidate_table)
            except Exception:
                continue
            if candidate_id and column_text.endswith(str(candidate_id)):
                suffix_matches.append(candidate_table)
        if len(suffix_matches) == 1:
            return suffix_matches[0]
        try:
            target_table = self.db.driver_wrapper.identify_table_from_column(column, error=False)
        except Exception:
            return None
        if not target_table:
            return None
        target_table = str(target_table)
        if target_table == str(table):
            if suffix_matches:
                return suffix_matches[0]
            return None
        if not self._table_exists(target_table):
            return None
        return target_table

    def _field_reference_options(self, *, table: str, column: str) -> tuple[str, list[tuple[str, str]]] | None:
        target_table = self._referenced_table_for_column(table=table, column=column)
        if not target_table:
            return None
        try:
            target_id_column = self._id_column(target_table)
        except Exception:
            return None
        options: list[tuple[str, str]] = []
        for row in self._preview_rows(target_table, limit=80):
            row_id = row[target_id_column]
            label = "{} - {}".format(row_id, _short_text(self._row_label(target_table, row), width=96))
            options.append((str(row_id), label))
        return target_table, options

    @staticmethod
    def _widget_kind(column: str, value: object) -> str:
        lowered = str(column or "").lower()
        if "json" in lowered:
            return "json"
        if lowered.endswith("_timestamp_ep_k") or (lowered.endswith("_ep_k") and "timestamp" in lowered):
            return "datetime_ms"
        if lowered.endswith("_date"):
            return "date"
        if isinstance(value, bool) or lowered.startswith("is_") or lowered.endswith(("_enabled", "_disabled", "_bool")):
            return "bool"
        if isinstance(value, float):
            return "float"
        if (isinstance(value, int) and not isinstance(value, bool)) or lowered.endswith(("_id", "_count", "_priority", "_index", "_number", "_bytes", "_size")):
            return "int"
        if any(token in lowered for token in ("note", "comment", "synopsis", "annotation", "html", "description", "text")):
            return "textarea"
        return "text"

    @staticmethod
    def _normalized_date_input_value(value: object) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except Exception:
                continue
        if len(text) >= 10:
            prefix = text[:10]
            try:
                return datetime.strptime(prefix, "%Y-%m-%d").strftime("%Y-%m-%d")
            except Exception:
                return ""
        return ""

    @staticmethod
    def _normalized_datetime_local_input_value(value: object) -> str:
        if value in (None, ""):
            return ""
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                return datetime.fromtimestamp(float(value) / 1000.0).strftime("%Y-%m-%dT%H:%M")
            except Exception:
                return ""
        text = str(value).strip()
        if not text:
            return ""
        if text.isdigit():
            try:
                return datetime.fromtimestamp(int(text) / 1000.0).strftime("%Y-%m-%dT%H:%M")
            except Exception:
                return ""
        for candidate in (text, text.replace("Z", "+00:00")):
            try:
                parsed = datetime.fromisoformat(candidate)
                return parsed.strftime("%Y-%m-%dT%H:%M")
            except Exception:
                continue
        for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%dT%H:%M")
            except Exception:
                continue
        return ""

    @staticmethod
    def _pretty_json_text(value: object) -> str:
        text = str(value or "")
        stripped = text.strip()
        if not stripped:
            return text
        try:
            parsed = json.loads(stripped)
        except Exception:
            return text
        return json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)

    @staticmethod
    def _coerce_form_value(raw: str, *, column: str, current_value: object = None, for_create: bool = False):
        text = str(raw or "")
        stripped = text.strip()
        kind = ReadWriteWebApplication._widget_kind(column, current_value)
        if stripped.upper() == "NULL":
            return None
        if kind == "json":
            if stripped == "":
                return None if for_create else text
            try:
                json.loads(stripped)
            except Exception as exc:
                raise ValueError("Invalid JSON for {}: {}".format(column, exc)) from exc
            return text
        if kind == "date":
            if stripped == "":
                return None if for_create else text
            normalized = ReadWriteWebApplication._normalized_date_input_value(stripped)
            if not normalized:
                raise ValueError("Invalid date for {}. Use YYYY-MM-DD.".format(column))
            return normalized
        if kind == "datetime_ms":
            if stripped == "":
                return None
            if stripped.isdigit():
                try:
                    return int(stripped)
                except Exception:
                    return text
            normalized = ReadWriteWebApplication._normalized_datetime_local_input_value(stripped)
            if not normalized:
                raise ValueError("Invalid datetime for {}. Use YYYY-MM-DDTHH:MM or epoch milliseconds.".format(column))
            try:
                return int(datetime.strptime(normalized, "%Y-%m-%dT%H:%M").timestamp() * 1000)
            except Exception as exc:
                raise ValueError("Invalid datetime for {}.".format(column)) from exc
        if kind == "bool":
            if stripped == "":
                return None
            lowered = stripped.lower()
            if lowered in {"1", "true", "yes", "on", "checked"}:
                return True
            if lowered in {"0", "false", "no", "off", "unchecked"}:
                return False
            return text
        if kind == "int":
            if stripped == "":
                return None
            try:
                return int(stripped)
            except Exception:
                return text
        if kind == "float":
            if stripped == "":
                return None
            try:
                return float(stripped)
            except Exception:
                return text
        if stripped == "" and for_create:
            return None
        return text

    def _form_field_html(
        self,
        *,
        table: str,
        column: str,
        value: object,
        input_name: Optional[str] = None,
        input_id: Optional[str] = None,
        label_text: Optional[str] = None,
    ) -> str:
        text_value = "" if value is None else str(value)
        input_name = str(input_name or column)
        input_id = str(input_id or input_name)
        label_text = str(label_text or column)
        kind = self._widget_kind(column, value)
        lowered = str(column).lower()
        choice_options = self._field_choice_options(table=table, column=column)
        reference_options = self._field_reference_options(table=table, column=column)
        help_parts: list[str] = []
        text_input_attrs = ""

        def _append_allowed_values_help(values: list[str]) -> None:
            visible = [str(one) for one in values if str(one).strip()]
            if not visible:
                return
            preview_limit = 12
            preview = visible[:preview_limit]
            message = "Allowed values: {}.".format(
                ", ".join("<code>{}</code>".format(_escape(raw)) for raw in preview)
            )
            remaining = len(visible) - len(preview)
            if remaining > 0:
                message += " <span class='meta'>+ {} more in dropdown.</span>".format(int(remaining))
            help_parts.append(message)

        is_uri_like = any(token in lowered for token in ("uri", "url"))
        is_path_like = any(token in lowered for token in ("path", "location"))
        if is_uri_like:
            help_parts.append("Enter an absolute URI or URL when possible.")
            text_input_attrs = " spellcheck='false' autocapitalize='off' autocomplete='off'"
        elif is_path_like:
            help_parts.append("Enter a filesystem path or location string.")
            text_input_attrs = " spellcheck='false' autocapitalize='off' autocomplete='off'"

        if choice_options:
            options = ["<option value=''>Unset</option>"]
            current = text_value.strip()
            for raw, label in choice_options:
                selected = " selected" if current == str(raw) else ""
                options.append(
                    "<option value='{raw}'{selected}>{label}</option>".format(
                        raw=_escape(raw),
                        selected=selected,
                        label=_escape(label),
                    )
                )
            control = "<select id='{id}' name='{name}'>{options}</select>".format(
                id=_escape(input_id),
                name=_escape(input_name),
                options="".join(options),
            )
            _append_allowed_values_help([str(raw) for raw, _label in choice_options])
        elif kind == "textarea":
            control = "<textarea id='{id}' name='{name}'>{value}</textarea>".format(
                id=_escape(input_id),
                name=_escape(input_name),
                value=_escape(text_value),
            )
        elif kind == "json":
            control = "<textarea id='{id}' name='{name}' spellcheck='false' autocapitalize='off' autocomplete='off'>{value}</textarea>".format(
                id=_escape(input_id),
                name=_escape(input_name),
                value=_escape(self._pretty_json_text(text_value)),
            )
            help_parts.append("Expected valid JSON text. Invalid JSON will be rejected.")
        elif kind == "date":
            normalized_date = self._normalized_date_input_value(text_value)
            if text_value.strip() and not normalized_date:
                control = "<input id='{id}' name='{name}' type='text' value='{value}' placeholder='YYYY-MM-DD'{attrs}>".format(
                    id=_escape(input_id),
                    name=_escape(input_name),
                    value=_escape(text_value),
                    attrs=text_input_attrs,
                )
            else:
                control = "<input id='{id}' name='{name}' type='date' value='{value}'{attrs}>".format(
                    id=_escape(input_id),
                    name=_escape(input_name),
                    value=_escape(normalized_date),
                    attrs=text_input_attrs,
                )
            help_parts.append("Use YYYY-MM-DD.")
        elif kind == "datetime_ms":
            normalized_datetime = self._normalized_datetime_local_input_value(text_value)
            if text_value.strip() and not normalized_datetime:
                control = "<input id='{id}' name='{name}' type='text' value='{value}' placeholder='YYYY-MM-DDTHH:MM or epoch ms'{attrs}>".format(
                    id=_escape(input_id),
                    name=_escape(input_name),
                    value=_escape(text_value),
                    attrs=text_input_attrs,
                )
            else:
                control = "<input id='{id}' name='{name}' type='datetime-local' value='{value}'{attrs}>".format(
                    id=_escape(input_id),
                    name=_escape(input_name),
                    value=_escape(normalized_datetime),
                    attrs=text_input_attrs,
                )
            help_parts.append("Use local date/time or epoch milliseconds; stored as epoch ms.")
        elif kind == "bool":
            options = []
            current = text_value.strip().lower()
            for raw, label in (("", "Unset"), ("true", "True"), ("false", "False")):
                selected = " selected" if current == raw else ""
                options.append("<option value='{raw}'{selected}>{label}</option>".format(raw=_escape(raw), selected=selected, label=label))
            control = "<select id='{id}' name='{name}'>{options}</select>".format(
                id=_escape(input_id),
                name=_escape(input_name),
                options="".join(options),
            )
            _append_allowed_values_help(["true", "false"])
        else:
            use_reference_datalist = reference_options is not None and kind in {"int", "text"}
            input_type = "number" if kind in {"int", "float"} and not use_reference_datalist else "text"
            step_attr = " step='any'" if kind == "float" else ""
            list_attr = ""
            datalist_html = ""
            if use_reference_datalist and reference_options is not None:
                referenced_table, options = reference_options
                datalist_id = "{}__options".format(input_id)
                list_attr = " list='{}'".format(_escape(datalist_id))
                datalist_html = "<datalist id='{id}'>{options}</datalist>".format(
                    id=_escape(datalist_id),
                    options="".join(
                        "<option value='{value}' label='{label}'></option>".format(
                            value=_escape(raw),
                            label=_escape(label),
                        )
                        for raw, label in options
                    ),
                )
                help_parts.append("Suggestions from <code>{}</code>.".format(_escape(referenced_table)))
            control = "<input id='{id}' name='{name}' type='{input_type}' value='{value}'{step}{list_attr}{attrs}>{datalist_html}".format(
                id=_escape(input_id),
                name=_escape(input_name),
                input_type=input_type,
                value=_escape(text_value),
                step=step_attr,
                list_attr=list_attr,
                attrs=text_input_attrs,
                datalist_html=datalist_html,
            )
        if value is None and str(column).lower().endswith("_id"):
            help_parts.append("Enter a row id or pick a suggested value.")
        help_html = ""
        if help_parts:
            help_html = "<div class='field-help'>{}</div>".format(" ".join(help_parts))
        return """
<div class='field'>
  <label for='{id}'>{label}</label>
  <code>{column}</code>
  {control}
  {help_html}
</div>
""".format(id=_escape(input_id), label=_escape(label_text), column=_escape(column), control=control, help_html=help_html)

    def _render_row_form(
        self,
        *,
        table: str,
        action: str,
        submit_label: str,
        values: dict[str, object],
        error_text: str = "",
        intro_html: str = "",
    ) -> str:
        fields = []
        for column in self._editable_columns(table):
            fields.append(self._form_field_html(table=table, column=column, value=values.get(column)))
        if not fields:
            fields.append("<p class='empty'>This table has no writable columns in the generic form.</p>")
        error_html = ""
        if error_text:
            error_html = self._render_notice_panel(kind="error", title="Write failed", message=str(error_text))
        body = """
{error_html}
<section class='panel'>
  <h2>{title}</h2>
  {intro_html}
  <form class='write-form' method='post' action='{action}'>
    <div class='write-grid'>{fields}</div>
    <div class='actions'>
      <button type='submit'>{submit_label}</button>
      <a href='/tables/{table}'>Cancel</a>
    </div>
  </form>
</section>
""".format(
            error_html=error_html,
            title=_escape("{} <{}>".format(submit_label, table)),
            intro_html=intro_html,
            action=_escape(action),
            fields="".join(fields),
            submit_label=_escape(submit_label),
            table=_escape(quote(table, safe="")),
        )
        return self._render_layout(title="{} {}".format(submit_label, table), body_html=body)

    def _field_label(self, table: str, column: str) -> str:
        spec = self._SPECIAL_FORM_SPECS.get(str(table), {})
        labels = dict(spec.get("field_labels") or {})
        if column in labels:
            return str(labels[column])
        return " ".join(part.capitalize() for part in str(column).replace("_", " ").split())

    def _writable_store_rows(self) -> list[object]:
        if not self._table_exists("stores"):
            return []
        rows: list[object] = []
        try:
            iterator = self.db.get_all_rows("stores")
        except Exception:
            return rows
        for row in iterator:
            if int(_coerce_int(_row_value(row, "store_is_read_only"), default=0) or 0) != 0:
                continue
            rows.append(row)
        rows.sort(key=lambda one: (str(_row_value(one, "store_name") or "").lower(), int(_row_value(one, "store_id") or 0)))
        return rows

    @staticmethod
    def _upload_basename(filename: str) -> str:
        text = str(filename or "").replace("\\", "/").split("/")[-1].strip()
        return text or "upload.bin"

    def _resolve_upload_context(self, context_table: Optional[str], raw_row_id: Optional[str]):
        if not context_table:
            return None
        table = str(context_table).strip()
        if table not in {"works", "items"}:
            raise ValueError("Unsupported upload context {!r}.".format(table))
        try:
            row_id = int(str(raw_row_id or "").strip())
        except Exception as exc:
            raise ValueError("Invalid row id for upload context.") from exc
        row = self.db.get_row_from_id(table, row_id)
        if row is None:
            raise ValueError("No {} row found for id {}.".format(table, row_id))
        return table, row

    def _storage_key_for_stored_file(self, *, store_row, stored_file) -> str:
        if isinstance(stored_file, Mapping):
            store_key = str(stored_file.get("store_key") or "").strip()
            if store_key:
                return store_key
            file_url = str(stored_file.get("file_url") or "").strip()
        else:
            file_url = str(getattr(stored_file, "file_url", "") or "").strip()
        if not file_url:
            return ""
        root_uri = str(_row_value(store_row, "store_root_uri") or "").strip()
        access_protocol = str(_row_value(store_row, "store_access_protocol") or "").strip().lower()
        local_root: Optional[Path] = None
        if root_uri.startswith("file://"):
            local_root = Path(root_uri[7:])
        elif root_uri and access_protocol in {"", "file", "local"}:
            local_root = Path(root_uri)
        if local_root is not None:
            try:
                resolved_file = Path(file_url).expanduser().resolve(strict=False)
                resolved_root = local_root.expanduser().resolve(strict=False)
                return str(resolved_file.relative_to(resolved_root)).replace("\\", "/")
            except Exception:
                return file_url
        return file_url

    def _render_file_upload_page(
        self,
        *,
        error_text: str = "",
        values: Optional[dict[str, object]] = None,
        context_table: Optional[str] = None,
        raw_row_id: Optional[str] = None,
    ) -> str:
        current_values = dict(values or {})
        upload_context = None
        if context_table:
            try:
                upload_context = self._resolve_upload_context(context_table, raw_row_id)
            except Exception as exc:
                return self._render_layout(
                    title="Upload file",
                    body_html="<section class='panel'><h2>Upload file</h2><p class='danger'>{}</p></section>".format(_escape(str(exc))),
                )
        stores = self._writable_store_rows()
        error_html = self._render_write_error_panel(error_text)
        if not stores:
            body = """
{error_html}
<section class='panel'>
  <h2>Upload file</h2>
  <p class='meta'>No writable stores are currently available. Create a writable store first, then return here.</p>
  <div class='actions'>
    <a href='/tables/stores/new'>Create store</a>
    <a href='/tables/files'>Back to files</a>
  </div>
</section>
""".format(error_html=error_html)
            return self._render_layout(title="Upload file", body_html=body)

        store_options = ["<option value=''>Choose a store</option>"]
        selected_store_id = str(current_values.get("store_id") or "").strip()
        for row in stores:
            store_id = int(_row_value(row, "store_id") or 0)
            label = "{} - {} ({})".format(
                store_id,
                _short_text(str(_row_value(row, "store_name") or "unnamed store"), width=48),
                str(_row_value(row, "store_kind") or "unknown"),
            )
            selected = " selected" if selected_store_id == str(store_id) else ""
            store_options.append("<option value='{value}'{selected}>{label}</option>".format(value=store_id, selected=selected, label=_escape(label)))

        metadata_fields = [
            self._form_field_html(table="files", column="file_role", value=current_values.get("file_role"), label_text="Role"),
            self._form_field_html(table="files", column="file_media_category", value=current_values.get("file_media_category"), label_text="Media category"),
            self._form_field_html(table="files", column="file_tag", value=current_values.get("file_tag"), label_text="Tag"),
            self._form_field_html(table="files", column="file_source", value=current_values.get("file_source", "web_upload"), label_text="Source"),
            self._form_field_html(table="files", column="file_mime_type", value=current_values.get("file_mime_type"), label_text="MIME type override"),
        ]
        context_intro = "<p class='meta'>Upload bytes into a writable store and create the matching <code>files</code> row.</p>"
        action_path = "/files/upload"
        cancel_path = "/tables/files"
        extra_sections = ""
        if upload_context is not None:
            resolved_table, resolved_row = upload_context
            row_id = int(_row_value(resolved_row, self._id_column(resolved_table)) or 0)
            action_path = "/tables/{}/{}/upload".format(quote(resolved_table, safe=""), row_id)
            cancel_path = self._row_path(resolved_table, row_id)
            context_intro = "<p class='meta'>Target row: <code>{table}:{row_id}</code> ({label}).</p>".format(
                table=_escape(resolved_table),
                row_id=row_id,
                label=_escape(_short_text(self._row_label(resolved_table, resolved_row), width=120)),
            )
            if resolved_table == "items":
                extra_sections = """
<section class='write-group'>
  <h3>Attachment target</h3>
  <p class='meta'>This upload will attach directly to the existing item row.</p>
  <div class='write-grid'>
    {item_source}
    {item_source_name}
  </div>
</section>
""".format(
                    item_source=self._form_field_html(table="items", column="item_source", value=current_values.get("item_source", "web_upload"), label_text="Item source"),
                    item_source_name=self._form_field_html(table="items", column="item_source_name", value=current_values.get("item_source_name"), label_text="Item source name"),
                )
            elif resolved_table == "works":
                extra_sections = """
<section class='write-group'>
  <h3>Generated chain</h3>
  <p class='meta'>This upload will create a new expression, manifestation, and item linked to the target work before the file row is created.</p>
  <div class='write-grid'>
    {expression_label}
    {expression_language}
    {manifestation_carrier}
    {manifestation_format}
    {item_type}
    {item_source}
    {item_source_name}
    {item_location}
  </div>
</section>
""".format(
                    expression_label=self._form_field_html(table="expressions", column="expression_label", value=current_values.get("expression_label"), label_text="Expression label"),
                    expression_language=self._form_field_html(table="expressions", column="expression_language_id", value=current_values.get("expression_language_id"), label_text="Expression language row id"),
                    manifestation_carrier=self._form_field_html(table="manifestations", column="manifestation_carrier_type", value=current_values.get("manifestation_carrier_type"), label_text="Carrier type"),
                    manifestation_format=self._form_field_html(table="manifestations", column="manifestation_format_detail", value=current_values.get("manifestation_format_detail"), label_text="Format detail"),
                    item_type=self._form_field_html(table="items", column="item_type", value=current_values.get("item_type"), label_text="Item type"),
                    item_source=self._form_field_html(table="items", column="item_source", value=current_values.get("item_source", "web_upload"), label_text="Item source"),
                    item_source_name=self._form_field_html(table="items", column="item_source_name", value=current_values.get("item_source_name"), label_text="Item source name"),
                    item_location=self._form_field_html(table="items", column="item_location", value=current_values.get("item_location"), label_text="Item location"),
                )
        body = """
{error_html}
<section class='panel'>
  <h2>Upload file</h2>
  <p class='meta'>This writes bytes through the storage manager, then creates the matching <code>files</code> row. The selected backend still decides the final on-disk placement policy.</p>
  {context_intro}
  <form class='write-form' method='post' action='{action_path}' enctype='multipart/form-data'>
    <section class='write-group'>
      <h3>Payload</h3>
      <div class='write-grid'>
        <div class='field'>
          <label for='upload-store-id'>Target store</label>
          <code>store_id</code>
          <select id='upload-store-id' name='store_id'>{store_options}</select>
          <div class='field-help'>Only writable stores are listed here.</div>
        </div>
        <div class='field'>
          <label for='upload-file'>File payload</label>
          <code>upload_file</code>
          <input id='upload-file' name='upload_file' type='file'>
          <div class='field-help'>Choose one local file to place into the selected store.</div>
        </div>
        <div class='field'>
          <label for='upload-file-name'>Filename override</label>
          <code>file_name</code>
          <input id='upload-file-name' name='file_name' type='text' value='{file_name}'>
          <div class='field-help'>Optional display name for the resulting <code>files</code> row. The backend may still choose a different storage key.</div>
        </div>
      </div>
    </section>
    {extra_sections}
    <section class='write-group'>
      <h3>File metadata</h3>
      <div class='write-grid'>{metadata_fields}</div>
    </section>
    <div class='actions'>
      <button type='submit'>Upload file</button>
      <a href='{cancel_path}'>Cancel</a>
    </div>
  </form>
</section>
""".format(
            error_html=error_html,
            context_intro=context_intro,
            action_path=_escape(action_path),
            cancel_path=_escape(cancel_path),
            store_options="".join(store_options),
            file_name=_escape(str(current_values.get("file_name") or "")),
            extra_sections=extra_sections,
            metadata_fields="".join(metadata_fields),
        )
        return self._render_layout(title="Upload file", body_html=body)

    def _create_file_row_from_upload(
        self,
        *,
        form: dict[str, str],
        upload: dict[str, object],
        store_row,
        store_id: int,
        preferred_store: str,
        item_id: Optional[int] = None,
    ):
        uploaded_name = self._upload_basename(str(upload.get("filename") or "upload.bin"))
        display_name = self._upload_basename(str(form.get("file_name") or uploaded_name))
        metadata: dict[str, object] = {
            "file_name": display_name,
            "original_name": uploaded_name,
            "file_extension": Path(display_name).suffix.lstrip("."),
            "file_store_id": store_id,
            "preferred_store": preferred_store,
        }
        if item_id is not None:
            metadata["file_item_id"] = int(item_id)
            metadata["item_id"] = int(item_id)
        raw_mime_type = str(form.get("file_mime_type", "") or "").strip() or str(upload.get("content_type") or "").strip()
        if raw_mime_type and raw_mime_type.lower() != "application/octet-stream":
            metadata["file_mime_type"] = raw_mime_type

        put_result = self.core.command(
            "storage.file.put",
            {
                "content_base64": base64.b64encode(
                    bytes(upload.get("bytes") or b"")
                ).decode("ascii"),
                "metadata": metadata,
                "preferred_store": preferred_store,
            },
        )
        if not isinstance(put_result, Mapping):
            raise TypeError("storage.file.put returned an invalid result.")
        stored_file = put_result.get("location", {})

        mime_type = str(metadata.get("file_mime_type") or "").strip()
        if not mime_type:
            mime_type = str(mimetypes.guess_type(display_name, strict=False)[0] or "")
        extension = Path(display_name).suffix.lstrip(".")
        file_payload: dict[str, object] = {
            "file_store_id": store_id,
            "file_storage_key": self._storage_key_for_stored_file(store_row=store_row, stored_file=stored_file),
            "file_name": display_name,
            "file_base_name": Path(display_name).stem,
            "file_extension": extension or None,
            "file_mime_type": mime_type or None,
            "file_size_bytes": len(bytes(upload.get("bytes") or b"")),
            "file_original_name": uploaded_name,
            "file_source": str(form.get("file_source") or "web_upload"),
        }
        if item_id is not None:
            file_payload["file_item_id"] = int(item_id)
        for optional_column in ("file_role", "file_media_category", "file_tag"):
            if optional_column not in self._editable_columns("files"):
                continue
            value = self._coerce_form_value(str(form.get(optional_column, "") or ""), column=optional_column, current_value=None, for_create=True)
            if value is not None:
                file_payload[optional_column] = value
        receipt = self.model.create_row("files", file_payload)
        record = receipt.get("record")
        if not isinstance(record, Mapping):
            raise TypeError("admin.row.create did not return a file record.")
        return self.model.row_from_record(record)

    def _create_item_chain_for_work_upload(self, *, work_row, form: dict[str, str], fallback_label: str):
        expression_payload: dict[str, object] = {
            "expression_label": str(form.get("expression_label") or fallback_label or "Web upload").strip() or "Web upload",
        }
        expression_language = self._coerce_form_value(str(form.get("expression_language_id", "") or ""), column="expression_language_id", current_value=None, for_create=True)
        if expression_language is not None:
            expression_payload["expression_language_id"] = expression_language
        manifestation_payload: dict[str, object] = {
            "manifestation_format_detail": str(form.get("manifestation_format_detail") or Path(fallback_label).suffix.lstrip(".").upper() or "upload").strip() or "upload",
        }
        manifestation_carrier = str(form.get("manifestation_carrier_type", "") or "").strip()
        if manifestation_carrier:
            manifestation_payload["manifestation_carrier_type"] = manifestation_carrier
        item_payload: dict[str, object] = {
            "item_source": str(form.get("item_source") or "web_upload"),
            "item_source_name": str(form.get("item_source_name") or fallback_label),
        }
        item_type = str(form.get("item_type", "") or "").strip()
        if item_type:
            item_payload["item_type"] = item_type
        item_location = str(form.get("item_location", "") or "").strip()
        if item_location:
            item_payload["item_location"] = item_location
        result = self.core.command(
            "catalog.wemi.create",
            {
                "work": {},
                "expression": expression_payload,
                "manifestation": manifestation_payload,
                "items": [item_payload],
                "origin": "web_upload",
                "work_id": int(work_row.row_id),
            },
        )
        if not isinstance(result, Mapping):
            raise TypeError("catalog.wemi.create returned an invalid result.")
        expression_row = self.model.row(
            "expressions",
            int(result["expression_id"]),
        )
        manifestation_row = self.model.row(
            "manifestations",
            int(result["manifestation_id"]),
        )
        item_ids = result.get("item_ids", ())
        if (
            expression_row is None
            or manifestation_row is None
            or not isinstance(item_ids, list)
            or not item_ids
        ):
            raise RuntimeError("Core did not return the created WEMI chain.")
        item_row = self.model.row("items", int(item_ids[0]))
        if item_row is None:
            raise RuntimeError("Core did not return the created item.")
        return expression_row, manifestation_row, item_row

    def _render_grouped_row_form(
        self,
        *,
        table: str,
        action: str,
        submit_label: str,
        values: dict[str, object],
        error_text: str = "",
        intro_html: str = "",
        mode: str,
    ) -> str:
        spec = self._SPECIAL_FORM_SPECS.get(str(table), {})
        columns = list(self._editable_columns(table))
        groups = list(spec.get("groups") or [])
        used_columns: set[str] = set()
        group_blocks: list[str] = []
        for title, group_columns in groups:
            ordered_columns = [str(column) for column in group_columns if str(column) in columns and str(column) not in used_columns]
            if not ordered_columns:
                continue
            used_columns.update(ordered_columns)
            fields = "".join(
                self._form_field_html(
                    table=table,
                    column=column,
                    value=values.get(column),
                    label_text=self._field_label(table, column),
                )
                for column in ordered_columns
            )
            group_blocks.append(
                """
<section class='write-group'>
  <h3>{title}</h3>
  <div class='write-grid'>{fields}</div>
</section>
""".format(title=_escape(str(title)), fields=fields)
            )
        remaining_columns = [column for column in columns if column not in used_columns]
        if remaining_columns:
            fields = "".join(
                self._form_field_html(
                    table=table,
                    column=column,
                    value=values.get(column),
                    label_text=self._field_label(table, column),
                )
                for column in remaining_columns
            )
            group_blocks.append(
                """
<section class='write-group'>
  <h3>Other fields</h3>
  <p class='meta'>Less common columns remain available here so the specialized form does not hide writable data.</p>
  <div class='write-grid'>{fields}</div>
</section>
""".format(fields=fields)
            )
        if not group_blocks:
            group_blocks.append("<p class='empty'>This table has no writable columns in the specialized form.</p>")
        error_html = ""
        if error_text:
            error_html = self._render_notice_panel(kind="error", title="Write failed", message=str(error_text))
        page_title = str(spec.get("{}_title".format(mode)) or "{} <{}>".format(submit_label, table))
        body = """
{error_html}
<section class='panel'>
  <h2>{title}</h2>
  {intro_html}
  <form class='write-form' method='post' action='{action}'>
    <div class='write-group-stack'>{groups}</div>
    <div class='actions'>
      <button type='submit'>{submit_label}</button>
      <a href='/tables/{table}'>Cancel</a>
    </div>
  </form>
</section>
""".format(
            error_html=error_html,
            title=_escape(page_title),
            intro_html=intro_html,
            action=_escape(action),
            groups="".join(group_blocks),
            submit_label=_escape(submit_label),
            table=_escape(quote(table, safe="")),
        )
        return self._render_layout(title=page_title, body_html=body)

    def _editable_link_columns(self, primary_table: str, secondary_table: str) -> tuple[str, list[str]]:
        link_table = str(self.db.driver_wrapper.get_link_table_name(primary_table, secondary_table) or "")
        if not link_table:
            raise ValueError("No interlink table exists for {} and {}.".format(primary_table, secondary_table))
        id_column = self._id_column(link_table)
        try:
            datestamp_column = self.db.driver_wrapper.get_datestamp_column(link_table)
        except Exception:
            datestamp_column = None
        primary_link_column = self.db.driver_wrapper.get_link_column(
            primary_table, secondary_table, self._id_column(primary_table)
        )
        secondary_link_column = self.db.driver_wrapper.get_link_column(
            primary_table, secondary_table, self._id_column(secondary_table)
        )
        excluded = {id_column, datestamp_column, primary_link_column, secondary_link_column}
        editable_columns = [
            str(column)
            for column in self.db.get_column_headings(link_table)
            if str(column) not in excluded and not str(column).endswith("_scratch")
        ]
        return link_table, editable_columns

    @staticmethod
    def _interlink_anchor(secondary_table: str) -> str:
        return "links-{}".format(str(secondary_table).replace("_", "-"))

    def _resolve_interlink_context(self, primary_table: str, raw_row_id: str, secondary_table: str) -> tuple[object, str, list[str]]:
        if not self._table_exists(primary_table):
            raise ValueError("Unknown table {!r}.".format(primary_table))
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception as exc:
            raise ValueError("Invalid row id {!r}.".format(raw_row_id)) from exc
        primary_row = self.db.get_row_from_id(primary_table, row_id)
        if primary_row is None:
            raise ValueError("No row found in {} for id {}.".format(primary_table, row_id))
        if not self._table_exists(secondary_table):
            raise ValueError("Unknown linked table {!r}.".format(secondary_table))
        link_table, editable_columns = self._editable_link_columns(primary_table, secondary_table)
        return primary_row, link_table, editable_columns

    def _link_section_spec(self, primary_table: str, secondary_table: str, link_table: str) -> dict[str, Any]:
        spec: dict[str, Any] = {
            "title": "Manage {}".format(self._pretty_table_name(secondary_table)),
            "intro": "Add, edit, or remove links to {}.".format(self._pretty_table_name(secondary_table).lower()),
            "target_label": "{} row id".format(self._pretty_table_name(secondary_table)),
            "add_label": "Add link",
            "browse_label": "Browse {}".format(self._pretty_table_name(secondary_table).lower()),
            "field_order": list(self.db.get_column_headings(link_table)),
            "field_labels": {},
            "open": False,
            "item_name": "link",
        }
        if str(primary_table) == "works":
            work_spec = self._WORK_LINK_SPECS.get(str(secondary_table))
            if work_spec:
                spec.update(work_spec)
        return spec

    def _link_target_field_name(self, column: str) -> str:
        return "create__{}".format(str(column))

    def _render_link_create_helper(
        self,
        *,
        primary_table: str,
        primary_row_id: int,
        secondary_table: str,
        link_table: str,
        ordered_columns: list[str],
        spec: dict[str, Any],
    ) -> str:
        create_fields = [str(one) for one in (spec.get("create_fields") or [])]
        create_disabled_reason = str(spec.get("create_disabled_reason") or "").strip()
        if create_disabled_reason:
            return "<div class='link-create-helper'><p class='meta'>{}</p></div>".format(_escape(create_disabled_reason))
        if not create_fields:
            return ""
        if not self._is_writable_table(secondary_table):
            reason = self._writability_error(secondary_table) or "This linked table is read-only."
            return "<div class='link-create-helper'><p class='meta'>{}</p></div>".format(_escape(reason))
        target_fields_html = []
        blank_target = {}
        try:
            blank_target = self.db.driver_wrapper.get_blank_row(secondary_table)
        except Exception:
            blank_target = {}
        field_labels = dict(spec.get("create_field_labels") or {})
        for column in create_fields:
            default_value = (spec.get("create_defaults") or {}).get(column, blank_target.get(column))
            target_fields_html.append(
                self._form_field_html(
                    table=secondary_table,
                    column=column,
                    value=default_value,
                    input_name=self._link_target_field_name(column),
                    input_id="{}-create-{}".format(secondary_table, column),
                    label_text=str(field_labels.get(column) or self._field_label(secondary_table, column)),
                )
            )
        link_fields_html = "".join(
            self._form_field_html(
                table=link_table,
                column=column,
                value="",
                input_id="{}-create-link-{}".format(secondary_table, column),
                label_text=self._link_field_label(column, spec),
            )
            for column in ordered_columns
        )
        return """
<div class='link-create-helper'>
  <h3>{title}</h3>
  <p class='meta'>{intro}</p>
  <form class='link-editor-form link-add-form' method='post' action='/tables/{primary_table_q}/{primary_row_id}/links/{secondary_table_q}/create#{anchor}'>
    <div class='write-grid'>{target_fields}</div>
    <div class='write-grid'>{link_fields}</div>
    <div class='actions'>
      <button type='submit'>{submit_label}</button>
    </div>
  </form>
</div>
""".format(
            title=_escape(str(spec.get("create_title") or "Create linked row")),
            intro=_escape(str(spec.get("create_intro") or "Create a linked row and attach it immediately.")),
            primary_table_q=_escape(quote(primary_table, safe="")),
            primary_row_id=int(primary_row_id),
            secondary_table_q=_escape(quote(secondary_table, safe="")),
            anchor=_escape(self._interlink_anchor(secondary_table)),
            target_fields="".join(target_fields_html),
            link_fields=link_fields_html,
            submit_label=_escape(str(spec.get("create_submit_label") or "Create + link")),
        )

    def _create_link_target_payload(self, *, secondary_table: str, spec: dict[str, Any], form: dict[str, str]) -> dict[str, object]:
        payload: dict[str, object] = {}
        defaults = dict(spec.get("create_defaults") or {})
        blank_target = {}
        try:
            blank_target = self.db.driver_wrapper.get_blank_row(secondary_table)
        except Exception:
            blank_target = {}
        for column in (spec.get("create_fields") or []):
            field_name = self._link_target_field_name(str(column))
            if field_name not in form:
                continue
            value = self._coerce_form_value(
                form[field_name],
                column=str(column),
                current_value=blank_target.get(str(column)),
                for_create=True,
            )
            if value is None:
                continue
            payload[str(column)] = value
        for column, value in defaults.items():
            if column not in payload and value is not None:
                payload[str(column)] = value
        if secondary_table == "agents":
            name = str(payload.get("agent_canonical_name") or "").strip()
            if name and not str(payload.get("agent_sort_name") or "").strip():
                payload["agent_sort_name"] = name
        if secondary_table == "series":
            name = str(payload.get("series") or "").strip()
            if name and not str(payload.get("series_sort") or "").strip():
                payload["series_sort"] = name
        if secondary_table == "tags":
            text = str(payload.get("tag") or "").strip()
            if text and "tag_phash" in self.db.get_column_headings("tags") and not str(payload.get("tag_phash") or "").strip():
                payload["tag_phash"] = self._metadata_tag_search_term(text)
        if secondary_table == "labels":
            text = str(payload.get("label_text") or payload.get("label") or "").strip()
            if text and "label_text_norm" in self.db.get_column_headings("labels") and not str(payload.get("label_text_norm") or "").strip():
                payload["label_text_norm"] = self._metadata_tag_search_term(text)
        return payload

    @staticmethod
    def _metadata_tag_search_term(text: str) -> str:
        return "".join(str(text or "").split()).lower()

    def _link_values_from_form(
        self,
        *,
        link_table: str,
        editable_columns: list[str],
        form: dict[str, str],
    ) -> tuple[Any, Any, dict[str, Any]]:
        priority: Any = "not_set"
        link_type = None
        extra_values: dict[str, Any] = {}
        link_column_base = self.db.driver_wrapper.get_column_base(link_table)
        blank_link = self.db.driver_wrapper.get_blank_row(link_table)
        for column in editable_columns:
            if column not in form:
                continue
            value = self._coerce_form_value(
                form[column],
                column=column,
                current_value=blank_link.get(column),
                for_create=True,
            )
            if value is None:
                continue
            suffix = str(column)
            prefix = "{}_".format(link_column_base)
            if suffix.startswith(prefix):
                suffix = suffix[len(prefix):]
            if suffix == "priority":
                priority = value
            elif suffix == "type":
                link_type = value
            else:
                extra_values[suffix] = value
        return priority, link_type, extra_values

    def _write_metadata_relation_link(
        self,
        *,
        primary_row,
        secondary_table: str,
        secondary_row,
        priority: Any,
        link_type: Any,
        extra_values: dict[str, Any],
    ):
        normalized_priority = (
            None
            if priority in (None, "", "not_set", "highest")
            else int(priority)
        )
        self.model.link(
            primary_row,
            secondary_row,
            priority=normalized_priority,
            link_type=link_type,
            values=extra_values,
        )
        return _CoreLinkReport()

    def _metadata_relation_notice_message(self, *, action: str, item_name: str, report: Any) -> str:
        del report
        return "{} {}.".format(action, item_name)

    def _refresh_read_source_after_write(self) -> bool:
        return self.model.refresh()

    @staticmethod
    def _with_cache_refresh_note(message: str, *, refreshed: bool) -> str:
        if not refreshed:
            return message
        return "{} Read cache refreshed.".format(message)

    def _ordered_link_fields(self, editable_columns: list[str], spec: dict[str, Any]) -> list[str]:
        requested = [str(one) for one in (spec.get("field_order") or [])]
        ordered = [column for column in requested if column in editable_columns]
        ordered.extend(column for column in editable_columns if column not in ordered)
        return ordered

    def _link_field_label(self, column: str, spec: dict[str, Any]) -> str:
        field_labels = dict(spec.get("field_labels") or {})
        if column in field_labels:
            return str(field_labels[column])
        trimmed = str(column)
        for suffix in ("_link_", "_work_", "_agent_", "_label_", "_series_", "_language_"):
            trimmed = trimmed.replace(suffix, " ")
        trimmed = trimmed.replace("_", " ").strip()
        return " ".join(part.capitalize() for part in trimmed.split())

    def _render_link_metadata_summary(self, link_row, ordered_columns: list[str], spec: dict[str, Any]) -> str:
        chips: list[str] = []
        for column in ordered_columns:
            value = link_row[column]
            if value in (None, ""):
                continue
            chips.append(
                "<span class='link-chip'><strong>{label}:</strong> {value}</span>".format(
                    label=_escape(self._link_field_label(column, spec)),
                    value=_escape(_short_text(str(value), width=64)),
                )
            )
        if not chips:
            return ""
        return "<div class='link-chip-list'>{}</div>".format("".join(chips))

    def _render_link_management_section(self, primary_row) -> str:
        sections: list[str] = []
        primary_table = str(primary_row.table)
        for secondary_table in self._ordered_related_tables(primary_row):
            try:
                link_table, editable_columns = self._editable_link_columns(primary_table, secondary_table)
                secondary_id_column = self._id_column(secondary_table)
                link_secondary_column = self.db.driver_wrapper.get_link_column(primary_table, secondary_table, secondary_id_column)
                link_rows = list(self.db.get_interlink_rows(primary_row=primary_row, secondary_table=secondary_table))
            except Exception:
                continue
            spec = self._link_section_spec(primary_table, secondary_table, link_table)
            ordered_columns = self._ordered_link_fields(editable_columns, spec)
            cards: list[str] = []
            for link_row in link_rows:
                secondary_row = self.db.get_row_from_id(secondary_table, int(link_row[link_secondary_column]))
                secondary_href = self._row_href(secondary_table, secondary_row) if secondary_row is not None else ""
                secondary_label = (
                    self._row_label(secondary_table, secondary_row)
                    if secondary_row is not None
                    else "{}:{}".format(secondary_table, link_row[link_secondary_column])
                )
                metadata_html = self._render_link_metadata_summary(link_row, ordered_columns, spec)
                field_html = "".join(
                    self._form_field_html(
                        table=link_table,
                        column=column,
                        value=link_row[column],
                        input_id="{}-{}-{}".format(secondary_table, int(link_row[self._id_column(link_table)]), column),
                        label_text=self._link_field_label(column, spec),
                    )
                    for column in ordered_columns
                )
                if not field_html:
                    field_html = "<p class='meta'>This link table has no editable metadata columns beyond the row ids.</p>"
                cards.append(
                    """
<article class='panel link-editor-card'>
  <div>
    <strong>{label}</strong>
    <div class='link-meta'>
      <code>{link_table}:{link_row_id}</code>
      {open_link}
    </div>
    {metadata}
  </div>
  <form class='link-editor-form' method='post' action='/tables/{primary_table_q}/{primary_row_id}/links/{secondary_table_q}/{link_row_id}/edit#{anchor}'>
    <div class='write-grid'>{fields}</div>
    <div class='actions'>
      <button type='submit'>Save link</button>
    </div>
  </form>
  <form method='post' action='/tables/{primary_table_q}/{primary_row_id}/links/{secondary_table_q}/{link_row_id}/delete#{anchor}'>
    <div class='actions'>
      <button type='submit'>Remove link</button>
    </div>
  </form>
</article>
""".format(
                        label=_escape(secondary_label),
                        link_table=_escape(link_table),
                        link_row_id=int(link_row[self._id_column(link_table)]),
                        open_link=(
                            "<a href='{href}'>Open linked row</a>".format(href=_escape(secondary_href))
                            if secondary_href
                            else "<span class='meta'>No direct row page</span>"
                        ),
                        primary_table_q=_escape(quote(primary_table, safe="")),
                        primary_row_id=int(primary_row.row_id),
                        secondary_table_q=_escape(quote(secondary_table, safe="")),
                        anchor=_escape(self._interlink_anchor(secondary_table)),
                        metadata=metadata_html,
                        fields=field_html,
                    )
                )
            add_fields = [
                self._form_field_html(
                    table=link_table,
                    column=link_secondary_column,
                    value="",
                    input_name="secondary_row_id",
                    input_id="{}-secondary-row-id".format(secondary_table),
                    label_text=str(spec.get("target_label") or "{} row id".format(self._pretty_table_name(secondary_table))),
                )
            ]
            for column in ordered_columns:
                add_fields.append(
                    self._form_field_html(
                        table=link_table,
                        column=column,
                        value="",
                        input_id="{}-new-{}".format(secondary_table, column),
                        label_text=self._link_field_label(column, spec),
                    )
                )
            create_helper_html = self._render_link_create_helper(
                primary_table=primary_table,
                primary_row_id=int(primary_row.row_id),
                secondary_table=secondary_table,
                link_table=link_table,
                ordered_columns=ordered_columns,
                spec=spec,
            )
            open_attr = " open" if bool(spec.get("open")) else ""
            sections.append(
                """
<details class='link-editor panel' id='{anchor}'{open_attr}>
  <summary>{title}</summary>
  <p class='meta'>{intro}</p>
  <p class='meta'>Link table: <code>{link_table}</code>. Existing links: {count}.</p>
  <div class='link-editor-stack'>{cards}</div>
  <form class='link-editor-form link-add-form' method='post' action='/tables/{primary_table_q}/{primary_row_id}/links/{secondary_table_q}/new#{anchor}'>
    <div class='write-grid'>{add_fields}</div>
    <div class='actions'>
      <button type='submit'>{add_label}</button>
      <a href='/tables/{secondary_table_q}'>{browse_label}</a>
    </div>
  </form>
  {create_helper}
</details>
""".format(
                    anchor=_escape(self._interlink_anchor(secondary_table)),
                    open_attr=open_attr,
                    title=_escape(str(spec.get("title") or "Manage {}".format(self._pretty_table_name(secondary_table)))),
                    intro=_escape(str(spec.get("intro") or "")),
                    link_table=_escape(link_table),
                    count=len(link_rows),
                    cards=("".join(cards) if cards else "<p class='empty'>No current links.</p>"),
                    primary_table_q=_escape(quote(primary_table, safe="")),
                    primary_row_id=int(primary_row.row_id),
                    secondary_table_q=_escape(quote(secondary_table, safe="")),
                    add_label=_escape(str(spec.get("add_label") or "Add link")),
                    browse_label=_escape(str(spec.get("browse_label") or "Browse {}".format(self._pretty_table_name(secondary_table).lower()))),
                    add_fields="".join(add_fields),
                    create_helper=create_helper_html,
                )
            )
        if not sections:
            return ""
        return """
<section class='panel'>
  <h2>Manage linked entities</h2>
  <p class='meta'>Add, update, or remove interlinks without dropping into raw link tables.</p>
  {sections}
</section>
""".format(sections="".join(sections))

    def _row_path(self, table: str, row_id: int) -> str:
        return "/tables/{}/{}".format(quote(table, safe=""), int(row_id))

    def _render_new_row_page(self, table: str, *, error_text: str = "", values: Optional[dict[str, object]] = None) -> str:
        if not self._table_exists(table):
            return self._render_layout(title="Missing table", body_html="<section class='panel'><h2>Unknown table</h2></section>")
        if not self._is_writable_table(table):
            reason = self._writability_error(table) or "This table is read-only."
            return self._render_layout(
                title="Read-only table",
                body_html="<section class='panel'><h2>Read-only table</h2><p>This table cannot be created through the write interface.</p><p>{}</p></section>".format(
                    _escape(reason)
                ),
            )
        values = dict(values or {})
        intro = "<p class='meta'>Blank inputs are omitted during insert. Enter <code>NULL</code> to force a null value for non-text fields.</p>"
        if table in self._SPECIAL_FORM_SPECS:
            return self._render_grouped_row_form(
                table=table,
                action="/tables/{}/new".format(quote(table, safe="")),
                submit_label="Create row",
                values=values,
                error_text=error_text,
                intro_html="<p class='meta'>{}</p>".format(_escape(str(self._SPECIAL_FORM_SPECS[table].get("create_intro") or ""))) + intro,
                mode="create",
            )
        return self._render_row_form(
            table=table,
            action="/tables/{}/new".format(quote(table, safe="")),
            submit_label="Create row",
            values=values,
            error_text=error_text,
            intro_html=intro,
        )

    def _render_edit_row_page(self, table: str, raw_row_id: str, *, error_text: str = "", values: Optional[dict[str, object]] = None) -> str:
        if not self._table_exists(table):
            return self._render_layout(title="Missing table", body_html="<section class='panel'><h2>Unknown table</h2></section>")
        if not self._is_writable_table(table):
            reason = self._writability_error(table) or "This table is read-only."
            return self._render_layout(
                title="Read-only table",
                body_html="<section class='panel'><h2>Read-only table</h2><p>This table cannot be edited through the write interface.</p><p>{}</p></section>".format(
                    _escape(reason)
                ),
            )
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception:
            return self._render_layout(title="Bad row id", body_html="<section class='panel'><h2>Invalid row id</h2></section>")
        row = self.db.get_row_from_id(table, row_id)
        if row is None:
            return self._render_layout(title="Missing row", body_html="<section class='panel'><h2>Row not found</h2></section>")
        current_values = {column: row[column] for column in self._editable_columns(table)}
        if values:
            current_values.update(values)
        intro = "<p class='meta'>Editing <code>{table}:{row_id}</code>. Enter <code>NULL</code> to clear numeric or boolean fields.</p>".format(
            table=_escape(table),
            row_id=row_id,
        )
        if table in self._SPECIAL_FORM_SPECS:
            return self._render_grouped_row_form(
                table=table,
                action="/tables/{}/{}/edit".format(quote(table, safe=""), row_id),
                submit_label="Save row",
                values=current_values,
                error_text=error_text,
                intro_html="<p class='meta'>{}</p>{}".format(_escape(str(self._SPECIAL_FORM_SPECS[table].get("edit_intro") or "")), intro),
                mode="edit",
            )
        return self._render_row_form(
            table=table,
            action="/tables/{}/{}/edit".format(quote(table, safe=""), row_id),
            submit_label="Save row",
            values=current_values,
            error_text=error_text,
            intro_html=intro,
        )

    def _render_delete_row_page(self, table: str, raw_row_id: str, *, error_text: str = "") -> str:
        if not self._table_exists(table):
            return self._render_layout(title="Missing table", body_html="<section class='panel'><h2>Unknown table</h2></section>")
        if not self._is_writable_table(table):
            return self._render_layout(title="Read-only table", body_html="<section class='panel'><h2>Read-only table</h2><p>Views and compatibility surfaces cannot be deleted through the generic form.</p></section>")
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception:
            return self._render_layout(title="Bad row id", body_html="<section class='panel'><h2>Invalid row id</h2></section>")
        try:
            impact = self.model.delete_impact(table, row_id)
        except Exception as exc:
            return self._render_layout(title="Delete failed", body_html="<section class='panel'><h2>Delete preview failed</h2><p class='danger'>{}</p></section>".format(_escape(exc)))

        warnings: list[str] = []
        for entry in impact["interlinked_counts"]:
            warnings.append("<li>linked rows in <code>{}</code>: {}</li>".format(_escape(entry["table"]), int(entry["count"])))
        for entry in impact["reference_counts"]:
            warnings.append("<li>referenced by <code>{}.{}</code>: {}</li>".format(_escape(entry["table"]), _escape(entry["column"]), int(entry["count"])))
        warning_block = "<ul class='warning-list'>{}</ul>".format("".join(warnings)) if warnings else "<p class='meta'>No linked or referencing rows were detected.</p>"
        error_html = ""
        if error_text:
            error_html = "<p class='danger'>{}</p>".format(_escape(error_text))
        body = """
<section class='panel'>
  <h2>Delete <code>{table}:{row_id}</code></h2>
  <p class='meta'>{label}</p>
  {error_html}
  <p class='danger'>{warning}</p>
  {warning_block}
  <form method='post' action='/tables/{table_q}/{row_id}/delete'>
    <div class='actions'>
      <button type='submit'>Delete row</button>
      <a href='/tables/{table_q}/{row_id}'>Cancel</a>
    </div>
  </form>
</section>
""".format(
            table=_escape(table),
            table_q=_escape(quote(table, safe="")),
            row_id=row_id,
            label=_escape(_short_text(self._row_label(table, self.db.get_row_from_id(table, row_id)), width=160)),
            error_html=error_html,
            warning=_escape(impact.get("warning") or "Delete cannot be undone."),
            warning_block=warning_block,
        )
        return self._render_layout(title="Delete {}:{}".format(table, row_id), body_html=body)

    def _create_payload_from_form(self, table: str, form: dict[str, str]) -> dict[str, object]:
        payload: dict[str, object] = {}
        for column in self._editable_columns(table):
            if column not in form:
                continue
            value = self._coerce_form_value(form[column], column=column, current_value=None, for_create=True)
            if value is None:
                continue
            payload[column] = value
        return payload

    def _update_payload_from_form(self, table: str, row, form: dict[str, str]) -> dict[str, object]:
        payload: dict[str, object] = {}
        for column in self._editable_columns(table):
            if column not in form:
                continue
            payload[column] = self._coerce_form_value(form[column], column=column, current_value=row[column], for_create=False)
        return payload

    def _handle_create_row(self, table: str, environ) -> _Response:
        form = self._parse_form(environ)
        if not self._is_writable_table(table):
            return self._html_response(self._render_new_row_page(table, error_text="This table is not writable through the generic form."), status="405 Method Not Allowed")
        try:
            payload = self._create_payload_from_form(table, form)
        except Exception as exc:
            return self._html_response(self._render_new_row_page(table, error_text=str(exc), values=form), status="400 Bad Request")
        if not payload:
            return self._html_response(self._render_new_row_page(table, error_text="No writable values were provided.", values=form), status="400 Bad Request")
        try:
            receipt = self.model.create_row(table, payload)
            record = receipt.get("record")
            if not isinstance(record, Mapping):
                raise TypeError("Core did not return the created row.")
            row = self.model.row_from_record(record)
        except Exception as exc:
            return self._html_response(self._render_new_row_page(table, error_text=str(exc), values=form), status="400 Bad Request")
        read_source_refreshed = self._refresh_read_source_after_write()
        row_path = self._row_href(table, row) or self._row_path(table, int(_row_value(row, self._id_column(table) or "")))
        return self._redirect_with_notice(
            row_path,
            kind="success",
            title="Row created",
            message=self._with_cache_refresh_note(
                "Created {} row.".format(table),
                refreshed=read_source_refreshed,
            ),
        )

    def _handle_edit_row(self, table: str, raw_row_id: str, environ) -> _Response:
        form = self._parse_form(environ)
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception:
            return self._html_response(self._render_edit_row_page(table, raw_row_id, error_text="Invalid row id.", values=form), status="400 Bad Request")
        row = self.db.get_row_from_id(table, row_id) if self._table_exists(table) else None
        if row is None:
            return self._html_response(self._render_edit_row_page(table, raw_row_id, error_text="Row not found.", values=form), status="404 Not Found")
        try:
            updates = self._update_payload_from_form(table, row, form)
        except Exception as exc:
            return self._html_response(self._render_edit_row_page(table, raw_row_id, error_text=str(exc), values=form), status="400 Bad Request")
        try:
            self.model.update_row(table, row_id, updates)
        except Exception as exc:
            return self._html_response(self._render_edit_row_page(table, raw_row_id, error_text=str(exc), values=form), status="400 Bad Request")
        read_source_refreshed = self._refresh_read_source_after_write()
        return self._redirect_with_notice(
            self._row_path(table, row_id),
            kind="success",
            title="Row updated",
            message=self._with_cache_refresh_note(
                "Saved changes to {}:{}.".format(table, row_id),
                refreshed=read_source_refreshed,
            ),
        )

    def _handle_delete_row(self, table: str, raw_row_id: str, environ) -> _Response:
        del environ
        try:
            row_id = int(str(raw_row_id).strip())
        except Exception:
            return self._html_response(self._render_delete_row_page(table, raw_row_id, error_text="Invalid row id."), status="400 Bad Request")
        try:
            self.model.delete_row(table, row_id)
        except Exception as exc:
            return self._html_response(self._render_delete_row_page(table, raw_row_id, error_text=str(exc)), status="400 Bad Request")
        read_source_refreshed = self._refresh_read_source_after_write()
        return self._redirect_with_notice(
            "/tables/{}?{}".format(quote(table, safe=""), _build_query_string({"deleted": row_id})),
            kind="success",
            title="Row deleted",
            message=self._with_cache_refresh_note(
                "Deleted {}:{}.".format(table, row_id),
                refreshed=read_source_refreshed,
            ),
        )

    def _handle_file_upload(self, environ, *, context_table: Optional[str] = None, raw_row_id: Optional[str] = None) -> _Response:
        content_type = str(environ.get("CONTENT_TYPE", "") or "")
        if "multipart/form-data" not in content_type.lower():
            return self._html_response(
                self._render_file_upload_page(error_text="File uploads require multipart/form-data.", context_table=context_table, raw_row_id=raw_row_id),
                status="400 Bad Request",
            )

        form, upload = self._parse_multipart_form(environ)
        upload_context = None
        if context_table:
            try:
                upload_context = self._resolve_upload_context(context_table, raw_row_id)
            except Exception as exc:
                return self._html_response(
                    self._render_file_upload_page(error_text=str(exc), values=form, context_table=context_table, raw_row_id=raw_row_id),
                    status="400 Bad Request",
                )
        if upload is None or not bytes(upload.get("bytes") or b""):
            return self._html_response(
                self._render_file_upload_page(error_text="A file payload is required.", values=form, context_table=context_table, raw_row_id=raw_row_id),
                status="400 Bad Request",
            )

        store_raw = str(form.get("store_id", "") or "").strip()
        if not store_raw:
            return self._html_response(
                self._render_file_upload_page(error_text="Choose a target store.", values=form, context_table=context_table, raw_row_id=raw_row_id),
                status="400 Bad Request",
            )
        try:
            store_id = int(store_raw)
        except Exception:
            return self._html_response(
                self._render_file_upload_page(error_text="Invalid store id.", values=form, context_table=context_table, raw_row_id=raw_row_id),
                status="400 Bad Request",
            )
        store_row = self.db.get_row_from_id("stores", store_id) if self._table_exists("stores") else None
        if store_row is None:
            return self._html_response(
                self._render_file_upload_page(error_text="Store row not found.", values=form, context_table=context_table, raw_row_id=raw_row_id),
                status="404 Not Found",
            )
        if int(_coerce_int(_row_value(store_row, "store_is_read_only"), default=0) or 0) != 0:
            return self._html_response(
                self._render_file_upload_page(error_text="Selected store is marked read-only.", values=form, context_table=context_table, raw_row_id=raw_row_id),
                status="400 Bad Request",
            )

        try:
            self.core.command(
                "storage.refresh",
                {
                    "startup_on_add": False,
                    "clear_existing": True,
                },
            )
        except Exception as exc:
            return self._html_response(
                self._render_file_upload_page(error_text="Storage bootstrap failed: {}".format(exc), values=form, context_table=context_table, raw_row_id=raw_row_id),
                status="400 Bad Request",
            )

        preferred_store = str(_row_value(store_row, "store_name") or "store-{}".format(store_id))
        try:
            target_item_id: Optional[int] = None
            uploaded_name = self._upload_basename(str(upload.get("filename") or "upload.bin"))
            if upload_context is not None:
                resolved_table, resolved_row = upload_context
                if resolved_table == "items":
                    target_item_id = int(_row_value(resolved_row, "item_id") or 0)
                    if "item_source" not in form:
                        form["item_source"] = str(_row_value(resolved_row, "item_source") or "web_upload")
                    if "item_source_name" not in form:
                        form["item_source_name"] = uploaded_name
                elif resolved_table == "works":
                    _expression_row, _manifestation_row, item_row = self._create_item_chain_for_work_upload(
                        work_row=resolved_row,
                        form=form,
                        fallback_label=uploaded_name,
                    )
                    target_item_id = int(_row_value(item_row, "item_id") or 0)
            file_row = self._create_file_row_from_upload(
                form=form,
                upload=upload,
                store_row=store_row,
                store_id=store_id,
                preferred_store=preferred_store,
                item_id=target_item_id,
            )
        except Exception as exc:
            return self._html_response(
                self._render_file_upload_page(error_text="Upload failed: {}".format(exc), values=form, context_table=context_table, raw_row_id=raw_row_id),
                status="400 Bad Request",
            )
        read_source_refreshed = self._refresh_read_source_after_write()
        if upload_context is not None:
            resolved_table, resolved_row = upload_context
            row_id = int(_row_value(resolved_row, self._id_column(resolved_table)) or 0)
            return self._redirect_with_notice(
                self._row_path(resolved_table, row_id),
                kind="success",
                title="File uploaded",
                message=self._with_cache_refresh_note(
                    "Stored file bytes and attached the new file to {}:{}.".format(resolved_table, row_id),
                    refreshed=read_source_refreshed,
                ),
            )
        return self._redirect_with_notice(
            self._row_path("files", int(file_row["file_id"])),
            kind="success",
            title="File uploaded",
            message=self._with_cache_refresh_note(
                "Stored file bytes and created the matching file row.",
                refreshed=read_source_refreshed,
            ),
        )

    def _handle_add_interlink(self, table: str, raw_row_id: str, secondary_table: str, environ) -> _Response:
        form = self._parse_form(environ)
        try:
            primary_row, link_table, editable_columns = self._resolve_interlink_context(table, raw_row_id, secondary_table)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")

        secondary_raw = str(form.get("secondary_row_id", "") or "").strip()
        if not secondary_raw:
            return self._html_response(
                self._render_row_page(table, raw_row_id, write_error_text="A linked row id is required."),
                status="400 Bad Request",
            )
        try:
            secondary_row = self.db.get_row_from_id(secondary_table, int(secondary_raw))
        except Exception:
            secondary_row = None
        if secondary_row is None:
            return self._html_response(
                self._render_row_page(table, raw_row_id, write_error_text="No {} row found for id {}.".format(secondary_table, secondary_raw)),
                status="404 Not Found",
            )

        try:
            priority, link_type, extra_values = self._link_values_from_form(
                link_table=link_table,
                editable_columns=editable_columns,
                form=form,
            )
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        spec = self._link_section_spec(str(primary_row.table), secondary_table, link_table)
        item_name = str(spec.get("item_name") or "link")
        read_source_refreshed = False
        try:
            report = self._write_metadata_relation_link(
                primary_row=primary_row,
                secondary_table=secondary_table,
                secondary_row=secondary_row,
                priority=priority,
                link_type=link_type,
                extra_values=extra_values,
            )
            if report is None:
                self.db.interlink_rows(primary_row=primary_row, secondary_row=secondary_row, priority=priority, type=link_type, **extra_values)
                read_source_refreshed = self._refresh_read_source_after_write()
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        if report is not None:
            errors = list(getattr(report, "errors", []) or [])
            if errors:
                return self._html_response(
                    self._render_row_page(table, raw_row_id, write_error_text="; ".join(str(error) for error in errors)),
                    status="400 Bad Request",
                )
            if not bool(getattr(report, "changed", False)):
                return self._redirect_with_notice(
                    self._row_path(table, int(primary_row.row_id)),
                    kind="info",
                    title="No changes",
                    message=self._metadata_relation_notice_message(
                        action="No change for",
                        item_name=item_name,
                        report=report,
                    ),
                    anchor=self._interlink_anchor(secondary_table),
                )
            read_source_refreshed = self._refresh_read_source_after_write()
        message = (
            self._metadata_relation_notice_message(
                action="Added",
                item_name=item_name,
                report=report,
            )
            if report is not None
            else "Added {}.".format(item_name)
        )
        return self._redirect_with_notice(
            self._row_path(table, int(primary_row.row_id)),
            kind="success",
            title="Link added",
            message=self._with_cache_refresh_note(
                message,
                refreshed=read_source_refreshed,
            ),
            anchor=self._interlink_anchor(secondary_table),
        )

    def _handle_create_link_target(self, table: str, raw_row_id: str, secondary_table: str, environ) -> _Response:
        form = self._parse_form(environ)
        try:
            primary_row, link_table, editable_columns = self._resolve_interlink_context(table, raw_row_id, secondary_table)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        spec = self._link_section_spec(str(primary_row.table), secondary_table, link_table)
        create_disabled_reason = str(spec.get("create_disabled_reason") or "").strip()
        if create_disabled_reason:
            return self._html_response(
                self._render_row_page(table, raw_row_id, write_error_text=create_disabled_reason),
                status="405 Method Not Allowed",
            )
        if not self._is_writable_table(secondary_table):
            return self._html_response(
                self._render_row_page(table, raw_row_id, write_error_text=self._writability_error(secondary_table) or "Linked table is read-only."),
                status="405 Method Not Allowed",
            )
        try:
            payload = self._create_link_target_payload(secondary_table=secondary_table, spec=spec, form=form)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        required_fields = [str(one) for one in (spec.get("create_required_fields") or [])]
        missing = [field for field in required_fields if not str(payload.get(field) or "").strip()]
        if missing:
            return self._html_response(
                self._render_row_page(
                    table,
                    raw_row_id,
                    write_error_text="Missing required fields for {}: {}.".format(secondary_table, ", ".join(missing)),
                ),
                status="400 Bad Request",
            )
        if not payload:
            return self._html_response(
                self._render_row_page(table, raw_row_id, write_error_text="No {} values were provided.".format(secondary_table)),
                status="400 Bad Request",
            )
        try:
            receipt = self.model.create_row(secondary_table, payload)
            record = receipt.get("record")
            if not isinstance(record, Mapping):
                raise TypeError("Core did not return the created linked row.")
            secondary_row = self.model.row_from_record(record)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")

        try:
            priority, link_type, extra_values = self._link_values_from_form(
                link_table=link_table,
                editable_columns=editable_columns,
                form=form,
            )
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        try:
            report = self._write_metadata_relation_link(
                primary_row=primary_row,
                secondary_table=secondary_table,
                secondary_row=secondary_row,
                priority=priority,
                link_type=link_type,
                extra_values=extra_values,
            )
            if report is None:
                self.db.interlink_rows(primary_row=primary_row, secondary_row=secondary_row, priority=priority, type=link_type, **extra_values)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        errors = list(getattr(report, "errors", []) or []) if report is not None else []
        if errors:
            return self._html_response(
                self._render_row_page(table, raw_row_id, write_error_text="; ".join(str(error) for error in errors)),
                status="400 Bad Request",
            )
        item_name = str(spec.get("item_name") or secondary_table).rstrip(".")
        read_source_refreshed = self._refresh_read_source_after_write()
        message = (
            self._metadata_relation_notice_message(
                action="Created and linked",
                item_name=item_name,
                report=report,
            )
            if report is not None
            else "Created and linked {}.".format(item_name)
        )
        return self._redirect_with_notice(
            self._row_path(table, int(primary_row.row_id)),
            kind="success",
            title="Linked row created",
            message=self._with_cache_refresh_note(
                message,
                refreshed=read_source_refreshed,
            ),
            anchor=self._interlink_anchor(secondary_table),
        )

    def _handle_edit_interlink(
        self,
        table: str,
        raw_row_id: str,
        secondary_table: str,
        raw_link_row_id: str,
        environ,
    ) -> _Response:
        form = self._parse_form(environ)
        try:
            primary_row, link_table, editable_columns = self._resolve_interlink_context(table, raw_row_id, secondary_table)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        try:
            link_row_id = int(str(raw_link_row_id).strip())
        except Exception:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text="Invalid link row id."), status="400 Bad Request")
        link_row = self.db.get_row_from_id(link_table, link_row_id)
        if link_row is None:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text="Link row not found."), status="404 Not Found")
        primary_link_column = self.db.driver_wrapper.get_link_column(table, secondary_table, self._id_column(table))
        if int(link_row[primary_link_column]) != int(primary_row.row_id):
            return self._html_response(
                self._render_row_page(table, raw_row_id, write_error_text="Link row does not belong to this primary row."),
                status="400 Bad Request",
            )

        updates: dict[str, Any] = {}
        try:
            for column in editable_columns:
                if column not in form:
                    continue
                updates[column] = self._coerce_form_value(form[column], column=column, current_value=link_row[column], for_create=False)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        if not updates:
            return self._redirect_with_notice(
                self._row_path(table, int(primary_row.row_id)),
                kind="info",
                title="No changes",
                message="No link changes were submitted.",
                anchor=self._interlink_anchor(secondary_table),
            )
        try:
            self.model.update_row(link_table, link_row_id, updates)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        spec = self._link_section_spec(str(primary_row.table), secondary_table, link_table)
        read_source_refreshed = self._refresh_read_source_after_write()
        return self._redirect_with_notice(
            self._row_path(table, int(primary_row.row_id)),
            kind="success",
            title="Link updated",
            message=self._with_cache_refresh_note(
                "Updated {}.".format(str(spec.get("item_name") or "link")),
                refreshed=read_source_refreshed,
            ),
            anchor=self._interlink_anchor(secondary_table),
        )

    def _handle_delete_interlink(
        self,
        table: str,
        raw_row_id: str,
        secondary_table: str,
        raw_link_row_id: str,
        environ,
    ) -> _Response:
        del environ
        try:
            primary_row, link_table, _editable_columns = self._resolve_interlink_context(table, raw_row_id, secondary_table)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        try:
            link_row_id = int(str(raw_link_row_id).strip())
        except Exception:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text="Invalid link row id."), status="400 Bad Request")
        link_row = self.db.get_row_from_id(link_table, link_row_id)
        if link_row is None:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text="Link row not found."), status="404 Not Found")
        primary_link_column = self.db.driver_wrapper.get_link_column(table, secondary_table, self._id_column(table))
        if int(link_row[primary_link_column]) != int(primary_row.row_id):
            return self._html_response(
                self._render_row_page(table, raw_row_id, write_error_text="Link row does not belong to this primary row."),
                status="400 Bad Request",
            )
        try:
            self.model.delete_row(link_table, link_row_id)
        except Exception as exc:
            return self._html_response(self._render_row_page(table, raw_row_id, write_error_text=str(exc)), status="400 Bad Request")
        spec = self._link_section_spec(str(primary_row.table), secondary_table, link_table)
        read_source_refreshed = self._refresh_read_source_after_write()
        return self._redirect_with_notice(
            self._row_path(table, int(primary_row.row_id)),
            kind="success",
            title="Link removed",
            message=self._with_cache_refresh_note(
                "Removed {}.".format(str(spec.get("item_name") or "link")),
                refreshed=read_source_refreshed,
            ),
            anchor=self._interlink_anchor(secondary_table),
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the LiuXin read-write web interface.")
    add_core_client_arguments(parser)
    parser.add_argument("--db-type", default="sqlite", help="Database driver type. Default: sqlite")
    parser.add_argument("--host", default=ReadWriteWebConfig.host, help="Bind host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=ReadWriteWebConfig.port, help="Bind port. Default: 8084")
    parser.add_argument("--page-size", type=int, default=50, help="Default page size.")
    parser.add_argument("--max-page-size", type=int, default=200, help="Maximum page size.")
    parser.add_argument("--title", default=ReadWriteWebConfig.title, help="Service title.")
    parser.add_argument("--expose-database-path", action="store_true", help="Show the backing database path in the UI.")
    parser.add_argument("--no-file-downloads", action="store_true", help="Disable file download / redirect links.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = ReadWriteWebConfig(
        title=str(args.title),
        host=str(args.host),
        port=int(args.port),
        default_page_size=max(1, int(args.page_size)),
        max_page_size=max(1, int(args.max_page_size)),
        expose_database_path=bool(args.expose_database_path),
        enable_file_downloads=not bool(args.no_file_downloads),
    )
    with open_surface_core_from_args(
        args,
        enable_storage_manager=True,
        enable_maintenance=False,
    ) as core_session:
        app = ReadWriteWebApplication(core_session.client, config=config)
        url = "http://{}:{}/".format(config.host, config.port)
        sys.stdout.write("Serving read-write web interface on {}\n".format(url))
        sys.stdout.flush()
        with make_server(config.host, config.port, app) as server:
            server.serve_forever()
    return 0


__all__ = [
    "ReadWriteWebApplication",
    "ReadWriteWebConfig",
    "build_arg_parser",
    "main",
]
