"""`ingest` command group for importing files into the database."""

from __future__ import annotations

import dataclasses
import json

from typing import Optional

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.storage.reconcile import register_existing_disk_as_unmanaged_store


@dataclasses.dataclass(frozen=True)
class _IngestDiskOptions:
    disk_path: str
    store_name: Optional[str]
    source_label: str
    ebook_extensions: Optional[list[str]]
    compute_hash: bool
    follow_symlinks: bool
    refresh_storage_manager: bool
    attach_store_links: bool
    json_output: bool


def _split_extensions(raw: Optional[str]) -> Optional[list[str]]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    for separator in (";", " ", "\t", "\n"):
        text = text.replace(separator, ",")
    parts = [part.strip().lstrip(".").lower() for part in text.split(",")]
    values = [part for part in parts if part]
    if not values:
        return None
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped


def _read_option_value(args: list[str], idx: int, *, option_name: str) -> tuple[str, int]:
    token = args[idx]
    if "=" in token:
        _, value = token.split("=", 1)
        if value.strip() == "":
            raise ValueError("Option {} requires a non-blank value.".format(option_name))
        return value, idx + 1
    if idx + 1 >= len(args):
        raise ValueError("Option {} requires a value.".format(option_name))
    value = args[idx + 1]
    if str(value).strip() == "":
        raise ValueError("Option {} requires a non-blank value.".format(option_name))
    return value, idx + 2


def _parse_ingest_disk_options(args: list[str], *, usage: str) -> _IngestDiskOptions:
    if not args:
        raise ValueError("Usage: {}".format(usage))

    disk_path: Optional[str] = None
    store_name: Optional[str] = None
    source_label = "on_disk_unmanaged_import"
    extensions_raw: Optional[str] = None
    compute_hash = True
    follow_symlinks = False
    refresh_storage_manager = True
    attach_store_links = True
    json_output = False

    idx = 0
    while idx < len(args):
        token = str(args[idx]).strip()

        if token in {"--store-name", "--store_name"} or token.startswith("--store-name=") or token.startswith(
            "--store_name="
        ):
            value, idx = _read_option_value(args, idx, option_name="--store-name")
            store_name = value.strip()
            if not store_name:
                raise ValueError("Option --store-name requires a non-blank value.")
            continue

        if token == "--source" or token.startswith("--source="):
            value, idx = _read_option_value(args, idx, option_name="--source")
            source_label = value.strip()
            if not source_label:
                raise ValueError("Option --source requires a non-blank value.")
            continue

        if token == "--extensions" or token.startswith("--extensions="):
            value, idx = _read_option_value(args, idx, option_name="--extensions")
            extensions_raw = value
            continue

        if token == "--no-hash":
            compute_hash = False
            idx += 1
            continue
        if token == "--hash":
            compute_hash = True
            idx += 1
            continue
        if token == "--follow-symlinks":
            follow_symlinks = True
            idx += 1
            continue
        if token == "--no-follow-symlinks":
            follow_symlinks = False
            idx += 1
            continue
        if token == "--no-refresh":
            refresh_storage_manager = False
            idx += 1
            continue
        if token == "--refresh":
            refresh_storage_manager = True
            idx += 1
            continue
        if token == "--no-links":
            attach_store_links = False
            idx += 1
            continue
        if token == "--links":
            attach_store_links = True
            idx += 1
            continue
        if token == "--json":
            json_output = True
            idx += 1
            continue

        if token.startswith("-"):
            raise ValueError("Unknown option: {!r}".format(token))

        if disk_path is not None:
            raise ValueError("Unexpected extra argument {!r}. Usage: {}".format(token, usage))
        disk_path = token
        idx += 1

    if disk_path is None:
        raise ValueError("Usage: {}".format(usage))

    return _IngestDiskOptions(
        disk_path=disk_path,
        store_name=store_name,
        source_label=source_label,
        ebook_extensions=_split_extensions(extensions_raw),
        compute_hash=compute_hash,
        follow_symlinks=follow_symlinks,
        refresh_storage_manager=refresh_storage_manager,
        attach_store_links=attach_store_links,
        json_output=json_output,
    )


class IngestDiskCommand(TerminalCommandAPI):
    """Register ebook files from an existing disk path into the database."""

    group = "ingest"
    group_aliases = ("import",)
    name = "disk"
    aliases = ("unmanaged-disk", "unmanaged_disk")
    summary = "Ingest disk files: ingest disk <path> [options]"
    usage = (
        "ingest disk <path> [--store-name <name>] [--extensions epub,mobi] "
        "[--source <label>] [--no-hash] [--follow-symlinks] [--no-refresh] [--no-links] [--json]"
    )
    expose_direct = False

    def execute(self, browser, args: list[str]) -> bool:
        options = _parse_ingest_disk_options(args, usage=self.usage)
        report = register_existing_disk_as_unmanaged_store(
            browser.db,
            disk_path=options.disk_path,
            store_name=options.store_name,
            ebook_extensions=options.ebook_extensions,
            source_label=options.source_label,
            compute_hash=options.compute_hash,
            follow_symlinks=options.follow_symlinks,
            attach_store_links=options.attach_store_links,
            refresh_storage_manager=options.refresh_storage_manager,
        )

        if options.json_output:
            browser.emit(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True, indent=2))
            return True

        browser.emit("Ingest completed:")
        browser.emit("  store_id: {}".format(report.store_row_id))
        browser.emit("  store_name: {}".format(report.store_name))
        browser.emit("  store_root_uri: {}".format(report.store_root_uri))
        browser.emit("  scanned_files: {}".format(report.scanned_files))
        browser.emit("  ebook_candidates: {}".format(report.ebook_candidates))
        browser.emit("  skipped_non_ebook_files: {}".format(report.skipped_non_ebook_files))
        browser.emit("  inserted_files: {}".format(report.inserted_files))
        browser.emit("  updated_files: {}".format(report.updated_files))
        browser.emit("  unchanged_files: {}".format(report.unchanged_files))
        browser.emit("  linked_files: {}".format(report.linked_files))
        browser.emit("  errors: {}".format(len(report.errors)))
        if report.errors:
            preview_count = min(5, len(report.errors))
            browser.emit("  error_preview:")
            for error in report.errors[:preview_count]:
                browser.emit("    - {}".format(error))
            if len(report.errors) > preview_count:
                browser.emit("    ... {} more".format(len(report.errors) - preview_count))
        return True


__all__ = [
    "IngestDiskCommand",
]
