"""`ingest` command group for importing files into the database."""

from __future__ import annotations

import dataclasses
import json

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


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
        submitted = browser.execute_core_command(
            "ingest.disk.start",
            payload={
                "disk_path": options.disk_path,
                "store_name": options.store_name,
                "ebook_extensions": options.ebook_extensions,
                "source_label": options.source_label,
                "compute_hash": options.compute_hash,
                "follow_symlinks": options.follow_symlinks,
                "attach_store_links": options.attach_store_links,
                "refresh_storage_manager": options.refresh_storage_manager,
                "label": "terminal ingest disk",
            },
        )
        job_id = str((submitted or {}).get("job_id") or "")
        if not job_id:
            raise RuntimeError("Core did not return an ingest job id.")
        completed = browser.execute_core_query(
            "jobs.result",
            payload={"job_id": job_id, "timeout_s": None},
        )
        execution = dict((completed or {}).get("execution", {}) or {})
        if not bool(execution.get("ok", False)):
            raise RuntimeError(
                str(execution.get("error") or "Ingest job failed.")
            )
        report = dict(execution.get("result", {}) or {})

        if options.json_output:
            browser.emit(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
            return True

        browser.emit_detail_sections(
            [
                (
                    "Store",
                    [
                        ("store_id", report.get("store_row_id", "")),
                        ("store_name", report.get("store_name", "")),
                        ("store_root_uri", report.get("store_root_uri", "")),
                    ],
                ),
                (
                    "Results",
                    [
                        ("scanned_files", report.get("scanned_files", 0)),
                        ("ebook_candidates", report.get("ebook_candidates", 0)),
                        ("skipped_non_ebook_files", report.get("skipped_non_ebook_files", 0)),
                        ("inserted_files", report.get("inserted_files", 0)),
                        ("updated_files", report.get("updated_files", 0)),
                        ("unchanged_files", report.get("unchanged_files", 0)),
                        ("linked_files", report.get("linked_files", 0)),
                        ("errors", len(report.get("errors", ()) or ())),
                    ],
                ),
            ],
            title="Ingest completed:",
            max_cell_width=120,
        )
        errors = list(report.get("errors", ()) or ())
        if errors:
            preview_count = min(5, len(errors))
            browser.emit("")
            browser.emit("Error preview")
            browser.emit(browser.render_table(["error"], [[error] for error in errors[:preview_count]], max_cell_width=120))
            if len(errors) > preview_count:
                browser.emit("... {} more".format(len(errors) - preview_count))
        return True


__all__ = [
    "IngestDiskCommand",
]
