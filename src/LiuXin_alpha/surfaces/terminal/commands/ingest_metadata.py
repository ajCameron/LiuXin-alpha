"""
Plan reading embedded metadata for one Store as a Core job, and show the plan's summary.

``ingest metadata <store> --dry-run`` is the read half of ingest's metadata
step: it reads the files a ``sync store`` registered, matches them against the
catalogue and writes a plan of the books it would add. It creates nothing, and
for now ``--dry-run`` is required because the write phase does not exist yet.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.commands.sync import (
    _emit_job_log,
    _optional_positive_float,
    _optional_positive_int,
    _resolve_store_row,
    _split_extensions,
    _wait_for_job,
)

_WRITE_PHASE_MISSING = (
    "Only `--dry-run` is available: the write phase of metadata ingest isn't built yet."
)


@dataclasses.dataclass(frozen=True)
class _IngestMetadataOptions:
    """
    Hold the parsed options for ``ingest metadata``.

    Example:
        >>> options = _parse_ingest_metadata_options(["3", "--dry-run", "--limit", "5"], usage="u")
        >>> options.store_ref, options.limit, options.dry_run
        ('3', 5, True)
    """

    store_ref: str
    dry_run: bool
    extensions: list[str] | None
    limit: int | None
    max_file_mb: int | None
    report_path: str | None
    background: bool
    job_backend: str | None
    job_timeout_s: float | None
    show_progress: bool
    json_output: bool


def _parse_ingest_metadata_options(
    args: list[str], *, usage: str
) -> _IngestMetadataOptions:
    """
    Parse a Store reference plus dry-run, selection, report and job options.

    Example:
        >>> _parse_ingest_metadata_options(["books", "--dry-run", "--extensions", "EPUB,.mobi"], usage="u").extensions
        ['epub', 'mobi']


    :param args: Store id or name and option tokens.
    :param usage: Usage text for parse errors.
    :return: Parsed options; the Store is resolved later.
    :raises ValueError: If parsing fails or a numeric option is not positive.
    """
    if not args:
        raise ValueError(f"Usage: {usage}")
    parser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    parser.add_argument("store_ref")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--extensions")
    parser.add_argument("--limit")
    parser.add_argument("--max-file-mb")
    parser.add_argument("--report")
    parser.add_argument("--background", dest="background", action="store_true")
    parser.add_argument("--foreground", dest="background", action="store_false")
    parser.add_argument("--job-backend")
    parser.add_argument("--job-timeout-s")
    parser.add_argument("--progress", dest="show_progress", action="store_true")
    parser.add_argument("--no-progress", dest="show_progress", action="store_false")
    parser.add_argument("--json", dest="json_output", action="store_true")
    parser.set_defaults(
        dry_run=False, background=False, show_progress=True, json_output=False
    )
    try:
        values = parser.parse_args(args)
    except (argparse.ArgumentError, SystemExit) as exc:
        raise ValueError(f"Usage: {usage}") from exc
    return _IngestMetadataOptions(
        store_ref=str(values.store_ref),
        dry_run=bool(values.dry_run),
        extensions=_split_extensions(values.extensions),
        limit=_positive_int_option(values.limit, "--limit"),
        max_file_mb=_positive_int_option(values.max_file_mb, "--max-file-mb"),
        report_path=(str(values.report).strip() or None) if values.report else None,
        background=bool(values.background),
        job_backend=None
        if values.job_backend is None
        else str(values.job_backend).strip(),
        job_timeout_s=(
            None
            if values.job_timeout_s is None
            else _optional_positive_float(
                values.job_timeout_s, option="--job-timeout-s"
            )
        ),
        show_progress=bool(values.show_progress),
        json_output=bool(values.json_output),
    )


def _positive_int_option(value: str | None, option: str) -> int | None:
    """
    Parse an optional positive integer option.

    Example:
        >>> _positive_int_option("4", "--limit"), _positive_int_option(None, "--limit")
        (4, None)


    :param value: Option text, or None when absent.
    :param option: Option spelling for errors.
    :return: The integer, or None when absent or given as ``none``.
    :raises ValueError: If the value is not a positive integer.
    """
    return None if value is None else _optional_positive_int(value, option=option)


def _plan_sections(
    report: Mapping[str, Any],
) -> list[tuple[str, list[tuple[str, Any]]]]:
    """
    Arrange a plan result as terminal detail sections.

    Example:
        >>> sections = _plan_sections({"store": {"store_id": 1}, "summary": {"books": 2}, "report_path": "p.json"})
        >>> [title for title, _ in sections]
        ['Store', 'Plan', 'Files skipped', 'Review flags', 'Names']


    :param report: Job result from ``ingest.metadata.plan``.
    :return: ``(title, rows)`` pairs.
    """
    store = dict(report.get("store") or {})
    summary = dict(report.get("summary") or {})
    names = dict(summary.get("names") or {})
    return [
        (
            "Store",
            [
                (key, store.get(key, ""))
                for key in ("store_id", "store_name", "store_kind")
            ],
        ),
        (
            "Plan",
            [
                ("books", summary.get("books", 0)),
                ("multi_format_books", summary.get("multi_format_books", 0)),
                ("files_read", summary.get("files_read", 0)),
                ("files_failed", summary.get("files_failed", 0)),
                ("report", report.get("report_path", "")),
            ],
        ),
        ("Files skipped", _pairs(summary.get("files_skipped"))),
        ("Review flags", _pairs(summary.get("flags"))),
        (
            "Names",
            [(kind, _decisions(counts)) for kind, counts in sorted(names.items())],
        ),
    ]


def _pairs(values: Any) -> list[tuple[str, Any]]:
    """
    Turn a count mapping into rows, or a single ``none`` row when empty.

    Example:
        >>> _pairs({}), _pairs({"b": 1})
        ([('none', '')], [('b', 1)])


    :param values: Mapping of label to count, or None.
    :return: Sorted rows.
    """
    rows = sorted(dict(values or {}).items())
    return rows or [("none", "")]


def _decisions(counts: Any) -> str:
    """
    Format per-decision name counts compactly.

    Example:
        >>> _decisions({"reuse": 3, "new": 1})
        'new 1, reuse 3'


    :param counts: Mapping of decision to distinct-name count.
    :return: Comma-separated ``decision count`` text, or ``none``.
    """
    items = sorted(dict(counts or {}).items())
    return ", ".join(f"{decision} {count}" for decision, count in items) or "none"


def _sample_lines(report: Mapping[str, Any]) -> list[str]:
    """
    Describe the sampled books and failures, one line each.

    Example:
        >>> _sample_lines({"books_sample": [{"index": 1, "title": "T", "authors": ["A"], "formats": ["epub"], "flags": []}]})
        ['Sample books:', '  #1 T — A [epub]']


    :param report: Job result carrying ``books_sample`` and ``failures_sample``.
    :return: Lines to print.
    """
    lines: list[str] = []
    books = list(report.get("books_sample") or [])
    if books:
        lines.append("Sample books:")
    for book in books:
        authors = ", ".join(book.get("authors") or []) or "no authors"
        formats = ", ".join(book.get("formats") or [])
        flags = ", ".join(book.get("flags") or [])
        suffix = f" ({flags})" if flags else ""
        lines.append(
            f"  #{book.get('index')} {book.get('title')} — {authors} [{formats}]{suffix}"
        )
    failures = list(report.get("failures_sample") or [])
    if failures:
        lines.append("Sample failures:")
    for failure in failures:
        key = dict(failure.get("file") or {}).get("storage_key", "")
        lines.append(f"  {key}: {failure.get('stage')}: {failure.get('error')}")
    return lines


class IngestMetadataCommand(TerminalCommandAPI):
    """
    Plan, as a Core job, the books a Store's unattached files would add.

    Example:
        >>> command = IngestMetadataCommand()
        >>> command.group, command.name
        ('ingest', 'metadata')
    """

    group = "ingest"
    group_aliases = ("import",)
    name = "metadata"
    aliases = ("embedded-metadata",)
    summary = (
        "Plan books from a Store's files: ingest metadata <store> --dry-run [options]"
    )
    usage = (
        "ingest metadata <store_id|store_name> --dry-run [--extensions epub,mobi] "
        "[--limit <n>] [--max-file-mb <n>] [--report <path>] [--background] [--json]"
    )
    expose_direct = False

    def execute(self, browser, args: list[str]) -> bool:
        """
        Resolve the Store, submit ``ingest.metadata.plan`` and show or hand off the result.

        In the foreground the command waits for the job, replays its progress log
        and prints the plan's summary and a sample; the full plan is in the JSON
        file it names. With ``--background`` it returns once the job is submitted.

        Example:
            >>> IngestMetadataCommand().execute(browser, ["1", "--dry-run"])  # doctest: +SKIP


        :param browser: Host providing Store lookup, Core job operations and output.
        :param args: Store reference and options.
        :return: True once the job is submitted or its result shown.
        :raises ValueError: If options are invalid, ``--dry-run`` is missing or the Store is unknown.
        :raises RuntimeError: If Core returns no job id or the job fails.
        """
        options = _parse_ingest_metadata_options(args, usage=self.usage)
        if not options.dry_run:
            raise ValueError(_WRITE_PHASE_MISSING)
        if options.background and options.json_output:
            raise ValueError(
                "--json is not supported with --background. Use `jobs show <id>`."
            )
        store_row = _resolve_store_row(browser, options.store_ref)
        store_id = int(store_row["store_id"])
        submitted = browser.execute_core_command(
            "ingest.metadata.plan",
            payload={
                "store_id": store_id,
                "extensions": options.extensions,
                "limit": options.limit,
                "max_file_mb": options.max_file_mb,
                "report_path": options.report_path,
                "progress_output": options.show_progress and not options.json_output,
                "job_backend": options.job_backend,
                "job_timeout_s": options.job_timeout_s,
                "label": f"ingest metadata plan:{store_id}",
            },
        )
        job_id = str((submitted or {}).get("job_id") or "")
        if not job_id:
            raise RuntimeError("Core did not return a metadata plan job id.")
        if options.background:
            browser.emit(f"Metadata plan job submitted: {job_id}")
            browser.emit(
                f"  Use `jobs show {job_id} --wait` to see the plan's summary."
            )
            return True
        report = _wait_for_job(browser, job_id)
        if options.show_progress and not options.json_output:
            _emit_job_log(browser, job_id)
        if options.json_output:
            browser.emit(
                json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2)
            )
            return True
        browser.emit_detail_sections(
            _plan_sections(report),
            title="Metadata plan (dry run, nothing written)",
            max_cell_width=120,
        )
        for line in _sample_lines(report):
            browser.emit(line)
        return True


__all__ = ["IngestMetadataCommand"]
