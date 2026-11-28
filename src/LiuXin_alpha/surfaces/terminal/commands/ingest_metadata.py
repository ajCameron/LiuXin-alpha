"""
Plan, then write, the books a Store's files describe, and show what happened.

``ingest metadata <store> --dry-run`` is the read half of ingest's metadata
step: a Core job reads the files a ``sync store`` registered, matches them
against the catalogue and writes a plan of the books it would add, creating
nothing. ``ingest metadata <store> --apply <plan>`` is the write half: Core
writes that plan, one book per transaction, re-checking each decision first.
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

_MODE_REQUIRED = "Pass `--dry-run` to plan, or `--apply <plan>` to write a plan."
_DRY_RUN_ONLY = (
    "--extensions",
    "--max-file-mb",
    "--background",
    "--job-backend",
    "--job-timeout-s",
)


@dataclasses.dataclass(frozen=True)
class _IngestMetadataOptions:
    """
    Hold the parsed options for ``ingest metadata``.

    Example:
        >>> options = _parse_ingest_metadata_options(["3", "--dry-run", "--limit", "5"], usage="u")
        >>> options.store_ref, options.limit, options.dry_run, options.apply_path
        ('3', 5, True, None)
    """

    store_ref: str
    dry_run: bool
    apply_path: str | None
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
    Parse a Store reference plus the mode, selection, report and job options.

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
    parser.add_argument("--apply", dest="apply_path")
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
        apply_path=(str(values.apply_path).strip() or None)
        if values.apply_path
        else None,
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


def _apply_sections(
    report: Mapping[str, Any],
) -> list[tuple[str, list[tuple[str, Any]]]]:
    """
    Arrange an apply result as terminal detail sections.

    Example:
        >>> sections = _apply_sections({"store": {"store_id": 1}, "summary": {"books": 2}, "report_path": "r.json"})
        >>> [title for title, _ in sections]
        ['Store', 'Applied', 'Outcomes', 'Names created', 'Not stored']


    :param report: Result from ``ingest.metadata.apply``.
    :return: ``(title, rows)`` pairs.
    """
    store = dict(report.get("store") or {})
    summary = dict(report.get("summary") or {})
    return [
        (
            "Store",
            [
                (key, store.get(key, ""))
                for key in ("store_id", "store_name", "store_kind")
            ],
        ),
        (
            "Applied",
            [
                ("books", summary.get("books", 0)),
                ("works_created", summary.get("works_created", 0)),
                ("manifestations_created", summary.get("manifestations_created", 0)),
                ("files_attached", summary.get("files_attached", 0)),
                ("plan", report.get("plan_path", "")),
                ("report", report.get("report_path", "")),
            ],
        ),
        ("Outcomes", _pairs(summary.get("outcomes"))),
        ("Names created", _pairs(summary.get("names_created"))),
        ("Not stored", _pairs(summary.get("not_stored"))),
    ]


def _apply_lines(report: Mapping[str, Any]) -> list[str]:
    """
    Describe the sampled skipped and failed books, one line each.

    Example:
        >>> _apply_lines({"problems_sample": [{"index": 2, "title": "T", "outcome": "skipped", "reason": "file 3 has moved"}]})
        ['Skipped or failed:', '  #2 T: skipped: file 3 has moved']


    :param report: Result carrying ``problems_sample``.
    :return: Lines to print.
    """
    problems = list(report.get("problems_sample") or [])
    lines = ["Skipped or failed:"] if problems else []
    for book in problems:
        lines.append(
            f"  #{book.get('index')} {book.get('title')}: {book.get('outcome')}: {book.get('reason')}"
        )
    return lines


class IngestMetadataCommand(TerminalCommandAPI):
    """
    Plan the books a Store's unattached files would add, or write a reviewed plan.

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
        "Plan books from a Store's files, then write the plan: "
        "ingest metadata <store> --dry-run | --apply <plan> [options]"
    )
    usage = (
        "ingest metadata <store_id|store_name> --dry-run [--extensions epub,mobi] "
        "[--limit <n>] [--max-file-mb <n>] [--report <path>] [--background] [--json]"
        " | ingest metadata <store_id|store_name> --apply <plan.json> [--limit <n>] "
        "[--report <path>] [--json]"
    )
    expose_direct = False

    def execute(self, browser, args: list[str]) -> bool:
        """
        Resolve the Store, then run the dry run or the apply step and show its result.

        Exactly one of ``--dry-run`` and ``--apply`` is required.

        Example:
            >>> IngestMetadataCommand().execute(browser, ["1", "--dry-run"])  # doctest: +SKIP


        :param browser: Host providing Store lookup, Core commands, job operations and output.
        :param args: Store reference and options.
        :return: True once the job is submitted or the result shown.
        :raises ValueError: If options are invalid or conflict, or the Store is unknown.
        :raises RuntimeError: If Core returns no job id or the job fails.
        """
        options = _parse_ingest_metadata_options(args, usage=self.usage)
        if options.dry_run == (options.apply_path is not None):
            raise ValueError(_MODE_REQUIRED)
        store_row = _resolve_store_row(browser, options.store_ref)
        if options.apply_path is not None:
            return self._apply(browser, options, args, int(store_row["store_id"]))
        return self._plan(browser, options, int(store_row["store_id"]))

    def _apply(
        self,
        browser: Any,
        options: _IngestMetadataOptions,
        args: list[str],
        store_id: int,
    ) -> bool:
        """
        Ask Core to write a reviewed plan, then show the outcome.

        The apply step runs in the foreground in the process that owns the cache,
        so the job options of the dry run don't apply.

        Example:
            >>> command._apply(browser, options, ["1", "--apply", "plan.json"], 1)  # doctest: +SKIP


        :param browser: Host providing Core commands and output.
        :param options: Parsed options.
        :param args: Raw option tokens, to reject dry-run-only options.
        :param store_id: Store the plan must be for.
        :return: True once the result is shown.
        :raises ValueError: If a dry-run-only option is given.
        """
        given = [option for option in _DRY_RUN_ONLY if option in args]
        if given:
            raise ValueError(f"{', '.join(given)} only apply to --dry-run.")
        report = browser.execute_core_command(
            "ingest.metadata.apply",
            payload={
                "plan_path": options.apply_path,
                "store_id": store_id,
                "limit": options.limit,
                "report_path": options.report_path,
            },
        )
        if options.json_output:
            browser.emit(
                json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2)
            )
            return True
        browser.emit_detail_sections(
            _apply_sections(report),
            title="Metadata ingest applied",
            max_cell_width=120,
        )
        for line in _apply_lines(report):
            browser.emit(line)
        return True

    def _plan(
        self, browser: Any, options: _IngestMetadataOptions, store_id: int
    ) -> bool:
        """
        Submit the dry run as a Core job and show or hand off its result.

        In the foreground the command waits for the job, replays its progress log
        and prints the plan's summary and a sample; the full plan is in the JSON
        file it names. With ``--background`` it returns once the job is submitted.

        Example:
            >>> command._plan(browser, options, 1)  # doctest: +SKIP


        :param browser: Host providing Core job operations and output.
        :param options: Parsed options.
        :param store_id: Store to plan.
        :return: True once the job is submitted or its result shown.
        :raises ValueError: If ``--json`` is combined with ``--background``.
        :raises RuntimeError: If Core returns no job id or the job fails.
        """
        if options.background and options.json_output:
            raise ValueError(
                "--json is not supported with --background. Use `jobs show <id>`."
            )
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
