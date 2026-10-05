#!/usr/bin/env python3
"""
Provide summarize benchmark report utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise summarize benchmark report through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
"""

from __future__ import annotations

import argparse
import json
import sys

from collections import defaultdict
from pathlib import Path
from typing import Iterable, Optional


def parse_args() -> argparse.Namespace:
    """
    Parse args under the format's safety and compatibility rules.

    Example:
        Exercise parse args through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = argparse.ArgumentParser(description="Summarize a LiuXin benchmark JSON report.")
    parser.add_argument("report", help="Path to a benchmark JSON report.")
    parser.add_argument(
        "--format",
        choices=("text", "markdown"),
        default="text",
        help="Summary output format.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Write the summary to this path instead of stdout.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Maximum number of slowest scenarios to show.",
    )
    return parser.parse_args()


def _load_report(path: Path) -> dict[str, object]:
    """
    Perform the load report operation under explicit file-format and conversion rules.

    Example:
        Exercise  load report through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def _flatten_results(report: dict[str, object]) -> list[dict[str, object]]:
    """
    Perform the flatten results operation under explicit file-format and conversion rules.

    Example:
        Exercise  flatten results through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param report: Value supplied for report under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    results: list[dict[str, object]] = []

    def append_group(kind: str, reports: Iterable[dict[str, object]]) -> None:
        """
        Perform the append group operation under explicit file-format and conversion rules.

        Example:
            Exercise  flatten results.append group through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param kind: Value supplied for kind under the utility contract.
        :param reports: Value supplied for reports under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for report_payload in reports:
            database = dict(report_payload.get("database") or {})
            db_source = str(database.get("source") or "")
            for result in list(report_payload.get("results") or []):
                row = dict(result)
                row["_group"] = kind
                row["_database_source"] = db_source
                results.append(row)

    if "results" in report:
        database = dict(report.get("database") or {})
        db_source = str(database.get("source") or "")
        for result in list(report.get("results") or []):
            row = dict(result)
            row["_group"] = "single"
            row["_database_source"] = db_source
            results.append(row)
        return results

    append_group("backend", list(report.get("backend_reports") or []))
    append_group("surface", list(report.get("surface_reports") or report.get("interface_reports") or []))
    return results


def _timed_results(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """
    Perform the timed results operation under explicit file-format and conversion rules.

    Example:
        Exercise  timed results through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param rows: Value supplied for rows under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return [row for row in rows if not row.get("skipped") and row.get("mean_ms") is not None]


def _format_ms(value: object) -> str:
    """
    Perform the format ms operation under explicit file-format and conversion rules.

    Example:
        Exercise  format ms through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value is None:
        return "-"
    return "{:.3f} ms".format(float(value))


def render_text_summary(report: dict[str, object], *, top: int) -> str:
    """
    Perform the render text summary operation under explicit file-format and conversion rules.

    Example:
        Exercise render text summary through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param report: Value supplied for report under the utility contract.
    :param top: Value supplied for top under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    rows = _flatten_results(report)
    timed = sorted(_timed_results(rows), key=lambda row: float(row.get("mean_ms") or 0.0), reverse=True)
    skipped = [row for row in rows if row.get("skipped")]

    group_counts: dict[str, int] = defaultdict(int)
    db_counts: dict[str, int] = defaultdict(int)
    for row in timed:
        group_counts[str(row["_group"])] += 1
        db_counts[str(row["_database_source"])] += 1

    lines: list[str] = []
    lines.append("Benchmark Summary")
    lines.append("script={}".format(report.get("script", "")))
    lines.append("created_utc={}".format(report.get("created_utc", "")))
    profile = dict(report.get("inputs") or {}).get("profile")
    if profile:
        lines.append("profile={}".format(profile))
    lines.append("timed_scenarios={}".format(len(timed)))
    lines.append("skipped_scenarios={}".format(len(skipped)))
    if group_counts:
        lines.append("groups={}".format(", ".join("{}={}".format(key, group_counts[key]) for key in sorted(group_counts))))
    if db_counts:
        lines.append("databases={}".format(", ".join("{}={}".format(key, db_counts[key]) for key in sorted(db_counts))))

    lines.append("")
    lines.append("Slowest Scenarios")
    for row in timed[: max(1, int(top))]:
        lines.append(
            "- {name} [{db}] mean={mean} median={median} max={maxv}".format(
                name=row.get("name", ""),
                db=row.get("_database_source", ""),
                mean=_format_ms(row.get("mean_ms")),
                median=_format_ms(row.get("median_ms")),
                maxv=_format_ms(row.get("max_ms")),
            )
        )

    if skipped:
        lines.append("")
        lines.append("Skipped Scenarios")
        for row in skipped[: max(1, int(top))]:
            lines.append(
                "- {name} [{db}] reason={reason}".format(
                    name=row.get("name", ""),
                    db=row.get("_database_source", ""),
                    reason=row.get("reason", ""),
                )
            )

    return "\n".join(lines) + "\n"


def render_markdown_summary(report: dict[str, object], *, top: int) -> str:
    """
    Perform the render markdown summary operation under explicit file-format and conversion rules.

    Example:
        Exercise render markdown summary through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param report: Value supplied for report under the utility contract.
    :param top: Value supplied for top under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    rows = _flatten_results(report)
    timed = sorted(_timed_results(rows), key=lambda row: float(row.get("mean_ms") or 0.0), reverse=True)
    skipped = [row for row in rows if row.get("skipped")]

    lines: list[str] = []
    lines.append("# Benchmark Summary")
    lines.append("")
    lines.append("- script: `{}`".format(report.get("script", "")))
    lines.append("- created_utc: `{}`".format(report.get("created_utc", "")))
    profile = dict(report.get("inputs") or {}).get("profile")
    if profile:
        lines.append("- profile: `{}`".format(profile))
    lines.append("- timed_scenarios: `{}`".format(len(timed)))
    lines.append("- skipped_scenarios: `{}`".format(len(skipped)))
    lines.append("")
    lines.append("## Slowest Scenarios")
    lines.append("")
    lines.append("| scenario | database | mean | median | max |")
    lines.append("|---|---|---:|---:|---:|")
    for row in timed[: max(1, int(top))]:
        lines.append(
            "| `{}` | `{}` | {} | {} | {} |".format(
                row.get("name", ""),
                row.get("_database_source", ""),
                _format_ms(row.get("mean_ms")),
                _format_ms(row.get("median_ms")),
                _format_ms(row.get("max_ms")),
            )
        )
    if skipped:
        lines.append("")
        lines.append("## Skipped Scenarios")
        lines.append("")
        for row in skipped[: max(1, int(top))]:
            lines.append(
                "- `{}` on `{}`: `{}`".format(
                    row.get("name", ""),
                    row.get("_database_source", ""),
                    row.get("reason", ""),
                )
            )
    return "\n".join(lines) + "\n"


def write_text(text: str, output: str) -> None:
    """
    Write text under the format's safety and compatibility rules.

    Example:
        Exercise write text through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param text: Text parsed, normalized or rendered.
    :param output: Value supplied for output under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if not output:
        sys.stdout.write(text)
        sys.stdout.flush()
        return
    path = Path(output).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    args = parse_args()
    path = Path(args.report).expanduser().resolve()
    report = _load_report(path)
    if args.format == "markdown":
        text = render_markdown_summary(report, top=max(1, int(args.top)))
    else:
        text = render_text_summary(report, top=max(1, int(args.top)))
    write_text(text, str(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
