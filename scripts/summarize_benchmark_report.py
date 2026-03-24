#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from collections import defaultdict
from pathlib import Path
from typing import Iterable, Optional


def parse_args() -> argparse.Namespace:
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
    return json.loads(path.read_text(encoding="utf-8"))


def _flatten_results(report: dict[str, object]) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    def append_group(kind: str, reports: Iterable[dict[str, object]]) -> None:
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
    append_group("interface", list(report.get("interface_reports") or []))
    return results


def _timed_results(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [row for row in rows if not row.get("skipped") and row.get("mean_ms") is not None]


def _format_ms(value: object) -> str:
    if value is None:
        return "-"
    return "{:.3f} ms".format(float(value))


def render_text_summary(report: dict[str, object], *, top: int) -> str:
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
    if not output:
        sys.stdout.write(text)
        sys.stdout.flush()
        return
    path = Path(output).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
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
