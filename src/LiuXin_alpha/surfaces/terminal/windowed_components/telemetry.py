"""Tracked table counts, activity snapshots, and telemetry panel content."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from .contracts import WindowedState


class TelemetryMixin(WindowedState):
    """Tracked table counts, activity snapshots, and telemetry panel content."""

    @staticmethod
    def _default_telemetry_tables() -> tuple[str, ...]:
        return ("files", "folders", "items", "works", "stores")

    def set_telemetry_tables(self, tables: Sequence[str] | None = None) -> bool:
        normalized: list[str] = []
        for raw in list(tables or self._default_telemetry_tables()):
            token = str(raw).strip()
            if token and token not in normalized:
                normalized.append(token)
        next_tables = (
            tuple(normalized) if normalized else self._default_telemetry_tables()
        )
        changed = self._telemetry_tables != next_tables
        self._telemetry_tables = next_tables
        self._telemetry_started_at = time.time()
        sampled = self._sample_telemetry_counts(next_tables)
        self._telemetry_baseline_counts = dict(sampled)
        self._telemetry_last_counts = dict(sampled)
        self._rebuild_windows()
        self._render(force_status=True)
        return changed or bool(next_tables)

    def clear_telemetry_panel(self) -> bool:
        had = self._telemetry_tables is not None
        self._telemetry_tables = None
        self._telemetry_started_at = None
        self._telemetry_baseline_counts = {}
        self._telemetry_last_counts = {}
        self._rebuild_windows()
        self._render(force_status=True)
        return had

    def _telemetry_content_width(self) -> int:
        win = self._telemetry_win
        if win is not None:
            _, cols = win.getmaxyx()
            return max(1, cols - 1)
        return max(1, self.terminal_width - 1)

    def _sample_telemetry_counts(
        self, tables: tuple[str, ...]
    ) -> dict[str, int | None]:
        browser = self.browser
        counts: dict[str, int | None] = {}
        for table in tables:
            if browser is None:
                counts[table] = None
                continue
            try:
                value = browser.get_table_row_count(table)
            except Exception:
                value = None
            counts[table] = None if value is None else int(value)
        return counts

    @staticmethod
    def _format_count_delta(current: int | None, previous: int | None) -> str:
        if current is None or previous is None:
            return "?"
        return f"{int(current) - int(previous):+d}"

    def _build_telemetry_lines(self, *, max_lines: int) -> list[str]:
        if max_lines <= 0:
            return []
        tracked_tables = self._telemetry_tables
        if not tracked_tables:
            return []

        browser = self.browser
        # Core query payloads are dynamic wire records, not browser host objects.
        snapshot: dict[str, Any] = {}
        if browser is not None:
            try:
                snapshot = dict(
                    browser.execute_core_query(
                        "database.telemetry",
                        payload={"recent_limit": max(2, max_lines)},
                    )
                    or {}
                )
            except Exception as exc:
                snapshot = {
                    "recent_events": [],
                    "snapshot_error": self._format_error(
                        "telemetry snapshot failed", exc
                    ),
                }

        current_counts = self._sample_telemetry_counts(tracked_tables)
        previous_counts = dict(self._telemetry_last_counts or {})
        baseline_counts = dict(self._telemetry_baseline_counts or {})
        self._telemetry_last_counts = dict(current_counts)

        uptime_s = 0
        if self._telemetry_started_at is not None:
            uptime_s = max(0, int(time.time() - self._telemetry_started_at))

        sections: list[tuple[str, list[tuple[str, object]]]] = [
            (
                "Activity",
                [
                    ("observed_total", snapshot.get("observed_total", 0)),
                    ("queue_depth", snapshot.get("queue_size", 0)),
                    ("persisted", snapshot.get("persisted_queue_size", 0)),
                    ("uptime_s", uptime_s),
                ],
            ),
            ("Tracking", [("", ", ".join(tracked_tables))]),
        ]
        if snapshot.get("snapshot_error"):
            sections.append(("Errors", [("telemetry", snapshot.get("snapshot_error"))]))

        source_counts = dict(snapshot.get("source_counts", {}) or {})
        if source_counts:
            segments = [
                f"{key}={source_counts[key]}" for key in sorted(source_counts.keys())
            ]
            sections.append(("Sources", [("", " | ".join(segments))]))

        for table in tracked_tables:
            current = current_counts.get(table)
            previous = previous_counts.get(table, current)
            baseline = baseline_counts.get(table, current)
            sections.append(
                (
                    table,
                    [
                        ("total", "?" if current is None else current),
                        ("since", self._format_count_delta(current, baseline)),
                        ("last", self._format_count_delta(current, previous)),
                    ],
                )
            )

        lines = self._render_compact_sections(sections, title="DB telemetry")
        self._append_recent_telemetry_events(lines, snapshot, max_lines)
        return lines[:max_lines]

    @staticmethod
    def _append_recent_telemetry_events(
        lines: list[str], snapshot: dict[str, Any], max_lines: int
    ) -> None:
        """Fit the recent-event tail after fixed telemetry sections."""
        recent_events = list(snapshot.get("recent_events", ()) or ())
        remaining = max(0, int(max_lines) - len(lines))
        if remaining > 0 and recent_events:
            lines.append("Recent events")
            remaining -= 1
            tail = recent_events[-remaining:] if remaining > 0 else []
            for event in tail:
                try:
                    timestamp = time.strftime(
                        "%H:%M:%S", time.localtime(float(event.get("timestamp", 0.0)))
                    )
                except Exception:
                    timestamp = "--:--:--"
                table = str(event.get("table", "") or "").strip() or "<unknown>"
                row_id = event.get("row_id", "")
                source = str(event.get("source", "") or "").strip() or "event"
                reason = str(event.get("reason", "") or "").strip()
                summary = f"{timestamp} | {source} | {table}:{row_id}"
                if reason:
                    summary += f" | {reason}"
                lines.append(summary)
