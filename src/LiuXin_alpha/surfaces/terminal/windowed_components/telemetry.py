"""
Collect table-count deltas and Core activity snapshots for the telemetry pane.

Sampling is synchronous and driven by rendering, not a background poller. Missing
counts are displayed as unknown; failed Core snapshots become visible error text.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from .contracts import WindowedState


class TelemetryMixin(WindowedState):
    """
    Own telemetry selection, baseline counts, and compact activity-panel content.

    The composed driver supplies browser access, window layout, and rendering.
    Re-selecting tables restarts their baseline even if the selection is unchanged.

    Example:
        >>> TelemetryMixin._format_count_delta(15, 12)
        '+3'
    """

    @staticmethod
    def _default_telemetry_tables() -> tuple[str, ...]:
        """
        Return the ordered table selection used when no explicit telemetry list survives.

        Example:
            >>> TelemetryMixin._default_telemetry_tables()
            ('files', 'folders', 'items', 'works', 'stores')


        :return: Default file, folder, item, work, and store table names in display order.
        """
        return ("files", "folders", "items", "works", "stores")

    def set_telemetry_tables(self, tables: Sequence[str] | None = None) -> bool:
        """
        Select tracked tables, restart the count baseline, and rebuild/redraw the panes.

        Names are stringified, stripped, and deduplicated in input order without
        schema validation or case folding. ``None``, an empty sequence, or entirely
        blank names selects the defaults. Every call resets the start time and
        samples counts, even if the normalized selection has not changed.

        Example:
            >>> driver.set_telemetry_tables(["works", "items"])  # doctest: +SKIP
            True


        :param tables: Optional ordered table names; omitted or empty input uses defaults.
        :return: ``True`` after successful setup; this is not a change-only indicator.
        """
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
        """
        Disable telemetry and clear its time/count snapshots before rebuilding panes.

        Layout and status redraw run even when telemetry was already disabled.

        Example:
            >>> was_enabled = driver.clear_telemetry_panel()  # doctest: +SKIP


        :return: Whether a telemetry table selection existed before clearing it.
        """
        had = self._telemetry_tables is not None
        self._telemetry_tables = None
        self._telemetry_started_at = None
        self._telemetry_baseline_counts = {}
        self._telemetry_last_counts = {}
        self._rebuild_windows()
        self._render(force_status=True)
        return had

    def _telemetry_content_width(self) -> int:
        """
        Reserve the telemetry pane's final column for safe curses drawing.

        Example:
            >>> width = driver._telemetry_content_width()  # doctest: +SKIP


        :return: At least one usable column, using terminal width when no pane exists.
        """
        win = self._telemetry_win
        if win is not None:
            _, cols = win.getmaxyx()
            return max(1, cols - 1)
        return max(1, self.terminal_width - 1)

    def _sample_telemetry_counts(
        self, tables: tuple[str, ...]
    ) -> dict[str, int | None]:
        """
        Read each selected table's row count, marking unavailable queries as unknown.

        A missing browser, raised count-query exception, or ``None`` result produces
        ``None`` for that table. Integer conversion happens outside the exception
        handler, so an invalid non-``None`` result still raises.

        Example:
            >>> counts = driver._sample_telemetry_counts(("works",))  # doctest: +SKIP


        :param tables: Ordered table names to query independently through the browser.
        :return: Mapping from each requested name to its integer count or ``None``.
        """
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
        """
        Format a signed count change, or an unknown marker if either sample is absent.

        Example:
            >>> TelemetryMixin._format_count_delta(9, 12)
            '-3'
            >>> TelemetryMixin._format_count_delta(9, 9)
            '+0'
            >>> TelemetryMixin._format_count_delta(None, 9)
            '?'


        :param current: Most recent count, or ``None`` when unavailable.
        :param previous: Baseline or preceding count to subtract, or ``None``.
        :return: Signed integer difference as text, or ``?`` for an unknown difference.
        """
        if current is None or previous is None:
            return "?"
        return f"{int(current) - int(previous):+d}"

    def _build_telemetry_lines(self, *, max_lines: int) -> list[str]:
        """
        Sample telemetry and return a height-limited activity/count/event summary.

        Disabled telemetry or a nonpositive height returns immediately without
        querying. Otherwise request a Core activity snapshot, independently sample
        table counts, and update the last-count snapshot before formatting deltas
        against both the preceding sample and the selection's original baseline.

        Snapshot lookup/conversion exceptions become an Errors section. This does
        not suppress malformed nested payloads or count-conversion failures. Fixed
        sections take priority over the recent-event tail; output is not width-wrapped.

        Example:
            >>> lines = driver._build_telemetry_lines(max_lines=12)  # doctest: +SKIP


        :param max_lines: Maximum number of compact lines to return, including the title.
        :return: Telemetry lines in display order, possibly truncated within the fixed sections.
        """
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
        """
        Append a recent-events heading and the newest events that fit after existing lines.

        Preserve the selected tail's input order and render timestamps in local
        time. Invalid timestamps use ``--:--:--``; blank table/source values use
        ``<unknown>``/``event``. Event records must otherwise provide mapping access.
        Existing lines are not truncated, and a single free line fits only the heading.

        Example:
            >>> lines = ["DB telemetry"]
            >>> TelemetryMixin._append_recent_telemetry_events(
            ...     lines, {"recent_events": [{"table": "works"}]}, 2
            ... )
            >>> lines
            ['DB telemetry', 'Recent events']


        :param lines: Existing summary lines to extend in place without clearing them.
        :param snapshot: Core snapshot containing the ordered ``recent_events`` sequence.
        :param max_lines: Total line budget for the existing summary and appended event tail.
        :return: ``None``; any heading and formatted events are appended to ``lines``.
        """
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
