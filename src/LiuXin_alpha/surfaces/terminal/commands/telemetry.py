"""Telemetry commands for database activity inspection."""

from __future__ import annotations

import time

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


class TelemetryPanelCommand(TerminalCommandAPI):
    """Attach/detach DB telemetry to a dedicated windowed panel."""

    group = "telemetry"
    group_aliases = ("debug",)
    expose_direct = False
    name = "panel"
    aliases = ("pane",)
    summary = "Attach DB telemetry to the windowed auxiliary panel."
    usage = "telemetry panel [on|off|reset] [table ...]"

    def execute(self, browser, args: list[str]) -> bool:
        tokens = [str(arg).strip() for arg in args if str(arg).strip()]
        if tokens and tokens[0].lower() in {"off", "none", "disable", "disabled"}:
            if browser.detach_telemetry_panel():
                browser.emit("Telemetry panel detached.")
            else:
                browser.emit("No active telemetry panel.")
            return True

        mode = "on"
        if tokens and tokens[0].lower() in {"on", "show", "attach", "enable", "enabled", "reset"}:
            mode = tokens[0].lower()
            tokens = tokens[1:]

        resolved_tables: list[str] = []
        for token in tokens:
            resolved = browser._resolve_table(token)
            if resolved not in resolved_tables:
                resolved_tables.append(resolved)

        snapshot = {}
        try:
            snapshot = dict(
                browser.execute_core_query(
                    "database.telemetry",
                    payload={"recent_limit": 5},
                )
                or {}
            )
        except Exception:
            snapshot = {}

        current_counts: list[tuple[str, object]] = []
        tracked_tables = resolved_tables
        if not tracked_tables:
            tracked_tables = ["files", "folders", "items", "works", "stores"]
        for table in tracked_tables:
            try:
                count = browser.get_table_row_count(table)
            except Exception:
                count = None
            current_counts.append((table, "?" if count is None else count))

        if not browser.supports_telemetry_panel():
            browser.emit_detail_sections(
                [
                    (
                        "Activity",
                        [
                            ("observed_total", snapshot.get("observed_total", 0)),
                            ("queue_depth", snapshot.get("queue_size", 0)),
                            ("persisted", snapshot.get("persisted_queue_size", 0)),
                            ("mode", "plain snapshot"),
                        ],
                    ),
                    ("Counts", current_counts),
                ],
                title="DB telemetry snapshot",
                max_cell_width=120,
            )
            recent_events = list(snapshot.get("recent_events", ()) or ())
            if recent_events:
                browser.emit("")
                browser.emit("Recent events")
                for event in recent_events:
                    timestamp = ""
                    try:
                        timestamp = time.strftime("%H:%M:%S", time.localtime(float(event.get("timestamp", 0.0))))
                    except Exception:
                        timestamp = "--:--:--"
                    table = str(event.get("table", "") or "").strip() or "<unknown>"
                    row_id = event.get("row_id", "")
                    source = str(event.get("source", "") or "").strip() or "event"
                    reason = str(event.get("reason", "") or "").strip()
                    line = "{} | {} | {}:{}".format(timestamp, source, table, row_id)
                    if reason:
                        line += " | {}".format(reason)
                    browser.emit("  {}".format(line))
            return True

        browser.attach_telemetry_panel(tuple(resolved_tables) if resolved_tables else None)
        browser.emit_detail_sections(
            [
                (
                    "Panel",
                    [
                        ("mode", "attached" if mode != "reset" else "reset"),
                        ("tables", ", ".join(tracked_tables)),
                    ],
                ),
                (
                    "Activity",
                    [
                        ("observed_total", snapshot.get("observed_total", 0)),
                        ("queue_depth", snapshot.get("queue_size", 0)),
                        ("persisted", snapshot.get("persisted_queue_size", 0)),
                    ],
                ),
                ("Counts", current_counts),
            ],
            title="Telemetry panel attached.",
            max_cell_width=120,
        )
        return True


__all__ = [
    "TelemetryPanelCommand",
]
