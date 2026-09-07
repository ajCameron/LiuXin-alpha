"""Database context, job summaries, and panel status presentation."""

from __future__ import annotations

import time
from collections import Counter

from .contracts import WindowedState
from .models import WindowedBrowser

type StatusSections = list[tuple[str, list[tuple[str, object]]]]


class StatusMixin(WindowedState):
    """Database context, job summaries, and panel status presentation."""

    def _list_jobs(self) -> list[object]:
        self._jobs_status_error = None
        if self.browser is None:
            return []
        if hasattr(self.browser, "supports_core_queries") and bool(
            self.browser.supports_core_queries()
        ):
            try:
                result = self.browser.execute_core_query(
                    "jobs.list",
                    payload={"offset": 0, "limit": 5000},
                )
                return list((result or {}).get("jobs", ()) or ())
            except Exception as exc:
                self._jobs_status_error = self._format_error(
                    "core jobs.list failed", exc
                )
                return []
        try:
            return list(self.browser.job_manager.list())
        except Exception as exc:
            self._jobs_status_error = self._format_error("local jobs unavailable", exc)
            return []

    def _build_status_lines(self) -> list[str]:
        title = self._status_title()
        sections: StatusSections = []
        if self.browser is None:
            return self._render_compact_sections([], title=title)
        self._append_runtime_status(sections, self.browser)
        self._append_context_status(sections, self.browser)
        self._append_rows_status(sections, self.browser)
        self._append_jobs_status(sections)
        self._append_panels_status(sections)
        self._append_interaction_status(sections)
        return self._render_compact_sections(sections, title=title)

    def _status_title(self) -> str:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        db_path = ""
        if self.browser is not None:
            try:
                db_path = str(self.browser.database_path)
            except Exception:
                db_path = ""
        title = f"LiuXin Terminal UI | {now}"
        if db_path:
            title += f" | db={db_path}"

        return title

    def _append_runtime_status(
        self, sections: StatusSections, browser: WindowedBrowser
    ) -> None:
        core_status = ""
        if hasattr(browser, "core_runtime_status_summary"):
            try:
                core_status = str(browser.core_runtime_status_summary() or "").strip()
            except Exception:
                core_status = ""
        if core_status:
            sections.append(("Runtime", [("", core_status)]))

    def _append_context_status(
        self, sections: StatusSections, browser: WindowedBrowser
    ) -> None:
        table = browser.current_table or "<none>"
        window = browser.window
        if window is None:
            window_text = "window: <none>"
        else:
            window_text = (
                f"window: {window.table} limit={window.limit} offset={window.offset}"
            )
        sections.append(
            (
                "Context",
                [
                    ("table", table),
                    ("page_size", browser.page_size),
                    ("window", window_text.replace("window: ", "", 1)),
                ],
            )
        )

    def _append_rows_status(
        self, sections: StatusSections, browser: WindowedBrowser
    ) -> None:
        table_counts: list[str] = []
        for name in (
            "works",
            "expressions",
            "manifestations",
            "items",
            "files",
            "stores",
        ):
            try:
                count = browser.get_table_row_count(name)
            except Exception:
                count = None
            if count is None:
                continue
            table_counts.append(f"{name}={count}")
        if table_counts:
            sections.append(("Rows", [("", " | ".join(table_counts))]))

    def _append_jobs_status(self, sections: StatusSections) -> None:
        jobs = self._list_jobs()
        counter = Counter(
            str(
                (
                    one.get("state", "")
                    if isinstance(one, dict)
                    else getattr(one, "state", "")
                )
                or ""
            )
            for one in jobs
        )
        job_rows: list[tuple[str, object]] = [("total", len(jobs))]
        if jobs:
            for key in sorted(counter.keys()):
                job_rows.append((key, counter.get(key, 0)))
        sections.append(("Jobs", job_rows))
        if self._jobs_status_error:
            sections.append(("Errors", [("jobs_error", self._jobs_status_error)]))

    def _append_panels_status(self, sections: StatusSections) -> None:
        panel_rows: list[tuple[str, object]] = []
        if self._job_output_job_id:
            panel_rows.append(("job_panel", self._job_output_job_id))
        if self._telemetry_tables:
            panel_rows.append(("telemetry_panel", ",".join(self._telemetry_tables)))
        if panel_rows:
            sections.append(("Panels", panel_rows))

    def _append_interaction_status(self, sections: StatusSections) -> None:
        ui_rows: list[tuple[str, object]] = []
        if self._job_output_job_id:
            ui_rows.append(
                ("focus", f"{self._active_scroll_target()} | F6 switch pane")
            )
        if self._console_scroll_offset > 0:
            ui_rows.append(
                (
                    "console_scrollback",
                    f"+{self._console_scroll_offset} | PgUp/PgDn Home/End",
                )
            )
        if self._job_output_job_id and self._job_output_scroll_offset > 0:
            ui_rows.append(
                (
                    "job_scrollback",
                    f"+{self._job_output_scroll_offset} | PgUp/PgDn Home/End",
                )
            )
        if self._completion_hint:
            ui_rows.append(("", self._completion_hint))
        if ui_rows:
            sections.append(("UI", ui_rows))
