"""
Build compact terminal status from database context, job counts, and panel state.

Browser queries are performed when a status snapshot is built. Optional runtime
and count failures omit those values, while failed job listings get an explicit
Errors section rather than silently appearing to be a successful empty listing.
"""

from __future__ import annotations

import time
from collections import Counter

from .contracts import WindowedState
from .models import WindowedBrowser

type StatusSections = list[tuple[str, list[tuple[str, object]]]]


class StatusMixin(WindowedState):
    """
    Assemble status sections using browser reads and the composed driver's UI state.

    Helpers append ordered section records; the shared presentation owner converts
    them into lines. This owner does not itself draw windows or enforce pane height.

    Example:
        >>> lines = driver._build_status_lines()  # doctest: +SKIP
    """

    def _list_jobs(self) -> list[object]:
        """
        List jobs through Core when supported, otherwise through the local job manager.

        Clear the previous job error before each attempt. Core requests the first
        5,000 jobs; local results are not capped here. A failed selected backend
        stores a diagnostic and returns an empty list, without trying the other
        backend. Exceptions from the capability check itself are not caught.

        Example:
            >>> jobs = driver._list_jobs()  # doctest: +SKIP


        :return: Available job records, or an empty list for no browser or a failed listing.
        """
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
        """
        Build the current title and ordered runtime, context, rows, jobs, panels, and UI lines.

        Without a browser only the title is returned. With a browser this performs
        count and job reads, so it also refreshes the stored job-list error state.
        No line-count limit or width wrapping is applied here.

        Example:
            >>> lines = driver._build_status_lines()  # doctest: +SKIP


        :return: Compact status lines, beginning with the timestamped application title.
        """
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
        """
        Compose the application title with local time and a best-effort database path.

        An unavailable or failing database-path property omits only the path suffix.

        Example:
            >>> title = driver._status_title()  # doctest: +SKIP


        :return: LiuXin terminal title with a second-resolution timestamp and optional path.
        """
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
        """
        Append a Runtime section when the browser supplies a nonblank runtime summary.

        A missing summary capability, call exception, or blank result omits the section.

        Example:
            >>> driver._append_runtime_status(sections, browser)  # doctest: +SKIP


        :param sections: Ordered status-section list to extend in place.
        :param browser: Browser whose optional runtime-summary method is queried.
        :return: ``None``; zero or one Runtime section is appended.
        """
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
        """
        Append the selected table, configured page size, and current browse-window bounds.

        Missing selections and windows are represented by ``<none>``. These are
        direct state reads; unexpected property errors are not hidden.

        Example:
            >>> driver._append_context_status(sections, browser)  # doctest: +SKIP


        :param sections: Ordered status-section list to extend in place.
        :param browser: Browser supplying current table, page size, and window state.
        :return: ``None``; one Context section is appended.
        """
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
        """
        Append known row counts for the fixed work/expression/manifestation/item/file/store set.

        Query each table independently. Exceptions and ``None`` counts omit that
        table, while a count of zero is retained. No section is added if all counts
        are unavailable.

        Example:
            >>> driver._append_rows_status(sections, browser)  # doctest: +SKIP


        :param sections: Ordered status-section list to extend in place.
        :param browser: Browser providing table row-count queries.
        :return: ``None``; at most one Rows section is appended.
        """
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
        """
        Append job totals grouped by state and any diagnostic from the current listing.

        Dictionary records use their ``state`` entry; other records use an attribute.
        Missing or falsey states become an empty string. State counts are sorted
        lexically, and a Jobs section is emitted even when the listing is empty.

        Example:
            >>> driver._append_jobs_status(sections)  # doctest: +SKIP


        :param sections: Ordered status-section list to receive Jobs and optional Errors records.
        :return: ``None``; listing also refreshes the driver's job-error state.
        """
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
        """
        Append identifiers for active job-output and telemetry panels, if either is enabled.

        Example:
            >>> driver._append_panels_status(sections)  # doctest: +SKIP


        :param sections: Ordered status-section list to extend in place.
        :return: ``None``; a Panels section is added only for truthy panel selections.
        """
        panel_rows: list[tuple[str, object]] = []
        if self._job_output_job_id:
            panel_rows.append(("job_panel", self._job_output_job_id))
        if self._telemetry_tables:
            panel_rows.append(("telemetry_panel", ",".join(self._telemetry_tables)))
        if panel_rows:
            sections.append(("Panels", panel_rows))

    def _append_interaction_status(self, sections: StatusSections) -> None:
        """
        Append applicable pane-focus, scrollback-key, and command-completion hints.

        Focus and job-scrollback hints require an active job panel. Console
        scrollback is shown independently when its offset is positive. A completion
        hint is appended as a bare value after the other interaction entries.

        Example:
            >>> driver._append_interaction_status(sections)  # doctest: +SKIP


        :param sections: Ordered status-section list to extend with a nonempty UI section.
        :return: ``None``; no section is added when no interaction hint applies.
        """
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
