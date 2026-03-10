"""Grouped `show` commands for viewing linked metadata."""

from __future__ import annotations

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.interfaces.terminal.commands.link import _split_row_ref


def _safe_int(value: str):
    try:
        return int(value)
    except Exception:
        return None


def _looks_like_id_selector(token: str) -> bool:
    text = str(token).strip()
    if not text:
        return False
    parts = text.split(",")
    if not parts:
        return False
    for raw_part in parts:
        part = raw_part.strip()
        if not part:
            return False
        if "-" in part:
            left, right = part.split("-", 1)
            if _safe_int(left.strip()) is None or _safe_int(right.strip()) is None:
                return False
        else:
            if _safe_int(part) is None:
                return False
    return True


def resolve_show_kind_table(browser, token: str) -> str:
    """Resolve a show-kind token to a concrete table name."""
    text = str(token).strip().lower()
    if not text:
        raise ValueError("Linked kind/table cannot be blank.")

    tables = set(browser.db.get_tables())

    if text in {"tag", "tags", "label", "labels"}:
        if "labels" in tables:
            return "labels"
        if "tags" in tables:
            return "tags"

    aliases = {
        "note": "notes",
        "notes": "notes",
        "genre": "genres",
        "genres": "genres",
        "subject": "subjects",
        "subjects": "subjects",
        "language": "languages",
        "languages": "languages",
        "lang": "languages",
        "series": "series",
    }
    alias_target = aliases.get(text)
    if alias_target in tables:
        return alias_target

    try:
        return browser.resolve_table(text)
    except Exception:
        raise ValueError(
            "Unknown linked kind/table {!r}. Try: tags, notes, genres, subjects, language, series, or all.".format(
                token
            )
        )


def _parse_target_row(browser, args: list[str], *, usage: str):
    """Parse target as either `<table:id>` or `<table> <id>`."""
    if not args:
        raise ValueError("Usage: {}".format(usage))

    if len(args) == 1:
        compact_candidate = str(args[0]).strip()
        if ":" in compact_candidate:
            table_token, id_token = compact_candidate.rsplit(":", 1)
            if table_token.strip() and _looks_like_id_selector(id_token) and _safe_int(id_token) is None:
                raise ValueError(
                    "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
                )

    compact = _split_row_ref(args[0])
    if compact is not None:
        if len(args) != 1:
            raise ValueError("Usage: {}".format(usage))
        table_token, id_token = compact
    else:
        if len(args) == 2 and _looks_like_id_selector(args[1]) and _safe_int(args[1]) is None:
            raise ValueError(
                "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
            )
        if len(args) != 2:
            raise ValueError("Usage: {}".format(usage))
        table_token, id_token = args[0], args[1]

    target_table = browser.resolve_table(table_token)
    target_id = _safe_int(id_token)
    if target_id is None:
        if _looks_like_id_selector(id_token):
            raise ValueError(
                "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
            )
        raise ValueError("Row id must be an integer.")

    target_row = browser.db.get_row_from_id(target_table, target_id)
    if target_row is None:
        raise ValueError("No row found in {} for id {}.".format(target_table, target_id))

    return target_table, target_id, target_row


def _get_linked_rows(browser, *, target_table: str, target_row, linked_table: str):
    link_table = browser.db.driver_wrapper.get_link_table_name(linked_table, target_table)
    if not link_table:
        raise ValueError("No link table exists between {} and {}.".format(linked_table, target_table))
    return browser.db.get_interlinked_rows(target_row=target_row, secondary_table=linked_table)


def _render_default_rows(browser, *, target_table: str, target_id: int, linked_table: str, rows):
    if not rows:
        browser.emit("No linked {} for {}:{}.".format(linked_table, target_table, target_id))
        return
    browser.emit("Linked {} for {}:{}: {}".format(linked_table, target_table, target_id, len(rows)))
    for row in rows[: browser.page_size]:
        browser.emit("  {}".format(browser.format_row(linked_table, row)))
    if len(rows) > browser.page_size:
        browser.emit("  ... {} more".format(len(rows) - browser.page_size))


class _ShowLinkedBaseCommand(TerminalCommandAPI):
    """Base class for `show <kind> ...` commands."""

    group = "show"
    expose_direct = False
    linked_table_token = ""

    def execute(self, browser, args: list[str]) -> bool:
        target_table, target_id, target_row = _parse_target_row(browser, args, usage=self.usage)
        linked_table = resolve_show_kind_table(browser, self.linked_table_token)
        rows = _get_linked_rows(
            browser,
            target_table=target_table,
            target_row=target_row,
            linked_table=linked_table,
        )
        self.render_rows(
            browser,
            target_table=target_table,
            target_id=target_id,
            linked_table=linked_table,
            rows=rows,
        )
        return True

    def render_rows(self, browser, *, target_table: str, target_id: int, linked_table: str, rows) -> None:
        _render_default_rows(
            browser,
            target_table=target_table,
            target_id=target_id,
            linked_table=linked_table,
            rows=rows,
        )


class ShowTagsCommand(_ShowLinkedBaseCommand):
    """Show tags/labels linked to a target row."""

    name = "tags"
    aliases = ("tag", "labels", "label")
    summary = "Show tags: show tags <table:id>"
    usage = "show tags <table:id> OR show tags <table> <id>"
    linked_table_token = "tags"

    def render_rows(self, browser, *, target_table: str, target_id: int, linked_table: str, rows) -> None:
        if not rows:
            browser.emit("No linked tags for {}:{}.".format(target_table, target_id))
            return

        labels: list[str] = []
        for row in rows:
            text = ""
            if "label_text" in row and row["label_text"] is not None:
                text = str(row["label_text"]).strip()
            elif "label" in row and row["label"] is not None:
                text = str(row["label"]).strip()
            elif "tag" in row and row["tag"] is not None:
                text = str(row["tag"]).strip()
            if text:
                labels.append(text)

        if not labels:
            _render_default_rows(
                browser,
                target_table=target_table,
                target_id=target_id,
                linked_table=linked_table,
                rows=rows,
            )
            return

        unique_labels: list[str] = []
        seen: set[str] = set()
        for label in labels:
            key = label.strip().lower()
            if key in seen:
                continue
            seen.add(key)
            unique_labels.append(label)

        browser.emit("Tags for {}:{} ({})".format(target_table, target_id, len(unique_labels)))
        for label in unique_labels[: browser.page_size]:
            browser.emit("  - {}".format(label))
        if len(unique_labels) > browser.page_size:
            browser.emit("  ... {} more".format(len(unique_labels) - browser.page_size))


class ShowNotesCommand(_ShowLinkedBaseCommand):
    """Show notes linked to a target row."""

    name = "notes"
    aliases = ("note",)
    summary = "Show notes: show notes <table:id>"
    usage = "show notes <table:id> OR show notes <table> <id>"
    linked_table_token = "notes"


class ShowGenresCommand(_ShowLinkedBaseCommand):
    """Show genres linked to a target row."""

    name = "genres"
    aliases = ("genre",)
    summary = "Show genres: show genres <table:id>"
    usage = "show genres <table:id> OR show genres <table> <id>"
    linked_table_token = "genres"


class ShowSubjectsCommand(_ShowLinkedBaseCommand):
    """Show subjects linked to a target row."""

    name = "subjects"
    aliases = ("subject",)
    summary = "Show subjects: show subjects <table:id>"
    usage = "show subjects <table:id> OR show subjects <table> <id>"
    linked_table_token = "subjects"


class ShowLanguageCommand(_ShowLinkedBaseCommand):
    """Show languages linked to a target row."""

    name = "language"
    aliases = ("languages", "lang")
    summary = "Show language(s): show language <table:id>"
    usage = "show language <table:id> OR show language <table> <id>"
    linked_table_token = "languages"


class ShowSeriesCommand(_ShowLinkedBaseCommand):
    """Show series linked to a target row."""

    name = "series"
    aliases = ()
    summary = "Show series: show series <table:id>"
    usage = "show series <table:id> OR show series <table> <id>"
    linked_table_token = "series"


class ShowAllCommand(TerminalCommandAPI):
    """Show all linked rows for a target row."""

    group = "show"
    expose_direct = False
    name = "all"
    aliases = ("*",)
    summary = "Show all links: show all <table:id>"
    usage = "show all <table:id> OR show all <table> <id>"

    def execute(self, browser, args: list[str]) -> bool:
        target_table, target_id, target_row = _parse_target_row(browser, args, usage=self.usage)
        candidate_tables = sorted(browser.db.driver_wrapper.get_interlinked_tables(target_table))
        shown_any = False
        for linked_table in candidate_tables:
            if linked_table == target_table:
                continue
            try:
                rows = browser.db.get_interlinked_rows(target_row=target_row, secondary_table=linked_table)
            except Exception:
                continue
            if not rows:
                continue
            shown_any = True
            _render_default_rows(
                browser,
                target_table=target_table,
                target_id=target_id,
                linked_table=linked_table,
                rows=rows,
            )
        if not shown_any:
            browser.emit("No linked rows for {}:{}.".format(target_table, target_id))
        return True
