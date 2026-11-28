"""
Write a reviewed metadata plan into the catalogue, one book per transaction.

This is the write half of ingest's metadata step. It takes a plan written by the
dry run, possibly edited by a reviewer, and creates what it describes, so what
was reviewed is what is written. It runs in the process that owns the cache:
each book is one ``metadata.batch``, which rolls back on any failure and
reconciles the cache once.

Before writing a book, every decision is checked against the catalogue as it is
now:

* each file must still be registered in the plan's Store, at the same key, with
  no Item; otherwise the book is skipped;
* a name planned as ``reuse`` must still exist, or the book is skipped;
* a name planned as ``new`` or ``ambiguous`` is looked up again: a unique match
  (often one an earlier book in this run created) is reused, anything else
  creates a new row, keeping the agreed preference for a reviewable duplicate;
* a language that is not seeded is never created, only reported.

A book whose ``action`` is ``skip`` in the plan is left alone. What the catalogue
cannot hold yet (series positions, extra languages, covers) is reported as not
stored for that book.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from LiuXin_alpha.ingest.embedded_metadata.models import (
    DECISION_ERROR,
    DECISION_REUSE,
    FLAG_PARTIAL_DATE,
)
from LiuXin_alpha.ingest.embedded_metadata.planning import NameResolver

ORIGIN = "ingest.embedded_metadata"
ACTION_CREATE = "create"
ACTION_SKIP = "skip"
OUTCOME_CREATED = "created"
OUTCOME_SKIPPED = "skipped"
OUTCOME_FAILED = "failed"
OUTCOME_NOT_ATTEMPTED = "not_attempted"
SAMPLE_SIZE = 10
_FORMAT_LABELS = {
    "azw3": "AZW3",
    "epub": "EPUB",
    "fb2": "FB2",
    "kepub": "KEPUB",
    "mobi": "MOBI",
    "pdf": "PDF",
}

Progress = Callable[[int, int], None]


class PlanError(ValueError):
    """
    Report a plan file that cannot be applied at all.

    Example:
        >>> isinstance(PlanError("bad"), ValueError)
        True
    """


class _SkipBook(Exception):
    """
    Stop one book before anything is written, with the reason to report.

    Example:
        >>> str(_SkipBook("file 3 is attached"))
        'file 3 is attached'
    """


@dataclass(frozen=True, slots=True)
class AppliedBook:
    """
    Record what applying one planned book did.

    Example:
        >>> AppliedBook(1, "Frankenstein", "skipped", reason="marked skip in the plan").to_dict()["outcome"]
        'skipped'


    :ivar index: The book's index in the plan.
    :ivar title: The planned title.
    :ivar outcome: ``created``, ``skipped``, ``failed`` or ``not_attempted``.
    :ivar reason: Why a book was skipped, failed or not attempted.
    :ivar work_id: The Work created.
    :ivar manifestation_ids: One Manifestation per format, primary first.
    :ivar item_ids: One Item per format, in the same order.
    :ivar file_ids: The files attached, in the same order.
    :ivar created: Count of new names created, by kind.
    :ivar not_stored: Values read from the files that the catalogue cannot hold yet.
    :ivar flags: The plan's review flags, carried through.
    :ivar authors: The planned author names, in file order.
    :ivar formats: The planned formats' extensions, primary first.
    """

    index: int
    title: str
    outcome: str
    reason: str = ""
    work_id: int | None = None
    manifestation_ids: tuple[int, ...] = ()
    item_ids: tuple[int, ...] = ()
    file_ids: tuple[int, ...] = ()
    created: Mapping[str, int] = field(default_factory=dict)
    not_stored: tuple[str, ...] = ()
    flags: tuple[str, ...] = ()
    authors: tuple[str, ...] = ()
    formats: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """
        Project the result for a JSON report.

        Example:
            >>> AppliedBook(2, "T", "created", work_id=5).to_dict()["work_id"]
            5


        :return: Every field, with tuples as lists.
        """
        return {
            "index": self.index,
            "title": self.title,
            "outcome": self.outcome,
            "reason": self.reason,
            "work_id": self.work_id,
            "manifestation_ids": list(self.manifestation_ids),
            "item_ids": list(self.item_ids),
            "file_ids": list(self.file_ids),
            "created": dict(self.created),
            "not_stored": list(self.not_stored),
            "flags": list(self.flags),
            "authors": list(self.authors),
            "formats": list(self.formats),
        }


@dataclass(slots=True)
class ApplyReport:
    """
    Collect what applying a plan did, book by book.

    Example:
        >>> ApplyReport(store={"store_id": 1}, plan_path="p.json").summary()["books"]
        0


    :ivar store: The plan's Store.
    :ivar plan_path: Where the plan was read from.
    :ivar books: One result per planned book, in plan order.
    """

    store: dict[str, Any]
    plan_path: str
    books: list[AppliedBook] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        """
        Count outcomes, records created and values not stored.

        Example:
            >>> report = ApplyReport(store={}, plan_path="p.json")
            >>> report.books.append(AppliedBook(1, "T", "created", work_id=1, file_ids=(4,)))
            >>> report.summary()["outcomes"], report.summary()["files_attached"]
            ({'created': 1}, 1)


        :return: Totals suitable for a terminal summary.
        """
        outcomes: dict[str, int] = {}
        created: dict[str, int] = {}
        not_stored: dict[str, int] = {}
        for book in self.books:
            outcomes[book.outcome] = outcomes.get(book.outcome, 0) + 1
            for kind, count in book.created.items():
                created[kind] = created.get(kind, 0) + count
            for value in book.not_stored:
                kind = value.partition(":")[0]
                not_stored[kind] = not_stored.get(kind, 0) + 1
        made = [book for book in self.books if book.outcome == OUTCOME_CREATED]
        return {
            "books": len(self.books),
            "outcomes": dict(sorted(outcomes.items())),
            "works_created": len(made),
            "manifestations_created": sum(len(book.manifestation_ids) for book in made),
            "files_attached": sum(len(book.file_ids) for book in made),
            "names_created": dict(sorted(created.items())),
            "not_stored": dict(sorted(not_stored.items())),
        }

    def to_dict(self) -> dict[str, Any]:
        """
        Project the whole report for a JSON file.

        Example:
            >>> sorted(ApplyReport(store={}, plan_path="p.json").to_dict())
            ['books', 'plan_path', 'store', 'summary']


        :return: Store, plan path, summary and every book's result.
        """
        return {
            "store": dict(self.store),
            "plan_path": self.plan_path,
            "summary": self.summary(),
            "books": [book.to_dict() for book in self.books],
        }


def load_plan(path: str | Path) -> dict[str, Any]:
    """
    Read a dry-run plan file and check it has the shape the apply step needs.

    Example:
        >>> load_plan("ingest-plans/metadata-plan-store-1.json")  # doctest: +SKIP


    :param path: Plan file written by the dry run.
    :return: The parsed plan.
    :raises PlanError: If the file is unreadable, not JSON, not a dry-run plan, or a book lacks its files.
    """
    try:
        plan = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"Cannot read plan {path}: {exc}") from exc
    if not isinstance(plan, dict) or plan.get("dry_run") is not True:
        raise PlanError(f"{path} is not a metadata dry-run plan.")
    store = plan.get("store")
    if not isinstance(store, dict) or not isinstance(store.get("store_id"), int):
        raise PlanError(f"{path} does not name its Store.")
    books = plan.get("books")
    if not isinstance(books, list):
        raise PlanError(f"{path} has no list of books.")
    for book in books:
        if not isinstance(book, dict) or not _plan_file_ids(book):
            raise PlanError(f"{path} has a book without files: {book!r:.120}")
    return plan


def apply_plan(
    plan: Mapping[str, Any],
    *,
    metadata: Any,
    catalog: Any,
    database: Any,
    plan_path: str = "",
    limit: int | None = None,
    progress: Progress | None = None,
) -> ApplyReport:
    """
    Create the planned books, in plan order, each in its own transaction.

    Books after the first ``limit`` created are reported as not attempted, so a
    large plan can be applied in runs; files already attached by an earlier run
    make their books skip.

    Example:
        >>> report = apply_plan(plan, metadata=services.metadata, catalog=services.catalog, database=db)  # doctest: +SKIP
        >>> report.summary()["works_created"]  # doctest: +SKIP
        12


    :param plan: Plan from :func:`load_plan`.
    :param metadata: Cache-aware metadata API whose ``batch`` owns each book's transaction.
    :param catalog: Catalog used for read-only lookups.
    :param database: Database whose macros attach files inside the transaction.
    :param plan_path: Where the plan came from, for the report.
    :param limit: Optional maximum number of books to create.
    :param progress: Optional callback receiving books done and books planned.
    :return: One result per planned book.
    """
    store = dict(plan["store"])
    writer = _BookWriter(
        metadata=metadata, catalog=catalog, database=database, store=store
    )
    report = ApplyReport(store=store, plan_path=plan_path)
    books = list(plan["books"])
    created = 0
    for position, book in enumerate(books, start=1):
        if limit is not None and created >= limit:
            report.books.append(_not_attempted(book))
        else:
            result = writer.apply(book)
            created += result.outcome == OUTCOME_CREATED
            report.books.append(result)
        if progress is not None:
            progress(position, len(books))
    return report


def write_report(report: ApplyReport, path: Path) -> Path:
    """
    Write an apply report as indented JSON, creating the folder if needed.

    Example:
        >>> write_report(ApplyReport(store={}, plan_path="p.json"), tmp_path / "r.json")  # doctest: +SKIP


    :param report: Report to write.
    :param path: Destination file.
    :return: The resolved path written.
    """
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return target.resolve()


def default_report_path(plan_path: str | Path, *, now: datetime | None = None) -> Path:
    """
    Choose a report file beside the plan, named for the plan and time.

    Example:
        >>> from datetime import datetime, timezone
        >>> default_report_path("/p/plan.json", now=datetime(2026, 11, 28, 1, 0, tzinfo=timezone.utc)).as_posix()
        '/p/plan.applied-20261128T010000Z.json'


    :param plan_path: Plan file applied.
    :param now: Time used in the name; defaults to the current UTC time.
    :return: Path next to the plan.
    """
    plan = Path(plan_path).expanduser()
    moment = (now or datetime.now(UTC)).astimezone(UTC)
    return plan.with_name(f"{plan.stem}.applied-{moment:%Y%m%dT%H%M%SZ}.json")


def apply_result(
    report: ApplyReport, report_path: Path, *, include_books: bool = False
) -> dict[str, Any]:
    """
    Build the compact command result: summary, report path and samples.

    A stream asks for every book, so it can show each one as it is added; its
    batches are small.

    Example:
        >>> sorted(apply_result(ApplyReport(store={}, plan_path="p.json"), Path("r.json")))
        ['books_sample', 'plan_path', 'problems_sample', 'report_path', 'store', 'summary']


    :param report: Completed report.
    :param report_path: Where the full report was written.
    :param include_books: Whether to add a ``books`` list with every book's result.
    :return: JSON-ready mapping.
    """
    problems = [
        book
        for book in report.books
        if book.outcome in {OUTCOME_SKIPPED, OUTCOME_FAILED}
    ]
    created = [book for book in report.books if book.outcome == OUTCOME_CREATED]
    result = {
        "store": dict(report.store),
        "plan_path": report.plan_path,
        "summary": report.summary(),
        "report_path": str(report_path),
        "books_sample": [book.to_dict() for book in created[:SAMPLE_SIZE]],
        "problems_sample": [book.to_dict() for book in problems[:SAMPLE_SIZE]],
    }
    if include_books:
        result["books"] = [book.to_dict() for book in report.books]
    return result


class _BookWriter:
    """
    Check and write planned books against one catalogue and Store.

    Example:
        >>> _BookWriter(metadata=api, catalog=catalog, database=db, store={"store_id": 1}).apply(book)  # doctest: +SKIP
    """

    def __init__(
        self, *, metadata: Any, catalog: Any, database: Any, store: Mapping[str, Any]
    ) -> None:
        """
        Bind the write API, lookups, database and the plan's Store.

        Example:
            >>> _BookWriter(metadata=None, catalog=None, database=None, store={"store_id": 1})._store_id
            1


        :param metadata: Cache-aware metadata API.
        :param catalog: Catalog for lookups.
        :param database: Database whose macros run on the transaction's connection.
        :param store: The plan's Store row.
        :return: None.
        """
        self._metadata = metadata
        self._catalog = catalog
        self._database = database
        self._store_id = int(store["store_id"])
        self._store_name = str(store.get("store_name") or "")

    def apply(self, book: Mapping[str, Any]) -> AppliedBook:
        """
        Apply one planned book, or explain why not.

        Example:
            >>> writer.apply({"index": 1, "title": "T", "action": "skip", "formats": []}).outcome  # doctest: +SKIP
            'skipped'


        :param book: Planned book from the plan.
        :return: Created, skipped or failed result; nothing is written unless created.
        """
        action = str(book.get("action") or ACTION_CREATE)
        if action == ACTION_SKIP:
            return _book_result(book, OUTCOME_SKIPPED, reason="marked skip in the plan")
        if action != ACTION_CREATE:
            return _book_result(
                book, OUTCOME_SKIPPED, reason=f"unknown action {action!r}"
            )
        try:
            receipt = self._metadata.batch(lambda api: self._write(api, book))
        except _SkipBook as skip:
            return _book_result(book, OUTCOME_SKIPPED, reason=str(skip))
        except (
            Exception
        ) as exc:  # One book's failure must not stop the run; its batch rolled back.
            return _book_result(
                book, OUTCOME_FAILED, reason=f"{type(exc).__name__}: {exc}"[:300]
            )
        return receipt.result

    def _write(self, api: Any, book: Mapping[str, Any]) -> AppliedBook:
        """
        Check every decision, then write the book's records and attach its files.

        Example:
            >>> writer._write(api, book)  # doctest: +SKIP


        :param api: The batch's metadata API.
        :param book: Planned book.
        :return: The created result.
        :raises _SkipBook: If a file or a reused name has changed since the plan.
        """
        formats = self._checked_formats(book)
        names = _NameWriter(api, self._catalog)
        authors = [
            names.agent(item, organisation=False) for item in _list(book, "authors")
        ]
        publisher = (
            names.agent(book["publisher"], organisation=True)
            if book.get("publisher")
            else None
        )
        tags = [names.tag(item) for item in _list(book, "tags")]
        series = names.series(book["series"]) if book.get("series") else None
        languages = [names.language(item) for item in _list(book, "languages")]
        language_ids = [item for item in languages if item is not None]
        primary = formats[0][0]
        stack = api.mutations.create_wemi_stack(
            work=_work_payload(book, primary),
            expression=_expression_payload(language_ids),
            manifestation=_manifestation_payload(primary),
            items=[_item_payload(primary, self._store_name)],
            origin=ORIGIN,
        ).result
        work_id = int(stack.work_id)
        manifestation_ids = [int(stack.manifestation_id)]
        item_ids = [int(stack.item_ids[0])]
        for facts, _file_id in formats[1:]:
            manifestation_id, item_id = self._add_format(
                api, int(stack.expression_id), facts
            )
            manifestation_ids.append(manifestation_id)
            item_ids.append(item_id)
        self._attach_work_values(api, work_id, authors, tags, series, primary)
        for manifestation_id, (facts, _file_id) in zip(
            manifestation_ids, formats, strict=True
        ):
            self._attach_manifestation_values(api, manifestation_id, publisher, facts)
        file_ids = [file_id for _facts, file_id in formats]
        for file_id, item_id in zip(file_ids, item_ids, strict=True):
            self._database.macros.update_row(
                "files", file_id, {"file_item_id": item_id}
            )
        return _book_result(
            book,
            OUTCOME_CREATED,
            work_id=work_id,
            manifestation_ids=tuple(manifestation_ids),
            item_ids=tuple(item_ids),
            file_ids=tuple(file_ids),
            created=names.created,
            not_stored=_not_stored(book, primary, languages, names.unknown_languages),
        )

    def _checked_formats(
        self, book: Mapping[str, Any]
    ) -> list[tuple[Mapping[str, Any], int]]:
        """
        Confirm every planned file is still registered, unmoved and unattached.

        Example:
            >>> writer._checked_formats(book)  # doctest: +SKIP


        :param book: Planned book.
        :return: ``(format facts, file id)`` pairs, primary first.
        :raises _SkipBook: If any file is missing, moved, in another Store or already attached.
        """
        checked = []
        for facts in _list(book, "formats"):
            planned = dict(facts.get("file") or {})
            file_id = int(planned["file_id"])
            row = self._database.macros.get_row("files", file_id)
            if row is None:
                raise _SkipBook(f"file {file_id} is no longer registered")
            if row.get("file_store_id") != self._store_id:
                raise _SkipBook(
                    f"file {file_id} is no longer in Store {self._store_id}"
                )
            if row.get("file_storage_key") != planned.get("storage_key"):
                raise _SkipBook(f"file {file_id} has moved")
            if row.get("file_item_id") is not None:
                raise _SkipBook(
                    f"file {file_id} is already attached to Item {row['file_item_id']}"
                )
            checked.append((facts, file_id))
        return checked

    def _add_format(
        self, api: Any, expression_id: int, facts: Mapping[str, Any]
    ) -> tuple[int, int]:
        """
        Create another format of the book: a Manifestation under its Expression, and its Item.

        Example:
            >>> writer._add_format(api, 3, facts)  # doctest: +SKIP


        :param api: The batch's metadata API.
        :param expression_id: The book's Expression.
        :param facts: The format's facts.
        :return: The new Manifestation and Item ids.
        """
        manifestation_id = int(
            api.manifestations.create(_manifestation_payload(facts)).result
        )
        api.mutations.link_wemi(
            parent_level="expression",
            parent_id=expression_id,
            child_level="manifestation",
            child_id=manifestation_id,
            primary=False,
            origin=ORIGIN,
        )
        item = {
            **_item_payload(facts, self._store_name),
            "manifestation_id": manifestation_id,
        }
        return manifestation_id, int(api.items.create(item).result)

    def _attach_work_values(
        self,
        api: Any,
        work_id: int,
        authors: Sequence[int],
        tags: Sequence[int],
        series: int | None,
        primary: Mapping[str, Any],
    ) -> None:
        """
        Credit the authors in order, and attach tags, series and description to the Work.

        Example:
            >>> writer._attach_work_values(api, 1, [4], [2], None, facts)  # doctest: +SKIP


        :param api: The batch's metadata API.
        :param work_id: The book's Work.
        :param authors: Agent ids, first author first.
        :param tags: Tag ids.
        :param series: Series id, or None.
        :param primary: The primary format's facts, for the description.
        :return: None.
        """
        if authors:
            # Each new credit is ranked above every existing one and higher ranks
            # list first, so crediting in reverse leaves the first author first.
            credits = [
                {"agent_id": agent_id, "role": "aut"} for agent_id in reversed(authors)
            ]
            api.mutations.attach_metadata(
                level="work", entity_id=work_id, data={"agents": credits}
            )
        for tag_id in tags:
            api.tags.link_to_work(entity_id=tag_id, work_id=work_id)
        if series is not None:
            api.series.link_to_work(entity_id=series, work_id=work_id)
        comments = primary.get("comments")
        if isinstance(comments, str) and comments.strip():
            api.comments.add_for_wemi(level="work", entity_id=work_id, text=comments)

    def _attach_manifestation_values(
        self,
        api: Any,
        manifestation_id: int,
        publisher: int | None,
        facts: Mapping[str, Any],
    ) -> None:
        """
        Credit the publisher and attach the format's own identifiers to its Manifestation.

        Example:
            >>> writer._attach_manifestation_values(api, 5, None, facts)  # doctest: +SKIP


        :param api: The batch's metadata API.
        :param manifestation_id: The format's Manifestation.
        :param publisher: Publisher Agent id, or None.
        :param facts: The format's facts.
        :return: None.
        """
        data: dict[str, Any] = {}
        if publisher is not None:
            data["agents"] = [{"agent_id": publisher, "role": "pbl"}]
        identifiers = dict(facts.get("identifiers") or {})
        if identifiers:
            data["identifiers"] = [
                {"scheme": str(scheme), "value": str(value), "source": ORIGIN}
                for scheme, value in sorted(identifiers.items())
            ]
        if data:
            api.mutations.attach_metadata(
                level="manifestation", entity_id=manifestation_id, data=data
            )


class _NameWriter:
    """
    Resolve a book's planned names to rows, re-checking and creating as the rules say.

    Lookups are never remembered between books, so a name an earlier book
    created is found and reused.

    Example:
        >>> _NameWriter(api, catalog).created  # doctest: +SKIP
        {}
    """

    def __init__(self, api: Any, catalog: Any) -> None:
        """
        Bind the batch's write API and the catalogue's lookups.

        Example:
            >>> _NameWriter(None, None).unknown_languages
            []


        :param api: The batch's metadata API.
        :param catalog: Catalog for lookups.
        :return: None.
        """
        self._api = api
        self._catalog = catalog
        self.created: dict[str, int] = {}
        self.unknown_languages: list[str] = []

    def agent(self, planned: Mapping[str, Any], *, organisation: bool) -> int:
        """
        Resolve an author or publisher, creating a person or organisation when needed.

        Example:
            >>> names.agent({"value": "Mary Shelley", "decision": "new"}, organisation=False)  # doctest: +SKIP


        :param planned: The plan's resolution.
        :param organisation: Create an organisation rather than a person.
        :return: Agent id.
        """
        create = (
            self._api.agents.create_organisation
            if organisation
            else self._api.agents.create_person
        )
        return self._resolve(
            "agents",
            planned,
            exists=self._catalog.agents.get,
            lookup=lambda resolver, value: resolver.agent(value),
            create=lambda value: create({"name": value}).result,
        )

    def tag(self, planned: Mapping[str, Any]) -> int:
        """
        Resolve a tag, creating it when needed.

        Example:
            >>> names.tag({"value": "Gothic", "decision": "reuse", "entity_id": 2})  # doctest: +SKIP


        :param planned: The plan's resolution.
        :return: Tag id.
        """
        return self._resolve(
            "tags",
            planned,
            exists=self._catalog.tags.get,
            lookup=lambda resolver, value: resolver.tag(value),
            create=lambda value: self._api.tags.create({"tag": value}).result,
        )

    def series(self, planned: Mapping[str, Any]) -> int:
        """
        Resolve a series, creating a root series when needed.

        Example:
            >>> names.series({"value": "Barsoom", "decision": "new"})  # doctest: +SKIP


        :param planned: The plan's resolution.
        :return: Series id.
        """
        return self._resolve(
            "series",
            planned,
            exists=self._catalog.series.get,
            lookup=lambda resolver, value: resolver.series(value),
            create=lambda value: self._api.series.create({"name": value}).result,
        )

    def language(self, planned: Mapping[str, Any]) -> int | None:
        """
        Resolve a language; one that is not seeded is noted, never created.

        Example:
            >>> names.language({"value": "zz", "decision": "unknown"})  # doctest: +SKIP


        :param planned: The plan's resolution.
        :return: Language id, or None when not seeded.
        :raises _SkipBook: If a planned language row no longer exists.
        """
        value = str(planned.get("value") or "")
        if (
            planned.get("decision") == DECISION_REUSE
            and planned.get("entity_id") is not None
        ):
            entity_id = int(planned["entity_id"])
            if self._catalog.languages.get(entity_id) is None:
                raise _SkipBook(f"language {entity_id} ({value!r}) no longer exists")
            return entity_id
        current = NameResolver(self._catalog).language(value)
        if current.decision == DECISION_REUSE and current.entity_id is not None:
            return int(current.entity_id)
        self.unknown_languages.append(value)
        return None

    def _resolve(
        self,
        kind: str,
        planned: Mapping[str, Any],
        *,
        exists: Callable[[int], Any],
        lookup: Callable[[NameResolver, str], Any],
        create: Callable[[str], Any],
    ) -> int:
        """
        Apply the re-check rules to one planned name.

        Example:
            >>> names._resolve("tags", planned, exists=get, lookup=find, create=make)  # doctest: +SKIP


        :param kind: Kind of name, used in reasons and counts.
        :param planned: The plan's resolution: value, decision and optional entity id.
        :param exists: Returns the row for an id, or None.
        :param lookup: Looks the name up now with a fresh resolver.
        :param create: Creates a new row for the name and returns its id.
        :return: The row id to link.
        :raises _SkipBook: If a reused row is gone or the lookup fails.
        """
        value = str(planned.get("value") or "").strip()
        if not value:
            raise _SkipBook(f"a planned {kind} name is empty")
        if (
            planned.get("decision") == DECISION_REUSE
            and planned.get("entity_id") is not None
        ):
            entity_id = int(planned["entity_id"])
            if exists(entity_id) is None:
                raise _SkipBook(f"{kind} {entity_id} ({value!r}) no longer exists")
            return entity_id
        current = lookup(NameResolver(self._catalog), value)
        if current.decision == DECISION_REUSE and current.entity_id is not None:
            return int(current.entity_id)
        if current.decision == DECISION_ERROR:
            raise _SkipBook(f"could not look up {kind} {value!r}: {current.reason}")
        self.created[kind] = self.created.get(kind, 0) + 1
        return int(create(value))


def _work_payload(
    book: Mapping[str, Any], primary: Mapping[str, Any]
) -> dict[str, Any]:
    """
    Build the Work's fields from the book's title and the primary format's sort strings.

    Example:
        >>> _work_payload({"title": "The End"}, {"title_sort": "End, The", "author_sort": "Bee, Ann"})
        {'title': 'The End', 'sort_title': 'End, The', 'creator_sort': 'Bee, Ann'}


    :param book: Planned book.
    :param primary: The primary format's facts.
    :return: Work payload.
    """
    payload: dict[str, Any] = {
        "title": str(book.get("title") or primary.get("title") or "")
    }
    if primary.get("title_sort"):
        payload["sort_title"] = str(primary["title_sort"])
    if primary.get("author_sort"):
        payload["creator_sort"] = str(primary["author_sort"])
    return payload


def _expression_payload(language_ids: Sequence[int]) -> dict[str, Any]:
    """
    Build the Expression's fields: a text realization in the first known language.

    Example:
        >>> _expression_payload([124, 7])
        {'label': 'Text', 'language_id': 124}


    :param language_ids: Resolved language ids, first first.
    :return: Expression payload.
    """
    payload: dict[str, Any] = {"label": "Text"}
    if language_ids:
        payload["language_id"] = int(language_ids[0])
    return payload


def _manifestation_payload(facts: Mapping[str, Any]) -> dict[str, Any]:
    """
    Build a format's Manifestation fields, storing only the year of a possibly partial date.

    Example:
        >>> _manifestation_payload({"file": {"extension": "epub"}, "pubdate": "1818-01-01", "notes": []})
        {'carrier_type': 'ebook', 'format_detail': 'EPUB', 'pub_year': 1818, 'pub_date': '1818-01-01'}


    :param facts: The format's facts.
    :return: Manifestation payload.
    """
    extension = str(dict(facts.get("file") or {}).get("extension") or "")
    payload: dict[str, Any] = {
        "carrier_type": "ebook",
        "format_detail": _FORMAT_LABELS.get(extension, extension.upper()),
    }
    pubdate = facts.get("pubdate")
    if isinstance(pubdate, str) and len(pubdate) >= 4 and pubdate[:4].isdigit():
        payload["pub_year"] = int(pubdate[:4])
        if FLAG_PARTIAL_DATE not in (facts.get("notes") or ()):
            payload["pub_date"] = pubdate
    return payload


def _item_payload(facts: Mapping[str, Any], store_name: str) -> dict[str, Any]:
    """
    Build a format's Item fields, recording where the copy came from.

    Example:
        >>> _item_payload({"file": {"storage_key": "a/b.epub"}}, "shelf")["source_path"]
        'a/b.epub'


    :param facts: The format's facts.
    :param store_name: The Store's name.
    :return: Item payload.
    """
    payload: dict[str, Any] = {
        "type": "digital",
        "source": "ingest",
        "source_detail": "embedded metadata",
        "source_path": str(dict(facts.get("file") or {}).get("storage_key") or ""),
    }
    if store_name:
        payload["source_name"] = store_name
    return payload


def _not_stored(
    book: Mapping[str, Any],
    primary: Mapping[str, Any],
    languages: Sequence[int | None],
    unknown_languages: Sequence[str],
) -> tuple[str, ...]:
    """
    List values read from the files that the catalogue has no place for yet.

    Example:
        >>> _not_stored({"series": {"value": "S"}, "series_index": 2.0}, {"cover": {"format": "jpeg", "bytes": 9}}, [1, 2], [])
        ('series_position: 2', 'language: second and later languages', 'cover: jpeg, 9 bytes')


    :param book: Planned book.
    :param primary: The primary format's facts.
    :param languages: Resolved language ids, None where not seeded.
    :param unknown_languages: Codes that are not seeded.
    :return: ``kind: detail`` strings.
    """
    values = []
    position = book.get("series_index")
    if book.get("series") and isinstance(position, (int, float)):
        values.append(f"series_position: {position:g}")
    if len([item for item in languages if item is not None]) > 1:
        values.append("language: second and later languages")
    values.extend(f"language: {code} is not seeded" for code in unknown_languages)
    cover = primary.get("cover")
    if isinstance(cover, Mapping):
        values.append(f"cover: {cover.get('format')}, {cover.get('bytes')} bytes")
    return tuple(values)


def _book_result(book: Mapping[str, Any], outcome: str, **values: Any) -> AppliedBook:
    """
    Build a result carrying the book's index, title, flags, authors and formats.

    Example:
        >>> _book_result({"index": 3, "title": "T", "flags": ["no_authors"]}, "skipped").flags
        ('no_authors',)


    :param book: Planned book.
    :param outcome: Outcome to record.
    :param values: Other AppliedBook fields.
    :return: The result.
    """
    return AppliedBook(
        index=int(book.get("index") or 0),
        title=str(book.get("title") or ""),
        outcome=outcome,
        flags=tuple(str(flag) for flag in book.get("flags") or ()),
        authors=tuple(str(item.get("value") or "") for item in _list(book, "authors")),
        formats=tuple(
            str(dict(item.get("file") or {}).get("extension") or "")
            for item in _list(book, "formats")
        ),
        **values,
    )


def _not_attempted(book: Mapping[str, Any]) -> AppliedBook:
    """
    Record a book left for a later run because the limit was reached.

    Example:
        >>> _not_attempted({"index": 9, "title": "T"}).reason
        'the limit of books to create was reached'


    :param book: Planned book.
    :return: A not-attempted result.
    """
    return _book_result(
        book, OUTCOME_NOT_ATTEMPTED, reason="the limit of books to create was reached"
    )


def _list(book: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    """
    Return a list of mappings from a plan entry, treating absence as empty.

    Example:
        >>> _list({"tags": [{"value": "x"}]}, "tags")
        [{'value': 'x'}]


    :param book: Planned book.
    :param key: Entry name.
    :return: The mappings, in order.
    """
    return [item for item in book.get(key) or () if isinstance(item, Mapping)]


def _plan_file_ids(book: Mapping[str, Any]) -> list[int]:
    """
    Return the file ids a planned book would attach.

    Example:
        >>> _plan_file_ids({"formats": [{"file": {"file_id": 4}}]})
        [4]


    :param book: Planned book.
    :return: File ids, or an empty list when the book lists none.
    """
    ids = []
    for facts in _list(book, "formats"):
        file_id = dict(facts.get("file") or {}).get("file_id")
        if isinstance(file_id, int):
            ids.append(file_id)
    return ids


__all__ = [
    "ACTION_CREATE",
    "ACTION_SKIP",
    "ORIGIN",
    "AppliedBook",
    "ApplyReport",
    "PlanError",
    "apply_plan",
    "apply_result",
    "default_report_path",
    "load_plan",
    "write_report",
]
