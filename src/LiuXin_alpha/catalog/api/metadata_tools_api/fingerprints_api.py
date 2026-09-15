"""
Describe best-effort legacy relationship fingerprint callables.
"""

from __future__ import annotations

from typing import Any, Mapping, Protocol, TypeAlias, runtime_checkable

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI

FingerprintSubject: TypeAlias = RowAPI | Mapping[str, Any]


@runtime_checkable
class GenerateBookFingerprintAPI(Protocol):
    """
    Union book-linked tokens with the resolved title family's tokens.

    Fallback to book_id occurs only when book_title is None. A missing title
    yields an empty starting set. Book-side reads exclude books and titles;
    per-table discovery/retrieval errors are skipped. Title lookup, initial
    title fingerprinting, and iteration of retrieved rows can still raise.

    Example:
        A book with no title but a linked tag with ID 7 can produce {"tags_7"}.
    """

    def __call__(self, db: DatabaseAPI, book_row: FingerprintSubject) -> set[str]:
        """
        Union book-linked tokens with the resolved title family's tokens.

        Fallback to book_id occurs only when book_title is None. A missing title
        yields an empty starting set. Book-side reads exclude books and titles;
        per-table discovery/retrieval errors are skipped. Title lookup, initial
        title fingerprinting, and iteration of retrieved rows can still raise.

        Example:
            A book with no title but a linked tag with ID 7 can produce {"tags_7"}.


        :param db: Database exposing legacy books/titles and relationship helpers.
        :param book_row: Book row or mapping; book_title is preferred over book_id.
        :return: Set of table_ID relationship tokens; an incomplete set is possible.
        """
        ...


@runtime_checkable
class GenerateTitleFingerprintAPI(Protocol):
    """
    Union a title's tokens with its immediate intralinked titles' tokens.

    Start with the title's own fingerprint. If title intralinks exist, visit
    both directions. Each directional loop suppresses Exception, retaining
    already unioned results; the initial title fingerprint and intralink-table
    capability check can still raise.

    Example:
        A translation linked directly to the title can contribute its linked
        metadata IDs; a translation-of-translation is not recursively traversed.
    """

    def __call__(self, db: DatabaseAPI, title_row: RowAPI) -> set[str]:
        """
        Union a title's tokens with its immediate intralinked titles' tokens.

        Start with the title's own fingerprint. If title intralinks exist, visit
        both directions. Each directional loop suppresses Exception, retaining
        already unioned results; the initial title fingerprint and intralink-table
        capability check can still raise.

        Example:
            A translation linked directly to the title can contribute its linked
            metadata IDs; a translation-of-translation is not recursively traversed.


        :param db: Database providing relationship discovery and retrieval.
        :param title_row: Title row whose direct and adjacent relationships are inspected.
        :return: Set of table_ID tokens; no recursive traversal or title-text normalization.
        """
        ...


@runtime_checkable
class GenerateOneTitleFingerprintAPI(Protocol):
    """
    Collect table_ID tokens for relationships of one legacy title.

    Link discovery/retrieval failures are skipped per table. Iteration and
    row_id access happen outside that exception handler and may still fail.
    Books are included when a link route exists. This does not follow title
    intralinks or hash the title's text.

    Example:
        A linked tag row with ID 7 contributes ``tags_7``.
    """

    def __call__(self, db: DatabaseAPI, title_row: RowAPI) -> set[str]:
        """
        Collect table_ID tokens for relationships of one legacy title.

        Link discovery/retrieval failures are skipped per table. Iteration and
        row_id access happen outside that exception handler and may still fail.
        Books are included when a link route exists. This does not follow title
        intralinks or hash the title's text.

        Example:
            A linked tag row with ID 7 contributes ``tags_7``.


        :param db: Database exposing main_tables, link discovery and row retrieval.
        :param title_row: Title row used as the primary endpoint.
        :return: Set of tokens from linked rows, excluding the titles table itself.
        """
        ...


@runtime_checkable
class FingerprintToolsAPI(Protocol):
    """
    Group callables producing table_ID relationship token sets.

    These are database-local comparison aids. They are neither content hashes
    nor normalized title/name tokens and may omit failed relationship reads.

    Example:
        Compare the intersection of two token sets from the same database; do not
        search them for a title word such as frankenstein.
    """

    generate_book_fingerprint: GenerateBookFingerprintAPI
    generate_title_fingerprint: GenerateTitleFingerprintAPI
    generate_one_title_fingerprint: GenerateOneTitleFingerprintAPI


__all__ = [
    "FingerprintSubject",
    "FingerprintToolsAPI",
    "GenerateBookFingerprintAPI",
    "GenerateOneTitleFingerprintAPI",
    "GenerateTitleFingerprintAPI",
]
