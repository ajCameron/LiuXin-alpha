"""Fingerprint API contracts for catalog metadata tools."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, TypeAlias, runtime_checkable

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI

FingerprintSubject: TypeAlias = RowAPI | Mapping[str, Any]


@runtime_checkable
class GenerateBookFingerprintAPI(Protocol):
    """Generate comparison tokens from a compatibility book row or mapping.

    The database supplies linked title/creator context needed by the generator.
    """

    def __call__(self, db: DatabaseAPI, book_row: FingerprintSubject) -> set[str]:
        """Return normalized identity tokens for a book-like row/mapping.

        Fingerprints are comparison aids, not durable cryptographic hashes.
        """
        ...


@runtime_checkable
class GenerateTitleFingerprintAPI(Protocol):
    """Generate comparison tokens for a title and its intralinked title family.

    Use the single-title callable when related title variants must be excluded.
    """

    def __call__(self, db: DatabaseAPI, title_row: RowAPI) -> set[str]:
        """Return normalized tokens from a title and its intralinked titles."""
        ...


@runtime_checkable
class GenerateOneTitleFingerprintAPI(Protocol):
    """Generate comparison tokens for exactly one title row.

    This callable does not include aliases or other intralinked title variants.
    """

    def __call__(self, db: DatabaseAPI, title_row: RowAPI) -> set[str]:
        """Return normalized comparison tokens for exactly one title row."""
        ...


@runtime_checkable
class FingerprintToolsAPI(Protocol):
    """Module-like group of compatibility fingerprint callables.

    Fingerprints are normalized token sets used as matching evidence, not
    cryptographic digests or stable public identifiers.

    Example::

        tokens = fingerprint_tools.generate_book_fingerprint(db, book_row)
        if "frankenstein" in tokens:
            ...
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
