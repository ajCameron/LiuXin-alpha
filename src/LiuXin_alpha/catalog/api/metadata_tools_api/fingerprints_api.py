"""Fingerprint API contracts for catalog metadata tools."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, TypeAlias, runtime_checkable

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI

FingerprintSubject: TypeAlias = RowAPI | Mapping[str, Any]


@runtime_checkable
class GenerateBookFingerprintAPI(Protocol):
    """Callable API for book fingerprint generation."""

    def __call__(self, db: DatabaseAPI, book_row: FingerprintSubject) -> set[str]:
        """Generate a fingerprint for a book-like row."""
        ...


@runtime_checkable
class GenerateTitleFingerprintAPI(Protocol):
    """Callable API for title fingerprint generation."""

    def __call__(self, db: DatabaseAPI, title_row: RowAPI) -> set[str]:
        """Generate a fingerprint for a title row and its intralinked titles."""
        ...


@runtime_checkable
class GenerateOneTitleFingerprintAPI(Protocol):
    """Callable API for single title fingerprint generation."""

    def __call__(self, db: DatabaseAPI, title_row: RowAPI) -> set[str]:
        """Generate a fingerprint for one title row."""
        ...


@runtime_checkable
class FingerprintToolsAPI(Protocol):
    """Module-like object exposing metadata fingerprint functions."""

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
