"""Read-only projection-view API contracts for WEMI metadata bundles."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, TypeAlias, runtime_checkable


ProjectionIdentifierMap: TypeAlias = Mapping[str, tuple[str, ...]]


class UnloadedMetadataProjectionError(RuntimeError):
    """Raised when a projection would otherwise omit unloaded lazy data."""

    def __init__(
        self,
        relation_key: str,
        unloaded_dependencies: tuple[str, ...] = (),
    ) -> None:
        self.relation_key = relation_key
        self.unloaded_dependencies = unloaded_dependencies
        detail = ""
        if unloaded_dependencies:
            detail = " Unloaded dependencies: {}.".format(
                ", ".join(unloaded_dependencies)
            )
        super().__init__(
            "Metadata projection {!r} has unloaded lazy data. "
            "Call load({!r}) before reading this projection.{}".format(
                relation_key,
                relation_key,
                detail,
            )
        )


@runtime_checkable
class MetadataValuesViewAPI(Protocol):
    """Structured read-only values projected from metadata relation targets."""

    def relation_values(self, relation_key: str) -> tuple[str, ...]:
        """Return string values projected from one relation-key bucket."""

    @property
    def tags(self) -> tuple[str, ...]:
        """Tag names."""

    @property
    def labels(self) -> tuple[str, ...]:
        """Label text values."""

    @property
    def genres(self) -> tuple[str, ...]:
        """Genre text values."""

    @property
    def subjects(self) -> tuple[str, ...]:
        """Subject text values."""

    @property
    def series(self) -> tuple[str, ...]:
        """Series text values."""

    @property
    def titles(self) -> tuple[str, ...]:
        """Title strings."""

    @property
    def primary_title(self) -> str | None:
        """Preferred title string."""

    @property
    def identifiers(self) -> ProjectionIdentifierMap:
        """Identifier values grouped by scheme."""

    @property
    def languages(self) -> tuple[str, ...]:
        """Language names or codes."""

    @property
    def ratings(self) -> tuple[str, ...]:
        """Rating values."""

    @property
    def agents(self) -> tuple[str, ...]:
        """Agent display names."""

    @property
    def agent_names(self) -> tuple[str, ...]:
        """Agent display names."""


@runtime_checkable
class MetadataTextViewAPI(Protocol):
    """Display/export strings projected from metadata relation targets."""

    def relation_text(self, relation_key: str, separator: str = ", ") -> str:
        """Return display text for one relation-key bucket."""

    @property
    def tags(self) -> str:
        """Tag names joined for display."""

    @property
    def labels(self) -> str:
        """Label text values joined for display."""

    @property
    def genres(self) -> str:
        """Genre text values joined for display."""

    @property
    def subjects(self) -> str:
        """Subject text values joined for display."""

    @property
    def series(self) -> str:
        """Series text values joined for display."""

    @property
    def title(self) -> str | None:
        """Preferred title string."""

    @property
    def titles(self) -> str:
        """Title strings joined for display."""

    @property
    def languages(self) -> str:
        """Language names or codes joined for display."""

    @property
    def ratings(self) -> str:
        """Rating values joined for display."""

    @property
    def agents(self) -> str:
        """Agent display names joined for display."""

    @property
    def agent_names(self) -> str:
        """Agent display names joined for display."""


__all__ = [
    "MetadataTextViewAPI",
    "MetadataValuesViewAPI",
    "ProjectionIdentifierMap",
    "UnloadedMetadataProjectionError",
]
