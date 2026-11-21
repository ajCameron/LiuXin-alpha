"""Content-based edit preconditions, separate from cache invalidation generations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from LiuXin_alpha.catalog.api.metadata_search import MetadataReference


@dataclass(frozen=True, slots=True)
class MetadataRevision:
    """Identify the hydrated metadata content observed for an existing WEMI root.

    Tokens compare content, not edit history: changing and restoring the same
    content may produce the same token. Custom fields use their own native field
    revisions. Tokens are scoped to the originating catalogue and API format.
    """

    reference: MetadataReference
    digest: str


@dataclass(frozen=True, slots=True)
class MetadataSnapshot[MetadataT]:
    """Carry detached metadata, public scalar fields and a matching precondition.

    fields includes writable scalar aliases omitted by older native identity
    containers. Omitted patch keys preserve data; these values do not auto-save.
    """

    value: MetadataT
    revision: MetadataRevision
    fields: Mapping[str, object]


class MetadataConflictError(RuntimeError):
    """Reject an edit before mutation because its observed content has changed."""

    def __init__(
        self, expected: MetadataRevision, actual: MetadataRevision | None
    ) -> None:
        super().__init__(f"Metadata changed since it was read: {expected.reference}")
        self.expected = expected
        self.actual = actual
