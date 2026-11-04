"""Declared backend facilities and limits for portable database consumers."""

from dataclasses import dataclass
from enum import StrEnum


class BackendFeature(StrEnum):
    """Facilities callers may require before starting an operation."""

    SAVEPOINTS = "savepoints"
    READ_SNAPSHOTS = "read_snapshots"
    RETURNING = "returning"
    CUSTOM_COLUMN_DDL = "custom_column_ddl"
    PROGRESS_CALLBACKS = "progress_callbacks"
    PHYSICAL_SCHEMA = "physical_schema"


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    """A live backend's supported facilities and per-statement binding limit.

    None means no finite binding limit is advertised by this adapter. It does
    not promise unlimited statement size or memory. Version is informational.
    """

    backend: str
    version: str
    features: frozenset[BackendFeature]
    max_bind_parameters: int | None

    def supports(self, feature: BackendFeature) -> bool:
        """Return whether this adapter provides the requested facility."""
        return feature in self.features

    def require(self, feature: BackendFeature) -> None:
        """Reject an unsupported facility before the caller begins its work."""
        if not self.supports(feature):
            raise NotImplementedError(
                f"{self.backend} does not support {feature.value}"
            )
