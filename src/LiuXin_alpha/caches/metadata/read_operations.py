"""Execute composed metadata reads wholly against one selected source."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Literal

from LiuXin_alpha.caches.api import CacheConsistency
from LiuXin_alpha.caches.metadata.reads import UNAVAILABLE, MetadataReadSource

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata.service import MetadataAPI


def complete_read[ResultT](
    owner: MetadataAPI, operation: Callable[[MetadataReadSource], ResultT]
) -> ResultT:
    """Discard an unavailable cache attempt and rerun the entire read on fallback.

    Each attempt uses one capture. Dirty state, corruption and confirmed misses
    are never fallback triggers. Owned temporary captures close on every exit.
    """
    with owner._reading():
        captured = None
        try:
            source = owner._reads()
            source.allow_database_fallback = False
            captured = source.cache if source.cache is not owner._cache else None
            return operation(source)
        except UNAVAILABLE:
            if owner._cache is None or not owner._fallback:
                raise
        finally:
            if captured is not None:
                captured.close()
        return operation(
            MetadataReadSource(owner._database, None, allow_database_fallback=False)
        )


def provenance(
    owner: MetadataAPI, source: MetadataReadSource
) -> tuple[Literal["cache", "database"], Literal["snapshot", "live"], int | None]:
    """Report the actual selected source and the owning cache's consistency."""
    cache = owner._cache
    if source.cache is None or cache is None:
        return "database", "live", None
    consistency: Literal["snapshot", "live"] = (
        "snapshot"
        if cache.capabilities.consistency == CacheConsistency.SNAPSHOT
        else "live"
    )
    return "cache", consistency, cache.generation
