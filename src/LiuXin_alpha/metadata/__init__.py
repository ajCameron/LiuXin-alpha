"""
Define the passive metadata package boundary.

Separation of concerns: metadata owns passive values, vocabulary, codecs, and pure
normalization and validation; it does not own persistence workflows.

Identifier vocabulary and validation live in ``metadata.identifiers``; MARC
relator vocabulary lives in ``metadata.relators``. Persistence adapters consume
those contracts rather than redefining them.

Import concrete value types, policies, containers, and workflows from their owning
modules. Persistence-free OPF conversion lives in ``LiuXin_alpha.metadata.facade``;
database/cache hydration lives in ``LiuXin_alpha.catalog.retrieval.metadata``.
Keeping this package initializer empty prevents a leaf import such as
``LiuXin_alpha.metadata.identifiers`` from assembling catalog, cache, database, and
container implementations as a side effect.

See ``dev-docs/04 - Seperation of Concerns.md`` and
``dev-docs/metadata_container/metadata_facade_workflows.md``.
"""

from __future__ import annotations

__all__: list[str] = []
