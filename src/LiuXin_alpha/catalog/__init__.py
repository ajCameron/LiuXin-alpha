
# Todo: These need to be mirror'd/moved to dev docs?
"""
Expose the concrete metadata-aware Catalog facade over a LiuXin database.

Catalog sits between raw database persistence and application/library workflows.
Its repositories combine row operations with Work, Expression, Manifestation,
Item, Agent, identifier, and related metadata semantics. Matching explains
identity decisions; retrieval assembles display-neutral WEMI views; mutations
coordinate semantic writes. Database drivers own SQL and transaction mechanics,
storage owners manage bytes and replicas, and application owners combine those
services into ingest and other user workflows.

This package exports Catalog. Import candidate/result values and structural
contracts from LiuXin_alpha.catalog.api. Supply an existing database and retain
responsibility for closing it; the facade provides no close/context-manager API.
Repository shortcuts and catalog.repositories refer to the same instances.

Example:
    >>> from LiuXin_alpha.catalog.api import MetadataCandidate
    >>> catalog = Catalog(db)  # doctest: +SKIP
    >>> work_id = catalog.works.match_or_create(  # doctest: +SKIP
    ...     MetadataCandidate({"title": "Frankenstein", "original_year": 1818}),
    ... )
    >>> work = catalog.works.require(work_id)  # doctest: +SKIP
    >>> bundle = catalog.retrieval.bundles.for_item(item_id)  # doctest: +SKIP

For an explained decision without creating an entity, use the corresponding
catalog.matching service. MatchResult distinguishes match, no_match, ambiguous,
and conflict; a missing selected ID alone is not permission to create. Repository
match_or_create methods apply that distinction and raise on unresolved outcomes.
The generic create_writer/write/write_one methods expose schema-driven field
updates, while add/ensure/apply/intralink retain legacy database Row workflows.

See dev-docs/catalog-api-usage.md and examples/catalog for complete examples.
"""


from LiuXin_alpha.catalog.catalog import Catalog

__all__ = ["Catalog"]
