
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

Separation of concerns: every public Catalog mutation establishes its own
semantic preconditions before persistence. It invokes persistence-free metadata
normalizers and validators, then applies operation, relationship, and transaction
rules. It does not trust surfaces, declarations, annotations, or earlier workflow
steps as proof of runtime validity. Database constraints remain the final
structural and concurrency-safe guard. Direct driver and raw-macro calls are
outside this semantic-validity guarantee.

Enforcement is being completed repository by repository. Curated identifier
generic CRUD uses the shared write-preparation seam; other semantic repositories
must not be assumed complete merely because the boundary is documented.

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
The public write boundary comprises repository CRUD and specialised mutations,
``mutations.writer`` operations, and the advanced schema-driven
``create_writer``/``write``/``write_one`` trio. ``add``/``ensure``/``apply``/
``intralink`` remain supported compatibility routes but are deprecated for new
code. Prepared-update applicators are private writer-host seams. ``catalog.db``,
database drivers, and raw macros are low-level escape hatches outside the
semantic-validity guarantee.

See dev-docs/04 - Seperation of Concerns.md,
dev-docs/catalog-api-usage.md, dev-docs/catalog-write-validation-contract.md,
and examples/catalog for complete examples and the dated enforcement status.
"""


from LiuXin_alpha.catalog.catalog import Catalog

__all__ = ["Catalog"]
