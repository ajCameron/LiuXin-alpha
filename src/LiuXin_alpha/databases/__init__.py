"""
Provide generic persistence services, schemas, constraints, and contracts.

Use ordinary submodule imports; importing this namespace loads no implementations.

Separation of concerns: databases own connections, drivers, SQL, migrations,
transactions, generic rows and links, and structural integrity through types,
nullability, foreign keys, uniqueness, and stable checks. They are the
authoritative record of persisted state, but do not own bibliographic meaning,
normalization, matching, or merge policy. Metadata defines pure value rules and
Catalog guarantees semantic public mutations before calling persistence.
Database schema generators import identifier and relator vocabularies from their
metadata owners; ``databases.db_types`` is limited to persistence-oriented aliases
and protocols.

Raw Database writes perform no general metadata-validity check. They may accept
values that satisfy storage representation and constraints while being invalid
under current metadata policy. Use Catalog repositories or
``catalog.mutations.writer`` for supported metadata changes.

Stored validation-profile labels describe writer policy; they are not proof that
a value was validated. Direct database and raw-macro writes remain intentionally
outside the Catalog semantic-validity guarantee.

See ``dev-docs/04 - Seperation of Concerns.md`` and
``dev-docs/catalog-write-validation-contract.md``.
"""
