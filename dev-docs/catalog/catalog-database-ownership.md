# Catalog behaviour and database persistence

Decision: **2026-10-29**. Physical-table SQL boundary clarified: **2026-10-30**.

Consolidate bibliographic behaviour under Catalog while keeping executable SQL
in `databases`. These are complementary ownership rules:

```text
Caller -> Catalog operation -> database table SQL / portable macros -> SQL driver
```

Catalog decides which WEMI entity owns a value, validates bibliographic input,
chooses relationship and replacement semantics, and groups changes that must
succeed together. The database implements that transaction and owns SQL,
bindings, schema checks, backend differences, joins and bulk persistence.
Pure value rules shared by these layers belong in `metadata`.

A database helper must not import Catalog to forward a semantic operation
upwards. Migrate its caller to the Catalog owner instead. Catalog must not gain
embedded SQL when a legacy helper is moved. Concrete table queries, joins and
SQL accelerators belong in databases.
`metadata_sql` can expose table-scoped routines, using portable macros where
applicable; specialized SQL must use real columns, bound inputs, shared
transactions and the supported backend contract. The singular title/book facade
and projection-ray interpretation remain Catalog responsibilities.

## Why `metadata_sql` needed splitting

Before this migration, the review found 24 mixins exposing 85 methods, with
several different histories:

- Legacy title/book writes target compatibility views that cannot be updated.
- Identifier setters write `entity_identifiers`, while readers and removers
  still expect old identifier link tables. The setter also bypasses Catalog's
  checksum validation.
- Catalog's primary-language Row adapter calls a removed language/title table.
- Generic deletion duplicates database primitives and bypasses their shared
  transaction boundary.
- File, feed and other compatibility helpers have independent schema and
  transaction problems. For example, a feed insertion can survive rollback of
  an enclosing portable transaction.

These failures were reproduced on fresh SQLite and APSW databases. This is a
review of the callable surface, not evidence that every legacy path is used by
the supported CLI.

## Migration order

1. Consolidate identifier reads and writes in `IdentifierRepository`, including
   compatibility callers. Share scheme/value normalization with `metadata`.
2. Route primary-language assignment to the Work repository's original-language
   field, preserving Expression languages as a separate concept.
3. Move generic link deletion to validated portable macros, migrate callers,
   and remove unused legacy row-deletion entry points.
4. Retire the remaining bibliographic helpers as their compatibility callers
   are migrated. Recorded storage state belongs to storage services backed by
   database primitives; feed persistence needs its own explicit owner decision.

## Implemented on 2026-10-29

Steps 1–3 are implemented:

- `catalog.identifiers.set_primary_for_wemi()` validates values, reuses
  equivalent owned rows, canonicalizes the selected scheme, and retains
  alternatives. `clear_for_wemi()` removes an owner's values or one scheme;
  generic ISBN clearing covers both lengths. `list_for_level()` supports
  inventory reads without a legacy identifier join.
- The legacy identifier-cache adapter uses that repository. Its multi-value
  replacement and batch updates share one portable transaction. This does not
  restore the entire legacy Calibre cache.
- `catalog.works.set_original_language()` validates the Work and Language.
  `catalog.apply.primary_language()` retains the Row-shaped interface and
  delegates to that owner. Expression languages are preserved.
- `db.macros.delete_link_rows()` and `delete_link_pair()` own generic link
  deletion. They validate schema identifiers, bind values and compose with
  outer transactions. The older one/two-column deletion macros also delegate
  to portable primitives instead of executing their own SQL.
- Five obsolete mixin modules and twelve `MetadataSQL` entry points were
  removed, including unused legacy row-deletion helpers. Generic row deletion
  remains available through `db.macros.delete_row()`.

The dependency guard permits the four existing Catalog imports for version
metadata and custom-column metadata; their ownership is separate remaining
work. It rejects new upward imports, including semantic forwarding adapters.

At this intermediate stage retirement was partial. The remaining object was
not a supported route for new bibliographic behaviour. The physical-table
boundary below clarifies which SQL helpers remain appropriate.

The same-day [WEMI audit](../databases/metadata-sql-wemi-audit-2026-10-29.md)
classified the then-remaining 73 methods individually. It fixes the feed transaction
escape described above, current compatibility-file columns and batch deletion,
tag deletion transactions and canonical Series lookup. At that stage sixty methods retained
explicit legacy flags pending caller/semantic migration. These include ray-ID
versus Work-ID confusion, missing Series position and unsupported Agent/Series
tag links; see that inventory before reusing any surviving helper.

The follow-up [Expression conversion policy](../calibre_compat/expression-conversion-policy.md)
now resolves explicit compatibility references under `catalog.calibre`. It
migrates six field families in the cache persistence hooks and retires three
uncalled SQL entry points. On **2026-10-30**, removal of another 48 methods
referencing missing tables left 22 methods, including 9 legacy flags. Eleven
empty mixins and fifteen unused frozen helpers were removed. Affected legacy
cache hooks now refuse unsupported operations before changing their maps; the
Work–Series hook uses current memberships without an obsolete index lookup.
The full old cache remained unsupported; the audit recorded view, column and
conversion-option defects for the next slice.

## Physical-table boundary on 2026-10-30

`db.metadata_sql` and `MetadataSQLAPI` expose 62 physical-table operations:
file-row updates/deletes, auxiliary feed CRUD/replacement, canonical Tag/Series
lookup/ensure/delete, scoped normalization audit/repair, relationship destinations,
foreign-key owner groups and explicit policy references. Their 38 mixins
share portable transactions and existing normalization declarations. Vocabulary
batches reduce key lookups; generic update/delete batches and data-only key
repair live in portable macros. See [batch contracts and examples](../databases/table-sql-batches.md).

The [table-owned SQL convention](../databases/table-sql-ownership.md) assigns
specialised reads to the table whose rows they return. Works, Expressions and
Manifestations and other destination-table helpers serve Catalog relationship
APIs through bounded reads. Item/identifier/annotation readers group by owner;
Core policy scans and backup enumeration batch their references as well. Catalog retains source validation, missing-row
errors and link presentation; it still owns interpretation and selection.

`CMTitlesMacrosMixin`, book/title/ray facades, obsolete path columns and unresolved
conversion helpers remain removed. Existing WEMI repositories and `catalog.calibre`
own title behavior and projection policy. Tag/Series repositories own semantic
matching and memberships; the SQL helper only addresses the vocabulary rows.
The unused `catalog.catalog_macros` facade also remains removed.

Cover flags and legacy format removal explicitly refuse unsupported writes
before side effects. Missing asset/replica selection, Series positions, relocation
and conversion policies remain [CAT06 work](next_tasks.md#cat06-replace-the-retired-metadata-sql-capabilities).
The [audit owner map](../databases/metadata-sql-wemi-audit-2026-10-29.md#physical-table-sql-boundary--2026-10-30)
records supported calls, a transactional feed recipe and cache limitations.

## Required checks

Exercise semantic validation, WEMI ownership isolation, equivalent identifiers,
and rollback through the public Catalog entry points. Exercise generic deletion
with real schemas, typed links, batch inputs and outer rollback on both SQLite
drivers. Retain the Catalog executable-SQL ban and guard against new upward
imports from databases to Catalog.

Verification on **2026-10-29**: 148 distinct tests passed across the broader
regression run and the corrected follow-up; one existing PostgreSQL-shaped
SQLite harness test was skipped because that harness has no `pg_temp` schema.
The final ownership, deletion and aggregate run passed all 33 tests on SQLite
and APSW. No live PostgreSQL run was performed for this change.

Coverage includes [semantic ownership](../../tests/catalog/test_metadata_sql_ownership.py),
[portable deletion](../../tests/databases/api/test_portable_link_deletion.py),
aggregate mutations, repository/API contracts, identifier normalization, writer
imports, and the executable-SQL and legacy-import boundaries. The follow-up
corrected test fixtures that had supplied empty records or assigned ISBNs at
WEMI levels where the schema disallows them.

See [legacy migration](catalog-legacy-mutation-migration.md),
[write validation](catalog-write-validation-contract.md), and
[API usage](catalog-api-usage.md).
