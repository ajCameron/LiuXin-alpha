# FRBR generator build contract

The SQLite FRBR generator builds **new databases**. Its public entry points and
caller-owned connection contract remain unchanged. This change repairs
[FRBR01–FRBR05](database-generator-frbr-review-2026-11-11.md); it does not migrate
an existing database or change PostgreSQL's separate schema generator.

## Compilation and execution

- `database_generator_frbr/vocabulary.py` renders SQL CHECK fragments from the
  canonical metadata enums. The former substitution import remains available
  through `database_generator.py` for existing callers, including PostgreSQL.
- `database_generator_frbr/sql_resources.py` reads immutable resource snapshots.
  SQLite's statement-completeness recognizer preserves quoted semicolons and
  trigger bodies. `-- BREAK` is an ordinary SQL comment; an unmatched marker
  cannot discard a declaration. An incomplete trigger, quote or block comment
  raises with its resource path before that stage executes. SQL syntax errors
  discovered during execution identify the resource and statement number.
- Both table and trigger resources are loaded before the first table DDL in
  `run()`. Resource order is retained; repeated references to the same physical
  file are deduplicated. Aggregate resources use the same loader, with explicit
  files before a sorted recursive scan.
- After the main schema exists, `build_link_plan()` resolves both TOML documents,
  validates constraints, and emits a tuple of frozen `LinkBuildStep` records.
  No relationship table is created until both documents compile. Each step
  captures DDL, copied type labels and the endpoint nullability policy. Execution
  does not reread mutable configuration maps.
- `SQL/link_columns.py` is the single optional-column catalog for both parsers and
  both link emitters. Standard metadata has one definition; bespoke safe names
  become TEXT and cannot duplicate standard or structural columns. `source`
  remains present on every link.

This is staged compilation, not whole-build transactional validation. SQLite may
only discover certain SQL errors while executing a stage. Stages commit, retain
the caller's handle, and can leave earlier work behind on failure. Ordinary
creation still uses the existing [private build and exclusive publication
boundary](creation-and-replacement.md). A borrowed populated connection is not a
migration target.

## Endpoint nullability

Both link request documents now declare `allow_placeholders = true`. It defaults
to true for existing custom specifications, preserving Catalog/DriverWrapper's
scratch-only blank-row construction. Effective endpoint nullability is:

```text
allow_placeholders OR link.nullable
```

For a standalone schema populated with complete INSERTs, set the document's
`allow_placeholders = false`. Each link's `nullable` value then controls physical
NULL/NOT NULL constraints; omitted `nullable` defaults to false. The compiled
step exposes both the requested and effective policy. The shared SQL emitter
honors the resolved flag instead of overriding it. Strict mode is unsuitable for
callers that construct rows with `get_blank_row()`.

## Identifier ownership

A wholly blank identifier placeholder is allowed. Once any ownership, scheme,
value or primary field is populated, the guards require a nonempty scheme/value.
Catalog also supports unassigned candidates: both owner type and ID may be NULL
together. A populated reference requires a known entity type and an existing
owner; half-filled references are rejected. Existing vocabulary CHECKs and
one-primary-per-owner-and-scheme guards remain active for INSERT and UPDATE.
The stored primary flag is constrained to NULL, 0 or 1. Catalog create/update
accept None, booleans and integer 0/1, normalizing booleans to integers; strings,
floats and other integers are rejected before persistence. This keeps Catalog's
primary projection consistent with the schema's uniqueness guards.
Normal Catalog creation fills the placeholder through its existing transaction.
Complete Catalog identifier replacement demotes the old owned primaries inside
that same transaction before promoting replacements, so it never violates the
structural uniqueness rule. A later failure restores the original flags and rows.

For Work, Expression, Manifestation, Item and Agent:

- Deleting the owner deletes its identifiers in the same transaction. Rolling
  back restores both. Equal numeric IDs in other entity namespaces are unaffected.
- Changing a referenced owner key is rejected, including SQLite's rowid, _rowid_
  and oid aliases. Moving a row onto an identified destination with UPDATE OR
  REPLACE is also rejected. Remove or reassign identifiers before intentionally
  changing either identified key; unreferenced keys may change.
- Inserting over an existing identified owner is rejected. This also protects
  `INSERT OR REPLACE` when SQLite's recursive delete triggers are disabled. An
  ignored duplicate insert against such an owner is consequently an error too.

These rules live in schema triggers, so ordinary direct SQL and Catalog writes
share them. They prevent an old identifier from following a reused row ID.
Foreign-key checks alone cannot verify this polymorphic relationship.

## Validation and remaining work

The added regressions exercise all packaged declarations, view preparation,
identifier writes and lifetimes, both registered SQLite Catalog drivers,
rollback and independent readers, folder INSERT/UPDATE guards, redundant-link
admission, resource failure boundaries, immutable plans, strict/default endpoint
policies, and all 1,024 optional-column subsets for each link emitter. The
regression manifest adds 117 cases without removing the prior 1,327, including
direct SQL and Catalog insert/update coverage for the primary flag domain.

The orchestration module still contains legacy TOML normalization and metadata
seeding. Moving that parsing behind a pure schema-catalog input and introducing a
versioned existing-database upgrade path are subsequent work. PostgreSQL's
polymorphic identifier lifecycle needs a separate parity review; these SQLite
repairs are not evidence of PostgreSQL behavior. See the review for executed
validation results and their limits.
