# Catalog write-validation contract

Decision date: **2026-10-11**

Status: **adopted architectural contract; enforcement is incomplete**

## Decision

Every public Catalog mutation must guarantee that the values and operation it
passes to persistence are valid under the metadata and Catalog rules known at
the time of the write. A Catalog implementation must not assume that its
caller, a surface, or an earlier workflow has already performed validation.

This is a guarantee made by the Catalog boundary, not a transfer of all
validation logic into `catalog`:

| Layer | Responsibility |
|---|---|
| `metadata` | Pure normalization and validation of metadata values, such as identifier syntax and checksums, language codes, and title or author value rules. These functions do not access persistence. |
| `catalog` | Invoke the metadata validators and enforce operation-level rules: required combinations, relationship validity, cardinality, allowed transitions, and transaction scope. |
| `databases` | Enforce storage integrity through database types, nullability, foreign keys, uniqueness, stable `CHECK` constraints, and transactions. It does not define bibliographic meaning. |

The intended mutation flow is:

```text
untrusted input
    -> metadata normalization and value validation
    -> Catalog operation and relationship validation
    -> repository storage adaptation
    -> database constraints and transaction
```

Normalization is not by itself validation. A function that cleans an
identifier scheme or canonicalizes a language value does not prove that the
result is semantically acceptable for the requested mutation.

## Supported public write boundary

The Catalog semantic-validity guarantee applies to these supported routes:

| Route | Status and intended use |
|---|---|
| `catalog.mutations.writer.*` | Preferred coordinated semantic mutations, including WEMI creation/linking, metadata attachment/replacement, and merges. |
| Public repository methods | Entity CRUD and specialised repository mutations. Semantic repositories must use their write-preparation policy for generic `create` and `update`, so CRUD cannot be a validation bypass. |
| `catalog.create_writer`, `catalog.write`, `catalog.write_one` | Supported advanced schema-driven writes. The selected writer performs every validator declared for that route before applying its prepared update. Prefer a semantic repository or mutation when one exists. |
| Library, Core, and application methods | Supported higher-level workflows only when they delegate ordinary metadata writes to one of the Catalog routes above. Their validation can improve feedback but cannot be the sole guard. |

`catalog.add`, `catalog.ensure`, `catalog.apply`, and `catalog.intralink` remain
supported compatibility routes for Row-oriented callers. They are deprecated for new code
and will be routed progressively through the canonical repository
and mutation paths without removing existing functionality.

The following are deliberately **outside** the public semantic boundary:

- `catalog.db`, database drivers, database macros, and direct SQL;
- the private writer-host methods `_apply_column_update`,
  `_apply_link_update`, and `_apply_owned_row_update`;
- cache internals and storage-cache plugins; and
- private helpers in any layer.

The private `_apply_*` methods accept already prepared update objects and exist
only so Catalog writers and the reconciliation-aware cache facade can reach
persistence. They are absent from `CatalogAPI`. Cache `create_writer`, `write`,
and `write_one` are public conveniences which delegate to the same Catalog
writers; cache reconciliation does not become a second semantic authority.

## Boundary rules

1. Public Catalog mutation methods establish their own preconditions. They do
   not trust a declaration, value object, type annotation, or upstream caller
   as proof that runtime validation occurred.
2. Generic repository entry points such as `create` and `update` must not offer
   a bypass around repository-specific semantic validation. A semantic
   repository validates through a common write-preparation hook or exposes only
   mutation methods that do so.
3. Validation that can fail without reading storage happens before the first
   mutation. Rules that require current database state are checked within the
   transaction that performs the write.
4. Database constraints remain mandatory for invariants affected by concurrent
   writers. An application-level uniqueness check cannot replace a unique
   constraint.
5. Calls made directly through database drivers or raw database macros are
   outside the Catalog semantic-validity guarantee. They remain subject to
   database integrity constraints and should be limited to schema, migration,
   repair, and explicitly low-level code.
6. Persistence adapters may reject values that cannot be represented or
   serialized, but they do not own bibliographic policy.
7. Database constraint failures are translated at the repository boundary into
   stable Catalog-facing failures while retaining the original cause and useful
   constraint details.
8. Prepared update objects are never a public proof of semantic validity. Only
   configured Catalog writers call the private `_apply_*` host seam.

## Existing data and migrations

Strictness is intentionally asymmetric:

- new Catalog writes are strict;
- ordinary reads tolerate historical values sufficiently to retrieve and
  report them;
- audits and migrations identify invalid historical values and apply explicit,
  reviewable remediation;
- reading old data must not silently rewrite it merely because current
  validation rules differ.

This prevents a newly adopted rule from making an existing library unreadable,
while ensuring that new mutations do not add more invalid state.

## Database validation profiles

A stored column-validation profile is declarative metadata about the intended
writer policy. It is not evidence that a value has been validated and is not a
substitute for the Catalog boundary.

Profile names and table-to-column assignments may remain persisted with schema
metadata. Pure implementations of metadata-semantic profiles belong below the
Catalog in `metadata`; storage-representation checks and SQL constraints remain
in `databases`.

## Known enforcement gaps on 2026-10-11

This decision describes the required contract, not the complete current state:

- `BaseRepository.create()` and `BaseRepository.update()` provide a common
  write-preparation hook, but the base implementation validates only writable
  column names. Semantic repositories other than curated identifiers can still
  expose a generic-CRUD bypass until they override that hook.
- Legacy single-identifier and bulk-identifier writers apply intentionally
  characterized but different validation behavior. Their codecs now have a
  metadata owner, but convergence remains an explicit compatibility migration.
- Low-level database and macro APIs remain callable directly by compatibility
  code.
- Row-oriented `add`/`ensure`/`apply`/`intralink` compatibility routes are not
  yet uniformly delegated through canonical repositories and mutations.

These gaps are migration work. They do not weaken the adopted contract or
authorize removal of existing functionality while closing them.

## Migration order

1. Consolidate each pure semantic validator under its metadata owner and
   preserve characterized legacy behavior where contracts differ.
2. Add one shared Catalog write-preparation seam used by both specialized
   mutation methods and generic repository CRUD.
3. Migrate identifier and language writes first, with regressions proving that
   invalid input fails before persistence begins.
4. Retain or add database constraints for structural, referential, concurrent,
   and stable scalar invariants.
5. Translate backend constraint failures consistently at the repository
   boundary.
6. Route compatibility writers through Catalog or the same validators, then
   explicitly quarantine any raw-write path that must remain.
7. Add an audit mode for historical invalid values before making additional
   read-time validation strict.
8. Keep an executable public-boundary check so prepared-update applicators
   cannot drift back into `CatalogAPI`.

Each slice follows the repository's architectural-migration rule: functionality
and observable supported behavior are preserved unless a separate, explicit
decision authorizes a capability change.

## Implementation record

### 2026-10-11 — curated identifiers

- `metadata.identifiers` now owns the pure curated-pair result and the ISBN,
  UUID, DOI, OCLC, scheme-alias, and unknown-scheme rules used by Catalog.
- `catalog.matching.policy.normalise_identifier()` remains as a thin adapter
  that preserves Catalog candidate provenance and hints; it no longer defines
  identifier value policy.
- `BaseRepository` gained one write-preparation seam used by both generic
  `create()` and `update()`.
- `IdentifierRepository` now requires and normalizes a complete scheme/value
  pair on creation.
- Partial scheme or value updates are combined with the stored counterpart and
  validated through the same ISBN, UUID, DOI, OCLC, and unknown-scheme policy
  used by matching. A changed pair is submitted to persistence together.
- Empty updates remain no-ops. Ownership/provenance-only changes validate but
  do not silently rewrite the stored pair.
- Invalid ISBN, DOI, and UUID inputs are regression-tested to fail before the
  insert or update macro is invoked.
- Unknown nonblank schemes remain supported, and raw database macros remain
  outside the Catalog semantic-validity guarantee.

### 2026-10-11 — legacy identifier containment

- `metadata.identifiers` now owns the historical logging, uncleaned ISBN-10/13
  checksum helpers used by the Library path and the exact legacy single-writer
  validator. Silent ebook-tool compatibility variants remain a distinct
  characterized contract.
- The legacy single writer still accepts only checksum-valid `isbn`; its bulk
  counterpart still cleans without validating schemes or checksums and retains
  empty cleaned pairs.
- Those contracts are separately named, tested, and delegated to by
  `library.legacy`; they were not silently homogenized with each other or with
  the current Catalog policy.
- `metadata.utils` retains compatibility exports for the moved checksum and
  validation names, and the metadata package root retains its historical
  `validate_identifier` export; neither facade implements that policy.
