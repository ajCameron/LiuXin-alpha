# Catalog write-validation contract

Decision date: **2026-10-11**

Implementation checked: **2026-10-19**, against `15080c6e`.

Status: **Shared write ownership implemented; field-level rule coverage remains incomplete.**

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

`catalog.add`, `catalog.ensure`, `catalog.apply`, and `catalog.intralink` retain
their Row-shaped requests and receipts. For Catalog-owned tables they delegate
to the same repositories and relationship owner as the other entrypoints.
Public helper workflows participate in the portable macro transaction.
These Row-shaped conveniences remain deprecated for new code; prefer repository
or coordinated mutation APIs.

The following are deliberately **outside** the public semantic boundary:

- `catalog.db`, database drivers, database macros, and direct SQL;
- the private writer-host methods `_apply_column_update`,
  `_apply_link_update`, and `_apply_owned_row_update`;
- cache internals and storage-cache plugins; and
- private helpers in any layer.

Metadata containers are passive inputs at this boundary. They do not expose
`write_to_database`, retain a database, or decide transaction policy. Use
`catalog.mutations.writer.write_container(...)` for an existing container, or
`write_values(...)` for a field/value command. There is intentionally no
compatibility alias on the container: callers must select the owning Catalog
operation explicitly.

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

## Remaining enforcement limits

This decision describes the required contract, not the complete current state:

- Shared preparation validates writable columns and derives declared identity
  keys. Curated identifiers also validate their scheme/value pair, Agent writes
  serialize aliases, and immutable entities reject mutation. This does not
  invent semantic validators for fields that do not yet have one.
- Historical single/bulk identifier codecs remain separately named pure helpers
  in `metadata.identifiers`, with characterization tests. Their former
  `library.legacy` / `library.backend` callers were retired; they are not live
  alternative Catalog write routes. New Catalog writes use curated-pair policy.
- Raw database, driver, macro and SQL operations remain available for schema,
  storage and repair work, outside the public Catalog validity guarantee.

The retained Row adapters are a separate compatibility case: for Catalog-owned
tables they use shared preparation and relationships; non-Catalog tables retain
low-level fallback paths. Neither a retained pure codec nor a raw fallback extends
the semantic guarantee. See the [migration record](catalog-legacy-mutation-migration.md).

## Current identity policy

The current schema stores full-text identity keys for tags, labels, genres,
subjects and series. The pure implementation in `metadata.text_identity` applies
NFKC, case folding and whitespace collapse. It preserves punctuation and every
word: `Sci-Fi & Fantasy` becomes `sci-fi & fantasy`, while `Sci-Fi & Horror`
remains a different identity. Genre and subject identities retain parent scope.
Approximate matching and title sorting remain separate policies.

Repositories, Row/driver writes and portable row macros derive these keys from
the database's `normalized_identities` declarations. Explicit derived values do
not override the source text. Partial updates preserve omitted source values;
clearing a source clears its key. Identity lookup uses the same declaration even
if separate column-comparison metadata disagrees.

No old-database import or old-schema profile-upgrade layer is included. Existing
audit/repair operations remain explicit: they report collisions before changing
data, never merge rows automatically, and repair changing keys transactionally.
Plain reads do not repair stored values.

## Original migration sequence

The sequence below records the 2026-10-11 implementation plan. Shared preparation,
current-schema Row delegation, private-host API checks and identity audit/repair
are now present; this is not a list of eight unstarted tasks. Remaining semantic
rule coverage and further data migration belong to the
[current roadmap](catalog-roadmap.md).

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

### 2026-10-19 — source-checked ownership checkpoint

- The shared routes below remain in use after the writes/normalization follow-up.
  Agent aggregates and Row helper workflows participate in the portable macro
  transaction; entity normalization and lookup use the declared identity policy.
- Category reads now share Core source/count policy. That is an application read
  contract, separate from Catalog's write guarantee.
- `library.legacy` and `library.backend` were retired on 2026-10-17. Their historical
  identifier-codec notes below describe the earlier checkpoint, not current live
  persistence APIs.
- The [API guide](catalog-api-usage.md), [cache contract](catalog-cache-boundary.md)
  and [roadmap](catalog-roadmap.md) distinguish implemented delegation from missing
  field-specific validators and remaining compatibility owners.

### 2026-10-15 — one current-schema mutation route

| Entrypoint | Shared owner |
|---|---|
| Repository `create` / `update` | `BaseRepository._prepare_write`, with entity-specific validation hooks |
| Schema column writes | Repository batch preparation, completed before the first mutation |
| Schema shared-value and owned-row writes | Repository creation/update followed by portable relationship operations |
| Container and Core `metadata.write` | Repository CRUD and `CatalogRelationships` inside one transaction |
| Core administration and Library row edits | Repository selected by physical table; non-Catalog tables keep their domain adapter |
| Row-shaped `add` / `ensure` / `apply` / `intralink` | `row_mutations` request/receipt adaptation to the same owners |
| Cache writer conveniences | Existing Catalog delegation and reconciliation |

Container persistence failures now propagate to roll back the operation. They
are no longer converted into a successful partial-write report. The report
shape and legitimate skips remain; clients must handle a failed command as a
failure. Column batches validate all complete partial-update pairs before
submitting any row changes. Repository identity preparation no longer uses fuzzy
matching keys, and container identifier reconciliation uses curated identifier
normalization rather than generic case folding.

`CatalogRepositories.from_database` supplies the shared composition and physical
table lookup. `CatalogRelationships` supplies endpoint checks, cardinality and
relationship persistence for repository, container and Row requests. The
database still owns constraints and portable SQL.

The Row relationship adapter preserves whole-column `highest` / `lowest`
priorities, reading extrema through the active portable transaction. Successive
links therefore see earlier uncommitted links. Unordered relationships ignore
the Row adapter's legacy priority argument. Unlinking preserves surviving link
priorities and metadata.

Flattened container identifiers retain their WEMI owner when written back;
for example, a manifestation ASIN is not copied onto its work. Unchanged
historical identifier values can survive a round trip, while new or changed
values pass the curated identifier validator.

### 2026-10-11 — passive metadata containers and Catalog write ownership

- The container writer, write-report contract, and public-value workflow moved
  from `metadata` into `catalog.mutations` and `catalog.api.mutations_api`.
- Core `metadata.write` now delegates to
  `catalog.mutations.writer.write_values(...)`.
- Concrete Calibre-like, LiuXin, and WEMI metadata containers and their
  protocols no longer expose `write_to_database`.
- In-repository scripts and behavior tests now call
  `catalog.mutations.writer.write_container(...)`; the behavior coverage was
  transplanted rather than removed.
- Catalog encloses container writes in a macro transaction when the database
  exposes one. Legacy handles without that capability retain the documented
  non-atomic fallback.
- This is an intentional breaking boundary correction. No compatibility shim
  preserves the old active-record API.

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

Historical checkpoint: the Library callers described here were subsequently
retired on 2026-10-17; the pure codecs and characterization tests remain.

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
