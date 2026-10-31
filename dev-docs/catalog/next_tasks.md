# Next Catalog tasks

Updated: **2026-10-30**. CAT02 and CAT01's first Rating slice are implemented for PR review; other field families remain open.

Keep the shared repository/relationship owner. The [roadmap](catalog-roadmap.md) records implemented consolidation; this list makes the next slices reviewable.

## CAT01. Complete validator coverage by public route

**First family implemented; architecture A08.** Continue inventorying domain fields without pure value rules, then add each rule through repository preparation and the existing relationship owner. Include generic CRUD, schema writers, container writes and retained Row adapters.

The first slice is [Rating values and scales](catalog-rating-validation.md):
pure metadata rules, shared preparation and reference validation, fractional
compatibility helpers and transactional retained metadata import. The route
matrix states modern container-write and PostgreSQL compatibility-view exclusions.
Continue with other field families using the same owner; do not introduce
another validator or persistence path.

Done when invalid input is rejected consistently, valid historical reads remain possible, and a failure late in a multi-attachment operation rolls back on supported transactional handles.

## CAT02. Unify default WEMI path selection

**Implemented for PR review, 2026-10-20; architecture A07.** Catalog owns the
[preferred-path contract](catalog-wemi-selection.md). Bundles match SQL's complete
ray ranking and retain the fullest partial path when no complete one exists.
Item availability does not rank editions; full relationship lists retain their order.

Regression fixtures cover bilingual editions, introductions, omnibuses, ties and
unlinking. Hydration retains all related Works; its relation-local primary accessor
has an explicit distinct contract and a contrasting-output test. It is not a
complete-ray selector. Pagination/truncation contracts are unchanged.

## CAT03. Make transaction capability explicit

**First for callers requiring atomic writes.** Audit optional `macros.transaction` fallbacks in metadata attachment, merging and container workflows. Define whether unsupported handles are rejected or expose a deliberately weaker contract; do not imply atomicity from the Catalog name alone.

Done when missing transaction capability and late identifier/title failures have documented outcomes, and supported operations share the driver transaction contract from DB02.

## CAT04. Consolidate remaining read adapters with measurements

**Following.** Core categories share semantics, but Work/search/acquisition adapters and legacy field metadata still overlap. Measure server-side scans before changing the owner. Keep cache freshness and unavailable-versus-empty distinctions explicit.

Done when one selected adapter delegates to the agreed read owner, counts/results/failures agree across consumers, and the measured workload improves without hiding truncation.

## CAT05. Check relationship priority scope across repeated imports

**Confirmed follow-up, 2026-10-20.** Two retained Library imports with the same
default primary language reject the second `(Work, Language)` link because another
uniqueness rule conflicts. This reproduces on unchanged A08 parent `ce43fccd`;
it is separate from Rating value validation. Inspect priority allocation against
the physical link's orientation and uniqueness scope, including other shared
metadata destinations. Do not weaken the database constraint to hide the mismatch.

Done when two Works can share their primary language through supported imports,
forward/reverse ordering has an explicit contract, and late failures roll back
both new and replacement imports. The focused Rating fixture omits language so
its injected rating failure reaches the intended boundary.

The 2026-10-30 Work membership APIs allocate Tag/Series priorities in their
physical orientation and test shared memberships across Works. That fixes those
new entry points and the migrated Series cache hook; it does not close this
primary-language/other-route audit.

## CAT06. Replace the retired metadata SQL capabilities

Added **2026-10-30**, following the
[missing-table retirement](../databases/metadata-sql-wemi-audit-2026-10-29.md).
Removing broken SQL entry points did not establish complete feature parity.
Keep these as focused changes, with explicit entity identities and supported
schema relationships. Do not restore the legacy tables or reinterpret ray IDs.

- [x] Retire the 48 missing-table methods and their obsolete callers. Existing
  Agent credit APIs cover authors/publishers; Agent CRUD covers sort names;
  Comment replacement and the Expression conversion policy cover comments.
- [x] **Explicit Work tag/Series memberships — implemented 2026-10-30.**
  Both repositories share `WorkMembershipRepositoryAPI`: add, remove, replace,
  list and existence operations, plus atomic batch replacement. Retained link
  metadata and unrelated owners are preserved. New links allocate priorities
  within the Tag/Series's Works; shared memberships have SQLite/APSW regression
  coverage. The Series cache hook delegates to the public batch operation.
  [Worked examples](catalog-api-usage.md#work-tag-and-series-memberships) execute
  in [tests](../../tests/catalog/test_work_memberships.py). Only Works support
  direct tags in the current schema; Series priority is not a volume number.
- [x] **Separate table SQL from compatibility facades — completed 2026-10-30.**
  Keep the 62 physical-table and shared normalization operations under
  `db.metadata_sql`,
  sharing portable transactions and canonical normalization. Table SQL and
  accelerators belong in databases; title/book projections and WEMI decisions
  belong in Catalog. Nine unsupported helpers and the unused `catalog_macros`
  facade remain removed. Cover flags and format removal refuse unsupported effects.
- [x] **First table-owned WEMI read slice — completed 2026-10-30.**
  Works, Expressions and Manifestations mixins batch destination reads for four
  existing Catalog relationship APIs. Link order/metadata, missing-row errors
  and contextual matching remain Catalog contracts. The
  [table inventory](../databases/table-sql-ownership.md) records the implementation.
- [x] **Remaining physical-table SQL owners — implemented 2026-10-30.**
  All 36 main tables have nonempty mixins. Catalog relationship, identifier,
  annotation and Item reads use their owners; Core policy scans batch Replicas
  and explicit policies, backup enumeration batches sources, and resource lists
  page in SQL. Shared macros retain batching and transaction mechanics.
  Existing cache/Row adapters and semantic policy gaps remain explicit.
- [ ] **Series positions and selection.** Define sequence positions, their
  scope and representation (including non-integer positions), and primary
  Series selection when several memberships apply. Add schema/API support and
  migration tests before restoring an index convenience.
- [ ] **Asset-backed catalogue inventory.** Define WEMI attachment traversal,
  cover selection, format inventory and replica/backup reporting. Reuse storage
  asset/replica APIs. Specify whether size means unique assets, selected payloads
  or physical replicas; test shared payloads and multi-Work manifestations.
- [ ] **Agent and Series tags.** Decide whether direct annotations or derived
  Work tags are wanted. Introduce approved relationships before offering APIs.
- [ ] **Unused-record cleanup.** Define Agent retention across roles, WEMI
  levels, subtype rows and other references, and Series retention across
  memberships and parent hierarchies. Explicit deletion is not an unused-row
  policy. Provide previewable candidates and transactional cleanup tests.
- [ ] **Creator URLs.** Define mapping to person/organisation web metadata or
  curated identifiers; preserve provenance and migrate explicit Agent IDs.
- [ ] **Storage relocation and conversion options.** Replace global path
  substitution with storage-owned URI/key operations and resolve conversion-option
  ownership. The nine view/column/conversion helpers are retired, but their removal
  does not provide these capabilities. Empty relationships replace old null-row
  conventions.

Membership-slice validation on **2026-10-30**: **134 passed, 22 skipped** across
the new membership tests, executable example, repository/convenience contracts,
API documentation, write-boundary parity, Catalog SQL boundary and metadata SQL
retirement regressions. SQLite and APSW passed; the skips require an unconfigured
`LIUXIN_TEST_POSTGRES_URL`. New modules/tests pass Ruff lint and formatting checks.
The full suite was not run for this slice.

The [physical-table SQL audit](../databases/metadata-sql-wemi-audit-2026-10-29.md#physical-table-sql-boundary--2026-10-30)
records the corrected ownership boundary and its focused validation. Full-suite
and live PostgreSQL coverage remain separate.
The corrected-boundary regression run passed **191 tests** on SQLite/APSW;
the final six-test lifecycle check also passed (2026-10-30).

## Validation and routing

Use [write parity](../../tests/catalog/test_write_boundary_parity.py), [normalization](../../tests/catalog/test_write_path_normalization.py), [legacy import boundaries](../../tests/catalog/test_legacy_mutation_boundary.py) and focused repository/retrieval tests. Coordinate cache state with [Caches](../caches/README.md), not a second persistence implementation.

Return to [Catalog](README.md).
