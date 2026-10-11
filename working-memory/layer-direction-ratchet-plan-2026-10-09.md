# Layer-direction ratchet plan — 2026-10-09

Status: planned. The lifecycle checkpoint committed with this note completes
the prerequisite only; it does not begin the dependency migration.

## Purpose

Turn the architectural direction described in
[`dev-docs/02 - Top Level Structure.md`](../dev-docs/02%20-%20Top%20Level%20Structure.md)
into an honest, incremental constraint without attempting a high-risk rewrite
of the current dependency component.

This plan follows the original findings in
[`working-memory/code-review-2026-09-26.md`](code-review-2026-09-26.md) and the
target decisions in
[`dev-docs/target-architecture.md`](../dev-docs/target-architecture.md). It
supersedes the review's dependency counts, but not its substantive findings.

## Current evidence

The current AST collector was rerun against the 2026-10-09 working tree. Its
model includes ordinary import-time and deferred imports, but not dynamic
import calls or implicit parent-package execution.

| Measure | 2026-09-26 review | 2026-10-09 measurement |
|---|---:|---:|
| Domain component across `caches`, `catalog`, `databases`, `metadata`, and `surfaces` | 121 modules | 111 modules |
| `catalog.api` / `catalog.write` modules in that component | All then-protected modules | 16 of 50 current modules |
| Existing zero-cycle ratchet | 221 of 1,813 modules (12%) | 215 of 1,780 modules (12.1%) |
| Largest whole-package runtime component | Not recorded on the same scope | 352 modules |
| Largest whole-package component including type-only edges | Not recorded on the same scope | 447 modules |

The 111-module domain component currently contains 29 cache, 34 Catalog, 12
database, 35 metadata, and one surface module. The 352-module whole-package
component is a different and broader problem: it includes 153 `file_formats`
modules and the legacy conversion/customization graph. Do not combine that
legacy/plugin migration with the domain-layer work merely to improve one SCC
number.

The whole-package measurement required tolerant decoding because these Python
sources are not UTF-8:

- `utils/libraries/liuxin_dateutil/parser.py`;
- `utils/libraries/inflector/languages/spanish.py`.

A production ratchet must use Python source-encoding rules (for example,
`tokenize.open`) or report unsupported encodings explicitly. It must not use
replacement decoding, which was acceptable only for this read-only estimate.

## Findings that remain current

1. `databases` still contains bibliographic and normalization policy. Examples
   include normalized-identity declarations, title/author/tag adapters,
   metadata constants consumed by schema generation, and Catalog legacy-version
   knowledge.
2. Metadata containers are active records. They expose `write_to_database`, and
   their hydrators/writer invoke database and driver-wrapper operations.
3. `metadata` composes cache implementations, imports database `Row`, and has a
   rendering method that imports a surface module.
4. The existing dependency checker protects only 215 modules. Its protected
   subgraph can be acyclic while cycles travel through an unprotected module.
5. The lifecycle migration removed the historical `storage_audit` dynamic
   import bypass. The scanners remain blind to `importlib.import_module` and
   `__import__`, so the enforcement defect itself is not fixed.
6. The current surface-boundary test has five file exceptions. Exceptions are
   useful migration markers, not evidence that the dependency direction is
   complete.

## Architectural direction to enforce

- Application surfaces call Core contracts rather than constructing or
  importing owned database, Catalog, cache, library, or storage implementations.
- Core owns composition and may depend on the lower-level services it composes.
- Library/application workflows coordinate Catalog and storage capabilities.
- Catalog owns metadata-aware persistence and may depend on passive metadata
  values/contracts plus generic database capabilities.
- Metadata value types do not depend on database, cache, Catalog, or surface
  implementations.
- Databases provide generic persistence, may consume passive metadata values
  and pure policies at an adapter boundary, and do not own bibliographic policy
  or import Catalog/surface implementations.
- The intended cache direction needs a separate explicit decision. Do not bake
  an inferred cache rank into the ratchet before deciding whether cache records
  implement a Catalog read contract or remain a Core-composed acceleration
  service.

## Migration plan

### 1. Checkpoint lifecycle ownership

Commit the completed `LibraryServices` lifecycle tranche before touching
dependency enforcement. Keep the existing dirty `LiuXin_alpha_data` submodule
outside that checkpoint.

Acceptance:

- lifecycle implementation, tests, opening-path review, and import-side-effect
  cleanup are one reviewable commit;
- `git diff --check`, focused lifecycle tests, and the project type/quality gate
  pass;
- no dependency-ratchet behavior changes are included.

### 2. Add a measurement-first whole-package ratchet

Extend or complement `scripts/check_modern_import_cycles.py` without weakening
its current zero-cycle protected scope.

The new check should:

- parse all LiuXin Python modules using Python source-encoding rules;
- retain import-time, deferred, and type-only classifications;
- recognize literal module names passed to `importlib.import_module` and
  `__import__`;
- report unresolved dynamic imports separately rather than pretending they are
  absent;
- enforce named package-direction rules against every source module;
- use a checked-in, exact allowlist for existing backward edges, including
  source, target, and context;
- reject new allowlist entries, a larger 111-module domain component, a larger
  352-module whole-runtime component, and reductions that are later reversed;
- print actual edges with paths and lines, not a sorted SCC presented as a
  traversal path;
- remain deterministic and read-only.

The initial commit is a no-behavior-change baseline. Existing debt passes only
through explicit entries; new debt fails.

### 3. Remove the metadata-to-surface rendering edge

Move HTML presentation entirely to `surfaces.renderers`. Preserve any required
legacy call shape at an upper compatibility boundary rather than hiding the
import dynamically inside `metadata.book.base`.

This is a small, comprehensible direction repair and should establish the
workflow for shrinking the allowlist.

### 4. Remove cache composition from metadata

Move `_loaded_cache` and the concrete cache/database read adapters to the
Core/Catalog composition side. Keep only passive metadata values and narrowly
defined read contracts in `metadata.api`.

Graph simulation, not an implementation estimate: removing the current
`metadata -> caches` edges reduces the largest domain component from 111 to 46.
That leverage justifies the tranche, but behavioral cache/fallback parity must
take precedence over the metric.

### 5. Decide and relocate database-held bibliographic policy

Before moving code, decide the owner of normalized-identity declarations and
the relationship between domain schema declarations and generic database
migrations. Do not merely copy tag/title normalization into a generic utility
module to make the graph green.

Graph simulation: removing the current `databases -> metadata` runtime edges
reduces the largest domain component from 111 to 49. The direction is
high-leverage, but the policy boundary—not the import spelling—is the objective.

### 6. Make metadata containers passive in vertical slices

For one WEMI aggregate at a time:

1. characterize hydration and write behavior with parity tests;
2. add the corresponding Catalog retrieval/mutation operation;
3. route live callers through Catalog;
4. remove database methods and `Row` knowledge from that metadata value;
5. delete the exact allowlist edges made obsolete by the slice.

Avoid a simultaneous Work/Expression/Manifestation/Item rewrite. Preserve
transaction, normalization, absent-field, relation-cardinality, and error
behavior explicitly in each slice.

### 7. Expand the zero-debt protected scope

After a package boundary has no allowlisted backward edges and no combined
cycle, add its complete module prefix to the existing zero-cycle ratchet. Never
claim whole-package coverage from the percentage alone; publish the protected
prefixes and remaining allowlist counts with every checkpoint.

## Safety rules

- **Do not remove functionality during architectural migration.** Moving code,
  changing ownership, breaking and fixing internal imports, or deleting a
  duplicate implementation must preserve the supported capability and its
  observable behavior. Apparently unused, broken, or obsolete functionality
  is characterized and transplanted or repaired rather than silently removed.
  Any intentional capability removal requires a separate, explicit user
  decision and must not be bundled into a layer-direction tranche.
- No import is made deferred or dynamic solely to satisfy the checker.
- Type-only edges still count for ownership and cycle analysis.
- A falling SCC number is evidence, not the definition of correct ownership.
- Compatibility adapters live at the higher boundary and have a removal note.
- Each tranche keeps executable behavior covered before deleting its old path.
- Legacy conversion/file-format containment remains a separate programme.
- Existing lifecycle, storage, and user-owned submodule changes are not mixed
  into later dependency tranches.

## Next action

The next planned architecture checkpoint remains stage 2 as its own commit:
tooling, baseline data, self-tests, CI integration, and documentation only.
The user-directed stage 5 policy extractions begun meanwhile are recorded below.

## Progress: first database-policy extraction (2026-10-10)

The first low-risk part of stage 5 has moved without changing persistence:

- the historical fuzzy-title comparison rule now belongs to
  `catalog.matching.title_normalization`;
- the standalone author/title candidate policy now belongs to
  `catalog.matching.legacy_books`;
- the live Calibre cache caller uses the Catalog-owned title policy; and
- the behavior tests moved from `tests/databases` to `tests/catalog` with exact
  output regressions.

`databases.normalized_identities` is not a Work-title normalizer: its
`TITLE_SEARCH_TERM` profile currently applies to genre, subject, and series
identity rows and should be reviewed with that declaration model rather than
folded into this title-comparison move.

The following author-sort slice now also has a single owner:

- `metadata.author_names` owns the pure prefix, suffix, copy-word, comma, and
  preference-selected author-sort transformation;
- duplicate implementations were removed from `metadata.utils`,
  `metadata.ebook_metadata_tools`, and the broken legacy name utility;
- all in-repository callers use the new owner directly, with former
  preference-driven callers passing `method=None` explicitly;
- the SQLite function retains its legacy SQL name, but its Python callable is
  now explicitly an SQL-value adapter and delegates the bibliographic rule; and
- the former tests were consolidated under `tests/metadata`, with a live
  SQLite-UDF regression retained under `tests/databases`.

Author-list parsing and serialization remain metadata codec concerns and were
not mixed into the author-sort move. Agent identity matching already belongs to
Catalog and is likewise outside this slice.

The title-sort slice now follows the same boundary:

- `metadata.title_names` is the single owner of language/article selection,
  opening-quote handling, pattern caching, and title-sort derivation;
- duplicate implementations were removed from `metadata.utils`,
  `metadata.ebook_metadata_tools`, and the SQL-driver utilities;
- Catalog, Library, cache, compatibility, and surface callers import the owner
  directly rather than relying on compatibility re-exports; and
- SQLite and APSW retain the legacy SQL function name required by schema
  triggers, but register the metadata-owned callable without defining policy.

Title search-key normalization remains distinct from title sorting. The former
belongs to matching/identity policy; this slice only consolidates the latter.

The identifier and language slices now use the same boundary:

- `metadata.identifiers` owns scheme/value cleanup, mapping normalization, and
  persistence-free curated-pair validation;
- `metadata.languages` owns canonicalization, special-value filtering, stable
  de-duplication, and result ordering;
- database adaptors retain only legacy serialized-field parsing before
  delegating to those metadata policies;
- `library.legacy` and the book metadata container use the identifier owner
  directly; and
- policy tests were transplanted to `tests/metadata`, while database tests
  continue to cover adapter composition and serialized input parsing.

The move intentionally preserves existing behavior, including ICU identifier
lowercasing, false identifier values becoming empty text at the legacy database
boundary, ISO-639 display names such as `Spanish; Castilian`, and the historical
result in which recognized `und` becomes `Undetermined` rather than being
filtered as a literal special code.

Identifier consolidation also retains the older strict metadata-container
codec contract separately from the false-safe database/book contract. Both now
have one owner in `metadata.identifiers`; they were not silently homogenized
where their edge-case behavior differed.

The curated Catalog policy has since joined that owner without merging those
legacy contracts. `metadata.identifiers.normalise_identifier_pair()` now owns
scheme aliases and ISBN, UUID, DOI, OCLC, and unknown-scheme behavior. Catalog
retains a thin `IdentifierCandidate` adapter so provenance, hints, matching,
and write decisions stay in Catalog. The former pure policy assertions were
transplanted to `tests/metadata`; Catalog retains adapter and persisted matching
integration coverage.

The legacy single/bulk ownership follow-up is also complete without changing
their observable asymmetry. `metadata.identifiers` now owns the historical
logging, uncleaned ISBN-10/13 checksum functions used by the Library path,
ISBN-only single-writer validation, and the permissive bulk-cleaning codec.
`library.legacy` delegates both paths directly to those explicitly named
helpers, while `metadata.utils` and the metadata package root retain
compatibility exports. The single writer still rejects invalid ISBNs and every
non-ISBN scheme; the bulk writer still performs no checksum/scheme validation
and retains empty cleaned pairs. Silent checksum variants in
`metadata.ebook_metadata_tools` remain a separate compatibility contract. Any
convergence remains a separate, explicit migration.

Verification for the legacy containment follow-up: 35 focused metadata,
compatibility-export, and source-wiring tests passed; the broader focused
metadata/Catalog identifier set passed with 96 tests; documentation boundary
and link checks passed with 24 tests; the full repository type/quality gate
passed; and scoped Ruff, formatting, import-order, and diff checks were clean.

Verification for this ownership move: the focused metadata/Catalog identifier
set passed with 85 tests; the complete Catalog suite passed with 610 tests; the
repository type/quality gate passed (including both type checkers and the modern
import-cycle ratchet); documentation boundary/link checks passed with 24 tests;
and scoped Ruff, formatting, and diff checks were clean.

## Decision recorded: Catalog write validity (2026-10-11)

The canonical
[`Catalog write-validation contract`](../dev-docs/catalog-write-validation-contract.md)
now records the layer boundary agreed during this migration:

- `metadata` owns pure value normalization and semantic validation;
- every public Catalog mutation guarantees validation rather than assuming an
  upstream caller performed it;
- `databases` retains representation checks, transactions, and structural,
  referential, uniqueness, and other concurrency-safe constraints; and
- existing reads remain tolerant while migrations and audits handle historical
  invalid values explicitly.

The contract is dated 2026-10-11 and explicitly identifies current enforcement
gaps, including generic repository CRUD and divergent legacy identifier paths.
It is an adopted target, not a claim that those gaps have already been closed.

The first enforcement slice was completed the same day:

- `BaseRepository` now routes generic create/update payloads through an
  overridable write-preparation seam;
- curated identifier CRUD validates a complete pair, including partial updates
  combined with their stored counterpart;
- changed identifier pairs are written together after validation;
- invalid ISBN, DOI, and UUID values fail before persistence macros run; and
- unknown schemes and the explicitly low-level raw macro boundary remain
  supported.

Focused identifier invariants passed with 24 tests. The selected Catalog
matching, semantic, convenience, Unicode, and entity regressions passed with
112 tests, and the complete Catalog suite passed with 610 tests. The full
type/quality gate also passed.

## Documentation consolidation (2026-10-11)

The validation boundary and wider separation of concerns are now recorded at
every intended discovery level:

- `dev-docs/04 - Seperation of Concerns.md` is the dated canonical ownership
  guide and links the write-validation contract;
- the older `dev-docs/seperation_of_concerns.md` is explicitly historical and
  superseded;
- the top-level structure, target architecture, Catalog usage, Catalog writer,
  and column-metadata guides distinguish semantic validation from database
  integrity;
- the package-root docstrings for Core, Library, Catalog, metadata, databases,
  storage, caches, surfaces, ingest, and jobs state their responsibility and
  anti-responsibility boundaries; and
- the public-documentation test now requires every one of those package roots
  to retain both its separation summary and canonical-guide pointer.

The focused documentation/import checks passed with 30 tests. Documentation
link validation passed with 19 tests, scoped Ruff checks passed (with two
pre-existing metadata f-string suggestions excluded), and `git diff --check`
was clean.
