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
- Databases provide generic persistence and do not own bibliographic policy or
  import metadata/Catalog/surface implementations.
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

- No import is made deferred or dynamic solely to satisfy the checker.
- Type-only edges still count for ownership and cycle analysis.
- A falling SCC number is evidence, not the definition of correct ownership.
- Compatibility adapters live at the higher boundary and have a removal note.
- Each tranche keeps executable behavior covered before deleting its old path.
- Legacy conversion/file-format containment remains a separate programme.
- Existing lifecycle, storage, and user-owned submodule changes are not mixed
  into later dependency tranches.

## Next action

After the lifecycle checkpoint, implement stage 2 as its own commit: tooling,
baseline data, self-tests, CI integration, and documentation only. Review that
baseline before changing any production import edge.
