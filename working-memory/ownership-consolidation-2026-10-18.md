# Ownership consolidation — 2026-10-18

Status: published for review as [PR #120](https://github.com/ajCameron/LiuXin-alpha/pull/120),
**Consolidate Agent writes and facet normalisation**. Code checkpoint: `f40881b6`.
Branch `ownership-consolidation` starts at
`85eaa2f0`, the pushed head of telemetry/storage PR #119. Work reuses the isolated
`/tmp/liuxin-test-vm-telemetry` checkout to avoid another repository copy. The main
Windows checkout's staged and unstaged user edits remain separate.

The user selected **focused PRs, starting with writes and normalisation** for
item 3 of the September code review. The other ownership areas remain explicitly
tracked below. The user also considers WSL healthy, with storage shortage the
probable explanation for its earlier crashes; diagnostics support that working
assessment without establishing the original crash cause.

## Updated ownership map

The September review predates the Catalog consolidation in #118. Its counts of
live independent writers and normalization rules are historical findings, not a
fresh inventory of this branch.

| Concept | Current owners and remaining gap | Completion evidence |
|---|---|---|
| Bibliographic writes | Catalog repositories, coordinated mutations and relationships own modern writes. `catalog.row_mutations` adapts compatibility requests. Agent subtype/organisation-relation tables are not standalone entity repositories, so the compatibility adapter fell through to Row writes on separate connections inside an active Catalog transaction. | Compatibility and direct aggregate entrypoints share subtype/parent validation, alias preparation and transaction ownership; late attachment failure rolls back the complete aggregate on each backend. |
| Stored identity keys | `metadata.text_identity` supplies pure profiles; database identity declarations select the policy per source/key pair. Terminal facet and genre/series lookup code still derives legacy compact/title keys independently, while persistence replaces them with declared keys. Some tests still assert the old policy. | Local and Core-backed lookup find exactly the keys that repositories store; spaced/unspaced and Unicode counterexamples prove parity; surfaces pass source values and do not supply alternate derived keys. |
| Catalogue reads | `core.browse_api` and `surfaces.read_model.api` still make category decisions separately. Core category counts sum tag/label candidates; the surface read model selects tags or labels. `surfaces.metadata_facets.preferred_tag_table` adds an explicit populated-tags preference. | One owner defines tag/label category source, count and membership; CLI, API, web and OPDS agree for tags-only, labels-only, both-populated and empty-tag fixtures. Preserve meaningful errors and remote-query bounds. |
| Configuration | `surfaces.system_profile` owns deployment selection; `preferences`, `utils.config` and constants consume separate stores. `constants.paths.config_dir` is also reassigned in `constants.__init__`. | One composed configuration boundary defines precedence and paths before consumers open resources; profile selection reaches consumers; overlapping keys have one owner. Verify isolated profiles, environment overrides and import behavior. |
| Jobs | Core uses `utils.jobs.InMemoryJobManager`; `jobs` has persistent repository, worker and scheduler contracts. Their distinct execution/durability responsibilities need explicit composition. | One public submission/status/cancellation/result contract, with documented executor and persistence roles; exercise restart, cancellation and progress without duplicating state owners. |
| Ingest | `core.workflow_jobs`, `core.program_services.ingest`, `ingest.mixed_application` and source-specific pipelines participate; storage reconciliation still owns some discovery/application operations. | Map entrypoints to the same ingest coordinator and policies, retaining source/backend adapters. Verify preview, write, cancellation, failure and local/remote parity. |
| Plugin registries | Registries exist for formats, metadata readers, storage, database drivers, caches and terminal lifecycle plugins. Some apparent duplicates (`storage.backend_registry`, `customize.builtins.registry`) are exports rather than independent implementations. | Distinguish domain-specific registries from competing registries for the same capability. Remove duplicate discovery/selection for a capability and test precedence, failures, plugin lifetime and lazy imports. |

The next catalogue-read slice is tracked in
[catalogue read ownership](catalogue-read-ownership-2026-10-18.md).

## First PR: writes and normalisation

Preserve the established full-text NFKC/casefold/whitespace default and
schema-selected policies. Matching heuristics and sort presentation are separate
from persisted identities. Do not reintroduce old-database upgrade branches or
fallback mutation implementations; the user previously ruled those out.

1. Delegate compatibility Agent aggregates and organisation relations to the
   Agent repository, removing the independent sidecar and alias writers.
2. Make terminal lookups consult the existing Core/database identity boundary.
   Supply display/source fields to Catalog mutation APIs; derived keys are owned
   by the persisted policy. Keep distinct full-text keys distinct during parsing.
3. Add real-backend parity and late-failure rollback regressions. Update stale
   expectations only where the approved storage contract changed, with explicit
   counterexamples rather than weakening assertions.
4. Run the previously failing write/normalisation cases, relevant Catalog/Core/
   terminal checks, PostgreSQL parity and the project quality checks. Keep the
   existing telemetry and aggressive cleanup enabled.
5. Publish this focused PR against #119, with the other ownership areas remaining
   as separate follow-ups. Existing stale surface contracts and image fallback
   failures are outside this first PR.

## Implementation

- `AgentCreatorOrgMixin` delegates person/organisation creation to the Agent
  repository and passes aliases through unchanged for its existing Unicode
  policy. The separate sidecar updater and alias normalizer are removed.
- `AgentRepository.relate_organisations` owns endpoint/type validation and edge
  persistence. Exact duplicate edges reuse the ID and retain the original note;
  other failures propagate. Aggregate creation and compatibility workflows use
  this same operation inside their enclosing transaction.
- Core surface views expose the existing `schema.identity.derive` and
  `schema.identity.resolve` operations. Terminal genre/subject lookups resolve
  their parent scope in the database, so filtering cannot lose a root row behind
  a limited result page. Series lookup uses its declared identity, not a sort or
  phonetic-key fallback. Missing declarations and read errors propagate.
- Facet creation sends source text instead of a competing derived key. The genre
  wizard no longer offers an editable identity that persistence would overwrite.
  Tag token parsing removes only exact duplicates; the catalogue decides identity
  equivalence. Spaced/unspaced strings remain distinct and Unicode equivalents
  reuse the canonical row.
- `Add.tag` no longer generates a legacy compact key before persistence replaces
  it. `Ensure.tag` loses its unreachable independent lookup/write fallback and
  requires the Catalog owner. Public argument signatures remain the same; supplied
  tag keys remain subject to the existing authoritative preparation policy.
- Repository and metadata API documentation now describe that persistence policy.
  Sort fields and optional phonetic matching metadata remain separate concepts.
  The unwired standalone `LabelMixin` is not part of the supported Add facade and
  was not counted as a live competing writer in this pass.

## Validation

Artifacts are retained outside test scratch in the Windows checkout under
`working-memory/test-results/ownership-write-20261018` (ignored by Git).

Before the Agent changes, ten new SQLite/APSW cases failed: six late-failure
rollback probes encountered lock errors first, two alias comparisons disagreed,
and two invalid-parent probes hit lock errors instead of the semantic rejection.
The five PostgreSQL parametrizations were skipped in that initial reproduction.
The post-fix Agent run passed all 15 cases, including all five on PostgreSQL.
Before the terminal fix, all four new SQLite/APSW tag lookup/token tests failed;
the two PostgreSQL variants were skipped in that reproduction.

The PostgreSQL 16 checks used a private Unix socket with TCP disabled and
disposable schemas; no existing database was used.

The first expanded run exercised 94 cases: 79 call phases passed, 15 new facet
cases exposed an incorrect method name in the new Core adapter, and two otherwise
passing Agent cases failed the root-artifact guard because a concurrent quality
run created `.mypy_cache`. The adapter call is corrected, and quality checks and
pytest are now run separately. Those 17 cases are included in the follow-up run;
the test guard remains intact. The quality run itself passed all project gates.


The follow-up `terminal-and-rerun` run passed **159 tests**, including every
intermediate failed/error case. Combining the latest outcome for each case across
`focused-after` and `terminal-and-rerun` gives **236 distinct passing cases**:

- **66 write-boundary cases: 22 each on SQLite, APSW and live PostgreSQL.**
  This also closes the earlier handoff's lack of live verification for the
  existing public Catalog/Core write-boundary tests.
- **60 terminal regression cases**, covering tags, genres, subjects and series.
- Shared normalization, tag application, aggregate/driver contracts, Core semantic
  contracts, surface view contracts and mutation-boundary checks.
- The application-surface acceptance test passes through both direct Core and a
  real loopback HTTP daemon, including Unicode tag lookup and scoped genre reuse.
- **All 18 previous write/normalisation failure cases now pass.** Updated stale
  assertions reflect the already-approved full-text policy; new spaced/unspaced,
  Unicode, alias, sort-collision and parent-scope cases verify actual behaviour.

This is targeted validation, not another full-suite run. The remaining **11 known
baseline failures** (three stale surface contract assertions and eight image
fallback failures) are outside this PR. Their exact IDs remain in
`validation-summary.json` beside the retained JSON reports and telemetry.

The two expanded runs observed a maximum pytest process-tree RSS of 194.4 MiB,
minimum available RAM of 5.94 GiB, and minimum C: headroom of 15.29 GiB. No swap
use or OOM kills were observed. Serial execution and immediate scratch cleanup
remained enabled. PostgreSQL reported zero remaining test schemas, shut down
cleanly, and its owned cluster directory plus all five task scratch directories
were removed (~156 MiB of logical files). Server/shutdown logs and `cleanup.json`
are retained with the other artifacts. Older full-suite evidence was preserved.

The final, separately run `scripts/run_type_checks.sh` passed all project gates:
formatting, file-format annotation coverage, lint/complexity, import cycles,
layer direction, import footprint, basedpyright, mypy and both internal typing
contract checks. `quality-final.log` contains the full output. A scoped Pyflakes
check of every changed Python file and `git diff --check` also passed.
