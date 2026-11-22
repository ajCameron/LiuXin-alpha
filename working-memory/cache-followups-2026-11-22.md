# Metadata cache follow-ups, 2026-11-22

The user requested all three review follow-ups, with Core delivered separately,
and explicitly included the existing-PostgreSQL schema upgrade. They then chose
to optimize live capture cost before completing the unchanged full soak.
No PR merge is authorized. Preserve the original checkout, data submodule and
six stashes; these remain unchanged.

## Delivered implementation

- Core `c641c488`, [PR #215](https://github.com/ajCameron/LiuXin-alpha/pull/215),
  targets `cache-review`. Generic field writes select the shared metadata owner,
  retain injected writers and propagate failures without retry. The neighboring
  vocabulary audit repairs native name selection and serialized hierarchy callers.
- Metadata [PR #214](https://github.com/ajCameron/LiuXin-alpha/pull/214) is stacked
  on `core-cache-write-routing`; foundation PR #213 targets main. Core remains
  separately reviewable. Neither PR has been merged.
- Schema writers share metadata transaction-completion guards, including live
  storage. Resolution is guarded before SQL work; raw borrowed units reject
  before mutation. Reads, reloads and writers follow database-before-cache lock
  ordering. Every thread sees pending publication; late callbacks cannot undo
  clear/close. The live reload override rebuilds once and keeps proxy identity.
- Native live queries create per-call demand-driven views. Point reads capture
  identities, population queries load needed tables, and relationship reads share
  physical routes within an operation. Driver value conversion, SQL snapshot and
  caller transaction ownership are retained. Views close after each call; no
  result is shared across calls. Lightweight adapters retain the eager fallback.
- The explicit PostgreSQL upgrade previews a fingerprinted plan, locks and
  revalidates it, then atomically repairs typed-link constraints and Series
  sibling position. It preserves existing rows and never runs during opening.

## Acceptance evidence

Core has 214 distinct targeted passes. Its corrected full CI suite passes
10,784 cases (2,306 skipped, 11 expected failures), and its database gate passes
5,611 (125 permitted skips). Metadata `7b2f5a4c` passes the combined full suite:
10,861 cases, 2,348 skips and 11 expected failures. Its push database gate passes
5,721 cases with 133 permitted skips; the PR-event duplicate remains running at
this documentation update. Formatting, lint, architecture and both typing gates
pass. The final evidence commit changes documentation only.

Local evidence includes 138 lifecycle passes, 16 focused live-capture passes
across SQLite/APSW/PostgreSQL, nine PostgreSQL upgrade passes plus three lock-list
reruns, 12 broader PostgreSQL metadata passes, and 73 distinct combined
Core/Catalog/capture passes. All 70 newly mandatory cases map to passing JUnit
records; all 3,069 preceding cases remain, yielding 3,139 requirements.

Earlier CI exposed a stale Core transaction mock and differential tests reusing
one live view after per-call cleanup. The mock now asserts serialized ownership;
the obsolete shared-view helper is removed. All 22 Core contract cases,
12 SQLite/APSW parity cases and two live PostgreSQL parity cases pass. The latter
took 1,337.47 seconds. These were test-only corrections. Both disposable
PostgreSQL clusters stopped cleanly. Five initial integrated teardown errors
were artifact-guard failures caused by concurrent pytest/quality tools creating
cache directories; clean reruns pass. Keep these tools sequential in fresh checkouts.

## Completed full soak and measurements

The original pre-optimization run was explicitly interrupted: only its uncached
call passed (3,524.95 seconds). The intermediate pre-conversion run was also
stopped. Neither is a passing lane or evidence for the final implementation.

The final unchanged full lane at cache commit `44dd5fe9` passes all four modes:
10,000 Tags, 1,000 Works, 12 cycles and 24,000 individual relationship reads per
mode. Gate: four required, four fully passed, no failures, exit zero. Setup,
call and teardown all pass. All 481 recorded source hashes match at completion.
Full call durations are uncached 1,855.54 s, live 5,333.19 s, NumPy 1,706.21 s,
and schema 1,385.06 s; total pytest duration is 10,288.13 s. Sampled peak process
tree RSS is 267.90 MiB, with zero process swap and no increase in host OOM/swap
counters. The earlier final-source smoke passed all four modes in 117.36 s.

The host wall clock jumped forward 35,729 seconds while monotonic time advanced
only 1.68 seconds. Timings use monotonic durations and exclude that civil-clock
interruption; do not call them the total wall-clock duration. The full-soak
source was frozen throughout. Integrated read_cache differs only in its docstring
(executable AST equal); two imported Catalog container writers are covered by
targeted integration tests, not this soak. Tests/docs added later do not change
the exercised cache runtime or long workload.

The final unprofiled workflow benchmark completed all four modes with 1,000 Tags,
100 Works and seven repetitions. Live selected-Tag and seven-record hydrated
search medians were 0.023 s and 0.510 s (prior diagnostic run: 4.375 s and 4.734 s).
Host load varied, so these are not controlled speedup ratios. The corrected
relationship samples rotate membership; the initial no-op samples are excluded.
Independent-writer contention passed. Remaining costs are population-scanning
pickers, complete relationship-route capture and conservative snapshot refreshes.

Evidence under `/tmp`:

- `liuxin-metadata-capture-final-full-20261122/`: gate, JUnit, source hashes,
  measurements and telemetry; `liuxin-full-soak-final-audit.json` checks all phases.
- `liuxin-workflow-capture-final-20261122/result.json`: complete unprofiled benchmark.
- `liuxin-followup-mandatory-audit.json`: all 70 new cases mapped to passes.
- `liuxin-core-routing-validation-final2.json`: 214 distinct Core passes.
- `liuxin-metadata-integration-validation.json`: 73 combined passes.
- `liuxin-cache-followups-acceptance.json`: final requirement/status audit.

See [the follow-up review](../dev-docs/caches/metadata-followups-2026-11-22.md)
for measurements and [the PostgreSQL upgrade](../dev-docs/databases/postgres-catalog-upgrade.md)
for preview/apply instructions. Original checkout stays on `cache-review`;
only isolated worktrees and the two PR branches are updated.
