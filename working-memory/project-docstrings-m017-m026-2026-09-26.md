# Ten-unit metadata documentation batch: M017–M026 complete

The explicit request for **ten more units** authorized **M017–M026** in inventory
order. All **219 selected declarations across 21 files** are verified:
**20 complete files containing 179 declarations**, plus **40 reviewed declarations**
in the partial title metadata API file.
**M027 is next:** the remaining **17 declarations** in
`src/LiuXin_alpha/metadata/api/containers_api/liuxin_metadata_api/liuxin_title_metadata_api.py`.
Their source segments and docstrings are unchanged. The requested ten are complete;
no further units are authorized yet.

| Unit | Declarations |
|---|---:|
| M017 | 10 |
| M018 | 3 |
| M019 | 40 |
| M020 | 1 |
| M021 | 35 |
| M022 | 13 |
| M023 | 40 |
| M024 | 35 |
| M025 | 2 |
| M026 | 40 |

The batch covers public container/API facades, write-report contracts, the minimal
BookMetadata container, legacy CalibreLikeLiuXinBookMetaData and its alias, all
creator/identifier/file/cover/rating/conversion/factory/help mixins, Calibre
protocols and bounded value types, and the first LiuXin title-protocol slice.
Docs specify storage ownership, input forms, null/default handling, update and
replacement rules, resource cleanup, conversion loss, and persistence delegation.

Source-review findings retained without implementation changes:

- The legacy all_non_none_fields selector and all_set_fields wrapper currently
  select null/default fields. Direct reads expose storage; creator/identifier
  attribute reads also expose their stores, while ordinary field reads usually copy.
- Some creator and external-identifier add paths return after a scalar string,
  leaving later mapping entries unprocessed. OrderedDict identifier replacement
  differs from string/sequence insertion and does not clear unmentioned schemes.
- File/cover insertion reads stream payloads into memory and registers the resulting
  payload, not the original stream. Explicit registration is needed for later
  stream cleanup. The cleanup registry remains populated after closing.
- Scalar series-index assignment uses a Python 2 keys-view indexing path. Unknown
  creator removal can mutate a mapping during iteration. Legacy title-row hydration
  expects identifier buckets supporting add even though defaults are mappings.
- Factory regressions use a fake database and explicitly patch normalization and
  identifier buckets around these historical limitations; they do not establish
  unpatched live database hydration. The small database protocol does not enumerate
  every capability used by concrete factory paths.
- Protocols declare interfaces rather than implementing storage or coercion.
  The extended finalize protocol declares None, while the legacy method returns
  itself. Shared value aliases do not imply every concrete setter accepts every
  shape. Minimal BookMetadata keeps its title attribute separate from _data.

Verification:

- All **219 selected declarations** pass audit and normalizer checks. All
  **179 declarations in 20 complete files** pass full-file checks. The title API
  remains partial at **40/57 declarations**, and all **17 M027 source segments**
  and docstrings remain identical. Executable AST, signatures, annotations,
  decorators, comments, data/regex literals, and baseline hashes are preserved.
  Inline protocol ellipses were moved to separate lines to insert docs while
  keeping the same executable structure. No added Ruff or whitespace findings.
- **79 regressions passed**, with no failures, skips, or expected failures:
  legacy container/mixin/factory tests, OPF adapters, and two focused WEMI writer
  callers **52**; public API/export/source-boundary contracts **27**. The existing
  utcnow deprecation warning remains in the legacy log.
- **78 runnable example statements passed**, with no doctest skips. Only selected
  modules containing examples were imported, with isolated preference/config
  directories. Protocol examples use concrete implementations. Minimal BookMetadata
  and rating assignment have direct examples where dedicated consumer tests are
  absent. All **195 pytest command examples** have valid paths and selectors;
  command text is counted separately from executed doctest statements.
- No source-level runtime doc consumers were found in the original scope. API
  source-text/annotation guards also passed through the public contract suite.
- Full configured quality runner passed: **156** formatted files, annotation
  coverage across **454** files, dependency checks over **215** protected modules,
  zero basedpyright errors, mypy over **182** files, and both type checkers rejecting
  all **37** invalid contract examples. Final scope hashes match all static,
  example, regression and quality evidence.
- Discovery remains **2,671 files**, with no new or missing paths. **33 out-of-scope
  storage files** differ from historical reviewed hashes. The prior 28 records are
  unchanged; five newly flagged files are operational_api.py, policies_api.py,
  reconciliation_api.py, replicas_api.py and retrieval_api.py in storage_manager_api.
  This batch does not change those sources or their historical review records.

M progress: **26/274 units**, **535/7,198 declarations**, **52/344 complete files**.
D remains archived complete at **232/232 units**, **5,685 declarations**, and
**374 files**; its archive, milestones and file/unit records are unchanged.
Project coverage is **1,112/2,671 complete-file review records**, containing
**15,408 declarations**, plus **77 partial-file declarations** (the prior catalog
37 and this title API slice's 40). Remaining: **1,559 files and 26,676 declarations**.
Across tracks, **290 documentation units are verified** and **1,082 remain**.
No commit/publication was requested, no dependencies were installed, and no live
PostgreSQL or network service was used. Token usage is unavailable.

- [Observations and exact commands](test-results/docstrings-m017-m026-2026-09-26/observations.json)
- [Static proof](test-results/docstrings-m017-m026-2026-09-26/static.json)
- [Runnable examples](test-results/docstrings-m017-m026-2026-09-26/examples-all.json)
- [Runtime-doc and command review](test-results/docstrings-m017-m026-2026-09-26/runtime-doc-review.json)
- [M027 unchanged declarations](test-results/docstrings-m017-m026-2026-09-26/M027-boundary.json)
- [Legacy regressions](test-results/docstrings-m017-m026-2026-09-26/legacy.log)
- [API contract regressions](test-results/docstrings-m017-m026-2026-09-26/contracts.log)
- [Quality checks](test-results/docstrings-m017-m026-2026-09-26/quality.log)
- [Discovery and hash drift](test-results/docstrings-m017-m026-2026-09-26/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m017-m026-2026-09-26/reconciliation.json)
- [Authoring refinements](test-results/docstrings-m017-m026-2026-09-26/authoring-refinements.json)
- [Documentation diff](test-results/docstrings-m017-m026-2026-09-26/docstrings.diff)
- [Previous checkpoint](project-docstrings-m007-m016-2026-09-25.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
