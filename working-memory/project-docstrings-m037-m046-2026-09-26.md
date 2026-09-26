# Ten-unit metadata documentation batch: M037–M046 complete

The request for **ten more units**, followed by a request to resume, completed
**M037–M046** in inventory order: **211 new declarations across 23 files**.
All **23 files are complete**, containing **251 declarations** including the
40 concrete WEMI declarations already reviewed in M036. No new partial files
remain from this batch. **M047 is next**, the one-declaration WEMI export facade
at `src/LiuXin_alpha/metadata/containers/metadata_containers/wemi_containers/__init__.py`.
Its whole-file hash still matches the inventory baseline. The authorized ten are
complete; no further units are authorized by this checkpoint.

| Unit | Declarations |
|---|---:|
| M037 | 40 |
| M038 | 12 |
| M039 | 23 |
| M040 | 40 |
| M041 | 29 |
| M042 | 11 |
| M043 | 8 |
| M044 | 8 |
| M045 | 32 |
| M046 | 8 |

The batch finishes concrete WEMI conversion, relation access, legacy synchronization,
serialization and copying, then covers the eager hydrator, complete writer,
non-WEMI row dataclasses, mapping base, facades and inline tree relations.

A separate correction updates one **already-reviewed M029 API docstring**:
unknown database-id names raise KeyError; only known but unset ids return None.
Concrete M037 source inspection exposed the earlier incorrect claim. Only that
API doc changed, with separate AST/comment/other-doc preservation proof and a
runnable concrete KeyError example. The correction contributes **zero new coverage**;
its file hash is updated through a documented maintenance entry while historical
M029 unit evidence and original baseline hashes remain unchanged.

Source findings retained without executable changes:

- Conversion projects a copy; relation buckets expose live links while target-list
  projections make new lists. Sidecar deserialization ignores schema/summary keys,
  retains supplied bundle instances and deep-copies legacy field data.
- The eager hydrator prefers graph-selected parent ids over source hints. Its
  empty-bundle fallbacks suppress only source-row ValueError. The writer's item
  fallback instead follows foreign-key hints and first resolvable interlinks.
- Writer operations are incremental and add no transaction or rollback boundary.
  Reports can contain both changes and errors. A failed link can leave a newly
  created term row; target resolution can choose a fallback WEMI level.
- Replace mode treats the filtered desired set as authoritative, unlinking stale
  terms and deleting stale entity identifiers while retaining vocabulary rows.
  Append mode can mark an existing identifier primary. Existing term-row ids are
  trusted without comparing their text; false link priority, including zero,
  becomes highest. Dirty marking is best-effort.
- Row dataclasses keep supplied values in memory without implicit normalization,
  persistence or linked-row creation. Mapping construction ignores unknown keys;
  primary ids require exact ints. Inline tree payloads do not automatically validate,
  and collection checks do not detect longer graph cycles.

Verification:

- **211 new declarations** pass audit/normalization checks. Full-file checks pass
  for **251 declarations in 23 completed files**, including the prior WEMI slice.
  Executable AST, signatures, annotations, decorators, comments and data literals
  are preserved. Existing unselected docs and original/maintenance baselines are
  unchanged. No new Ruff findings or whitespace errors. The API correction also
  passes full-file audit/normalization and adds no lint findings.
- **162 regressions passed**, with no failures, skips or expected failures:
  WEMI/API/hydration/writer/conversion/OPF **127**, and row/schema/API/string
  consumers **35**. Database operations use existing fake/read-source fixtures;
  row schema checks use local SQLite. No live PostgreSQL or network service was used.
- **296 runnable statements passed**, with no doctest skips. Examples import only
  selected modules that need them, using isolated preference/config directories.
  All **86 pytest command examples** refer to valid consumer paths and are counted
  separately from executed statements.
- No source-level runtime doc dispatch was found in the selected files. Existing
  inspect.getdoc assertions in the related WEMI API remain intact and pass.
- Full quality runner passed: **156** formatted files, annotation coverage over
  **454** files, dependency checks over **215** protected modules, zero basedpyright
  errors, mypy over **182** files, and both type checkers rejecting all **37** invalid
  contract examples. Final source hashes match the saved evidence.
- The interruption lost running sessions before their completion records were saved.
  Available logs are preserved. Source hashes stayed unchanged, and the same
  regression/quality commands were rerun to observed successful completion.
- Discovery remains **2,671 files**, with no new or missing paths. **36 unrelated
  storage files** differ from historical review hashes: stores_api.py and
  store_api/convenience_api.py are newly flagged since the prior checkpoint;
  router_api.py changed again. These sources and review records were not edited
  by this batch. The explicit API correction is tracked separately from this drift.

M progress: **46/274 units**, **1,011/7,198 declarations**, **85/344 complete files**.
D remains archived complete at **232/232 units**, **5,685 declarations** and
**374 files**, with its archive and milestones unchanged. Project coverage is
**1,145/2,671 complete-file review records**, containing **15,924 declarations**,
plus the existing **37 partial catalog declarations**. Remaining: **1,526 files
and 26,200 declarations**. Across tracks, **310 documentation units are verified**
and **1,062 remain**. No commit/publication or dependency installation was performed.
Token usage is unavailable.

- [Observations and exact commands](test-results/docstrings-m037-m046-2026-09-26/observations.json)
- [Static proof](test-results/docstrings-m037-m046-2026-09-26/static.json)
- [Runnable examples](test-results/docstrings-m037-m046-2026-09-26/examples-all.json)
- [Runtime-doc and command review](test-results/docstrings-m037-m046-2026-09-26/runtime-doc-review.json)
- [API doc correction](test-results/docstrings-m037-m046-2026-09-26/reviewed-doc-correction.json)
- [M047 unchanged facade](test-results/docstrings-m037-m046-2026-09-26/M047-boundary.json)
- [WEMI regressions](test-results/docstrings-m037-m046-2026-09-26/wemi.log)
- [Row regressions](test-results/docstrings-m037-m046-2026-09-26/rows.log)
- [Quality checks](test-results/docstrings-m037-m046-2026-09-26/quality.log)
- [Interruption record](test-results/docstrings-m037-m046-2026-09-26/interruption.json)
- [Discovery](test-results/docstrings-m037-m046-2026-09-26/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m037-m046-2026-09-26/reconciliation.json)
- [Documentation diff](test-results/docstrings-m037-m046-2026-09-26/docstrings.diff)
- [Previous checkpoint](project-docstrings-m027-m036-2026-09-26.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
