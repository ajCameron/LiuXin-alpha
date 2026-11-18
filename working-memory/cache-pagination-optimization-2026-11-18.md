# Native cache pagination optimization — 2026-11-18

The user explicitly requested the next sorting/pagination optimization after the
candidate/definition pass. Work began on November 17 and continued after midnight.
Keep the previous adapter optimization uncommitted alongside this work. Preserve
the unrelated architectural-review index row, untracked file-formats review and
`Claude outputs/`, dirty data submodule and existing stashes. No new commits or PR.

## Implementation

- `caches/query.py`: reuse frozen cached membership for read-only paging; count
  matches before ordering. Integer-normalize bounds with `operator.index`, retaining
  rejection of floats even for empty pages. Count-only and out-of-range requests
  skip sorting/value reads after normal query validation.
- Small pages select the smaller prefix/suffix with `heapq`, retaining at most
  `min(page_end, total-offset)` keys when this is less than one quarter of matches.
  Other pages retain stable full sorting. Both paths resolve sort fields once.
- Composite keys preserve missing-last behavior, mixed field directions and
  ascending row identity ties. Use the primary field's direction directly in the
  heap; only opposite secondary directions need a comparison wrapper. This fixes
  the descending-ID slowdown caught in the first benchmark attempt.
- Detect scalar/nested float NaNs and fall back to the original stable full-sort
  semantics. Do not infer stored ID values from row identities.
- `caches/projection.py`: malformed/unqualified missing field lookup raises
  `KeyError`, allowing `UnknownCacheFieldError` instead of a split `ValueError`.

## Tests and benchmark

- New `tests/databases/caches/test_query_paging.py`: 29 parameterized cases covering
  independent page oracles, boundaries, complete ties, mixed values, NaNs, value
  read failures, field-resolution work bounds and mutations on all three backends.
- Initial red run: nine work-bound failures, 20 passing controls. First corrected
  run exposed three typed-error failures in the projection lookup. After repair:
  **111 passed/four expected capability skips** across paging/predicate/stress.
- After the final flat-key refinement: **29/29 paging tests passed**, 10.63 s.
- All 29 new cases are mandatory under
  `native-cache-pagination-order-and-work-bounds`; manifest now **2,675** cases.
- New `scripts/benchmark_cache_paging.py` covers 12 request shapes at 1k/10k/100k
  with five samples, independent source-row oracles and separate traced allocations.
  It records hashes of the code actually imported. The final benchmark source is
  identical before and after. Membership is warm; matching/capture are excluded.
- Baseline query/projection modules were saved from HEAD (`c1d2cb74`) in
  `/tmp/liuxin_paging_baseline`; its `sitecustomize.py` imports only those saved
  modules for isolated baseline processes. The working tree was never rolled back.
- Raw files: `/tmp/liuxin-cache-paging-before.json`, `-after.json`; first attempt
  retained as `-first-attempt.json`. Corresponding logs use the same stems.

The final matched benchmark is complete and published with raw samples in
`dev-docs/caches/pagination-measurements-2026-11-18.json`. At 100k, a small mixed
page fell 803.079 → 358.815 ms, peak 25,894,640 → 7,314 bytes; warm count-only
998.154 → 0.028 ms. Descending stored-ID pages remain a trade-off: 281.596 →
332.110 ms (18% slower), with peak about 16.5 MiB → 7 KiB. Do not claim a universal
speedup or hide this case. The user has been told. Full result improved
1910.951 → 1218.630 ms. Benchmark source hashes match working files.
Format/lint scope now includes query.py and both new Python files. The implementation
doc is `dev-docs/caches/pagination-optimization-2026-11-18.md`, linked from cache
README, architecture follow-up and the regression guide; validation is complete.

## Final validation

The ordinary cache/Catalog run has completed: **422 passed, 16 skipped, 647.34 s**.
All **207 required cases** passed setup/call/teardown. Skips are six disabled
legacy Calibre test modules and ten expected backend-capability controls.
Full quality passed: format 412, lint, annotation/import/layer checks, both type
checkers and internal positive/negative contracts. Tooling **79 passed, 1 explicitly
deselected, 132.43 s**. Working-tree documentation targets, manifest baseline,
measured source hashes and diff whitespace all passed. Sessions 81736, 69106, 7266
and 46470 are complete.

All **eight selected full long cases passed**, with no skips/xfails and complete
mandatory phase audits. Two snapshot soaks passed in **638.14 s**, each with 10k
rows, 1k mutations and 1k sorted pages. Six SQLite/APSW cases passed in **801.05 s**:
72 load/close cycles and 96 real live captures, plus commit/rollback checks.
Runtime hashes in all three run manifests match the final query/projection files.
Long/acceptance/quality runs overlapped, so their durations are not performance
comparisons. The isolated before/after benchmark did not overlap these tests.

## Evidence and handoff

Artifacts (all logs `/tmp/liuxin-cache-paging-<suffix>.log`):
- `acceptance`, completed: `/tmp/liuxin_run_paging_acceptance.py`, all ordinary
  cache tests except live-PostgreSQL and opt-in long files, plus Catalog search.
- `long-sqlite`, completed session 84808: standard runner, full SQLite/APSW, all six cases.
- `snapshot-soaks`, completed session 32195: `/tmp/liuxin_run_paging_snapshot_soaks.py`
  selects the two full 10k/1k-step native snapshot soaks with the runner's mandatory
  phase gate. This is a subset of the synthetic lane, not all four cases. Live
  capture is covered by the real SQLite lane; Calibre full soak passed in the
  preceding adapter pass and is not repeated here.
- `quality`, completed: standard full quality runner.
- `tooling`, completed: docs/ratchet/format/type-runner tests, with the tracked
  documentation-target test explicitly deselected pending staging.
- completed session 46470: working-tree doc destination audit, manifest baseline
  preservation, measured source hash audit, then git diff --check. Passed.

No work remains for this bounded optimization pass. PostgreSQL, the full database
ratchet, native Windows and full ICU were not rerun. The full synthetic lane was
not rerun: only its two snapshot cases. Cache lifecycle/registry sign-off remains
open. The next measured performance target is small-invalidation cost; do not
start it without a new request.

All changes remain uncommitted; no staging, commit, push or PR was requested for
this pass. Keep the tracked-content documentation test separate while new files
and the user's review draft remain untracked. Existing unrelated dirt is preserved.
