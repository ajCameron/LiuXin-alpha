# Native cache sorting and pagination

Date: **2026-11-18**. This implements the third performance target from the
[cache review](../architectural_reviews/caches-review-2026-11-17.md). It changes
native query execution without adding stored sorted results or another cache
invalidation contract.

## Behaviour and ownership

The [query engine](../../src/LiuXin_alpha/caches/query.py) reuses cached
membership for reads. The later [invalidation pass](invalidation-and-lifecycle-2026-11-18.md)
replaces frozen membership with private sets that can be repaired under the facade
lock; the measurements below retain this pass's original implementation.
It computes the total count before ordering. A count-only
request or an offset beyond the result set returns an empty page without reading
sort values. Query validation still runs first: unknown fields and invalid bounds
remain errors even when the requested page is empty.

For a small page, heap selection retains the smaller of the prefix ending at the
page boundary and the suffix beginning at its offset. This path applies only when
that retained portion is less than a quarter of the matches. Larger pages and
deep middle pages use stable full sorting. Both paths resolve each sort field
once, instead of resolving it again for every row.

The selected page preserves missing values last in either direction, mixed field
directions, existing mixed-type ranks, Unicode normalization and ascending row
identity as the final tie-breaker. An explicit ID field is read from storage; it
is not assumed to equal row identity. Scalar or nested float NaNs trigger the
existing stable full-sort path because heap selection requires a total ordering.
Other value-read errors propagate. Only visible records are materialized.

The primary field's direction is used directly in selection. Only secondary
fields with the opposite direction need a comparison wrapper. The first measured
implementation reversed every descending component and made descending ID pages
substantially slower; the retained implementation avoids that overhead, although
the final measurements still show a smaller trade-off for that request.

Empty-page tests also exposed a projection lookup error: a missing bare field
raised `ValueError` while splitting its name. The projection now raises `KeyError`,
allowing the query engine to expose its existing `UnknownCacheFieldError` contract.

Catalog retains bibliographic and mutation policy; databases retain persistence.
Cache lifecycle, generation, refresh and locking remain with the facade. No
pinned Calibre code or licence/provenance record changed.

## Matched measurements

The [benchmark](../../scripts/benchmark_cache_paging.py) covers twelve request
shapes at 1,000, 10,000 and 100,000 rows, five untraced samples per case and a
separate traced-allocation sample. Each scale uses a fresh subprocess. Independent
source-row oracles check IDs, counts and projected values outside timing.
The [raw results](pagination-measurements-2026-11-18.json) retain every sample,
platform details and hashes of the code actually imported.

```sh
.venv/bin/python scripts/benchmark_cache_paging.py \
  --scales 1000 10000 100000 --repeats 5
```

Both runs used the identical benchmark source, Python 3.12.3 and Linux/WSL, without
concurrent test suites. The baseline imported saved query/projection modules from
`c1d2cb74`; the after run imported the working implementation. The checkout was
not rolled back to obtain the baseline. Membership was already warm; imports,
input construction, predicate evaluation and database capture are excluded.

At **100,000 matches**, median times in milliseconds were:

| Request | Before | After |
| --- | ---: | ---: |
| Count only | 998.154 | 0.028 |
| Offset past the end | 997.370 | 0.029 |
| First 20 rows, default order | 10.993 | 3.058 |
| 20 rows, descending stored ID | 281.596 | 332.110 |
| 20 rows, ascending numeric field | 407.996 | 214.710 |
| 20 rows, descending numeric field | 361.896 | 208.691 |
| 20 rows, descending text | 417.705 | 255.429 |
| 20 rows near the start, mixed directions | 803.079 | 358.815 |
| 20 rows in the middle, mixed directions | 787.341 | 505.368 |
| 20 rows near the end, mixed directions | 724.611 | 322.574 |
| Last 25 rows, unlimited limit | 686.427 | 352.405 |
| Full result, mixed directions | 1910.951 | 1218.630 |

Peak additional traced allocation for the small mixed-direction page fell from
25,894,640 bytes to 7,314 bytes: approximately **24.7 MiB to 7.1 KiB**. Count-only
peak fell to 2,264 bytes. These are per-request allocations after membership and
projection construction, not total process or retained-cache memory.

Descending stored-ID paging was **18% slower** at 100,000 rows in this run while
its traced peak fell from about 16.5 MiB to 7 KiB. Sorted integer data can favour
the old stable sort; bounded selection is not a universal latency improvement.
At 1,000 rows this case was approximately level, and at 10,000 it improved. Five
samples do not establish portable latency guarantees or CI timing thresholds.

Every nonempty arbitrary-field page still reads sort values for every match.
Selection bounds retained keys, not the number of candidate reads. Large pages
still use full sorting; deep offsets are cheap only when the remaining suffix is
small. Database-backed capture, cold filtering and projected row construction can
still dominate an end-to-end request. Small-invalidation costs remain a separate
optimization target.

## Validation

The [new paging regressions](../../tests/databases/caches/test_query_paging.py)
add **29 mandatory cases**: independent ordering oracles with warm and uncached
membership, empty-page validation, prefix/suffix work bounds, NaN fallback,
noninteger bounds, propagated read failures and mutation/deletion on all three
native backends. The initial run reproduced nine work-bound failures while twenty
controls passed. The final focused run passed all 29 cases in 10.63 seconds.

The `native-cache-pagination-order-and-work-bounds` group increases the
[mandatory manifest](../../tests/databases/regression_ratchet.json) from 2,646 to
**2,675** cases, preserving every previous case.

The broader ordinary cache and Catalog integration selection passed **422 tests**
in 647.34 seconds, with **16 expected skips**: six disabled legacy Calibre modules
and ten backend-capability controls. All **207 selected mandatory cases** passed
setup, call and teardown. This includes direct-SQL differential checks through
SQLite and APSW. These counts include the new focused cases and are not additive.

The full quality runner passed formatting for 412 files, lint, annotation coverage,
import/layer checks and both type checkers. Documentation, ratchet, formatting and
type-runner tooling passed **79 tests**. The tracked-content documentation-link
test was explicitly deselected while new files and the user's existing review
draft remain untracked; a separate working-tree audit confirmed all destinations
exist. The mandatory manifest preserves its baseline, and measured source hashes
match the final runtime and benchmark.

All **eight selected full-size long cases** passed their mandatory phase audits,
with no skips or expected failures:

- Two native snapshot soaks: 10,000 rows and 1,000 mutation/recovery steps per
  backend, including repeated sorted pages, destructive failure recovery and
  bounded query caches. Both passed in 638.14 seconds.
- All six SQLite/APSW cases: three cache backends per driver, 1,000 database rows
  and twelve load/query/close cycles per case. These passed in 801.05 seconds,
  covering 72 load/close cycles and 96 real live captures, including durable edits
  and rollback checks.

These long suites overlapped each other and integration/quality checks; their
elapsed times are validation evidence, not matched performance comparisons.
Only the two snapshot cases of the synthetic lane were selected. Its fake live
backend case and Calibre soak were not repeated; real live reads were exercised
by the database lane, and the Calibre soak passed in the preceding adapter pass.
The [working handoff](../../working-memory/cache-pagination-optimization-2026-11-18.md)
records exact local artifacts. Recorded runtime hashes match the final code.
PostgreSQL, the complete database ratchet, native Windows and full ICU validation
are outside this pass. Whole-cache lifecycle and registry sign-off remain open.

Return to the [cache guide](README.md).
