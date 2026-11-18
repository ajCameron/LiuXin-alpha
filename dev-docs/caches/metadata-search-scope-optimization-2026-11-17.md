# Calibre metadata search: candidate and definition work

Date: **2026-11-17**. This implements the second optimization target from the
[cache review](../architectural_reviews/caches-review-2026-11-17.md) within
`utils.calibre_compat`. Native database, Catalog and cache ownership are unchanged.

## Change and contracts

The adapter indexes each complete `CalibreBookRef` once. Resolving a supplied
candidate iterable now visits those candidates instead of scanning every book.
Equal references, duplicate inputs, unknown contexts, empty generators and
distinct Work/Expression/Manifestation paths retain their existing meanings.
Sorting and the template database bridge use the same reverse lookup.

An unrestricted request reuses the projection's frozen ID scope. The bounded
result cache retains the public immutable reference set, so a warm hit does not
rebuild its candidate IDs or returned reference set. Query expansion, validation
and template permission checks still precede lookup. Dynamic queries still bypass
result caching; caller clocks and one-use permissions retain their request scope.

Definition edits clear plans/results and rebuild aliases and descriptors. They
reuse the fixed engine projection and inferred field types. The first non-null
sample determines an inferred type, as before; gathering every example is no
longer necessary. Changed records still require a replacement adapter. Input,
provider and public field-value copying remain in place. Construction still owns
both the adapter's rows and the native projection's defensive copy; this pass
does not merge those ownership boundaries.

A regression also exposed partial alias publication: after rejecting an ambiguous
definition, restoring the last valid definitions could leave the rejected aliases
in use. Alias construction and validation now finish before replacing the published
metadata. The regression checks two failed requests followed by restoration of
the exact previous state and fresh queries.

No pinned Calibre source or licence/provenance record changed.

## Matched measurements

The [benchmark](../../scripts/benchmark_metadata_search_matrix.py) now includes
ten-candidate requests, candidate sorting and alternating saved definitions over
an inferred field. The [raw before/after results](metadata-search-scope-measurements-2026-11-17.json)
retain samples, source hashes, platform and allocation evidence. Both invocations
used the same benchmark source, Python 3.12.3 and Linux/WSL machine, with one scale
per fresh subprocess and no concurrent test suite. Assertions use independent
oracles outside the timed samples.

```sh
.venv/bin/python scripts/benchmark_metadata_search_matrix.py \
  --scales 1000 10000 100000 --repeats 5
```

At **100,000 records**, median times in milliseconds were:

| Request | Before | After |
| --- | ---: | ---: |
| Warm unrestricted match-all | 41.523 | 0.051 |
| Warm unrestricted Boolean query | 19.348 | 0.038 |
| Cold result, ten supplied candidates | 25.571 | 2.872 |
| Warm result, ten supplied candidates | 22.636 | 0.015 |
| Sort ten supplied candidates | 22.606 | 0.047 |
| Edit saved definition, then search ten candidates | 951.982 | 3.849 |

Peak traced allocation during one definition edit/search fell from 63,438,350
bytes to 87,244 bytes, about **60.5 MiB to 85 KiB**. This is a separate traced
sample; the timing table uses untraced runs. The retained construction allocation
increased by 5,243,424 bytes, about **5 MiB**, for the reverse reference index.
Initial construction peak increased from about 105.2 MiB to 109.5 MiB.

These measurements exclude database capture, caller input construction, imports,
GUI work and iteration over returned results. Returning an immutable set can be
cheap even when consuming it still takes time proportional to its size. Cold
whole-collection predicates still inspect their candidates. The native sorting,
pagination and invalidation algorithms are unchanged. Five samples are evidence
for these workloads, not portable latency guarantees or CI timing thresholds.

## Validation

The initial focused run reproduced seven failures and passed two controls. After
the change, all **71 adapter and adapter-stress tests passed**. Nine new mandatory
cases cover candidate identity/iterators, bounded reference hashing, no projection
value copying during definition refresh, and recovery from rejected aliases.
They join `calibre-candidate-and-definition-work-bounds` in the
[regression manifest](../../tests/databases/regression_ratchet.json), retaining all
earlier cases and increasing its required count from 2,637 to **2,646**.

The final adapter/native-predicate/Catalog acceptance selection passed **122 tests**
with four expected capability skips in 472.77 seconds. All **102 selected mandatory
cases** passed setup, call and teardown. This includes the full Calibre soak:
10,000 records, 1,000 query/definition steps, concurrent scopes, bounded LRUs and
independent result oracles. The skips are native backend controls: live backends
retain no membership results, and snapshot backends do not capture live views.
SQLite and APSW integration passed; PostgreSQL was not selected for this pass.

The complete quality runner passed formatting for 409 files, lint, import/layer
checks, annotation coverage and both type checkers. Documentation, ratchet and
formatting tooling passed **64 tests**, with the tracked-content link test
explicitly deselected while new documents and the user's existing review draft
remain untracked. A separate working-tree audit verified every documentation
destination exists, and the mandatory manifest preserves its previous baseline.
These counts overlap the focused runs; they are not independent totals.

The [working handoff](../../working-memory/cache-search-scope-optimization-2026-11-17.md)
records local artifacts and the final audit. The complete database gate and the
other long lanes were not rerun for this adapter-only runtime change. Whole-cache
lifecycle/registry sign-off, remaining Calibre application parity, native Windows
and full ICU locale validation remain open.

Return to the [cache guide](README.md).
