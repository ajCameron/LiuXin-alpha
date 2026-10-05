# Ten-unit metadata documentation batch: M007–M016 complete

The explicit request for the **next ten units** authorized **M007–M016** in
inventory order. All **132 new declarations across 21 files** are verified.
All 21 files are now complete, containing **172 declarations**, including the
**40 utility declarations** reviewed previously in M006 and preserved here.
**M017 is next:** ten declarations across the containers/API package exports
and metadata_write_api. All three files still match their saved hashes.
The requested ten are complete; no further metadata units are authorized yet.

| Unit | Declarations |
|---|---:|
| M007 | 7 |
| M008 | 6 |
| M009 | 1 |
| M010 | 40 |
| M011 | 16 |
| M012 | 33 |
| M013 | 17 |
| M014 | 4 |
| M015 | 5 |
| M016 | 3 |

The batch covers the remaining language/manifest utilities; shared WEMI write
workflows; Calibre metadata field sets and book container; template formatting,
JSON and dictionary codecs, and rendering wrappers; metadata aliases and 13
controlled string enums; and standalone genre, region, and motif regex tables.
Docs describe actual null, copying, mutation, merge/replacement, error, encoding,
and return-value behavior. Executable AST, signatures, decorators, comments,
regexes, enum values and all other non-docstring literals remain unchanged.
The original and maintenance baseline hashes are preserved.

Important source-review findings:

- Language normalization preserves a selected region, not every script/private
  subtag. Book metadata has distinct full-instance and stored-metadata copy APIs;
  some setters and replacement paths retain mutable references.
- JSON decoding logs and contains failures while retaining earlier appended books.
  Dictionary serialization can retain custom metadata and raw cover data by
  reference; its reader does not automatically decode base64 cover data.
- The legacy canonicalize_id_name internal branch subscripts a frozenset after a
  substring match. Inputs such as uuid currently raise TypeError. That existing
  behavior is documented, with no implementation repair in this batch.
- The genre_maps files have no imports into the active genre classifier, which
  maintains separate tables. Their docs and examples describe the actual data
  exports and caller-chosen matching policy. horror_genres is an empty module
  placeholder and defines no horror map. No unrelated classifier test is claimed
  as coverage for these standalone tables.

Verification:

- Selected-declaration audit/normalizer: **132 clean**. Full-file audit/normalizer:
  **172 declarations across 21 complete files clean**. All 40 prior utility
  docstrings are unchanged. No new Ruff findings or whitespace errors.
- **152 regressions passed**, with no failures, skips, or expected failures:
  book/utility/OPF/hydrator suites **82**; real Core workflow/cache round trips
  **2**, with SQLite and APSW selected; vocabulary/container/schema-placeholder
  and OPF-reader callers **68**. Existing OPF utcnow deprecation warnings remain
  in the vocabulary log. The exact commands and source hashes are saved below.
- **98 runnable example statements passed**, with no skips. Only selected modules
  containing examples were imported, with isolated preference/config directories.
  **75 pytest command examples** have valid paths and selectors; command text is
  checked separately from executed doctest statements. Standalone data modules
  use direct regex/export/merge examples rather than unrelated caller tests.
- No source-level runtime doc consumers were found in the original scope.
- Full configured quality runner passed: **156** formatted files, annotation
  coverage across **454** files, dependency checks over **215** protected modules,
  zero basedpyright errors, mypy over **182** files, and both type checkers rejecting
  all **37** invalid contract examples. Final scope hashes match static, example,
  regression and quality evidence.
- Discovery remains **2,671 files**, with no new/missing inventory paths.
  **28 out-of-scope storage files** differ from historical reviewed hashes:
  27 were already flagged; location_factory.py is newly flagged, and location_api.py
  has a different current hash from the preceding checkpoint. These sources and
  their historical review records were not changed by this documentation batch.

M progress is **16/274 units**, **316/7,198 declarations**, and **32/344 complete
files**. D remains archived complete at **232/232 units**, **5,685 declarations**,
and **374 files**; its archive, milestones and unit/file records are unchanged.
Project coverage is **1,092/2,671 complete-file review records**, containing
**15,229 declarations**, plus **37 previously reviewed partial-file declarations**.
Remaining: **1,579 files and 26,895 declarations**. Across tracks, **280 units are
verified** and **1,092 remain**. No commit/publication was requested, no dependencies
were installed, and no live PostgreSQL or network service was used. Token usage is
unavailable.

- [Observations and exact commands](test-results/docstrings-m007-m016-2026-09-25/observations.json)
- [Static proof](test-results/docstrings-m007-m016-2026-09-25/static.json)
- [Runnable examples](test-results/docstrings-m007-m016-2026-09-25/examples-all.json)
- [Runtime-doc and command review](test-results/docstrings-m007-m016-2026-09-25/runtime-doc-review.json)
- [M017 unchanged files](test-results/docstrings-m007-m016-2026-09-25/M017-boundary.json)
- [Book/utility regressions](test-results/docstrings-m007-m016-2026-09-25/book.log)
- [Core workflow regressions](test-results/docstrings-m007-m016-2026-09-25/workflow.log)
- [Vocabulary/OPF regressions](test-results/docstrings-m007-m016-2026-09-25/vocabulary.log)
- [Quality checks](test-results/docstrings-m007-m016-2026-09-25/quality.log)
- [Discovery and hash drift](test-results/docstrings-m007-m016-2026-09-25/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m007-m016-2026-09-25/reconciliation.json)
- [Final parameter wording corrections](test-results/docstrings-m007-m016-2026-09-25/final-wording-amendments.json)
- [Documentation diff](test-results/docstrings-m007-m016-2026-09-25/docstrings.diff)
- [Previous checkpoint and D completion](project-docstrings-d229-m006-2026-09-25.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
