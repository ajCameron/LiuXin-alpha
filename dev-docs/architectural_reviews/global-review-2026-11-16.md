# Global project review and task list

Date: 2026-11-16

Status: **Open review with a proposed task list.** See the
[follow-up of 2026-11-16](#follow-up-2026-11-16) for the first completed items.
Source checkpoint: working tree based on `3a0e22cb`.

Evidence limits: this was a static review. The top-level guides and configuration
were read, and all 3,016 tracked Python files (about 1.67M lines) were scanned for
size, docstring and annotation coverage, exception handling and licence strings.
Most code was sampled rather than read. **No tests were run**; the review shell had
Python 3.10 and the project requires 3.12. Counts are regex or AST measurements
from that scan and should be re-measured before being used as a ratchet baseline.

It complements the [whole-codebase review of 2026-10-27](codebase-review-2026-10-27.md),
which carries reproduced defects; this review covers maintainability,
documentation and progress toward the [project goals](../project-level-goals.md).

## Summary

The modern packages (`databases`, `storage`, `catalog`, `core`, `ingest`) are in
good shape: typed, documented with real contracts, and protected by CI ratchets.
The main risks are:

1. a licence mismatch between the MIT `LICENSE` and 396 modules declaring GPL v3;
2. destructive storage paths without enforced retention, which the introduction
   already acknowledges;
3. no canonical Calibre import, so existing libraries have no on-ramp;
4. the volume of inherited code (`file_formats` is 572k lines, a third of the
   repository); and
5. about 14,000 templated docstrings that make the 95% coverage figure misleading.

## Measurements

| Area | Files | Lines | Fully annotated functions | `except Exception` | Bare `except:` | Python 2 idioms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `file_formats` | 463 | 571,754 | 100% | 364 | 217 | 408 |
| `tests` | 960 | 378,877 | 50% | 184 | 1 | 27 |
| `utils` | 255 | 125,364 | 26% | 273 | 112 | 85 |
| `metadata` | 198 | 112,186 | 70% | 538 | 14 | 28 |
| `storage` | 199 | 103,797 | 95% | 135 | 0 | 0 |
| `databases` | 229 | 77,122 | 100% | 136 | 3 | 6 |
| `surfaces` | 174 | 68,048 | 80% | 248 | 1 | 9 |
| `catalog` | 155 | 49,480 | 79% | 100 | 0 | 19 |
| `caches` | 64 | 34,357 | 92% | 13 | 2 | 103 |
| `core` | 69 | 31,879 | 99% | 97 | 0 | 0 |
| `library` | 37 | 30,425 | 32% | 34 | 6 | 301 |
| `plugins` | 23 | 13,010 | 52% | 47 | 4 | 14 |

Docstrings are present on 35,278 of 37,004 functions. Templated phrases counted
under `src/LiuXin_alpha`: "Exercise … through a consuming regression" about
13,900 times and "Value supplied for … under the … contract" about 13,500 times,
almost all in `file_formats`, `utils`, `library` and `plugins`. `storage`,
`databases`, `core`, `caches` and `ingest` contain none. About 5,000 doctest
examples carry `+SKIP`.

Largest non-data modules: numpy `storage_cache.py` (5,982 lines),
`portable_macros_mixin.py` (5,683), `library/caches/calibre/cache.py` (5,018) and
`fields.py` (4,805), `surfaces/web_readonly/app.py` (4,077),
`surfaces/web_readwrite/app.py` (4,041) and `core/application_api.py` (3,455,
including a 569-line `install()`).

## Licensing options

This section states the factual position; it is not legal advice.

Calibre is GPL v3 and 396 modules carry its licence string. A distribution that
includes or derives from that code has to be offered under GPL-3.0, so the MIT
`LICENSE` cannot cover the wheel as it stands. No source file has an SPDX header
and the REUSE workflow is manual-only.

| Route | What it means | Assessment |
| --- | --- | --- |
| Relicense the project to GPL-3.0 | Root licence becomes GPL-3.0. Files written from scratch may stay MIT-tagged, since MIT code can be included in a GPL work. | **Recommended.** Cheapest, and matches Calibre. Prevents embedding in closed-source products. |
| MIT core plus a separate GPL package | The core must not import GPL code or contain anything derived from it. | Hard: `library/caches/calibre`, `metadata/book`, parts of `caches` and `utils/calibre_compat` have Calibre ancestry even where headers are gone. Worth it only if permissive reuse of the core matters. |
| Clean-room rewrite | Replace all Calibre-derived code. | Not realistic at this size. |

## Task list

P0 precedes any release; P1 defines the alpha; P2 and P3 can proceed alongside in
small pull requests. Sizes: S is under a day, M a few days, L a week or more.
IDs are stable; use them in follow-ups.

### P0: Licensing

- [x] **G-L1 (S)** Decide the route above and record the decision and reasons in
  this folder.
- [ ] **G-L2 (M)** Build a provenance inventory: one row per file with origin
  (own, Calibre, other vendored), upstream licence and copyright holder. Start
  from the `__license__` and `__copyright__` strings, then check files with
  Calibre ancestry that lost their header. Done when every file under `src/` has
  a row and none says "unknown".
- [ ] **G-L3 (S)** Replace the root `LICENSE`, add a `LICENSES/` folder with every
  licence in use (GPL-3.0, MIT, BSD, PSF and any others found), and add a notice
  file crediting Calibre and each vendored library.
- [ ] **G-L4 (M)** Add SPDX headers or a `REUSE.toml`. Calibre-derived files keep
  their original copyright line and take `GPL-3.0-only`. Done when `reuse lint`
  passes locally.
- [ ] **G-L5 (S)** Set the licence fields in `pyproject.toml` and confirm the
  built wheel contains the licence texts.
- [ ] **G-L6 (M)** Audit non-code assets under `resources/calibre`: fonts,
  dictionaries, images and content-server files each have their own terms.
- [ ] **G-L7 (S)** Check redistribution rights for tracked book content: EPUB, PDF
  and RAR fixtures and the Faded Page data. Public domain in Canada does not
  settle the status elsewhere.
- [ ] **G-L8 (S)** If anyone else has contributed code, record their agreement to
  the licence change.
- [ ] **G-L9 (S)** Make the REUSE workflow a required pull-request check. Done
  when a pull request adding an unlicensed file fails CI.

### P0: Archival safety

- [ ] **G-A1 (M)** List every code path that can delete, overwrite or move bytes
  in a Store, with its caller and current guard, under
  `dev-docs/storage_and_backup/`.
- [ ] **G-A2 (L)** Route those paths through one guard that refuses to remove the
  last verified Replica of an Asset without an explicit, logged override. Done
  when a test proves each listed path fails closed.
- [ ] **G-A3 (L)** Make partial failure recoverable: a destructive operation
  records intent before acting and result after, so an interrupted run can be
  resumed or rolled back.
- [ ] **G-A4 (M)** Add a restore rehearsal test: ingest a small fixture library,
  back it up, destroy the primary Store, restore, then compare digests and
  catalogue rows. Done when it runs in CI on every pull request.
- [ ] **G-A5 (M)** Add fault-injection tests: crash mid-copy, crash between byte
  move and catalogue commit, and a Store that becomes read-only.

### P1: Alpha exit and Calibre import

The proposed alpha exit is one end-to-end slice passing in CI: import a Calibre
library, browse and edit it as WEMI, back up, and restore on a clean machine.

- [ ] **G-X1 (S)** Write the alpha exit criteria as a one-page guide linked from
  the root README.
- [ ] **G-X2 (M)** Specify the canonical Calibre import: which `metadata.db`
  tables and custom columns map to which WEMI entities, what is lossy, and where
  source provenance is kept.
- [ ] **G-X3 (L)** Implement `liuxin import calibre PATH` as a resumable job that
  never modifies the source library. Done when the generated Calibre fixture
  libraries import with a report of mapped, lossy and skipped fields.
- [ ] **G-X4 (M)** Add a round-trip test: import, export through the Calibre
  compatibility projection, and diff against the source.
- [ ] **G-X5 (M)** Add the end-to-end slice test.
- [ ] **G-X6 (S)** Write a WEMI editing walkthrough with one worked example, such
  as a translated novel with an introduction by a second author.
- [ ] **G-X7 (S)** Assign a real version number and start a changelog once the
  slice passes.

### P1: Repository hygiene

Over 40 MB of tracked files are local artefacts rather than source.

- [ ] **G-H1 (S)** Untrack and ignore `.idea/`, `src/wget-log`,
  `extensions_source.zip`, `fadedpage-ebooks.json` and
  `fadedpage-wget-discovery.sqlite3` with its `-wal` and `-shm` companions.
- [x] **G-H2 (S)** Untrack the full-suite JSON reports under
  `working-memory/test-results/`; at least nine of 3 to 4.5 MB predate the ignore
  rule.
- [ ] **G-H3 (S)** Move the 7.7 MB
  `dev-docs/hacking/project-docstrings-work-units.json` out of the docs tree, or
  regenerate it on demand.
- [ ] **G-H4 (M)** Resolve `src/LiuXin_tests` (22,700 lines), which differs from
  `tests/support/test_databases` in 83 places. Keep one copy and migrate the 8
  files that reference the other.
- [ ] **G-H5 (S)** Remove `src/past`, the Python 2 shim that ships in the wheel,
  after replacing its remaining imports.
- [ ] **G-H6 (S)** Move `LiuXin_prefs/`, `LiuXin_data/` and `src/LiuXin_prefs/`
  under `tests/fixtures/` or out of the repository, and delete
  `src/LiuXin_alpha/working-memory`.
- [ ] **G-H7 (S)** Decide whether to rewrite history to drop the large blobs or
  accept the clone size.

### P2: Docstrings

- [ ] **G-D1 (S)** Add a filler detector to `scripts/audit_project_docstrings.py`
  for the known templates: "Perform the … operation under explicit …", "Value
  supplied for … under the … contract", "Exercise … through a consuming
  regression" and "Initialize and validate the … state". Done when the audit
  reports filler separately from missing docstrings.
- [ ] **G-D2 (M)** Strip filler with a `libcst` codemod. Where an original
  docstring survives inside the template, keep that text and drop the wrapper.
- [ ] **G-D3 (S)** Record the policy in the [style guide](<../hacking/Style Guide.md>):
  public APIs in modern packages need a real docstring; inherited private
  helpers may have none.
- [ ] **G-D4 (S)** Ratchet the filler count to zero in CI.
- [ ] **G-D5 (L)** Review the `+SKIP` doctests. Make the cheap ones runnable
  under `pytest --doctest-modules` and turn the rest into prose examples.
- [ ] **G-D6 (S)** Fix inherited typos in passing, for example "Preforms a
  update".

### P2: Documentation

- [ ] **G-O1 (S)** Rewrite `claude.md`. It is dated 2026-03-11, says there is no
  console script, lists `lxml` as the only dependency and uses paths from another
  machine. Keep it short and point to the developer index.
- [ ] **G-O2 (S)** Add a test that fails when `claude.md` contradicts
  `pyproject.toml` on entry points or Python version.
- [ ] **G-O3 (M)** Reduce `working-memory/`: 554 tracked files, 257 from
  September alone. Fold lasting decisions into subsystem guides and archive the
  rest on a separate branch or in release assets.
- [ ] **G-O4 (L)** Write user guides for installation, first library, WEMI
  concepts, metadata editing, backup and restore, and Calibre migration. User
  documentation is 7 files against 218 developer documents.
- [ ] **G-O5 (M)** Generate an API reference for the supported public surface
  (Core, Catalog and storage APIs) and build it in CI.
- [ ] **G-O6 (S)** Rename the "Seperation of Concerns" files to the correct
  spelling, updating links in the same change.
- [ ] **G-O7 (S)** Add a short `CONTRIBUTING.md` linking the coding guide, style
  guide and quality gates.

### P2: Maintainability

- [ ] **G-M1 (M)** Split `core/application_api.py` into one module per operation
  family (schema, rows, catalogue reads, catalogue writes, admin, storage) and
  replace `install()` with declarative registration. Done when no Core module
  exceeds the complexity ceiling and wire names are unchanged.
- [ ] **G-M2 (M)** Extract shared routing, rendering and error handling from the
  two web `app.py` modules.
- [ ] **G-M3 (L)** Break up `portable_macros_mixin.py` and the numpy
  `storage_cache.py` along their existing internal sections.
- [ ] **G-M4 (M)** Triage inherited formats into owned (typed, tested,
  maintained), frozen (unchanged, behind one adapter, outside the ratchets) and
  dropped. LRF, LIT, RB and PDB are candidates to freeze or drop. Done when every
  `file_formats` subpackage lists its tier in its guide.
- [ ] **G-M5 (M)** Decide the fate of `library/caches/calibre`. Retire it if the
  native caches now cover its callers.
- [ ] **G-M6 (L)** Review each `except Exception` in `storage`, `databases`,
  `catalog` and `core`; narrow it or record why it must stay broad. Then enable
  Ruff `BLE001` for those packages.
- [ ] **G-M7 (M)** Remove bare `except:` clauses in `file_formats` and `utils`
  for formats in the owned tier.
- [ ] **G-M8 (L)** Shrink the 214-module import cycle recorded in
  `scripts/layer_direction_baseline.json` by clearing its 13 excepted edges one
  at a time, starting with `databases.runtime` importing `storage.api` and
  `metadata.book.base` importing `catalog.field_metadata`.
- [ ] **G-M9 (S)** Replace the 395 hand-listed format paths and 61 mypy targets
  in `pyproject.toml` with directory includes plus an explicit legacy exclude
  list, so new modules are covered by default.
- [ ] **G-M10 (L)** Raise annotation coverage in `utils`, `library` and `plugins`
  for the modules that survive triage.

### P3: CI and surfaces

- [ ] **G-C1 (S)** Add a Windows job running the confidence stream. Recent
  commits fix UNC paths and Windows publication; every job observed runs on
  Ubuntu.
- [ ] **G-C2 (S)** Name two supported surfaces, most likely the `liuxin` CLI and
  one web application, and label the terminal browser, Tk GUI, `gui2`, OPDS and
  read-only variants as experimental in their guides.
- [ ] **G-C3 (L)** Merge the read-only and read-write variants of each web and
  OPDS surface into one application with a capability switch.
- [ ] **G-C4 (S)** Publish coverage from the full-suite job and ratchet it per
  package.
- [ ] **G-C5 (S)** Schedule the benchmark suite weekly and publish its Markdown
  summary.

## Follow-up, 2026-11-16

Source checkpoint: `main` at `47156490`. Changes below are comment-only or
index-only; no test suite was run for them.

| Task | Status | Evidence and remainder |
| --- | --- | --- |
| G-L1 | **resolved** | Maintainer decision: distribute the whole project under GPL-3.0. The MIT-core split is not being pursued. G-L3 to G-L5 apply that decision and are still open. |
| G-L2 | **partial** | [Provenance inventory](licence-provenance-2026-11-16.csv): one row per tracked file under `src/`, with declared licence, copyright and, for headerless Python files, the best match against calibre 9.18. 35 headerless modules contain calibre code; 34 received notices in PR #162 and `utils/calibre_compat/custom_columns/facade.py` is pending because it is not on `main`. The maintainer confirmed 15 name-only matches as original work. The comparison fires on 186 of 254 modules that declare calibre origin, so "no match" is likely rather than proven. Non-code assets are not classified (G-L6). |
| G-H1 | **partial** | `.idea/`, `src/wget-log`, `fadedpage-ebooks.json` and the `fadedpage-wget-discovery.sqlite3` files are untracked and ignored. `extensions_source.zip` is retained: it holds C extension sources (including `unrar`, `hunspell` and `chm`) that may have no other copy, and its licences belong with G-L6. |
| G-H2 | **resolved** | The nine `full-suite-*.json` reports are untracked; the existing ignore rule now holds. Benchmark JSON reports and summaries are kept. |
| G-H3 | **open** | `project-docstrings-work-units.json` is linked from two guides and read by a documented verification snippet, so removing it needs those guides updated in the same change. |

Return to the [architecture index](README.md).
