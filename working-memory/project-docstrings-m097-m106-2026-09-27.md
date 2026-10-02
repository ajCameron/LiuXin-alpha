# Ten-unit metadata documentation batch: M097–M106 complete

The request for **ten more units** completed **M097–M106**: **256 new declarations
across twelve files**, all now complete. Titles include the **24 unchanged M096
declarations**, bringing the complete-file promotion to **280 declarations**.
The batch finishes title containers, work identity/collection/bundle/hydration,
shared identity/relation/projection/link/target APIs, and agent identity APIs.
**M107 is next** on a new request. Its **24 selected declarations** and entire
**84-declaration agent profile API file** are unchanged. The authorized ten are complete.

| Unit | Declarations |
|---|---:|
| M097 | 27 |
| M098 | 40 |
| M099 | 4 |
| M100 | 40 |
| M101 | 10 |
| M102 | 37 |
| M103 | 29 |
| M104 | 30 |
| M105 | 26 |
| M106 | 13 |

Source findings documented without executable changes:

- Title display chooses MAIN, TRANSLATED, ALTERNATIVE and SUPPLIED before the first
  remaining title. A selected record can return empty raw text. Sorting prefers an
  explicit SORT record, then main-title hints, then display text. Title slices
  combine work/expression/manifestation/item display titles with exact deduplication.
  Bucket getters register missing buckets; text reads do not. Joins default to a
  space, semicolon and space.
- Work ids become write-once after a non-None value. The legacy word_id constructor
  fallback applies only when work_id is None. Construction retains fiction values
  raw; assignment/from_mapping use bool-like conversion and export uses truthiness.
  Work collections own their lists but share identities and retain duplicates.
- Work bundles expose live link lists, shallow serialization and writer delegation.
  Identity deserialization recognizes work_id, work_title or work_canonical_title.
  Hydration follows descendant rows and explicit hints, preserving the first keyed
  Row link without merging later metadata. It gathers descriptive metadata, assets,
  identifiers and folders, but does not collect title relations. Guarded query
  failures and direct lookup errors have different propagation behavior.
- Shared relation helpers preserve link/target references, match ids without
  conversion and replace the first match. Primary editing mutates flags before
  setter validation, without rollback. Removal by object catches setter ValueError;
  removal by id propagates it. Cardinality checks enforce local maximum counts only.
- Projection protocols describe the public surface while leaving extraction and
  lazy policy to implementations. Unloaded projection errors retain context and
  advise explicit loading without performing it. Runtime protocols check structure,
  not literal cardinalities or return values.
- Target ids follow mapping or object fallback order without calling to_mapping.
  Zero and False count as present; a present invalid candidate stops fallback.
  Agent identity defaults expose an absent/read-only sort name, a four-field mapping
  and a display-name placeholder; concrete setters may impose additional policy.

Verification:

- All **256 selected declarations** pass audit and normalization. Full-file checks
  pass for **280 declarations across twelve complete files**. Executable AST,
  signatures, decorators, annotations, comments and non-doc literals are unchanged.
  The prior **24 title docstrings** remain unchanged. Original and maintenance hashes
  are preserved, with no added Ruff findings or whitespace errors.
- **195 existing regressions passed**, without failures, skips or expected failures:
  package/parity/symmetry, relation/identity APIs, projections, bundles and hydrators.
  Existing hydration/write tests use fake sources and test doubles; no live database
  or network service was used.
- **728 runnable statements passed**, without skips or failures, using normal module
  imports and isolated preference/config directories. **18 command-only examples**
  reference existing owning regressions included in the passing suite. Two projection
  examples initially used the unsupported tag_text key instead of tag_name; one string
  expectation had trailing whitespace. Those three failures are archived and corrected.
  Title-slice example field spellings were corrected during review before execution.
- No explicit runtime doc dispatch occurs in the twelve original files. Existing
  metadata doc-inspection tests target other declarations and pass in the suite.
  **144 title accessors across 48 buckets** passed captured-default, getter-document
  propagation, empty-read and bucket-registration checks.
- Reused title helpers have executable AST proof against reviewed subject helpers
  after family-name and separator substitutions. Nine shared work hydrator helpers
  have equivalent proof against manifestation helpers. Title preferences, work
  traversal, first-link retention, identifier order and folder handling were reviewed
  independently.
- Full configured quality checks passed: **156** formatted files, annotation coverage
  over **454** files, dependency checks over **215** protected modules, zero basedpyright
  errors, mypy over **182** files, and both checkers rejecting **37** invalid contract
  examples. Final source hashes match all final verification records.
- Discovery remains **2,671 files**, with no new or missing paths. **38 unrelated
  storage review-hash differences** were observed: facade_api.py is newly different
  and driver_backed_api.py changed again relative to the previous checkpoint. This
  batch did not edit those sources or their review records. Architecture-review notes
  and their index entry are retained.

M progress: **106/274 units**, **2,613/7,198 declarations**, **122/344 complete files**.
D remains archived complete at **232/232 units**, **5,685 declarations** and **374 files**.
Project coverage is **1,182/2,671 complete-file review records**, containing **17,526
declarations**, plus **37 partial declarations** in the existing catalog slice.
Remaining: **1,489 files and 24,598 declarations**. Across tracks, **370 documentation
units are verified** and **1,002 remain**. Maintenance entries, unrelated unit/file
records, D archive and milestones are preserved. No dependencies were installed and
no commit or publication was performed. Token usage is unavailable.

Evidence helpers are historical records; do not replay preflight, authoring or
finalization scripts against this completed batch.

- [Observations and exact commands](test-results/docstrings-m097-m106-2026-09-27/observations.json)
- [Static proof](test-results/docstrings-m097-m106-2026-09-27/static.json)
- [Runnable examples](test-results/docstrings-m097-m106-2026-09-27/examples-all.json)
- [Example corrections](test-results/docstrings-m097-m106-2026-09-27/example-corrections.json)
- [Runtime-doc and generated-accessor review](test-results/docstrings-m097-m106-2026-09-27/runtime-doc-review.json)
- [M107 whole-file boundary](test-results/docstrings-m097-m106-2026-09-27/M107-boundary.json)
- [Metadata regressions](test-results/docstrings-m097-m106-2026-09-27/metadata.log)
- [Quality checks](test-results/docstrings-m097-m106-2026-09-27/quality.log)
- [Discovery](test-results/docstrings-m097-m106-2026-09-27/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m097-m106-2026-09-27/reconciliation.json)
- [Documentation diff](test-results/docstrings-m097-m106-2026-09-27/docstrings.diff)
- [Previous checkpoint](project-docstrings-m087-m096-2026-09-27.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
