# Ten-unit metadata documentation batch: M087–M096 complete

The request for **ten more units** completed **M087–M096**:
**268 new declarations across four files**. Resources, series and subjects are
complete (**244 declarations in three files**); titles are partial at **24/95
declarations**. **M097 is next** on a new request. All **71 unselected title
declarations** remain unchanged, including the **27 selected for M097**.
The authorized ten are complete.

| Unit | Declarations |
|---|---:|
| M087 | 24 |
| M088 | 37 |
| M089 | 17 |
| M090 | 24 |
| M091 | 37 |
| M092 | 17 |
| M093 | 24 |
| M094 | 27 |
| M095 | 37 |
| M096 | 24 |

Source findings documented without executable changes:

- Resources display a truthy label before the URI, including whitespace-only
  labels. Validation checks URI blankness and nonnegative record position only;
  it does not check URI syntax, reachability, MIME type, kind or access/public hints.
  No resource is fetched or opened by these value objects.
- Series display prefers truthy numbering_text over non-None position_in_series,
  including zero. Bucket position and series position are separate. Authority
  scheme/identifier pairs are checked by truthiness; numbering consistency and
  numeric series-position bounds are not checked. Series joins use a semicolon
  and space; resource and subject joins use a comma and space.
- Subject and title records retain supplied normalized/sort text and reference
  hints. Validation checks stringified kind/display-text blankness and record
  position, without computing normalization or resolving references. Title payloads
  retain level-specific transliteration, title-page and cataloguer flags.
- Buckets retain supplied lists/dicts and shared record objects. Mutation checks
  shape and renumbers positions; full validation is explicit. An unmatched primary
  index clears all flags. Outer validation delegates to buckets without checking
  their ownership against the outer target.
- Resource and series bucket target_kind fields are constructor-overridable;
  subject target kinds are class constants. Subject sort_texts applies a truthy
  stored fallback without sorting. Primary-subject selection falls back to the
  first record without marking it primary.
- Generated bucket getters register missing buckets; text reads do not. Payload
  methods return new dictionaries/lists without validating or persisting data.

Verification:

- All **268 selected declarations** pass audit and normalization checks. Full-file
  checks pass for **244 declarations across three complete files**. Executable AST,
  signatures, decorators, annotations, comments and data literals are unchanged.
  Original/maintenance baseline hashes, prior source work and unselected docs are
  preserved. No added Ruff findings or whitespace errors.
- **96 existing regressions passed**, with no failures, skips or expected failures:
  package/API parity, relation properties and symmetry, concrete relation-container
  contracts, family smoke and formatting. No live database or network service was used.
- **748 runnable statements passed**, with no skips or failures, importing the four
  selected modules using isolated preference/config directories. Every selected
  declaration has runnable examples. Abstract contracts use concrete implementations
  and nested closures use installed public accessors; there are no command-only examples.
- No explicit runtime doc dispatch occurs in the original four files. Python's
  property constructor propagates new getter docs to generated descriptors. **204
  accessors across 68 buckets** passed captured-default, doc-propagation, empty-read
  and bucket-registration checks: 108 resource, 48 series and 48 subject accessors.
- **139 reused container functions** have executable AST-equivalence proof against
  previously reviewed rating/label implementations after explicit family-name,
  series separator and diagnostic-label substitutions. Record behavior, class
  fields and fixed stem tables were reviewed independently.
- All **71 unselected title source segments and docstrings** are unchanged.
  M097 remains queued with its exact 27-declaration scope; later title scopes
  are preserved too. The title file remains outside the complete-file manifest.
- Full quality runner passed: **156** formatted files, annotation coverage over
  **454** files, dependency checks over **215** protected modules, zero basedpyright
  errors, mypy over **182** files, and both type checkers rejecting all **37** invalid
  contract examples. Final selected-source hashes match all verification records.
- Discovery remains **2,671 files**, with no new or missing paths. The same **37
  unrelated storage review-hash differences** remain, unchanged from the previous
  checkpoint. These sources and review records were not edited by this batch.
  The architecture-review notes and their index entry are retained.

M progress: **96/274 units**, **2,357/7,198 declarations**, **110/344 complete files**.
D remains archived complete at **232/232 units**, **5,685 declarations** and
**374 files**. Project coverage is **1,170/2,671 complete-file review records**,
containing **17,246 declarations**, plus **61 partial declarations**: 37 in the
existing catalog slice and 24 in titles. Remaining: **1,501 files and 24,854
declarations**. Across tracks, **360 documentation units are verified** and
**1,012 remain**. Prior maintenance entries, unrelated unit/file records, D archive
and milestones are preserved. No dependencies were installed and no commit or
publication was performed. Token usage is unavailable.

Evidence helpers are historical records; do not replay preflight, authoring or
finalization scripts against this completed batch.

- [Observations and exact commands](test-results/docstrings-m087-m096-2026-09-27/observations.json)
- [Static proof](test-results/docstrings-m087-m096-2026-09-27/static.json)
- [Runnable examples](test-results/docstrings-m087-m096-2026-09-27/examples-all.json)
- [Runtime-doc and generated-accessor review](test-results/docstrings-m087-m096-2026-09-27/runtime-doc-review.json)
- [M097 and all unselected title declarations](test-results/docstrings-m087-m096-2026-09-27/M097-boundary.json)
- [Metadata regressions](test-results/docstrings-m087-m096-2026-09-27/metadata.log)
- [Quality checks](test-results/docstrings-m087-m096-2026-09-27/quality.log)
- [Discovery](test-results/docstrings-m087-m096-2026-09-27/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m087-m096-2026-09-27/reconciliation.json)
- [Documentation diff](test-results/docstrings-m087-m096-2026-09-27/docstrings.diff)
- [Previous checkpoint](project-docstrings-m077-m086-2026-09-27.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
