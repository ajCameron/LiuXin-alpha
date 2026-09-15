# C02 — curated identifier repository and API, 2026-09-12

Fully reviewed and documented the
[curated identifier repository](../src/LiuXin_alpha/catalog/repositories/identifiers.py)
and [API](../src/LiuXin_alpha/catalog/api/repositories/identifiers.py): two modules,
two classes, and 22 functions, totaling **26 declarations**. The private assignment
and owner-listing helpers previously had no docstrings. No executable code changed.

## Contracts clarified

- Shared normalization returns canonical comparison text separately from the
  stripped original value; provenance/hints are retained without validation.
  Generic CRUD does not itself apply this scheme-specific normalization.
- Find and match select a logical storage copy across all owners, ignoring
  provenance and primary flags. Duplicate storage copies do not imply ambiguity
  here; Work/Agent matchers resolve ownership separately. Invalid stored rows can
  be skipped while invalid incoming candidates raise.
- Match-or-create can reuse an already-owned row without changing spelling or
  provenance. Only a new row starts unowned. Matching and insertion do not share
  an outer transaction or guarantee concurrent uniqueness.
- Assignment requires the destination before the identifier/priority. Compatible
  ownership components are updated; otherwise copying preserves the original owner
  and all fields except the ID and timestamp-suffix fields. No destination
  deduplication or encompassing assignment transaction is added.
- Priority is only a primary-flag input: zero sets, another non-boolean integer
  clears, and None preserves. It is not an ordering value or uniqueness check.
- Owner lists scan/filter direct ownership fields, return primary and non-primary
  rows in repository-ID order, and add no relationship metadata. Primary projection
  returns stored strings without normalization and rejects duplicate exact scheme
  keys; the previous normalized-output promise was inaccurate.
- Replacement validates/normalizes the full mapping before owner checks, then
  matches/assigns desired primary identifiers and deletes omitted owned rows inside
  one macro transaction. Empty replacement clears all rows for that owner. Other
  owners survive through copying, and failures roll back writes in the transaction.

## Verification

- **15 focused regressions passed**, 18.50s: scheme normalization, normalized
  equality/reuse, deterministic copies versus ambiguous owners, scheme collision
  rejection, injected rollback, owner isolation, cache consistency, Agent identifier
  evidence, primary metadata projection, and Catalog API documentation contracts.
- Source doctests: **4 passed, 20 skipped**, 7.21s. Normalization and runtime protocol
  membership examples run; database-dependent examples are explicitly skipped.
- Both complete files pass the strict audit and normalizer. All 26 declaration
  names and executable ASTs match pre-edit snapshots; tokenized comments and
  headers are preserved. Ruff remains **3 to 3**, with no added findings.
  Root/data-submodule whitespace and checkpoint-link checks pass.

Runtime doc consumers are the Catalog API documentation checks using inspect.getdoc
and source inspection. No new runtime-doc exception was needed. The broad quality
runner and whole-project audit were not repeated for this bounded module.
[Observations](test-results/project-docstrings-C02-2026-09-12-observations.json)
record hashes, declaration selections, static evidence, terminal exits, inventory,
and links. Matching log/done files use `regression`, `doctests`, and `static` suffixes.

## Progress and resume

Reviewed coverage advances to **668/2,732 files**, 874 classes and 7,211 functions
(**8,753 declarations**). Catalog source coverage is 43/113; concrete repositories
are 11/13 and repository APIs 13/15. Remaining: **2,064 files, 33,515 declarations,
1,370 queued documentation modules**. Baseline counts/hashes remain historical.

**C02 is complete. Stop here.** Next is **C03**, observed Item identifier repository
and API documentation, on explicit request. No commit or push was made.
