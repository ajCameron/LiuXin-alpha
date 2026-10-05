# C01 — Agent repository and API documentation, 2026-09-12

Completed the next planned module after G02, as requested. There is no G03 in the
plan. Fully reviewed and documented the
[Agent repository](../src/LiuXin_alpha/catalog/repositories/agents.py) and
[Agent API](../src/LiuXin_alpha/catalog/api/repositories/agents.py): two modules,
two classes, and 27 functions, totaling **31 declarations**. Three private helpers
previously had no docstring; all existing descriptions were reviewed too.

## Contracts clarified

- Alias normalization consumes iterables, preserves first spellings, and deduplicates
  by normalized text. Strings are single aliases; this helper does not split/escape
  embedded storage delimiters. Public/storage alias fields are prepared independently.
- Generic create/update affect core Agent rows. Update normalizes mapping/alias
  input before checking existence, then delegates remaining field validation.
- Person/organisation helpers create subtype rows and sequential metadata inside
  a macro transaction. Parent preflight happens outside it, requires an organisation
  subtype row, and validates relation_type only when a parent is supplied. Aggregate
  creation requires repository binding even with empty optional metadata.
- Identifier creation precedes priority validation; an aggregate caller supplies
  rollback. The private metadata helper alone can leave earlier writes on failure.
- Resolve scans canonical names and returns the first normalized match in requested
  Agent-ID order. It does not search aliases, filter by role, or enforce uniqueness;
  the previous API description incorrectly promised those behaviors.
- Matching delegates identity decisions. Reuse neither updates details nor checks
  subtype-row presence; creation transactions do not encompass the preceding match.
- Credits preserve role scope and existing priorities when omitted. Portable SQL
  traversal uses descending priority, correcting the API's previous lower-first
  claim. Replacement preflights endpoints outside its macro transaction and can
  reject duplicate logical credits; listing may repeat an Agent across roles.

## Verification

- **13 focused regressions passed**, 19.35s: duplicate names, aliases/types,
  identifier evidence, Unicode identity, subtype round trips/reuse, aggregate
  rollback, role replacement, preserved priority, dangling-link handling, and the
  Catalog API documentation contracts. Exact selectors are saved in the inventory.
- Source doctests: **6 passed, 23 skipped**, 8.86s. Pure normalization, preparation,
  blank-name resolution, and class examples run; examples needing a live Catalog
  are explicitly skipped and related behavior is exercised by the selected tests.
- Strict audit and normalizer pass for both full files. All 31 declaration names
  are preserved. Executable ASTs, tokenized comments, and module headers match the
  pre-edit snapshots. Ruff remains **3 to 3**, with no added findings. Root and
  data-submodule whitespace checks pass.

Runtime doc consumers are the Catalog API documentation contracts using inspect.getdoc.
No implementation, signature, constant, fixture, or assertion changed, and no new
runtime-doc exception was needed. This is a bounded module, not a subsystem
milestone; the whole-project audit and broad quality suite were not repeated.
[Observations](test-results/project-docstrings-C01-2026-09-12-observations.json)
retain current hashes, selected declaration IDs, test exits, static comparisons,
inventory reconciliation, and checkpoint-link verification. Matching log/done
records use `regression`, `doctests`, and `static` suffixes.

## Progress and resume

The reviewed manifest advances from 664 to **666/2,732 files**, with 872 classes
and 7,189 functions: **8,727 reviewed declarations**. Catalog source coverage is
41/113; concrete repositories are 10/13 and repository APIs 12/15. Remaining work:
**2,066 files, 33,541 declarations, 1,371 queued documentation modules**. Baseline
counts/hashes remain historical; current inventory values record C01's progress.

**C01 is complete. Stop here.** Next is **C02**, curated identifier repository/API
documentation, on explicit request. Guard modules G01/G02 remain verified. No
commit or push was made.
