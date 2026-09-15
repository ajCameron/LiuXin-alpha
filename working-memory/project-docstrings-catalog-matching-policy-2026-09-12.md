# Catalog matching policy documentation — 2026-09-12

## Scope and checkpoint

Continues the whole-project goal after the
[Catalog foundation pass](project-docstrings-catalog-foundation-2026-09-12.md).
Branch remains `codex/project-docstrings` at `9790d020`. No commit or push; existing
example, Catalog foundation, working-memory, and data-submodule changes remain.

Read and documented these complete files, each unchanged from HEAD before editing:

- `src/LiuXin_alpha/catalog/matching/policy.py`
- `src/LiuXin_alpha/catalog/matching/exact_matcher.py`
- `src/LiuXin_alpha/catalog/matching/entity_specs.py`
- `src/LiuXin_alpha/catalog/matching/__init__.py`
- `src/LiuXin_alpha/catalog/api/matching_api/__init__.py`
- `src/LiuXin_alpha/catalog/api/matching_api/exact_entity_matcher.py`
- `tests/catalog/test_matching_policy.py`
- `tests/catalog/test_additional_entity_matching.py`

Batch: **8 modules, 6 classes, 62 functions = 76 declarations**, including every
private parser/scoring/scope helper, default factory, constructor/post-init method,
and named test helper. Forty-two previously missing function docs are now supplied;
other edits replace incomplete or inaccurate existing descriptions.

Cumulative [reviewed set](project-docstrings-reviewed-files.txt): **631 modules,
833 classes, 6,978 functions = 8,442 declarations**. Catalog source now covers
**11 of 113 modules**, Catalog tests **4 of 19**, the matching implementation tree
**4 of 8**, and matching API contracts **2 of 6**.

## Contracts clarified

- Policy values are independently checked in [0, 1] without float conversion or
  ordering constraints. Match-text normalization preserves Unicode word characters
  and underscores, expands ampersands, and removes other punctuation; exact text
  comparison preserves punctuation. Empty normalized strings score zero even
  against each other, and SequenceMatcher is a character comparison heuristic.
- Identifier normalization prefers a truthy normalized override, preserves original
  provenance/hints, and returns a new candidate. ISBN checks cover shape/checksum,
  not allocation or valid publishing prefixes. UUID parsing applies to uuid and
  calibre_uuid. DOI validation is deliberately limited; OCLC prefix stripping can
  produce an empty value. Generic schemes retain case-sensitive stripped text.
- Identifier hints preserve declaration order and duplicates, with different
  provenance behavior for supplied candidates versus mapping/pair forms. Agent
  hints retain first-seen distinct normalized names. Identifier-owner lookup returns
  identifier rows containing owner references, not fetched owner entities; it reads
  the repository before parsing hints and skips selected malformed stored values.
- Confidence uses positive weights without finite/range enforcement; nonfinite or
  overflowing arithmetic can escape the usual unit-interval interpretation. Final
  selection ranks decisive evidence first, so a below-acceptance decisive candidate
  can prevent selection of a higher-confidence nondecisive one. Ambiguity peers need
  not meet acceptance individually, duplicate IDs are not removed, and precomputed
  conflict entries with no selected ID are filtered rather than combined.
- Contextual matching trusts caller-supplied parent scope. Any exact identity field,
  or approximate identity plus exact corroboration, can qualify a row; disagreements
  still affect confidence. It does not generate terminal conflict decisions itself.
- ExactEntitySpec is a shallow frozen configuration, with mutable nested mappings
  and no schema validation. Candidate matching checks required scope/identity,
  removes supplied entity IDs, and combines all supplied non-None identity fields.
  Primary-field values can match any scalar field. Conflicts depend on incompatible
  nonempty per-field ID sets after complete-row matching has failed.
- candidates suppresses terminal missing-input/conflict results as an empty sequence,
  so callers need best for final decisions. It slices only after matching, including
  limit zero, and does not perform final acceptance/ambiguity selection. Approximate
  fallback cannot override terminal exact-stage decisions. Scalar exact lookup uses
  required scope but does not enforce full candidate required-identity fields.
- Group composition forwards its policy directly to Work/Agent matchers, while other
  repository factories use their already-bound policy. Constructing or later changing
  the group policy does not rebind those repositories/matchers. Partial factory
  failure retains earlier assignments. Name lookup exposes only eleven exact-entity
  families, not the specialized Work/Agent/identifier attributes.
- All eleven constant specifications retain their executable values, ordered registry,
  alias dictionaries, flags, and derived-column declarations. Test documentation
  describes the selected assertions and distinguishes error observations from absent
  post-error row-count checks. No validation, matching policy, or test assertion changed.

## Verification

- Eight-file executable AST comparison against `9790d020` passes after stripping only
  leading literal docstrings. Ruff remains **4 to 4**, with no added finding. Existing
  headers/comments and all specification/fixture constants remain unchanged. No new
  runtime-doc exception was needed.
- Strict audit and normalizer pass for the eight files and the complete **631-file**
  reviewed set. The final small wording changes receive another batch audit and
  AST/Ruff check; unchanged reviewed files retain the full-set verification.
- Full `tests/catalog`: **511 passed**, 214.24s.
- Eight-file doctest and matching-test selection: **57 passed, 50 skipped**, 95.10s.
  The selected normal tests overlap the full Catalog suite; totals are separate
  execution evidence. Skips describe existing database/matcher objects. Pure examples
  exercise actual normalizers, records, scoring, decisions, and small in-memory rows.
- Migration/public-documentation/developer-link contracts: **38 passed**, 49.22s.
- Full quality runner passed: 159 formatted files, 456 annotated modules, 221 protected
  dependency modules, lint/complexity, both production type checkers, 188 strict-mypy
  files, and both sets of 37 rejected invalid-call examples.
- All four pytest/quality process terminal exits were observed as zero. Durable
  log/done pairs are under `working-memory/test-results/` with prefix
  `docstrings-catalog-matching-policy-2026-09-12-` and suffixes `regression`, `doctests`,
  `contracts`, and `quality`.
- Independent AST recount confirms the manifest totals and Catalog coverage above.
  Root/data-submodule diffs are whitespace-clean, and handoff links resolve.
  The same results prefix with `observations.json` preserves final source hashes,
  AST/Ruff comparisons, reviewed counts, and all four process completion records.

Static reports: `/tmp/liuxin-catalog-matching-policy-ast-lint-2026-09-12.json`,
`/tmp/liuxin-docstring-catalog-matching-policy-batch-2026-09-12.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-12.json`, and
`/tmp/liuxin-docstring-catalog-matching-policy-2026-09-12.json`.

Whole-project scope remains **2,730 modules, 4,400 classes, 35,119 functions**, with
no parse failures. Missing/blank documentation: **1,065 modules, 1,894 classes,
20,083 functions = 23,042 declarations**, down 42. Other overlapping findings:
3,459 delimiter layout, 9,646 examples, 2,775 parameter fields, 3,629 return fields,
3,151 empty parameter descriptions, 4,119 empty return descriptions, 28 missing
summaries. These categories must not be added to the missing-docstring total.

## Next

The [specialized matcher continuation](project-docstrings-catalog-specialized-matchers-2026-09-12.md)
now completes all four concrete matchers and their four API modules. The counts
above remain this earlier batch's checkpoint. Continue from the newer note into
Catalog repository contracts and implementations, remaining Catalog owners/tests,
and the wider project backlog. The two main matching regression modules are
complete; do not repeat their prose pass. Repository dependency excerpts remain
read-ahead rather than completed-file claims.

Preserve the three narrow runtime-doc exceptions, all seven unresolved docstring-
sensitive ownership-guard failures, and the legacy FRBR fingerprint follow-up in
the [main ledger](project-docstrings-2026-09-08.md). Those seven guard tests were not
rerun or modified here. The whole-project goal remains active.
