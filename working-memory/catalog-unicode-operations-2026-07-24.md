# Catalog Unicode operations coverage

Date: 2026-07-24

## Fresh full-run baseline

The completed external rerun is
`working-memory/test-results/coverage-2026-07-24-005354`:

- pytest: `5362 passed, 58 skipped, 17 xfailed, 2 failed`;
- statements: `120527 / 210132` (`57.36%`);
- branches: `29790 / 73098` (`40.75%`);
- combined statement/branch coverage: `53.07%`.

The two failures are isolated to the Python `bzzdec` fallback timing out on
two-byte inputs. Coverage XML, HTML, and the raw coverage database completed
successfully, so the report is valid for Catalog triage. The failures are not
on Catalog call paths.

Raw Catalog misses are still dominated by the frozen `metadata_tools` and
`catalog_macros` reference implementation. Those packages have no production
callers and are not targets for new behavior. The live gaps selected from this
run were coordinated metadata mutation, Agent repository/matching behavior,
logical titles, WEMI retrieval, and schema-selected writers.

## Added contract

`tests/catalog/test_catalog_unicode_operations.py` adds twelve real-database
behavioral tests. They use the shared multiscript, control-character, long
text, path-shaped, and SQL-injection-shaped corpora rather than isolated
non-ASCII examples.

The contract covers:

- exact Work persistence and inert SQL-shaped text;
- NFKC/case/whitespace-equivalent Work, Agent, alias, Tag, Label, Genre,
  Subject, Series, Synopsis, and Note identity;
- atomic multiscript WEMI-stack creation, provenance, structured metadata
  attachment, bundle retrieval, and display projection;
- requested and existing Work-ID stack replacement;
- embedded logical title create/update/delete behavior across Work,
  Expression, Manifestation, and the unsupported Item boundary;
- Unicode Notes, Comments, and Synopses on every schema-declared WEMI route,
  including rollback when a relationship is unsupported;
- normalized same-table and shared-value link writers;
- existing Agent/identifier attachment, malformed-group preflight, and
  rollback;
- Work and Item merges, missing-value fill, relationship transfer, and
  same-scheme primary-identifier demotion;
- Agent identifier ambiguity, conflict, decisive resolution, and type
  conflict;
- complete Person and Organisation aggregates with Unicode sidecars,
  aliases, provenance, notes, synopses, native language, and parent relation;
- role-scoped credit replacement while preserving other roles; and
- aggregate rollback after a late invalid identifier priority.

## Defects exposed and repaired

1. Agent alias deduplication used raw `casefold()` while matching used NFKC,
   case, and whitespace normalization. Canonically equivalent aliases could be
   stored twice. Alias deduplication now uses the shared repository
   `normalise_text(...)` rule.
2. Aggregate Agent creation stores aliases with `(#BREAK#)`, but
   `AgentMatcher` did not split that delimiter. Aliases created through the
   current aggregate API therefore could not match. The matcher now recognizes
   the canonical separator as well as legacy semicolon, pipe, newline, and JSON
   forms.
3. Ordinary Agent `create`, `update`, and `match_or_create` did not normalize
   alias sequences. `MetadataWriter.attach_metadata(...)` could consequently
   pass a tuple to SQLite and fail during parameter binding. Agent repository
   create/update now share the documented string-or-iterable alias contract.

## Coverage result

The external full-run data was combined with the focused Unicode lane without
modifying the saved external artifacts. Across the selected active
Catalog-operation modules, Coverage.py reports `87%` combined coverage.

Notable live boundaries:

| Module | External data on current sources | Combined after tranche |
| --- | ---: | ---: |
| `catalog/mutations/metadata_writer.py` | 77% | 90% |
| `catalog/repositories/agents.py` | 47% | 89% |
| `catalog/matching/agent_matcher.py` | 51% | 84% |
| `catalog/repositories/titles.py` | 45% | 79% |
| `catalog/retrieval/bundles.py` | 90% | 90% |
| `catalog/write/link_update.py` | 99% | 99% |
| selected active operation set | 81% | 87% |

The remaining branches are mostly invalid argument forms, simulated
schema-discovery failures, malformed legacy identifier rows, and abstract or
defensive guards. The next Catalog tranche should prioritize the 71% Identifier
repository and the 71% Work repository only where a caller-visible operation
or corruption/rollback invariant justifies the test.

## Verification

- Unicode/hostile Catalog lane: `12 passed in 48.42s`;
- branch-instrumented Unicode lane: `12 passed in 76.61s`;
- complete Catalog regression: `490 passed in 341.65s`;
- terminal creator/organisation/publisher callers:
  `9 passed, 165 deselected in 53.43s`;
- isolated strict Catalog mypy scope: no issues in `74` source files;
- `py_compile`: passed for both changed production modules and the new test;
- `git diff --check`: passed.

No new full-tree coverage percentage is claimed after this tranche; the
`53.07%` aggregate belongs to the external rerun before these twelve tests.
