# SQL utility docstrings complete: D116–D117

Started 2026-09-19; completed 2026-09-20. The request for “perhaps 50” was
interpreted as individual docstrings. An optional clarification received no reply;
the stated scope was D116–D117: **56 declarations in one complete file**.
**Stop here; D118 requires a new request.** Older broad authorizations remain paused.

Reviewed SQL/databasedriver/utils.py: adapters/converters, aggregate classes and
nested callbacks, collations, dynamic filtering and title/author sorting.
The file was promoted only after both exact selections passed.

| Unit | Declarations | Checkpoint |
|---|---:|---|
| D116 | 38 | [Verified](test-results/docstrings-d116-d117-2026-09-19/D116/observations.json) |
| D117 | 18 | [Verified](test-results/docstrings-d116-d117-2026-09-19/D117/observations.json) |

Verification:

- Executable AST, signatures, literals outside docstrings and comments unchanged.
  Original baseline hash preserved; no added Ruff findings or whitespace errors.
- All 56 declarations pass the auditor and normalizer. Prose paragraphs are
  wrapped for readability.
- **85 runnable examples passed**, including an in-memory SQLite aggregate query
  and the legacy callback factories. AST discovery includes nested definitions.
- **Two focused SQLite regressions passed**, no skips: container adapter
  round-trips and registered connection functions. The queued test_utils.py
  candidate exercises a different module and was replaced after source review.
- Full configured quality runner passed: 156 formatted files, 454 files with
  annotation coverage, 215 protected modules, basedpyright zero errors, mypy
  182 files; both contract checkers rejected all 37 invalid examples.

The initial collation example exposed an existing limitation: icu_collator
passes an explicit encoding to str and therefore rejects Python str operands.
Its documentation and example now use byte inputs. Failed and corrected evidence
is retained. The subsequent example/prose correction changed no executable code
or gated files, so the successful quality run was reused; static, example and
documentation checks were rerun on the final source.

Docs also explain lossy legacy set serialization, JSON converters without
container-type enforcement, last-value-wins indexed aggregates, empty/null
results, deliberate assertion behavior, and preference-backed title-pattern
caching. These are existing behaviors, not fixes or new guarantees.

Coverage: **840/2,671 files**, **12,120 declarations in complete files**
(840 modules, 1,206 classes, 10,074 functions), plus **37 partial-file declarations**.
Remaining: **1,831 files and 30,004 declarations**. D progress: **117/232 units**,
**2,892/5,685 declarations**, **154/374 complete files**. Across all tracks:
**149 documentation units verified, 1,223 remaining**.

Next: **D118**, the first selection in SQL/databasedriver/value_casting_mixin.py.
Pre-existing storage/adaptor changes and the architecture review were preserved.
No commit requested. Token usage is unavailable in this session.

- [Batch record](test-results/docstrings-d116-d117-2026-09-19/campaign.json)
- [Static evidence](test-results/docstrings-d116-d117-2026-09-19/static.json)
- [Examples](test-results/docstrings-d116-d117-2026-09-19/examples.json)
- [Example correction](test-results/docstrings-d116-d117-2026-09-19/example-corrections.json)
- [Regressions](test-results/docstrings-d116-d117-2026-09-19/regressions.json)
- [Quality](test-results/docstrings-d116-d117-2026-09-19/quality.json)
- [Documentation checks](test-results/docstrings-d116-d117-2026-09-19/documentation-checks.json)
- [Reconciliation](test-results/docstrings-d116-d117-2026-09-19/reconciliation.json)
- [Batch diff](test-results/docstrings-d116-d117-2026-09-19/docstrings.diff)
- [Plan](../dev-docs/project-docstrings-completion-plan.md)
