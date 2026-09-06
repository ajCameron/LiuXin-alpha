# Maintainability checkpoint — 2026-09-07

## Current state

Stages 1–4 of the readability/maintainability review are complete and grouped
in this checkpoint on `codex/package-calibre-resources`. The earlier packaging
work is already in the branch history. Stage 5 has not begun.

| Stage | Completed scope | Detailed handoff |
| --- | --- | --- |
| 1 — Internal contracts | Named storage helpers and Core handlers; positive and negative typing checks | [Internal call contracts](internal-call-contracts-2026-09-06.md) |
| 2 — Workflow ownership | Bounded Core services and storage CLI commands; typed evacuation safety workflow | [Workflow extraction](workflow-extraction-2026-09-06.md) |
| 3 — Dependency direction | Cache-writer and shared-surface leaf imports; combined-context dependency checks | [Dependency direction](dependency-direction-2026-09-06.md) |
| 4 — Failure visibility | Failed reads remain errors; supported absence and explicit capability outcomes remain distinct | [Read-model failure visibility](read-model-failure-visibility-2026-09-06.md) |

Canonical developer guidance:

- [Maintainability quality gates](../dev-docs/maintainability-quality-gates.md)
- [Core workflows and storage CLI ownership](../dev-docs/core-program-workflows.md)
- [Read-model failure boundaries](../dev-docs/read-model-failures.md)

## Verification at stage-4 completion

- The quality gate passed: zero production type errors, strict mypy in 147
  selected files, 25 negative examples rejected by both checkers, and 105
  protected modules in the dependency gate. Lint, complexity, and annotation
  checks also passed.
- Final read-model/image/HTTP failure contracts: **199 passed**.
- Final affected real-database surface regressions: **80 passed**.
- Final Core application, direct/RPC failure, and cross-surface acceptance
  tests: **22 passed**, with explicitly permitted local sockets.

Earlier stage-specific checks are recorded in their handoffs. Runs can overlap;
do not add them into a distinct-test total. The database runs used SQLite, not
PostgreSQL, and do not constitute a full-project test run.

## Checkpoint verification — 2026-09-07

The final tidy-up changes documentation and docstrings only; it does not alter
the completed stages' runtime behavior. Historical handoffs now distinguish
their original verification counts from the current state, and obsolete
uncommitted/next-stage wording has been removed.

- `bash scripts/run_type_checks.sh`: passed, retaining the 147-file typing,
  25-negative-example, and 105-module dependency checks.
- Focused documentation, workflow ownership, type-contract verifier, quality
  runner, dependency, storage composition, evacuation, facade compatibility,
  shared-surface, and failure-contract tests: **339 passed**.
- Core application, read-model direct/RPC failures, and cross-surface acceptance
  rerun: **22 passed**, with explicitly permitted local sockets.
- Syntax/undefined-name checks of the three docstring-edited modules passed.
- All 21 relative links across the checkpoint and five stage/baseline notes
  resolve. `git diff --check` is clean.

The broader 80-test surface run above is retained as stage-4 evidence, not
claimed as a new checkpoint rerun.

## Next: stage 5 — incremental formatting enforcement

Start with the extracted Core services, storage CLI commands, shared helpers,
and their tests. Define an explicit maintained-code scope, make it formatter
clean, and enforce `ruff format --check` through the local quality runner and
CI. Test the runner integration and document how the scope grows. Keep this
mechanical work separate from the stages 1–4 behavioral checkpoint.

Existing lint/type/dependency gates must remain green. No whole-tree formatter
gate is claimed: inherited and vendored code stays outside this first slice.
Deferred CLI/terminal cycles and lower-level compatibility recovery policies
also remain separate work.
