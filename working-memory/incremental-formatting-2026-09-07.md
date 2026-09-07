# Incremental formatting — 2026-09-07

## Scope and implementation

Stage 5 follows the pushed stages 1–4 checkpoint `859ab804` on
`codex/package-calibre-resources`. It is committed together with stage 6 in the
checkpoint containing this note; that checkpoint has not been pushed.

- `[tool.liuxin.format].paths` is the explicit formatting scope: 109 Python
  files covering the prior modern lint scope, Core/storage CLI regression
  tests, the new formatter helper/tests, and static internal-call examples.
- `scripts/run_format_checks.py` checks by default; `--write` is the explicit
  rewrite operation and `--dry-run` resolves and displays the command only.
  It validates every target before invoking repo-local Ruff, includes new
  nested Python modules, deduplicates overlaps, rejects missing/empty/escaping
  scopes, and uses the root configuration even from another working directory.
- The normal quality runner and CI use check mode and propagate failures.
  The new helper/tests enter the lint gate, and the helper enters complexity-10
  checking. Existing production typing and dependency scopes are unchanged.
- Ruff is pinned to 0.16.5 in the typing extra and runtime configuration; the
  existing Python 3.12/88-column style now explicitly uses LF line endings.
- Initial formatting changed 21 of 109 files. Most application edits normalize
  endpoint-provider indentation; the extracted workflow owners were already
  clean. No application behavior is intentionally changed.

Canonical guide: [maintainability quality gates](../dev-docs/maintainability-quality-gates.md#incremental-formatting).

## Line-sensitive negative type examples

The first quality run exposed a real interaction: formatting moved trailing
`expect-error` comments away from the lines reported by the type checkers.
Both checkers still rejected the mistakes; the exact-line verifier correctly
refused to accept the misplaced expectations.

Markers now sit on the offending argument or definition line. The one call
whose location differs between checkers uses separate markers with `-` in the
non-applicable checker slot. The verifier still matches exact file/line/rule,
rejects unexpected diagnostics, and requires actual negative expectations.
No formatter skips, line-range matching, or type suppressions were introduced.

## Verification

- Formatter check: all 109 selected Python files are clean.
- Full quality gate passed: 147 selected source files pass strict mypy,
  basedpyright reports zero production errors, and both checkers accept valid
  calls and reject all 25 negative examples. Annotation, lint, complexity,
  and 105-module dependency checks remain green.
- Broader Core/direct-RPC, storage, CLI, shared-surface, failure, documentation,
  and architecture regression run: **459 passed**.
- Final formatter, quality-runner, and diagnostic-verifier contracts:
  **49 passed**, including version enforcement, exact-line marker handling,
  and coverage of every modern lint target. These overlap the broader run.
- Final AST comparison against `859ab804`: all 17 formatting-only Python files
  retain identical complete syntax trees. The static fixture's tree is also
  identical apart from its explanatory module docstring; its 25 mistakes and
  valid examples are unchanged.
- Relative handoff links resolve, `bash -n scripts/run_type_checks.sh` passes,
  and `git diff --check` is clean.

The selected database tests use SQLite and local Core/RPC sockets; no
PostgreSQL, full-project, or remote-CI sign-off is implied.

Stage 5 is complete and committed together with stage 6. Pushing this checkpoint
remains a separate action.

## Remaining programme

Further packages can enter formatting package by package after review and
verification. Stage 6 subsequently repaired the deferred CLI cycle and expanded
the reviewed formatter scope to 118 files; see [CLI dependency direction](cli-dependency-direction-2026-09-07.md).
The terminal cycle and lower-level compatibility recovery policies remain
separate maintenance work, not part of this tranche. Counts above record the
stage-5 checkpoint, not the subsequent stage-6 scope.
