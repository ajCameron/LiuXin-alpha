# Maintainability programme close-out — 2026-09-07

## Scope and current state

This is the bounded CI/documentation close-out requested after stage 8,
`f850cfc7`, on `codex/package-calibre-resources`. It does not start another
production-code refactoring stage. This note accompanies the final close-out
checkpoint for [PR #115](https://github.com/ajCameron/LiuXin-alpha/pull/115),
which also contains the packaging baseline and stages 1–8.

## Changes

- `.github/workflows/tests-on-push.yml` is the sole general-test workflow.
  Retired `review.yml` and its duplicate full suite. Its whole-tree syntax
  smoke check moves into the quality job with an honest name; actual lint
  stays in the existing quality runner. The deleted workflow remains
  recoverable from Git history.
- Preserve `Tests Passed` as a PR/manual summary, now depending on successful
  packaging, modern quality, both format lanes, and the full suite. The
  always-run summary explicitly fails for failed, skipped, cancelled, unknown,
  or wholly missing result input. Existing lane event selection and dependency
  extras remain intact. License, secret, and opt-in live-storage workflows
  are untouched and remain outside the Python summary.
- Read-only GitHub checks found `main` and this branch unprotected and no
  repository rulesets. No remote configuration was changed. The CI guide
  records the retired matrix status names for future required-check setup.
- Add [developer navigation](../dev-docs/README.md), linked from the root
  README, and [CI ownership](../dev-docs/continuous-integration.md). Distinguish
  maintained contracts from design notes and roadmaps.
- Replace developer-specific links in the test-database guide and the local
  startup example. Two March benchmark outputs are absent from the checkout;
  label them historical rather than invent replacement files or broken links.
- Add YAML-based workflow contracts and Markdown-based navigation contracts.
  The latter reject absolute file links throughout developer docs and verify
  local targets in seven reviewed guides; historical relative links, heading
  anchors, and external sites are not claimed as fully validated.
- Declare PyYAML and markdown-it-py in the `test` extra, not runtime dependencies.
  Both new suites enter CI and the formatter/lint scope. Formatter coverage is
  now 156 files; production typing remains 188 strict-mypy files, with 221
  protected dependency modules and 37 negative examples per checker.

## Verification

- Full `bash scripts/run_type_checks.sh`: passed. All 156 selected files are
  formatter-clean; annotation coverage remains complete across 456 modules;
  lint, complexity, the 221-module dependency graph, both production type
  checkers, and both sets of 37 negative examples pass.
- Focused close-out and adjacent tooling regression run: **129 passed** in
  77.35 seconds. Selected the new CI/link suites plus public documentation,
  quality-runner, formatter, internal-type-verifier, wheel-verifier, and
  terminal-component ownership contracts. The wheel-verifier tests are not
  an installed-wheel execution claim.
- The relocated `python -m compileall -q src tests scripts` syntax check
  passed against the whole source/test/script trees. Local bytecode output
  used a temporary cache outside the checkout.
- All **118** local link destinations across the seven reviewed guides resolve.
  Parsed **95** developer Markdown files, containing **131** link/image
  destinations, with no absolute file links. The root README is also guarded.
- Read-only YAML comparison proves the packaging, confidence, fast-format,
  heavy-format, and full-suite execution jobs are unchanged from stage 8.
  Production sources and the three separate workflows have no diff.
- The initial link-contract run caught the startup guide's missing navigation
  link; it was added before the successful final regression run.
- Final formatter/lint recheck of both new suites passed. The final diff is
  whitespace-clean; no unrelated changes are included.

## Completion boundary

The eight structural stages and this close-out define the completed improvement
programme. Wider compatibility typing/formatting, inherited command-family
implementations, and lower-level recovery policies remain ordinary incremental
maintenance, driven by concrete work rather than another automatic stage.

These local checks do not claim a fresh whole-project pytest run, installed-wheel
execution, live PostgreSQL, interactive curses, or remote-CI success.
Stage 8's 455-regression result remains historical evidence for the unchanged
production implementation.

## PR handoff — 2026-09-08

The user requested publication after reviewing the close-out summary. Reuse
the existing PR against `main`, rather than create a duplicate: publish the
stages-5–6, stage-7, stage-8, and final close-out checkpoints together. Refresh
its title and description to cover the complete branch, retaining the earlier
packaging evidence as historical verification rather than a fresh wheel result.

Publication preparation reran the two CI/documentation contract suites:
**46 passed**. Only working-memory publication status changed after those
checks; the broader 129-test run and full quality gate above remain the
implementation evidence. The PR's current head and checks are authoritative
for remote publication and CI status. No merge is requested; pause for review.
