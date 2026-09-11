# CLI storage source completion — 2026-09-10

Continuation of the [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following [storage administration](project-docstrings-cli-storage-administration-2026-09-10.md).
This records the completed source batch whose handoff was interrupted by the PR
status request. The following [application/operator batch](project-docstrings-ingest-application-operator-2026-09-10.md)
rechecked the strict audit of all CLI sources and completed the final lifecycle
scratch checks. Earlier regression results below retain their original scope.

Branch remains `codex/project-docstrings`, based on `edf6bf05`. No implementation
change, test assertion change, guard relaxation, commit, push, or PR update is
part of this documentation batch.

## Source-reviewed contracts

Twelve files, all under `src/LiuXin_alpha/surfaces/cli/storage_commands`:

- [ingest_config.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/ingest_config.py):
  an explicit database without a profile/root bypasses ambient defaults. Profile
  application can partially mutate arguments before a later missing-database
  refusal. Early validation is not schema/readiness or free-space validation.
  Tiny positive GiB values truncate to zero; boolean values pass float conversion;
  NaN/infinity can fail later conversion. Backend NaN passes the early <=0 test
  but is rejected by coordinator validation.
- [ingest_paths.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/ingest_paths.py):
  source, control, database, and materialization checks run in that order without
  reservation or exhaustive pairwise collision checks. A dangling report symlink
  can pass existence preflight but fail no-clobber publication. Log fallback and
  lock naming are documented; different log directories permit different locks
  for the same database. Discovery/preflight disable the lock. Lock records remain
  after release; body LockError also becomes a competing-ingest usage error.
- [ingest.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/ingest.py):
  early configuration failures return usage status before JSON/log setup. The
  outer logging catch also covers dispatch and cleanup, so a late OSError can
  be labeled logging initialization failure. Signal and lock scopes end before
  report publication. Latched signals override a nominal success; late log/report
  failures can follow a published success report or persistent application work.
- [ingest_run.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/ingest_run.py):
  either preview flag selects discovery; preflight wins the label if both are
  supplied directly. The application retains rich report objects. Preflight
  readiness combines report health with error-severity checks, not warnings;
  failed-check counts still include warnings. Console callbacks recognize only
  their exact event names and require the corresponding detail keys.
- [ingest_preflight.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/ingest_preflight.py):
  checks observe current access/tool availability, not reservations, successful
  decoding, or enough free space for the budget. Existing-parent selection may
  find a file. Disk free bytes are reported without a budget comparison. Optional
  RAR/ISO support is warning-level while missing SquashFS/7z support is error-level.
- [ingest_reporting.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/ingest_reporting.py):
  start-event argument filtering is not general secret sanitization. Enrichment
  records artifact paths without proving publication. Report publication creates
  parents before serialization, writes/fsyncs a temporary file, then replaces or
  hard-links without clobbering. Late cleanup errors do not undo publication.
  Failure reporting can preserve an earlier occupied report while printing a
  failure receipt. Disabled stdout skips serialization; BrokenPipeError is swallowed.
- [store_wizard.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/store_wizard.py):
  plans retain descriptor references despite frozen fields. Name generation is
  not uniqueness validation. Backend choice matches exact kind, not descriptor
  aliases. Blank advanced answers preserve nonempty defaults even where labels
  suggest clearing. Summaries omit some options/fields and are not secret scrubs.
  Plan application copies tags/options but leaves other arguments intact. Final
  confirmation precedes argument mutation; saving opens a fresh session rather
  than committing a reserved provider snapshot. Fully specified automation
  defaults check to true; interactive selection requires a TTY before Core access.
- [ingest_options.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/ingest_options.py),
  [parser_files.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/parser_files.py),
  [parser_integrity.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/parser_integrity.py),
  [parser_stores.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/parser_stores.py),
  and [parsers.py](../src/LiuXin_alpha/surfaces/cli/storage_commands/parsers.py):
  every builder documents exact grammar/defaults, bytes-versus-JSON output,
  preview/confirmation boundaries, and validation deferred to execution. The
  top-level Store-add grammar differs deliberately from compatibility store add.
  Parser registration performs no probing or file publication.

Batch: **12 modules, 1 class, 66 functions = 79 AST declarations**. At this
checkpoint the reviewed set reached **271 modules, 276 classes, 2,978 functions
= 3,525 declarations**. All **47 CLI source modules, 10 classes, and 394 functions**
are now source-reviewed and pass the strict audit. This is not a claim that every
CLI-related regression module or the whole project was documented at that point.

## Verification

- Strict twelve-file audit and normalizer passed, as did both checks across all
  271 reviewed Python files. All twelve executable ASTs matched HEAD after genuine
  leading-docstring removal, without a new runtime-doc exception. Ruff was clean.
- Full quality gate passed: 159 formatter files, 456 annotation-covered modules,
  221 protected dependency modules, 188 strict-mypy files, and 37 invalid examples
  rejected per checker. No limits or scopes changed.
- Twelve-source doctests plus storage/operator-hardening/operational/dependency/
  initialization regressions: **142 passed, 60 explicitly skipped**, 131.40 seconds.
- Adjacent migration/public-doc/link/ownership contracts: **62 passed, 6 failed**,
  49.09 seconds. Same six failing guard parameters as the administration batch.
  Additional physical-line conflicts within the already failing storage parameter:
  ingest_preflight._preflight_checks spans **166 > 160** lines; ingest_reporting.py
  has **461 > 450** lines; store_wizard.py has **539 > 450** lines. The first reported
  storage failure remains administration.py at **623 > 450**. No guards were altered.
- All **298 parser paths**, root help, and bash/zsh/fish completion fingerprints
  matched the CLI-foundation baseline. Argument/help literals were unchanged.
- Isolated temporary/mock checks passed numeric corner cases, profile partial
  mutation, dangling-link report refusal, log fallback/lock scope, actual advisory
  lock record retention, body LockError translation, warning/error preflight
  policy, parent creation before failed serialization, disabled/broken-pipe stdout,
  and failure reporting against an already occupied success report.
- Application-event collision, discovery request, wizard exact-kind selection,
  retained defaults, summary omissions, plan application, automation defaults,
  and cancellation-before-save checks passed. No external Store was modified.
- The final extra lifecycle check's output was lost at handoff. On resumption,
  a process listing confirmed no verification process remained live; a fresh
  focused check passed cancellation override, report publication before terminal
  event failure, early refusal without logging/stdout, and late-cleanup usage
  classification. No unobserved result was counted as a pass.

Historical whole inventory at this checkpoint: **2,730 modules, 4,400 classes,
35,119 functions**, no parse failures; missing/blank docs on **1,100 modules,
2,015 classes, 22,197 functions = 25,312 declarations**. Other overlapping counts:
4,140 delimiter, 10,408 example, 3,016 parameter-field, 3,955 return-field,
4,073 empty-param, 5,505 empty-return, and 28 summary findings.

Reports `/tmp/liuxin-docstring-cli-storage-completion-2026-09-10.json` and
`/tmp/liuxin-docstring-cli-complete-2026-09-10.json` remain bounded clean audits.
The shared current/reviewed inventory paths advance with subsequent batches.
The next batch completes the operator-hardening suite and ingest application
boundary; the wider source/tests/scripts/examples/data-submodule goal stays active.
