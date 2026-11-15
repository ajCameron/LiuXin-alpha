# Database sign-off follow-up — in progress

Active goal: address the remaining review work, document qualifications and
deferred scope, then prepare the database changes for PR/sign-off. The prior
RR03–RR06 work is still uncommitted above 5d5c0c88. Preserve the unrelated dirty
LiuXin_alpha_data submodule and untracked file-formats review.

Implemented this turn (verification in progress):
- RR07: plain DatabaseIntegrityError text; separate recovery command line;
  integration test executes displayed command with spaces/apostrophe in path;
  Windows rendering regression added.
- RR08: explicit create required by facade/Core; both SQLite names connect with
  mode=rw unless private creation. Missing-file and disappearance-race tests.
  Two tmpdir fixture tests now request creation explicitly. Other callers audited.
- RR09: LIUXIN_SCRATCH_DIR native override; session and per-test environment
  isolation, inherited by children. Real pytest subprocess test of collection,
  session setup/teardown and per-test native scratch use. Exact historic caller
  remains unknown; document this rather than claiming a traced reproduction.
- RR10: inspect generated SQLite schema before fixtures can create missing tables.
  Existing tests already catch column renames; review qualification appended.

Current processes at write time (revalidate handles):
- Complete database gate session 25231, /tmp/liuxin-database-signoff-gate.log,
  artifacts /tmp/liuxin-database-signoff-gate-2026-11-15. Verified source snapshot
  /tmp/liuxin-database-signoff-tree-2026-11-15 (4637 files); absolute PYTHONPATH
  ensures children also import snapshot code. No implementation edits afterward.
- Private PostgreSQL started at localhost:55439, database liuxin_followup_utf8,
  user blackjane, data /tmp/liuxin-cc-followup-pg/data. Stop after validation.

Completed verification:
- Focused session 76166 exit0: 110 passed in352.31s. Windows renderer test was
  added after its collection, then included in the consumer run below.
- Consumer session 9708 exit0: 98 passed,46 deselected in121.13s. Includes Core
  lifecycle, surface errors, runner contracts and actual displayed recovery.
- Quality session62275 exit0: full format/lint/architecture/type gates passed;
  380 format files,463 annotation files,25 cold imports,474 mypy files,95/93
  invalid-contract examples. Logs /tmp/liuxin-signoff-quality.log and
  /tmp/liuxin-signoff-consumers.log.
- Native PowerShell argument roundtrip passed via an in-memory function (script
  files are disabled by Windows policy, which was left intact). Artifact
  .tmp/database-signoff-2026-11-15/windows-result.json. No native Windows Python
  SQLite rebuild claim.
- Mandatory manifest now2448, retaining2401 and adding47; both HEAD5d5c0c88's
  2314 and remote main47156490's1444 requirements independently verified retained.

## Later correction before final full gate

Initial closure commit 1b4c450a is pushed on databases-signoff-2026-11-15; no PR
created yet. A clean detached worktree exists at
/tmp/liuxin-database-pr-tree-2026-11-15 on that commit. Local Git has no author
identity; supply the name/email from HEAD via per-command -c options (do not
change global configuration). GH network access requires escalation.

The first complete-gate attempt, session25231, was deliberately interrupted
with SIGINT to fix a confirmed Windows UNC URI regression. It is terminal exit1;
the interruption itself caused a .pytest_cache leak-guard teardown error.
Before interruption four tests had also failed: two bootstrap tests, both SQLite
backends, explicitly asked create=False for fresh paths. Their two creation calls
now use create=True; existing reopening/bootstrap checks remain intact.

SQLBaseDriver now emits file:////server/share for UNC paths, avoiding SQLite's
forbidden hostname authority. Four drive/UNC URI cases were added. Their focused
selection with missing/racing opens passed12,36 deselected (/tmp/liuxin-signoff-uri.log).
Mandatory manifest is now2456 (2401 retained +55); added four URI cases and four
bootstrap cases after the original47.

New active checks: bootstrap module session84070, /tmp/liuxin-signoff-bootstrap.log;
second full quality run session57877, /tmp/liuxin-signoff-final-quality.log.
URI collection session76105 and URI execution session34919 both terminal0.
AST explicit-create-false caller scan session83977 may still need output inspection.
Next: inspect these results; refresh source snapshot and manifest for the new
helper/tests; restart a full gate in a NEW artifact directory; update progress
monitor path. Old25231 must never be described as active/passing. Private PG still
running. Commit/push URI+fixture repairs, create draft PR, verify PR-tree docs,
finish complete gate and record final evidence. Original user leftovers preserved.

PR preparation: branch databases-signoff-2026-11-15 exists. GitHub access works
with escalated networking; sandboxed gh auth incorrectly reports invalid login.
Remote main47156490 is an ancestor of local5d5c0c88: PR includes prior three
database commits a88a5230,7780b7eb,5d5c0c88 plus closure fixes. No existing open PR.
Stage only the database row in architectural_reviews/README.md; leave the user's
file-formats draft row and untracked document out, along with dirty data submodule.

Still required: complete database gate with live private PostgreSQL and APSW;
validate documentation against the PR tree (excluding unrelated draft index row);
record exact evidence; prepare concrete PR without merging. Existing prior
verification is in working-memory/database-rereview-fixes-2026-11-14.md and
.tmp/database-rereview-fixes-2026-11-14/. Never claim old results cover new edits.

## Final gate restart

- Final gate session7916 is running against refreshed snapshot, log
  /tmp/liuxin-database-signoff-final.log, artifacts
  /tmp/liuxin-database-signoff-final-2026-11-15. /tmp/liuxin-signoff-progress.py
  now reads this directory only. Refreshed hashes are in ignored artifact
  final-snapshot-manifest.json. Initial25231 is terminal and not full evidence.
- Bootstrap module session84070 exit0:34passed in73.37s. Remaining explicit
  create=False constructor/caller uses were audited: provisioned or reopened
  catalogues; Core durable/reopen paths also already request create=True initially.
- Source/test/config are frozen again for the final gate. Further docs/PR metadata
  updates do not require a new gate; any production change must be accounted for.
- The supplemental URI/helper/bootstrap/manifest changes still need a follow-up
  commit/push. Initial1b4c450a is already pushed. Clean PR worktree exists but must
  be advanced to the follow-up commit before docs checks. No PR yet.

Second full quality run session57877 completed exit0 after the URI correction;
all format/lint/architecture/type checks and invalid-contract suites pass again.
