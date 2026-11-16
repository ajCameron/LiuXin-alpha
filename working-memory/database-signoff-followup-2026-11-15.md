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

## PR opened and published head verified

Draft PR161: https://github.com/ajCameron/LiuXin-alpha/pull/161, published head
e7b45407fb4b8b80aa28006203acf8ec366df4e2. Both new commits1b4c450a/e7b45407
are pushed, alongside the prior three local database commits. The43 changed
implementation/test/config files exactly match the final gate snapshot bytes;
evidence committed-input-audit.json. PR is DRAFT, not merged or signed off.

Clean PR-tree docs/tooling/scratch checks session25581 running,
/tmp/liuxin-signoff-pr-tree-checks.log. PRtree is detached e7b45407; .venv and
LiuXin_alpha_data linked, its tracked LiuXin_data directory retained. User draft
index row and file are absent from PR tree. Main workspace only user leftovers
before this memory update.

CI run43775314559 is the PR run, jobs include Database Regression Ratchet
133000179110 and full suite133000178877. Push run43775198199 confidence
job132999817727 failed; reason being fetched. gh run view --job --log-failed
refuses logs while parent run active; use gh api repos/ajCameron/LiuXin-alpha/
actions/jobs/132999817727/logs, saving /tmp/liuxin-signoff-ci-confidence.log.
Other CI jobs running. Do not infer gate success from earlier job/check names.


## CI consumer corrections in progress

Clean PR-tree docs/tooling/scratch selection25581 finished:92passed in150.72s.
Confidence22496 finished:313passed,3skipped in562.79s. Live PostgreSQL follow-up
57517 finished:9passed in185.95s. No-psycopg collection probe92720 passed (1passed,
1skipped), confirming pytest.importorskip permits optional general collection;
mandatory database gate still requires all nine cases with the dependency installed.

CI e7b failed confidence because hydrator fake identified item_identifiers as items;
the full-suite collection also failed because review PG tests imported optional
psycopg2 unconditionally. Fixes are test-only, production Row schema validation
unchanged. Hydrator fakes now identify a unique declared schema, cache fake advertises
link-table columns, edge cases explicitly declare additional fields. Expanded
retrieval/metadata/Core/Tkinter selection98568 running; log
/tmp/liuxin-signoff-hydrator-final.log. Some further failures pending inspection.
No new lint diagnostics across five changed test files; existing item fake has nine
baseline lint findings and existing formatting differences outside quality scope.
Do not claim the entire file is lint/format clean or silently clean unrelated code.

Full database7916 continues in frozen snapshot without failures at >1000 completed.
No source code changed since e7b; only optional PG test import changed inside its
suite and independently tested both dependency states. Snapshot remains unchanged.
Do not stop private PG until7916 completes. PR161 remains draft and unmerged.


Expanded retrieval98568 finished:185passed,3failed,1skipped in292.45s. One
remaining edge fixture omitted file_folder_id (now declared). Two real-backend
failures exposed (a) cached link records incorrectly wrapped as Row against a
main-table-only cache schema; (b) local prebuilt metadata_rich_db_1 had no revision.

Correction (a) is in catalog/retrieval/hydration/read_sources.py: return immutable
CacheRecord mappings directly, as MetadataLinkRowSequence permits. Native Row
validation and CacheAPI main-table schema remain unchanged. Strengthened existing
read-source regression now uses real association columns with fallback disabled.
The temporary fake link-schema additions were REMOVED; keep only schema-based
identification in three doubles plus explicitly declared edge-case columns.
Correction (b): integration test now calls deterministic populate_bundle(tmp_path)
so it always generates the current format, without mutating/stamping old fixtures.

Current checks:10999 /tmp/liuxin-signoff-hydrator-repaired.log (four impacted
modules);61916 /tmp/liuxin-signoff-consumer-quality.log (quality after catalog
change). Focused lint/format for four new/changed consumer files passed60831.
Collection94031 complete0; redundant searches11720/57598 terminated/finished.

Review appended PR consumer follow-up with implementation/existing evidence;
final results still pending. This catalog source change is OUTSIDE frozen native
full-gate snapshot. Record that distinction and final focused/CI evidence; do not
claim full snapshot equals final PR source after this change.


Consumer verification10999 completed0:112passed,1 pre-existing catalog marker
warning in137.00s. All three failures from185-pass diagnostic run are covered by
this clean selection, including actual database/cache/lazy parity and write-back.
Review and PR body updated. Ten own files staged; README user's unrelated row,
untracked file-formats draft, dirty data submodule remain excluded. Quality61916
still running (format/architecture/import/lint done, types pending). Full7916
continues clean at1534 cases; private PG in use. Next commit/push after quality,
update PR body, preserve full gate then publish final evidence and stop own PG.


2026-11-16 00:01: quality61916 completed0 after the catalog correction; all
format/lint/architecture/import/type/invalid-contract checks pass. Publishing
consumer follow-up commit now, with112passing affected consumers. No native
production code changed after e7b. Full7916 remains active, no failures yet.


Published90f86685a56fcdffe33c4f959e2945e56494b24f to PR161. `gh pr edit`
fails on retired projectCards API; body updated successfully via REST PATCH
using /tmp/liuxin-database-signoff-pr-update.json (generated from body Markdown).
PR remains draft. New PR run43777977612 (database133008516332,
full133008516181); push43777974223 (confidence133008505489 alreadySUCCESS,
database133008505313). Older e7b native CI jobs still running, not canceled.
Private clean PR worktree advanced to90f; intentional data symlink shows type T.
Main workspace only original user leftovers before this memory update.

consumer-followup-audit.json records the ten files changed since e7b and proves
only production change is catalog read_sources.py; tests/databases difference is
optional PG import only. Progress monitor now counts mandatory fully passed
setup/call/teardown separately and reports mandatory nonpass events. At1840
completed:468/2456 mandatory passed, no violations or suite failures.


## Native Windows follow-up — 2026-11-16, CURRENT WORK

Further Windows verification found remaining UNC-authority URI constructions in
query_indexes rebuild, creation replacement snapshot, and sqlite_files backup.
New pure databases/sqlite_uri.py helper is now used by all four callers (including
SQLBaseDriver existing helper). Native file access modes unchanged. Six new
UNC-spelling/offline reader integration cases bridge to local SQLite files; they
cover rebuild/backup/replacement under both driver names. Manifest now2462.
No real Windows network share is claimed.

Initial10-case probe99603:8passed,2failed due my test helper's positional build
argument; fixed to build= before run finished, so trace source can show updated
text despite old loaded code. Final owning modules now running4390,
/tmp/liuxin-signoff-offline-operations-final.log. Quality96237 running,
/tmp/liuxin-signoff-offline-quality.log. Collection26355 is6new cases, alladded
to review_closure_open_and_recovery. New source helper added to format/lint scope;
both type checkers already include databases package.

Existing native Windows Python3.14.7 available via py.exe -3.14. Initial full
creation23074 failed at creation._sync: os.fsync(rb descriptor) raisesEBADF on
Windows. Corrected _sync to open owned temporary files r+b on Windows, rb on
POSIX. Windows probe79816 then PASSED full creation, displayed PowerShell recovery
on path containing spaces/apostrophe/#, reopened normalized search, and missing
open no-effects. SQLite3.50.4, Unicode16.0.0. Artifacts are ignored
.tmp/database-signoff-2026-11-15/windows-runtime/{probe.py,result.json}.
Windows script-file policy remains unchanged; only inline PowerShell invokes
Python. Isolated LIUXIN prefs/config/scratch env set per process.

Additional native Windows backup/replacement probe12124 running against that
same disposable catalogue; offline_probe.py will update result.json on success.

IMPORTANT: native source now CHANGED after e7b/90f; previous claim of unchanged
native source is historical only. Local frozen gate7916 has been interrupted,
no full-pass claim. Confirm terminal status, then stop owned private PostgreSQL
(only this old run still used it). New published commit needs fresh full CI gate
requiring2462. Current old CI jobs remain running; have not canceled them.
Need record these additional Windows findings in review/docs, finish focused and
quality checks, commit/push new native fix, and use fresh complete CI evidence.


Native Windows offline probe12124 completed0: standalone backup, replacement and
reopening source/both backups preserve normalized metadata. result.json updated.
Local gate7916 terminal1 after requested SIGINT (last preinterrupt2084completed,
705mandatory, no failures). Private PostgreSQL stopped successfully; pg_ctl status
exit3 confirms no server. No local PG callers remain.

Review appended Native Windows closure follow-up, creation doc updated, README
OWN database row now2462pending. Preserve unrelated user's file-formats row when
staging README via index blob technique. PR-body /tmp Markdown updated for native
Windows evidence and required fresh final CI, not yet published. Current source
and docs are uncommitted above90f; new sqlite_uri.py is UNTRACKED and must be
explicitly staged. 13 own paths expected incl README partial; old data/draft excluded.
All2456 previous mandatory cases retained in same groups;6newofflinecases added.
4390 and96237 still running; expected next complete focused/quality, commit/push,
new2462-case CI gate. Earlier e7b/90f CI runs are outdated native snapshots.


Final native Windows closure verification:4390 finished125passed in352.13s
(exit result still to reap if necessary; log complete). Quality96237 terminal0,
all gates passed (format381, mypy475, invalid contracts95/93). Native Windows
probes79816/12124 terminal0. No active local PostgreSQL server. 13 ownpaths
staged21783, README only own database row (userdraft rowunstaged). Publishing
native URI/sync fix next; fresh fullCI2462 is still REQUIRED before sign-off.


## Published final native fix and fresh CI — CURRENT

3a0e22cb63c0f385aa01f584c0753ec8848d28c0 pushed to draftPR161. Windows closure
native code/test/config (9inputs) matches committed bytes; windows-closure-inputs.json.
Fresh PRrun43780999004: databasejob133017914846; fullsuite133017914595.
Fresh pushrun43780992646. PRbody successfully patched viaREST (ghpr editbroken).
All local checks terminal0; no local test/PG process remains (4390exit0 reaped).
Native Windows actual and125Linux focused tests+fullquality passed. Only pending
validation is complete CI on3a0e22cb (2462mandatory) and relevant full-suite results.

Older e7b full DATABASE CI succeeded! PRrun43775314559/job133000179110:
5004passed,104skipped,26warnings in3279.27s. Downloaded artifact
/tmp/liuxin-ci-e7b-native-2026-11-16/ratchet.json confirms2456required/2456fullypassed,
no failures,pytest0. Log /tmp/liuxin-signoff-ci-e7b-database.log. The workflow overall
failed due already-repaired full-suite collection; do not confuse with green DBjob.
Push e7b databasejob132999817987 alsoSUCCESS. This is clean historical gate evidence,
NOT final3a0e22cb. Preserve both chronology and final-source distinction.

Next: monitor freshCI, inspect failures if any. Update final review/index/body with
exact source/results once2462gatecomplete. Do not mark goalcomplete yet. PR still
DRAFT/unmerged. Native source frozen at3a0e22cb; avoid new changes without evidence.
Current READMEworktree has only userdraftrow beyondcommit; own memory update isdirty.
Original dirtydata submodule/untracked file-formats draft stay excluded. Private
PRworktree still90f; advance to3a0e22cb/final docs before final doc-link check.


Final clean PRtree now3a0e22cb (intentional data symlink T). Doc checks32058
completed0:22passed in11.22s, /tmp/liuxin-signoff-final-docs.log. CI watcher85175
active: gh pr checks161 --watch --interval45 --fail-fast; revalidate live session
before replacing. Current3a0e CI packaging, confidence, typing/architecture,
file-formats and TruffleHog allSUCCESS; fullsuite/database remainIN_PROGRESS.
Canceled only superseded duplicate90f PUSHrun43777974223 (after verifying branch,
event andSHA); earlier PRrun43777977612 remains for full-suite evidence. E7b
runsalreadycomplete withsuccessfulDBgate described above.

No further implementation work planned absent new test evidence. Native source
frozen3a0e22cb. Continue monitoring CI, fetch any failed job logs directly viaAPI,
then final review/index/body + draft-to-ready step once required gatecomplete.
Do not merge. Do not mark goalcomplete while final2462gate stillpending.


## Full-project consumer fixes — CURRENT

Goal continuation inspected older90f fullsuite job133008516181 (finished00:23:58).
Log /tmp/liuxin-signoff-ci-90f-full.log:24failed,9361passed,2192skipped,11xfail,
693warnings in1278.92s. All24 failures group into7APSW-unavailable FRBR cases,
1outdated bootstrap-failure assertion,16storage MiniDriverWrapper missing
get_relation_type (and subsequent Row table-hint support absent by source trace).

Three TEST-ONLY files now changed above3a0e:
- test_frbr_generator_integrity.py: indirect backend fixture importorskipAPS W only
  forSQLite_apsw. Original nodeIDs preserved; no assertions removed. Dedicated
  gate still installsAPSW and requires all cases, so skips stillfailthere.
- tests/storage/_mini_db.py: real parameterized SQLite table/view introspection;
  add_row/update_row honor and remove explicit table hints supplied by Row.
- tests/storage/api/test_storage_manager_database_reload.py: assert completedFalse
  andzero counts after enumeration fails; retainreason/strictpropagation checks.
Native production code remains frozen3a0e22cb; do NOT stop its full databasegate.

No-APSW probe72028 completed:7passed,7skipped,90deselected in61.52s,
/tmp/liuxin-signoff-no-apsw.log. FRBR collection47133 retains required nodeIDs.
FRBR directlint/formatpass; the two storagefiles have unchanged baseline Ruff
issues(1/5), no introduced diagnostics. Do not cleanup unrelated style.
Combined FRBR+all MiniDB consumers+storage bootstrap owner run95637 ACTIVE,
/tmp/liuxin-signoff-final-storage-consumers.log. Need inspect results and fix any
further incomplete fixture contracts, then update review and commit/push testonly
followup so full-suite CI becomesgreen. Current fresh3a0e fullsuite may repeat24
failures; do not mistake expected stale-consumer failures for new native regressions.

Goal completion-audit.json prepared in ignored artifactroot with requirement
mapping, deferredscope andpendinggates. Current scope confirmedcustomfields no
v1support orcache ownership; broadCalibre cleanup remains separate architecturework.


## Consumer repairs validated and ready to publish — CURRENT

Session95637 completed0:210passed in602.78s, all7selected FRBR/storage modules.
No-APSW probe remains7passed7skipped90deselected;104FRBR mandatory IDs retained.
Three test files + review follow-up + this memory are the only own commit paths.
Production source remains3a0e22cb. Direct FRBR lint/format and diffcheck passed.
Current3a0e full CI job133017914595 repeated the same24consumer failures
(9364passed2192skipped11xfail); log /tmp/liuxin-signoff-ci-3a0e-full.log.
Watcher85175 finished1 due that known failure; no longerlive. Databasejob
133017914846 stillrunning. Keep that final-native gate running.
Next publish test-only fixes, update PR body viaREST, inspect newfullsuite and
2462-case nativegate. Do not signoff until those complete. User leftovers
unchanged:dirtydata, untrackedfileformatsdraft, singleREADMEdraftrow.
