# CI baseline repair, 2026-10-20

The active goal remains the **entire A01–A16 programme**, delivered as focused
PRs/commits. This is A15, not completion of that goal. Keep all acceptance
criteria in `dev-docs/architectural_reviews/next_tasks.md`; next runtime work is
A04 transaction/close ownership, followed by A05 deletion policy.

Branch `ci-baseline-repair` starts at `4483c2ad`, targeting
`database-create-safety` while PR #130 is open. Earlier slices are #128 (A01),
#129 (A02), #130 (A03), and #127 (A14 documentation).

Published as [PR #131](https://github.com/ajCameron/LiuXin-alpha/pull/131).
The initial implementation is `21e2604e`.

## Implementation

- Repair the forty shared #128/#129 failures: test Ruff dependency and fixture
  lookup, explicit full-suite encryption extra, two stale lazy-export tests,
  the public image API's existing Pillow route, and imageops trim/grayscale/
  overlay fallback when ImageMagick is absent or fails.
- Retain the Qt implementation behind explicit image aliases; unrelated import
  errors propagate. Pixel assertions cover actual grayscale/compositing and
  failed-overlay preservation, not just PNG magic bytes.
- Extract bundled executable-directory lookup so image optimizers do not
  initialize the PDF converter. PDF and image callers share that passive owner.
  The whole runtime/combined import component shrinks from 217 to 214; no named
  direction allowance was added.
- Remove thirty confirmed generated benchmark cache files (102,843,070 bytes)
  and guard the tracked `.tmp` namespace. Existing fixtures, historical reports,
  configuration and the data submodule remain classified separately in the
  canonical `ci-baseline-2026-10-20.md` review.
- Full CI runs serially with immediate test scratch cleanup and uploads durable
  event/resource telemetry after success or failure.
- Installed-wheel verification now also checks `past` origins and public
  headless image scaling.

## Evidence and limits

- An unchanged isolated checkout reproduced all ten surface/image failures.
  The other thirty failures in CI were the missing Ruff/encryption groups.
- A fresh environment installed `test,conversion,encryption,typing` successfully;
  `pip check` found no broken requirements.
- Full formatting, annotations, architecture/layer/import-footprint, lint,
  complexity, basedpyright/mypy and internal-contract gates passed.
- Initial focused verification: 200 passed, one newly introduced import test
  failed because its subprocess could select the earlier installed wheel after
  a CWD change. The corrected subprocess and absolute source paths passed the
  50-test image/PDF/surface/documentation follow-up.
- The benchmark smoke fixture regenerated with the expected shape after
  deleting the tracked caches.
- Installed-wheel checks passed: origins/assets, repeated CLI init,
  HTML-to-EPUB conversion and public image scaling.
- Full-suite validation is pending. Do not report A15 or the entire programme
  complete until its result is checked and any remaining failures addressed.
- Qt rendering and optional remote integrations are not claimed by this
  headless run. Earlier live PostgreSQL evidence belongs to A03.
- GitHub read-only inspection found no classic protection or effective rules
  for `main` or `configuration-ownership`. No settings were changed.

The isolated verification checkout contains only proposed changes and its own
fresh environment. Preserve the six unrelated staged storage files and dirty
data submodule in the main checkout; saved file hashes and index entries match.
Ignored telemetry/logs and the portable handoff should be retained while the full
suite runs; do not remove an active test checkout or environment.

## Follow-up while full runs execute

The completed #130 run has the original forty failures plus one terminal
no-create regression (41 failed, 7,332 passed). Refuse an absent explicit path
before Core startup when `--no-create-if-missing` is set; cover both existing and
missing parent directories. This correction is on the same A15 PR.
All five selected terminal creation/existing-path cases passed in the clean
environment. The final helper/documentation follow-up before publication also
passed all thirty checks.

The first local full run was interrupted because its sandbox prohibited
`socket()` and thus RPC tests. A separate permitted run is active on `21e2604e`
with durable telemetry and immediate scratch cleanup. Do not count the
interrupted run as a full application failure or silently treat that older
commit's eventual result as validation of this follow-up.

A04 has begun separately on `database-transaction-ownership`: nine new
regressions reproduced pending-write commits, borrowed/nested transaction
defects and the connection masquerading as a mutex. Initial fixes pass those
nine; caller migration, complete documentation, backend parity and quality
gates remain unfinished. They are not part of PR #131.

## Completed full-run evidence and CI option isolation

The permitted local run finished: 7,389 passed, 248 skipped, 17 xfailed,
one terminal no-create failure on the older `21e2604e` source. Run #39377685789
on `dd44c953` passed 7,384, skipped 249, xfailed 17, and failed four nested pytest
tripwires because CI's PYTEST_ADDOPTS leaked plugin-specific flags into them.
The workflow now supplies those flags on the outer command line. Reproduction
failed all four; corrected invocation passed those four and both no-create cases.
A new current-head CI run is needed before closing A15. Local logs remain under
`/tmp/liuxin-a15-*`; the completed full-run checkout may now be cleaned after
retaining needed evidence. A04 remains separate and unfinished.

## Verified current-head full CI

[A15 run #39441669661](https://github.com/ajCameron/LiuXin-alpha/actions/runs/39441669661)
completed successfully on `a0e4d2ed`: 7,388 passed, 249 skipped, 17 xfailed,
1,125 warnings in 1,847.27 seconds. Quality, wheel, both format lanes and the
aggregate gate also passed. A15 is verified for PR review; earlier pending notes
above are historical. The full A01–A16 programme remains open. A04 is PR #132;
A05 has begun on storage-deletion-policy. No PR or branch-rule changes were merged.
