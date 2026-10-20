# CI baseline repair, 2026-10-20

The active goal remains the **entire A01–A16 programme**, delivered as focused
PRs/commits. This is A15, not completion of that goal. Keep all acceptance
criteria in `dev-docs/architectural_reviews/next_tasks.md`; next runtime work is
A04 transaction/close ownership, followed by A05 deletion policy.

Branch `ci-baseline-repair` starts at `4483c2ad`, targeting
`database-create-safety` while PR #130 is open. Earlier slices are #128 (A01),
#129 (A02), #130 (A03), and #127 (A14 documentation).

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
