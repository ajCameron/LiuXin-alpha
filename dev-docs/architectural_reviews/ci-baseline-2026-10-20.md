# CI and repository baseline review

Date: **2026-10-20**. A15 implementation branch: `ci-baseline-repair`, based on
`4483c2ad` (A03). This records concrete observations; it does not close the
remaining A01–A16 programme in [next tasks](next_tasks.md).

## Reproduced failure groups and repairs

PRs #128 and #129 had the same forty failing test IDs. PR #129's full run
passed 7,308 tests, with 242 skips and 17 expected failures. The unchanged
surface/image subset reproduces ten failures in an isolated checkout.

PR #130's completed run subsequently reported **41 failures, 7,332 passes, 249
skips and 17 expected failures**: the same forty plus terminal
`--no-create-if-missing`. The terminal now refuses an absent explicit path before
opening Core, including when its parent already exists. The regression checks
that refusal creates neither the target nor a missing parent directory.
All five selected terminal creation/existing-path cases passed after the fix.

| Group | Cause | Repair |
| --- | --- | --- |
| Eight formatter tests | CI had neither the required executable nor the developer's `.venv` layout. | Pin Ruff in `test`; fixture uses the running environment's Ruff binary while retaining the runner's local-executable contract. |
| Twenty-two encrypted-storage tests | Full CI omitted the encryption extra. | Full suite explicitly installs `encryption`; missing-dependency runtime behaviour remains tested. |
| Two surface export tests | Assertions expected retired lazy namespace hooks. | Test documented ordinary imports, identity, and fresh-process namespace passivity. |
| Five image API tests | Public `img` required PyQt5 even though a Pillow implementation already existed. | Explicit public aliases select Qt or the existing Pillow owner; unrelated import failures propagate. |
| Three imageops tests | Missing/broken ImageMagick had no final route for trim, grayscale or overlay. | Reuse Pillow image operations; verify pixel changes, alpha composition and preservation on failed decoding. |

The public image wrapper initially increased the whole-package import component.
Extracting bundled executable-directory lookup from PDF conversion removed that
dependency from image helpers. PDF and image optimizer callers share the passive
helper. The measured runtime/combined component decreases from **217 to 214**;
named direction allowances are unchanged. The existing dynamic Qt import moves
line numbers without adding an allowance.

The Qt implementation is retained. Headless validation exercises Pillow; it does
not certify rendering with an installed GUI toolkit.

## Tracked-material inventory

Inventory used tracked paths, ignore matches, current file sizes and the
benchmark provisioner's source, rather than deleting files by suffix alone.

| Material | Observation and disposition |
| --- | --- |
| `.tmp/` | Thirty generated benchmark cache/provisioned files, 102,843,070 bytes, were still tracked despite ignore rules. Removed from the proposed tree. A tracked-path guard prevents recurrence. |
| Historical `working-memory/test-results/` evidence | Seventeen existing report/summary files, 31,827,441 bytes. Retained as historical evidence, not current acceptance results. Existing ignore rules exclude new raw reports; new portable summaries belong in dated working-memory notes. |
| `.idea/` | Seven small IDE project/settings files remain; these are local-tool configuration, not runtime package assets. This repair does not rewrite developers' settings. |
| Preference profiles and startup folder descriptors | Retained compatibility configuration; operational startup/path ownership remains A12. The wheel explicitly owns the startup descriptors, not an arbitrary checkout snapshot. |
| Test resources and data submodule | Retained fixture sources, format samples and external artifacts. Benchmark fixtures are reproducible through `TestResourcesManager`; generated `.tmp` copies are not their source of truth. |
| Build outputs, telemetry and caches | Ignored outputs remain outside the wheel. CI uploads full-run telemetry from runner temporary storage; completed tests release scratch immediately. |

Removing tracked cache files improves future checkouts. It does not reclaim
historical Git objects or authorize history rewriting.

## Dependency and installed-package contract

The clean validation environment installs `test,conversion,encryption,typing`
for the combined local gates. CI keeps the individual job extras documented in
[continuous integration](../hacking/testing/continuous-integration.md).
Optional live PostgreSQL, remote storage and GUI tests require their own
environments; they must not be inferred from a green headless run.

The wheel verifier checks retained `past` files and their installed origins,
runtime assets, repeated catalogue initialization, real HTML-to-EPUB conversion
and public image scaling. See the [packaging contract](../hacking/packaging.md).

Completed checks: clean-environment dependency validation, the entire quality
runner, installed-wheel verification, 200 passing focused tests followed by a
50-test passing follow-up. The initial focused run also had one new subprocess
test failure: its relative import path selected the earlier installed package
after a CWD change. Giving that subprocess an explicit checkout working directory
and using absolute source paths in the validation environment resolves it.
The benchmark smoke fixture regenerated successfully without tracked scratch.

A full suite remains pending. Focused tests, whole quality gates, installed-wheel
checks and full-suite results are separate evidence; publication is not a claim
that the full suite has passed.

The first local full run was interrupted after sandbox-denied socket creation
caused RPC failures; a targeted run confirmed `PermissionError` at `socket()`.
It is not application-failure evidence. A new serial run with socket access and
separate telemetry was started on the initial A15 commit. The subsequent
terminal correction needs its own focused result and the new PR-head CI result.

## Remote settings

Read-only API inspection on this date found no classic protection on `main` or
`configuration-ownership`; the effective branch-rules endpoint returned no rules
for either. No branch settings changed. Once the repaired full suite is green,
the existing `Tests Passed` aggregate is a candidate required check: it already
requires the wheel, architecture/typing, both format lanes and the full suite.
Separate security/license workflows retain their own policy decisions.
