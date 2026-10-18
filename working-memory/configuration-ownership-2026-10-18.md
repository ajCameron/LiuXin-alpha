# Configuration ownership — 2026-10-18

The user requested the next focused branch through to a PR after the write and
catalogue-read PRs were merged. Work continues in `/tmp/liuxin-test-vm-telemetry`;
the Windows checkout's staged/unstaged edits and the shared data symlink are untouched.

## Branch integration

The PRs were merged in parent-first order. #118 reached main, then #119 merged into
`catalog-write-consolidation`, and #120/#121 merged into `test-vm-telemetry`. Thus
main at `84f97893` did not contain the later reviewed work despite all four PRs
showing MERGED.

[PR #122](https://github.com/ajCameron/LiuXin-alpha/pull/122), branch
`integrate-ownership-prs`, combines those reviewed heads into main. Its new code
correction updates the stale guard for retired `surfaces/cli/storage.py`, asserts
that facade remains absent, retains command-owner size/dependency checks, and fixes
the command-package docstring. Integration checkpoint: `e1308b65`. Its 54 targeted
checks passed; both GitHub architecture/typing jobs subsequently passed.

`configuration-ownership` starts at that integration checkpoint. Its PR is based
on `integrate-ownership-prs`, keeping the configuration diff separate. Merge the
integration PR into main first, then retarget/merge the configuration PR.

## Implementation

- Core owns manifest/profile selection in `core/settings.py`; every live caller
  moved from the retired surface module. `ConfigurationPaths` owns explicit,
  environment and deployment/user path precedence without filesystem writes.
- Core loads one application Preferences instance before database construction,
  injects it into the database and CoreServices, and retains it when maintenance
  collaborators are reinitialized. Remote surfaces leave server preferences alone.
- Application `preferences`, Calibre `prefs` and `tweaks` share one lazy view.
  Defaults, literal tweak inputs, former global JSON and application INI have an
  explicit read precedence; ordinary writes go only to the INI. Shared default
  values are preserved. Nearly 900 lines of duplicate defaults are removed;
  only actual LiuXin differences from packaged tweaks remain declared separately.
- Tweak inputs are parsed as literal assignments, never executed. Invalid input
  propagates before Core opens a catalogue. Importing configuration, metadata/ICU
  or Core selection does not load or write application settings.
- Dispatch uses ContextVars, managed workers carry the submitting selection, and
  process workers explicitly receive it under both fork and spawn. Codecs are
  rebuilt after unpickling without re-reading files. This does not implement
  cross-process live cache synchronization or a new jobs lifecycle.
- Component serializers capture their selected/explicit path at construction.
  Conversion defaults no longer create directories during import/read. The unused
  eager DynamicConfig singleton is removed. Installation resources, component
  namespaces and database-local DBPrefs retain distinct responsibilities.
- Title article patterns use actual rules as cache keys. ICU reads settings lazily
  and caches collators per thread and effective locale/numeric policy; a profile
  no longer inherits another profile's cached sort settings.
- The new modules join the continuing format/lint/type gates. Import budgets gain
  dedicated two-module checks for the two pure configuration leaves; five existing
  boundary counts increase by exactly one to account for the new foundation module.
  Domain prohibitions are unchanged. One unresolved-dynamic baseline line moves
  with the existing job callable import; no new dynamic import is allowed.

The public contract is in [configuration ownership](../dev-docs/configuration-ownership.md).
Jobs lifecycle composition, ingest coordination and plugin-registry consolidation
remain separate focused follow-ups. Some component-level singleton integrations
(e.g. cover/plugin option objects) retain their construction-bound lifetime; this
PR unifies application settings and gives component constructors an explicit path
boundary rather than merging unrelated domains into application preferences.

## Validation

Artifacts are retained in the ignored Windows-checkout directory
`working-memory/test-results/configuration-20261018`, outside disposable scratch.
All pytest runs are serial, with durable phase/resource telemetry and aggressive
scratch cleanup. Quality gates and pytest never run concurrently.

Intermediate runs reproduced and corrected relative-manifest path handling, the
spawn codec serialization problem, and cached sort-rule leakage. Fixture assertions
were updated to use explicit isolated stores and the new single persistence owner.
The expanded run had 302 passing cases and two stale assertions (relative named
profile paths and the newly forwarded configuration argument), both corrected.
Before/after default comparison preserved all 123 values except generated IDs.

The project quality gates passed in `quality-fifth.log`: 172 formatted files,
463 annotated format modules, 243 protected import-cycle modules, 1,798 layer-checked
modules with the same 15 reviewed backward edges, 23 cold-import budgets, lint and
complexity, basedpyright with zero errors, mypy across 191 files, and both internal
contracts rejecting all 37 invalid examples.

The final combined run completed with 942 passes and one stale import-budget
assertion (30 versus 31 modules after introducing the shared path owner). After
correcting that assertion, the import checker, CLI operator and documentation-link
follow-up passed all 44 cases. Latest outcomes therefore cover **943 distinct
passing cases** in the final selected regression set, plus the earlier 54-case
integration check. These are targeted runs, not a new full-suite claim. Native
ICU is not available here; fallback and recording-collator regressions cover its
profile selection/cache behavior.

The final run peaked at **264.1 MiB** process-tree RSS, retained at least **6.39 GiB**
available RAM and **14.87 GiB** C: headroom, and observed zero swap use and no OOM
kills. All seven task-owned scratch directories were removed after testing;
`cleanup.json` and `validation-summary.json` retain the evidence. Durable reports
and previous diagnostic logs remain. Scoped syntax/Pyflakes review found no new
findings; `git diff --check` passes.

## Integration CI baseline

The integration PR's full suite finished with **7,176 passed, 40 failed, 242 skipped
and 17 xfailed**. Its log is retained alongside the local validation artifacts.
Eight failures require the missing repo-local Ruff executable; 22 require the
encryption dependency. The remaining ten are the previously recorded lazy-export
and image-backend failures. This run predates the configuration commit, so it is
base-branch evidence, not configuration validation. All other integration CI lanes
passed. Keep CI dependency setup and those existing failures visible as separate
follow-up work; do not describe the integration PR as fully green.
