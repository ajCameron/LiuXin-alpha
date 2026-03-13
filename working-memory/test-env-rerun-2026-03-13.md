# Test Environment Cleanup And Rerun - 2026-03-13

## Decision

Clean up the missing-dependency test noise first, then rerun the full suite to get a more honest failure signal.

## Repo Changes

Updated `pyproject.toml`:

- runtime deps:
  - `cssselect`
  - `chardet`
- test deps:
  - `pytest-xdist`
  - `pytest-json-report`
  - `python-dateutil`

Updated the full-suite runners so they now rely on `-e .[test,search]` alone:

- `scripts/run_full_test_suite.sh`
- `scripts/run_full_test_suite.py`

Added a minimal internal `past.builtins` compatibility shim:

- `src/past/__init__.py`
- `src/past/builtins.py`

That unblocks legacy imports of:

- `basestring`
- `unicode`
- `str`
- `cmp`

## Local Environment Notes

This tool environment cannot reach PyPI and cannot install Ubuntu packages as root.

To get a cleaner local rerun here, the existing `.venv` was bridged to packages already present on disk:

- `cssselect` -> `/usr/lib/python3/dist-packages/cssselect`
- `chardet` -> `/usr/lib/python3/dist-packages/chardet`
- `dateutil` -> repo-vendored `src/LiuXin_alpha/utils/libraries/liuxin_dateutil`

Those bridges are local `.venv` state, not tracked repo changes.

## Rerun Result

Old report:

- `working-memory/test-results/full-suite-.json`
- `82 failed`

New report:

- `working-memory/test-results/full-suite-2026-03-13-024552.json`
- `91 failed`

That looks worse numerically, but the signal is cleaner:

- `20` previously failing tests were resolved outright
- the broad `cssselect` / `chardet` import-failure cluster disappeared
- the remaining `date` failures are now real behavior regressions, not missing imports

New failures added in this tool run are partly sandbox artifacts:

- HTTP daemon/socket tests fail with `PermissionError: [Errno 1] Operation not permitted`
- some multi-process lock tests are also likely constrained by the sandbox

## Meaningful Remaining Clusters

Largest remaining real-code groups from the rerun:

- `tests/utils/plugins/fallbacks/test_bzzdec.py`
- `tests/utils/plugins/fallbacks/test_lzx.py`
- calibre fixture snapshot/report tests
- database driver contract tests
- metadata/plugin API regressions
- resource/config regressions
- date utility regressions
- archive dispatch regressions

## Suggested Next Fix Order

1. `utils/date` and `utils/decompression/archives` because they are now small, concrete failures with low blast radius
2. plugin API regressions in `customize`
3. resource/config bootstrapping
4. database driver contract failures
5. calibre fixture/report snapshots after the lower layers stabilize
