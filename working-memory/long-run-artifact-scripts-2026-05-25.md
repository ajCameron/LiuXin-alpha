# Long-Run Artifact Scripts - 2026-05-25

## Context

After the failed/missing overnight coverage run, we decided long-running test
commands need a predictable artifact contract under `working-memory/test-results`.
The collaboration contract records the desired shape; this note records the
script-level implementation.

## Updated Scripts

- `scripts/run_full_test_suite.sh`
- `scripts/run_full_test_suite_cov.sh`
- `scripts/run_live_web_sources.sh`

The shell runners are the long-run artifact entry points. The older
`scripts/run_full_test_suite.py` runner was reviewed but not changed in this
slice; use the shell runner when the run needs the log/done-marker contract.

## Default Artifact Behavior

`run_full_test_suite.sh` now defaults to:

```text
run id:      full-suite-YYYY-MM-DD-HHMMSS
JSON:        working-memory/test-results/<run-id>.json
Log:         working-memory/test-results/<run-id>.log
Done marker: working-memory/test-results/<run-id>.done
```

`run_full_test_suite_cov.sh` now defaults to:

```text
run id:        coverage-YYYY-MM-DD-HHMMSS
JSON:          working-memory/test-results/<run-id>.json
Coverage data: working-memory/test-results/.coverage-<run-id>
Coverage HTML: working-memory/test-results/<run-id>-html/
Coverage XML:  working-memory/test-results/<run-id>.xml
Log:           working-memory/test-results/<run-id>.log
Done marker:   working-memory/test-results/<run-id>.done
```

`run_live_web_sources.sh` now defaults to:

```text
run id:      live-web-sources-YYYY-MM-DD-HHMMSS
Log:         working-memory/test-results/<run-id>.log
Done marker: working-memory/test-results/<run-id>.done
```

The `working-memory/test-results/` tree is intentionally ignored by git. Track
durable run summaries in working-memory notes, not the raw JSON, log, done, or
coverage payloads.

## New Options

The shell runners support explicit artifact naming:

- `--run-id NAME`
- `--log-file PATH`
- `--done-file PATH`

The full-suite runner also supports:

- `--no-log`
- `--no-done-marker`

The coverage wrapper uses those suppression flags when it delegates to the
full-suite runner, so a coverage run gets one top-level log and one top-level
done marker rather than nested duplicate artifacts.

## Done Marker Contents

Done markers are plain text and include:

- run id
- repo root
- script path
- start/finish timestamps
- exit code
- artifact paths
- invocation
- relevant command steps

This makes a completed, failed, or interrupted run inspectable without needing
to infer what happened from partial stdout.

Because the artifact logs use `tee`, pytest would otherwise stop emitting ANSI
color. The shell runners now pass `--color=yes` to pytest so interactive
full-suite, coverage, and live-web output stays colored.

## Validation

- `bash -n scripts/run_full_test_suite.sh scripts/run_full_test_suite_cov.sh scripts/run_live_web_sources.sh`
- `bash scripts/run_full_test_suite.sh --dry-run --skip-install --run-id smoke-full -- --maxfail=1`
- `bash scripts/run_full_test_suite_cov.sh --dry-run --skip-install --run-id smoke-cov -- --maxfail=1`
- `bash scripts/run_live_web_sources.sh --dry-run --run-id smoke-live --quiet -- -k google`
- Failure-path marker smoke:
  `bash scripts/run_live_web_sources.sh --python /bin/false --results-dir /tmp/liuxin-artifact-smoke-20260525 --run-id live-marker-smoke --quiet`
  produced `/tmp/liuxin-artifact-smoke-20260525/live-marker-smoke.log` and
  `/tmp/liuxin-artifact-smoke-20260525/live-marker-smoke.done` with
  `exit_code: 1`. The log includes the script header plus the failed command
  result.
