# Continuous integration ownership

The [Tests workflow](../.github/workflows/tests-on-push.yml) is the single owner
of general Python validation. Its full-suite job runs once per PR workflow run;
the smaller lanes intentionally overlap that suite to give earlier feedback.
Push and PR events can still create separate runs for the same commit.

| Check | Push | Pull request / manual run |
| --- | --- | --- |
| Installed Wheel | Yes | Yes |
| Modern Architecture and Typing | Yes | Yes |
| Confidence Tests (push) | Yes | No |
| File Formats Fast Lane | Yes | Yes |
| File Formats Heavy Lane | No | Yes |
| Full Test Suite (PR) | No | Yes |
| Tests Passed summary | No | Yes |

The quality job runs the same [maintainability helper](maintainability-quality-gates.md)
used locally, a separately named whole-tree Python syntax check, and the listed
architecture/tooling contracts. Compilation is a syntax smoke check, not lint.
The full suite and format lanes retain their `test,conversion` dependency set;
the quality job uses `test,typing`. YAML and Markdown parsers in the `test` extra
support executable workflow and documentation contracts.

## Summary and status-name migration

`Tests Passed` keeps the old summary name, but now requires successful wheel,
quality, fast-format, heavy-format, and full-suite jobs. Its `always()` condition
lets it inspect failed or skipped dependencies; its shell check rejects every
non-success result and empty input. This follows GitHub's documented
[job dependency behavior](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-jobs).
It never launches pytest or installs a second test environment.

The old `Auto-Review` workflow (`review.yml`) is retired. Its
`lint Python code (ubuntu/py3.12)` and `test Python code (ubuntu/py3.12)` checks
are replaced by `Modern Architecture and Typing` and `Full Test Suite (PR)`.
If configuring required checks or external automation, use those current names
or the `Tests Passed` summary; do not require the retired matrix names.

The close-out's read-only GitHub inspection found neither `main` nor the work
branch protected, and no repository rulesets, on 2026-09-07. No branch settings
were changed. That observation is not a guarantee about later configuration.

## Separate workflows

- [Live storage reads](../.github/workflows/live-storage-readonly.yml) remain
  explicit, credential-dependent checks; see the [runbook](storage/live_storage_ci.md).
- [License compliance](../.github/workflows/spdx.yml) and
  [secret scanning](../.github/workflows/trufflehog.yml) retain their own owners.

These checks are not dependencies of the Python `Tests Passed` summary. Set
separate required checks if they are part of a repository's merge policy.

## Local verification

```bash
bash scripts/run_type_checks.sh
.venv/bin/python -m pytest -q \
  tests/scripts/test_ci_workflow_contracts.py \
  tests/scripts/test_developer_documentation_links.py
```

The workflow contracts parse real YAML, protect event selection and sole
full-suite ownership, check the summary dependency set and failure condition,
and execute its actual shell body against success/failure/skip/cancel cases.
They also parse every shell body in the Tests workflow with Bash. These local
checks do not execute GitHub Actions, build an installed wheel, or replace a
remote full-suite result. See [test streams](test-streams.md) for local test
selection and [packaging](packaging.md) for artifact verification.
