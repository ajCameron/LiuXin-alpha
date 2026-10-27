# Test streams

Source reviewed **2026-10-20**, working tree based on `fc5de7af`.

For event selection and the single full-suite CI owner, see
[continuous integration](testing/continuous-integration.md).

The ordinary pytest command still selects the complete suite:

```bash
.venv/bin/python -m pytest
```

The named-stream runner provides smaller local feedback loops without changing
that default:

```bash
# Full suite: this is also what omitting --stream selects.
.venv/bin/python scripts/run_test_stream.py

# Representative database API, driver, SQLite, PostgreSQL, and lifecycle tests.
.venv/bin/python scripts/run_test_stream.py --stream database

# Smoke-named tests plus sentinels for every non-database test area.
.venv/bin/python scripts/run_test_stream.py --stream smoke

# The normal fast confidence check: database + smoke.
.venv/bin/python scripts/run_test_stream.py --stream confidence
```

The smaller streams use quiet output, short tracebacks, and no warning summary.
The full stream continues to report warnings. Extra pytest arguments go after
`--`:

```bash
.venv/bin/python scripts/run_test_stream.py --stream confidence -- --maxfail=1
.venv/bin/python scripts/run_test_stream.py --stream database -- -k schema
```

Use `--list-streams`, `--list-files`, or `--dry-run` to inspect the selection
without executing it.

## Scope

| Stream | Selection |
| --- | --- |
| `full` | The entire `tests` tree. This is the default. |
| `database` | Curated API parity, driver contract, CRUD/error, lifecycle, SQLite, and mocked PostgreSQL modules. |
| `smoke` | Every non-database `*smoke*.py` module plus an explicit sentinel for each otherwise-uncovered active test area. |
| `confidence` | The deduplicated union of `database` and `smoke`. |

The stream configuration lives in `scripts/run_test_stream.py`. Guard tests
ensure configured paths exist, the confidence stream remains an exact union,
and every active non-database top-level test area remains represented. New
properly named smoke modules are discovered automatically.

These streams give scoped development feedback. The CI full-suite lane owns
broad merge evidence; select local checks according to the change and report what
actually ran. Do not treat a smoke pass as a full-suite result.

For a storage-conscious serial run with durable diagnostics, pass pytest options
through the separator:

```bash
.venv/bin/python scripts/run_test_stream.py --stream confidence -- \
  -n 0 -o tmp_path_retention_policy=none -o tmp_path_retention_count=0 \
  --telemetry-dir=/tmp/liuxin-confidence-telemetry
```

Telemetry must live outside disposable test scratch. The `plugins` directory is
a test area covered by the smoke sentinel; it is not a separate named stream in
this runner. See [fixture/cleanup policy](<../07 - Test Databases.md#scratch-cleanup-and-telemetry>).
