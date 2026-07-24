# Test streams

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

These streams are development feedback, not merge proof. Run the complete
suite externally before merging.
