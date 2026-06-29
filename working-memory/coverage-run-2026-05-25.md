# Coverage Run - 2026-05-25

## Artifact Set

Run id: `coverage-2026-05-25-154712`

- Done marker: `working-memory/test-results/coverage-2026-05-25-154712.done`
- Log: `working-memory/test-results/coverage-2026-05-25-154712.log`
- Pytest JSON: `working-memory/test-results/coverage-2026-05-25-154712.json`
- Coverage XML: `working-memory/test-results/coverage-2026-05-25-154712.xml`
- Coverage HTML: `working-memory/test-results/coverage-2026-05-25-154712-html/`
- Raw coverage data: `working-memory/test-results/.coverage-coverage-2026-05-25-154712`

## Result

Pytest itself completed successfully:

```text
4513 passed, 60 skipped, 18 xfailed, 674 warnings in 2947.19s (0:49:07)
```

The pytest JSON reports `exitcode: 0` with `4513` passed, `51` skipped,
`18` xfailed, and `4582` collected. The log summary reports `60` skipped.

The top-level wrapper `.done` marker reports `exit_code: 2` because the
then-current `run_full_test_suite.sh` emitted a shell syntax error after pytest
completed:

```text
scripts/run_full_test_suite.sh: line 300: syntax error near unexpected token `then'
```

After the run, the current `run_full_test_suite.sh` passed `bash -n` and a
cheap successful wrapper smoke:

```text
bash scripts/run_full_test_suite.sh --skip-install --results-dir /tmp/liuxin-artifact-smoke-20260525 --run-id full-version-smoke --no-log --no-done-marker -- --version
```

## Coverage

Current XML totals:

```text
Line coverage:     54.83%  109778 / 200211
Branch coverage:   38.33%   27020 / 70486
Combined coverage: 50.54%
```

Previous comparable XML, `coverage-2026-05-20-191521.xml`:

```text
Line coverage:     53.31%  106045 / 198933
Branch coverage:   36.36%   25432 / 69954
Combined coverage: 48.90%
```

Delta:

```text
Line coverage:     +1.52 percentage points
Branch coverage:   +1.97 percentage points
Combined coverage: +1.64 percentage points
```

Largest uncovered line counts in this run:

```text
1838/1838  0.00%   library/caches/calibre/cache.py
1678/1678  0.00%   utils/databases/apsw_shell.py
1152/1152  0.00%   utils/libraries/liuxin_dateutil/test.py
1145/1529 25.11%   file_formats/lrf/html/convert_from.py
1066/1369 22.13%   file_formats/opf/__init__.py
1005/1245 19.28%   utils/libraries/liuxin_html5lib/tokenizer.py
 904/1497 39.61%   utils/libraries/liuxin_html5lib/html5parser.py
 818/1487 44.99%   file_formats/lrf/pylrs/pylrs.py
 818/818   0.00%   utils/libraries/liuxin_dateutil/rrule.py
 801/801   0.00%   library/legacy.py
```
