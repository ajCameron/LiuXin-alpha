# Coverage Syntax Warning Cleanup - 2026-05-15

Branch: `coverage-syntax-warning-cleanup`

## Context

After PR #44 landed, `scripts/run_full_test_suite_cov.sh` completed the full
suite and generated coverage output, but coverage reporting was very noisy. The
full-suite JSON was green:

- `working-memory/test-results/full-suite-2026-05-15-192241.json`
- exitcode `0`
- `3326 passed`, `162 skipped`, `22 xfailed`, `5 xpassed`, `3515 collected`

The noise came from coverage source analysis surfacing old `SyntaxWarning`s,
mostly invalid escape sequences in vendored/legacy regex strings.

## Changes

- Added `scripts/run_full_test_suite_cov.sh` as a coverage-specific wrapper for
  the existing full-suite runner.
- The wrapper installs `pytest-cov`, delegates to `run_full_test_suite.sh`, and
  sets `COVERAGE_FILE` under `working-memory/test-results` so xdist coverage
  worker files do not trip the repo-root leak guard.
- Cleaned the coverage-triggered `SyntaxWarning` set across `17` files:
  - raw strings for regex patterns
  - escaped literal backslashes in documentation/Word field text
  - cleaned generated unicode transliteration entries that had pointless
    escaped `{`, `}`, and `$`
  - changed `is not 0` to `!= 0`
- Also fixed three nearby regex nested-character-class `FutureWarning`s in
  `file_formats/conversion/preprocess.py`.

## Validation

- SyntaxWarning compile scan over `src/LiuXin_alpha`: `0 warnings/errors`
- `git diff --check`: clean
- Focused tests:

```bash
python3 -m pytest \
  tests/file_formats/txt/test_txt_modernized.py \
  tests/file_formats/txt/test_txt_unicode_torture.py \
  tests/file_formats/textile/test_textile_modernized.py \
  tests/file_formats/textile/test_textile_unicode_torture.py \
  tests/file_formats/docx/test_docx_modernized.py \
  tests/file_formats/lrf/test_lrf_modernized.py \
  tests/file_formats/lrf/test_lrf_output_modernized.py \
  tests/file_formats/oeb/test_oeb_backend_smoke.py \
  tests/file_formats/oeb/test_oeb_unicode_torture.py \
  tests/file_formats/pdb/test_pdb_modernized.py \
  tests/utils/plugins/test_speedup_parse_date_epoch_ints.py \
  tests/utils/language_tools/test_pluralizers.py \
  tests/metadata/test_standardization_torture.py -q
```

Result: `84 passed`, `1 skipped`, `4 xfailed`, `3 xpassed`.

- `coverage report` against the existing full-suite coverage data completed
  with empty stderr:

```bash
.venv/bin/python -m coverage report \
  --data-file=working-memory/test-results/.coverage-full-suite-2026-05-15-192049 \
  --include='*/src/LiuXin_alpha/*' \
  --skip-covered
```

## Still Open

- The full coverage suite has not been rerun after the warning cleanup. Next
  check should be:

```bash
scripts/run_full_test_suite_cov.sh
```
