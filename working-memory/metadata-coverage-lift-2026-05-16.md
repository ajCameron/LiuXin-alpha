# Metadata Coverage Lift - 2026-05-16

Branch: `renderer-coverage-tests`

## Scope

Focused coverage work after the renderer helper tests, covering recent metadata
API/container surfaces without tracking generated coverage artifacts.

Added/expanded tests for:

- metadata source contracts and WEMI identity aliases
- WEMI relation properties, relation helpers, and projection views
- lazy relation value-to-id container behavior
- WEMI identity containers and family smoke/projection edge paths
- central, lazy, work, expression, manifestation, and item metadata hydrators

## Validation

Focused hydrator coverage:

```bash
.venv/bin/python -m pytest \
  tests/metadata/containers/test_work_metadata_hydrator.py \
  tests/metadata/containers/test_expression_metadata_hydrator.py \
  tests/metadata/containers/test_manifestation_metadata_hydrator.py \
  tests/metadata/containers/test_item_metadata_hydrator.py \
  tests/metadata/containers/test_hydrator_edge_cases.py \
  --cov=LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_hydrator \
  --cov=LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_hydrator \
  --cov=LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_hydrator \
  --cov=LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_hydrator \
  --cov=LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_hydrator \
  --cov=LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_lazy_metadata_hydrator \
  --cov-report=term-missing -q
```

Result:

- `62 passed`
- six focused hydrator modules at `100%`
- `1614` statements, `0` missing

Hygiene:

- `git diff --check` clean before commit
- `py_compile` clean for
  `tests/metadata/containers/test_hydrator_edge_cases.py`

## Notes

The durable source-of-truth is the tests and this summary. Coverage XML/HTML
outputs and `.coverage*` data files remain local run artifacts.

## Full Coverage Rerun

The full coverage runner needs local socket binding for the core HTTP daemon
tests. A sandboxed run therefore failed four daemon tests with
`PermissionError: [Errno 1] Operation not permitted`; the same daemon tests
passed when rerun outside the sandbox.

Two unrelated terminal-width-sensitive tests also failed only under xdist:

- `tests/utils/test_fadedpage_wget_discovery_script.py::test_wget_discovery_renders_live_progress_footer_on_tty`
- `tests/surfaces/test_text_browser.py::test_text_browser_store_list_filters_and_sort[sqlite]`

Both tests now pin the display width they need when asserting exact rendered
text. Focused xdist reruns passed for both tests.

Final escalated full coverage run:

```bash
scripts/run_full_test_suite_cov.sh --skip-install
```

Artifacts:

- Pytest JSON: `working-memory/test-results/full-suite-2026-05-16-033946.json`
- Coverage XML: `working-memory/test-results/coverage-2026-05-16-033938.xml`
- Coverage HTML: `working-memory/test-results/coverage-html-2026-05-16-033938/`

Outcome:

- `3405 passed`
- `169 skipped`
- `22 xfailed`
- `5 xpassed`
- `660 warnings`
- exit code `0`

Coverage:

- Project lines: `96223 / 195385` (`49.25%`)
- Project branches: `20937 / 68300` (`30.65%`)

## Post-Merge Coverage Rerun

After merging the metadata coverage/writer branch, the rerun artifacts were:

- Pytest JSON: `working-memory/test-results/full-suite-2026-05-16-194315.json`
- Coverage XML: `working-memory/test-results/coverage-2026-05-16-194134.xml`

Outcome:

- `3598 passed`
- `1 failed`
- `45 skipped`
- `22 xfailed`
- `5 xpassed`

The failure was
`tests/utils/plugins/fallbacks/test_bzzdec.py::test_decompress_small_random_fuzz_does_not_hang`.
The test claimed to use a fixed seed but generated payloads with `os.urandom`,
and the logged 12-byte payload decoded an implausible multi-megabyte block
before eventually raising EOF. The fallback now rejects tiny streams with
implausible block expansion before entering the expensive decode loop, and the
test corpus is deterministic.

Durable docs:

- `docs/development/malformed-input-fuzzing.md`

Follow-up direction:

- Add deterministic wrong-format tests for metadata extractors. Individual
  extractors should error deliberately when handed non-credible inputs; a later
  central "best effort metadata from this file" API can own fallback routing.

Focused validation after the fix:

- `.venv/bin/python -m pytest tests/utils/plugins/fallbacks/test_bzzdec.py -q`
  passed with `19 passed`.
- `.venv/bin/python -m pytest tests/utils/plugins/fallbacks -q` passed with
  `90 passed`.
- `.venv/bin/python -m pytest tests/utils/plugins/fallbacks/test_bzzdec.py --cov=LiuXin_alpha.utils.plugins.fallbacks.bzzdec --cov-report=term-missing:skip-covered -q`
  passed with `19 passed`.
