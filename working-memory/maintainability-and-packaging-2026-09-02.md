# Maintainability and packaging checkpoint — 2026-09-02

## Request and status

The project-wide review rated the current codebase for human readability and
long-term maintainability, recorded the next work in durable notes, and began
with the highest-impact operational problem: a wheel which built successfully
but did not contain the schema inputs required to create a catalogue.

Both packaging tranches are complete as of 2026-09-04. The wider
maintainability programme is intentionally not described as complete.

## Baseline rating

- Readability: **6/10** overall.
- Maintainability: **6.5/10** overall.
- Modern application spine: approximately **8/10**.
- Test system: approximately **8.5/10**.
- Packaging before this tranche: approximately **4/10**.

These are review judgements, not generated quality scores. They describe an
uneven project: recently maintained Core, Catalog, storage, ingest, and CLI
seams are much clearer than the inherited compatibility and conversion tree.

## Evidence behind the rating

- The production tree contains about 1,730 Python files and 1,005,000 raw
  lines. Roughly 45% is generated or resource-lookup-style compatibility code,
  so one whole-tree average conceals two very different maintenance profiles.
- The test tree contains about 750 files, 197,000 lines, and 4,700 test
  functions. Recent database, Catalog, storage, Unicode, hostile-container,
  and CLI work has strong executable coverage.
- The maintained top-level packages have complete module documentation at the
  enforced boundary. The strict type ratchet covers 97 files and about 37,000
  lines, however, which is only about one sixth of the active modern packages.
- A broad Ruff scan of the modern package set reported about 5,800 default
  findings, and 481 of 737 modules were not formatter-clean. These totals are a
  migration map, not a useful all-at-once gate.
- Static import analysis found seven strongly connected components involving
  81 modern modules. The largest clusters are terminal (38 modules) and
  database API (14 modules), followed by cache writer, schema cache, Core,
  surface read-model, and CLI clusters.
- About 1,267 TODO-like markers remain, concentrated in database, library,
  cache, and catalog compatibility code. Core and storage no longer contribute
  to that count.
- Several files remain structural concentration risks, including
  `core/program_api.py` (about 4,300 lines), `surfaces/cli/storage.py` (about
  3,100), `database_repository.py` (about 2,850), large web applications, and
  a 6,400-line text-browser test module.
- CI currently runs the full pull-request suite in two workflows, while the
  older review workflow labels `compileall` as lint. Developer documentation
  is extensive but has no single index and still contains machine-specific
  absolute links.

## Packaging defect and repair

The old implicit setuptools discovery produced a wheel containing
`LiuXin_tests` but none of the SQL/TOML files opened by the FRBR database
generator. Import and `--help` smoke tests therefore passed while a clean
installed `liuxin init` could not create its database.

This tranche makes the boundary executable:

- `pyproject.toml` explicitly discovers `LiuXin_alpha*` and the local `past*`
  compatibility shim, and excludes `LiuXin_tests*` plus embedded vendored test
  and demo subpackages. Implicit manifest-driven package data is disabled so
  tracked files below an included namespace cannot bypass those exclusions.
- Package-data ownership is explicit for the FRBR SQL/TOML schema, Catalog
  typing marker, legacy startup layout descriptors, ISO-639 data, bundled
  dateutil zoneinfo, and runtime CoffeeScript sources.
- `scripts/verify_wheel_install.py` compares the wheel with every current
  schema SQL file, rejects test-package leakage, installs the wheel into an
  isolated target, proves the import came from that target, and runs
  `liuxin init` twice to cover create and reopen behavior.
- CI has an `Installed Wheel` job which builds and runs that verifier.
- Shared warning output now goes to stderr so missing optional compatibility
  resources cannot corrupt compact JSON receipts on stdout.
- The project README is included as wheel metadata.

The verified wheel contained 1,814 entries and 86 required runtime assets. Its
isolated install created and reopened a real catalogue, loaded one Store, and
imported `LiuXin_alpha` from the target installation rather than the checkout.

## Package-owned conversion boundary

The former top-level `LiuXin_resources/calibre_resources` tree now lives under
`LiuXin_alpha.resources/calibre`. All 317 files are explicit package data and
the resolver uses `importlib.resources` for its immutable fallback. A new
`LIUXIN_CALIBRE_RESOURCES_DIR` overlay and both historical
`LIUXIN_BASE_DIR` layouts remain supported, with per-file fallback to packaged
data.

The wheel verifier installs the artifact and all direct dependencies into an
isolated target, proves their import origins, creates and reopens a catalogue,
then performs HTML-to-EPUB conversion using a packaged CSS template. The
exercised conversion path established a named `conversion` extra containing
`cssutils`, Pillow, and `regex`; those packages are not imposed on minimal
catalogue/storage installations.

The conversion probe also repaired narrow compatibility defects it exposed:
scratch roots are created on demand, absent optional cover metadata is handled
normally, XML facade iteration no longer passes a constructor as a tag filter,
CSS sorting/font-size/dimension handling no longer relies on Python 2 ordering
or types, and MOBI/KF8 guide, link, title, and index generation now tolerates
the incomplete structures exercised by the existing conversion corpus.

The final wheel contained 2,138 entries, including 1,730 Python files and all
403 inventoried runtime assets (317 from the Calibre compatibility bundle).
Its isolated install imported LiuXin and all seven direct runtime/conversion
dependencies from the temporary target, initialized and reopened a catalogue,
and produced a structurally valid 10-entry EPUB from Unicode HTML. The wheel's
SHA-256 was
`9b6851dafe65bbe6feb5d590202565034ccb313da7589184e36e8bce2eb71313`.

## Next maintainability order

1. Break the seven modern import cycles at leaf protocols and registry seams.
2. Extract bounded command/services from the largest modern orchestration
   files, starting with `core/program_api.py` and `surfaces/cli/storage.py`.
3. Expand the zero-error typing/Ruff ratchet package by package; do not create
   a permanent whole-tree error baseline.
4. Consolidate duplicate CI ownership and rename or replace the compile-only
   `lint` job.
5. Add a developer-documentation index and replace remaining machine-specific
   links with repository-relative links.

## Verification evidence

```text
python3 -m pytest -q tests/scripts/test_verify_wheel_install.py \
  tests/utils/test_logging_streams.py tests/metadata/test_standardize_coverage.py
35 passed

python3 -m pip wheel --no-deps --no-build-isolation \
  --wheel-dir /tmp/liuxin-wheel-fix-20260902-e .
Successfully built liuxin-alpha

python3 scripts/verify_wheel_install.py \
  /tmp/liuxin-wheel-fix-20260902-e/liuxin_alpha-0.0.0-py3-none-any.whl
database_created=true; database_reopened=true; store_count=1

bash scripts/run_type_checks.sh
0 basedpyright errors; strict mypy clean in 95 source files; all ratchets passed

python3 scripts/run_test_stream.py --stream confidence -- --maxfail=1
229 passed, 2 skipped

python3 scripts/run_file_formats_lane.py --lane fast --pytest-args --maxfail=1
797 passed, 1 skipped

python3 scripts/run_file_formats_lane.py --lane heavy --pytest-args --maxfail=1
178 passed

python3 -m pytest -q tests/scripts/test_verify_wheel_install.py \
  tests/scripts/test_build_deployment_package.py \
  tests/utils/resources/test_resources.py tests/utils/test_paths.py \
  tests/test_constants.py
14 passed

python3 scripts/verify_wheel_install.py \
  /tmp/liuxin-calibre-wheel-commit.u8EZDI/liuxin_alpha-0.0.0-py3-none-any.whl
2138 entries; 403 runtime assets; 317 Calibre resources;
database create/reopen and installed HTML-to-EPUB conversion passed
```
