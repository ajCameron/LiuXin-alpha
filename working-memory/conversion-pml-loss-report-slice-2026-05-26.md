# Conversion PML Loss-Report Slice - 2026-05-26

## Context

The conversion pipeline work is moving from format-by-format hardening toward
sign-off-able pipeline behavior. The agreed next implementation slice is to
make one known lossy boundary visible before changing planner shape.

## Current Slice

Target: PML output.

Current behavior:

- `PMLMLizer.clean_text()` converts many non-ASCII characters to PML `\aNNN`
  or `\UNNNN` escapes.
- Characters outside PML's supported set fall back to `?`.
- Existing tests already pin that output fallback.

Problem:

- The fallback is acceptable as recoverable output, but it is silent.

Desired first contract:

- Keep PML output behavior unchanged.
- Add a small structured conversion/loss report surface.
- Record unsupported-character replacement as a recoverable loss event.
- Assert both output and report behavior in focused PML tests.

## Why This First

This gives the conversion pipeline a real diagnostic contract before larger
work on explicit conversion edges, fallback planning, or external adapter
registration. PML is a good first edge because the lossy boundary is already
known, deterministic, and covered by existing output tests.

## Step 1 Baseline

Before implementation, run:

```text
python3 scripts/run_file_formats_lane.py --lane fast
```

Record the result before changing conversion behavior.

Baseline result:

```text
Discovered 115 file_formats test files
Lane 'fast' has 96 test files
751 passed, 1 skipped, 15 warnings in 42.11s
```

Skip:

```text
tests/file_formats/oeb/test_oeb_polish_smoke.py:26:
cssutils not installed; embedded oeb polish unittest suite requires it
```

## Step 2 Implementation

Added:

- `src/LiuXin_alpha/file_formats/conversion/report.py`
- `tests/file_formats/conversion/test_conversion_report.py`

Updated:

- `src/LiuXin_alpha/file_formats/conversion/plugins/pml_output.py`
- `src/LiuXin_alpha/file_formats/pml/pmlml.py`
- `tests/file_formats/conversion/test_conversion_top_level_smoke.py`
- `tests/file_formats/pml/test_pml_unicode_framework.py`

Behavior:

- `ConversionReport` records source format, target format, edge name,
  warnings, and loss events.
- `ConversionLossEvent` records phase, code, message, count, recoverability,
  context, samples, and details.
- `PMLOutput` now attaches `opts.conversion_report` when possible and exposes
  the same report on `plugin.conversion_report`.
- `PMLMLizer.clean_text()` still produces the same recoverable `?` output for
  unsupported PML characters, but now records one aggregate
  `unsupported-character-replacement` loss event when a conversion report is
  present.

Validation:

```text
python3 -m pytest -q \
  tests/file_formats/conversion/test_conversion_report.py \
  tests/file_formats/conversion/test_conversion_top_level_smoke.py \
  tests/file_formats/pml/test_pml_unicode_framework.py
10 passed in 9.96s
```

Post-change fast lane:

```text
python3 scripts/run_file_formats_lane.py --lane fast
Discovered 116 file_formats test files
Lane 'fast' has 97 test files
755 passed, 1 skipped, 15 warnings in 57.40s
```

## Step 3 Edge Model

Added:

- `src/LiuXin_alpha/file_formats/conversion/edges.py`
- `tests/file_formats/conversion/test_conversion_edges.py`

Updated:

- `src/LiuXin_alpha/file_formats/conversion/plumber.py`
- `src/LiuXin_alpha/file_formats/conversion/plugins/pml_output.py`
- `tests/file_formats/conversion/test_conversion_top_level_smoke.py`
- `tests/file_formats/pml/test_pml_unicode_framework.py`

Behavior:

- `ConversionEdge` can describe OEB-backed, direct, and external-tool edges.
- `ConversionEdgeRegistry` stores deterministic, priority-ordered candidate
  edges.
- `legacy_oeb_edge()` describes the current legacy path:
  `input plugin -> OEB transforms -> output plugin`.
- `Plumber` now exposes the selected legacy edge on `plumber.conversion_edge`
  and `opts.conversion_edge`, without changing execution behavior.
- PML loss reports use the explicit edge name when `opts.conversion_edge` is
  available.

Validation:

```text
python3 -m pytest -q \
  tests/file_formats/conversion/test_conversion_edges.py \
  tests/file_formats/conversion/test_conversion_report.py \
  tests/file_formats/conversion/test_conversion_top_level_smoke.py \
  tests/file_formats/pml/test_pml_unicode_framework.py
15 passed in 9.24s
```

Post-edge fast lane:

```text
python3 scripts/run_file_formats_lane.py --lane fast
Discovered 117 file_formats test files
Lane 'fast' has 98 test files
760 passed, 1 skipped, 15 warnings in 58.82s
```

## Step 4 Shared Archive Preflight

Added:

- `src/LiuXin_alpha/file_formats/archive_preflight.py`
- `tests/file_formats/test_archive_preflight.py`

Updated:

- `src/LiuXin_alpha/file_formats/fb2/archive.py`
- `src/LiuXin_alpha/file_formats/conversion/plugins/htmlz_input.py`
- `src/LiuXin_alpha/file_formats/conversion/plugins/epub_input.py`
- `src/LiuXin_alpha/file_formats/docx/container.py`
- `src/LiuXin_alpha/file_formats/odt/input.py`
- `src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py`

Behavior:

- Centralized ZIP member normalization and preflight budget checks:
  member-count limits, per-member uncompressed size, total expanded size,
  invalid compressed size, suspicious compression ratio checks, and default
  unsafe path rejection.
- Kept format-specific structure validation local: HTMLZ top-level HTML,
  EPUB mimetype/container/OPF/manifest/spine checks, DOCX and ODT required
  members, FBZ single-FB2-member selection, and comic RAR password/name-only
  backend behavior.
- Preserved ODT's existing policy where unsafe `Pictures/...` archive entries
  are skipped during picture extraction rather than rejecting the whole
  document; ODT still uses the shared helper for ZIP budget checks and required
  member normalization.
- FB2 keeps its existing `FB2ZipError` wrappers and default constants, but the
  actual ZIP member scan now uses the shared helper.

Validation:

```text
python3 -m pytest -q \
  tests/file_formats/test_archive_preflight.py \
  tests/file_formats/fb2/test_fb2_malformed_hostile.py \
  tests/file_formats/htmlz/test_htmlz_malformed_hostile.py \
  tests/file_formats/docx/test_docx_malformed_hostile.py \
  tests/file_formats/epub/test_epub_malformed_hostile.py \
  tests/file_formats/odt/test_odt_container_framework.py \
  tests/file_formats/odt/test_odt_malformed_hostile.py \
  tests/file_formats/comic/test_comic_malformed_hostile.py
153 passed in 20.72s
```

Post-shared-preflight fast lane:

```text
python3 scripts/run_file_formats_lane.py --lane fast
Discovered 118 file_formats test files
Lane 'fast' has 99 test files
775 passed, 1 skipped, 15 warnings in 46.85s
```

## Sign-Off Status

The PML output lossy-boundary row was signed off on 2026-06-03. The signed-off
scope is the deterministic PML output boundary, recoverable `?` replacement for
unsupported characters, and the aggregate `unsupported-character-replacement`
`ConversionReport` event with edge context. Broader report plumbing across other
lossy formats and planner/fallback semantics remain provisional pipeline work.

Focused validation:

```text
python3 -m pytest tests/file_formats/pml tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py -q
57 passed in 8.97s

python3 -m pytest tests/metadata/file_sources/test_pml_metadata_source.py -q
12 passed in 9.69s

python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
4 passed in 7.37s
```
