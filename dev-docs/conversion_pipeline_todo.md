# Conversion Pipeline TODO

Date: 2026-05-24

## Purpose

The file-format hardening lane has given LiuXin a much stronger test spine.
The next job is to turn that into a sign-off path for the conversion pipeline
itself, so individual formats and pipeline behaviors can be called "done"
instead of remaining open-ended hardening work.

## Current Shape

The current tests are strongest around:

- reusable format fixtures under `tests/support/`
- unicode and multilingual input/output payloads
- malformed and hostile input coverage
- strict archive/container preflight for ODT, EPUB, DOCX, HTMLZ, comics, FB2,
  LIT, MOBI, and PDB
- metadata reader strict/fallback behavior
- named domain failures instead of raw parser exceptions
- lane execution through `scripts/run_file_formats_lane.py`

The conversion pipeline itself is still mostly the legacy:

```text
input plugin -> OEB normalization -> output plugin
```

That path remains important, but it should become one pipeline edge, not the
whole model.

## Target Shape

Treat conversion as a graph of explicit capabilities:

- normalized OEB-backed edges
- direct format-to-format edges where they preserve more structure
- external-tool adapter edges with version, timeout, and diagnostics reporting
- explicit fallback ordering
- explicit loss reporting
- conversion-product validation after every successful edge

The pipeline should be able to answer:

- which edge was used
- which fallback edges were attempted or skipped
- what metadata, text, images, styles, or structure were lost
- whether loss was expected, recoverable, or a conversion failure
- which diagnostics should be shown to users and stored for later inspection

## Sign-Off Criteria

A format or pipeline edge can be treated as signed off when it has:

- valid fixture coverage for representative multilingual input
- malformed/wrong-format coverage with named failures
- hostile input coverage for the relevant container or parser boundary
- conversion-product assertions, not just "did not crash"
- metadata reader/writer coverage where the format carries metadata
- explicit lossy-boundary tests where the format cannot preserve all input
- clear diagnostics for recoverable loss
- focused validation commands recorded in the format dossier or working memory

## Workstreams

1. Baseline the lane.
   - Confirm `scripts/run_file_formats_lane.py --lane fast` is green.
   - Confirm `--lane heavy` is either green or has intentional, documented
     skips/failures.
   - Record the current counts before changing pipeline behavior.

2. Add a conversion report contract.
   - Record source format, target format, edge name, fallbacks, warnings, and
     loss events.
   - Start with visible/reportable PML unsupported-character replacement.
   - Tests should assert both recoverable output and reported loss.
   - Current implementation slice, selected 2026-05-26: keep PML output bytes
     stable while adding a small structured loss event for unsupported
     character replacement. Initial implementation adds
     `ConversionReport`/`ConversionLossEvent` and wires PML output into an
     aggregate `unsupported-character-replacement` event.

3. Model conversion edges explicitly.
   - Keep OEB-backed edges.
   - Add room for direct `A -> B` edges.
   - Add room for external-tool adapters without binding tests to local tool
     availability.
   - Make fallback decisions deterministic and inspectable.
   - Initial implementation, 2026-05-26: added `ConversionEdge`,
     `ConversionEdgeRegistry`, OEB-backed/direct/external edge constructors,
     and a `Plumber.conversion_edge`/`opts.conversion_edge` description of the
     current legacy OEB-backed path without changing execution behavior.

4. Share archive/container preflight.
   - ODT, EPUB, DOCX, HTMLZ, comics, and FBZ now have similar policy shapes.
   - Pull repeated member-count, path-safety, size, total-expansion, and
     compression-ratio checks into a shared helper when the next format slice
     makes the duplication cost obvious.
   - Initial implementation, 2026-05-26: added
     `file_formats.archive_preflight` for shared ZIP member normalization and
     budget checks, then wired FBZ, HTMLZ, EPUB, DOCX, ODT, and comic ZIP
     preflight through it while keeping format-specific structure checks local.
     ODT keeps its existing skip-unsafe-picture-entry extraction policy while
     sharing the same budget checks.

5. Finish current format hardening tails.
   - PDB Haodoo hostile subreader coverage. Completed 2026-05-27: added
     CP950/UTF-16LE fixture coverage, plugin-path output assertions, malformed
     header/count/title/range tests, and `PDBError` hardening for Haodoo.
   - MOBI bounded decompression and realistic KF8 resource products. Completed
     2026-05-27: PalmDOC/HUFF text expansion now has per-record and total
     budgets, HUFF/CDIC phrase expansion is bounded, and direct/CRES KF8 image
     resources have concrete extraction-product assertions.
   - LIT writer output boundary around unavailable LZX compression. Completed
     2026-05-27: `LitWriter` raises `LitWriterError` before opening filesystem
     output when the compressor backend is unavailable, and storage assembly
     uses the same named guard.

6. Create a sign-off table.
   - Track format, reader, writer, metadata, hostile input, product validation,
     loss reporting, and remaining blockers.
   - Keep the table in a durable development doc once the first edge/reporting
     slice is implemented.
   - Initial table created 2026-05-28:
     `dev-docs/conversion_pipeline_signoff.md`. It separates candidate,
     provisional, blocked, and open rows so format sign-off decisions can be
     reviewed deliberately instead of inferred from the fast lane alone.
   - First signed-off row, 2026-05-31: FB2/FBZ input/output/metadata for the
     current format scope, with focused file-format, metadata, and archive
     preflight validation recorded in the sign-off matrix.
   - Second signed-off row, 2026-06-01: ODT input/container conversion for the
     current format scope, with focused ODT/ODF, metadata, hostile-container,
     and shared archive preflight validation recorded in the sign-off matrix.
   - Third signed-off row, 2026-06-02: EPUB input/container conversion for the
     current format scope, with focused EPUB/OPF, metadata, hostile-container,
     and shared archive preflight validation recorded in the sign-off matrix.
   - Fourth signed-off row, 2026-06-02: DOCX input/container conversion for the
     current format scope, with focused DOCX, metadata, hostile-container, and
     shared archive preflight validation recorded in the sign-off matrix.
   - Fifth signed-off row, 2026-06-03: PDB input/metadata hardening for the
     current format scope, with focused PDB subreader, metadata fallback,
     malformed-input fuzzing, and conversion smoke validation recorded in the
     sign-off matrix.

## Open Decisions

- Whether conversion reports live in the existing plugin option/log surface or
  get a small first-class data object.
- Whether direct/external edges should be registered in conversion plugins or
  in a separate pipeline planner.
- How much loss-report detail should be stored by default versus only emitted
  in verbose/debug modes.
- Whether archive preflight should remain conversion-only or be shared with
  metadata file-source readers.
