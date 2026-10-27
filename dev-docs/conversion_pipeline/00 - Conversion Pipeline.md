# Conversion pipeline

Reviewed **2026-10-20** against the current implementation. This guide separates
the executable conversion path from the proposed planner and durable job contract.

## Current execution and owners

The ordinary path is `Plumber` → input plugin → OEB processing → output plugin.
[Plumber](../../src/LiuXin_alpha/file_formats/conversion/plumber.py) owns the
conversion sequence and option recommendations. Format implementations own their
parsers, product validation and format-specific loss behaviour.
[Input/output contracts](../../src/LiuXin_alpha/file_formats/conversion/plugin.py)
belong to `file_formats.conversion`; the
[application plugin host](../plugins/plugin-composition.md) discovers and composes
providers. A standalone converter can use those contracts without a database.

Application execution enters through Core's
[conversion job worker](../../src/LiuXin_alpha/core/workflow_jobs.py). That worker
constructs `Plumber`, applies requested options, runs it and reports paths, format
labels, output existence and byte size. It currently does **not** return the full
structured conversion-loss report. A completed worker or nonempty file therefore
does not by itself mean a lossless conversion.

## Reports and edge descriptions

[ConversionReport](../../src/LiuXin_alpha/file_formats/conversion/report.py) holds
source/target/edge context, warnings and structured loss events, with `to_mapping()`
for serialization. Reports are mutable accumulators; their loss-event/sample
records and the registry's edge values have their own data contracts.
PML, TXT, HTMLZ and comic diagnostics use portions of this reporting machinery.
Coverage varies by format and recovery path.

[ConversionEdgeRegistry](../../src/LiuXin_alpha/file_formats/conversion/edges.py)
describes OEB, direct and external edges. It is not yet a production planner that
selects and executes a fallback graph. External tool discovery, versions, timeouts
and fallback outcomes need a consuming workflow before they can be claimed as
end-to-end capabilities.

## Scratch and concurrency

`InputFormatPlugin.__call__` temporarily changes the **process** working directory
and clears entries in the supplied output directory before conversion. Callers
must supply a fresh directory owned by that conversion. Do not pass a source
directory, shared output directory or directory containing unrelated files.

The implementation is not safe for concurrent threaded conversions merely
because each caller chose a different path: process CWD is shared. Explicit
directory handling or process isolation, ownership-aware cleanup and failure
recovery are follow-up work. Output publication and database persistence are
separate effects; the format pipeline alone does not provide one transaction
covering them.

## Availability and success

Provider registration is distinct from usable conversion. The retained DOCX EMF
handler references an absent module, and `HTML2ZIP.run` explicitly rejects the
unavailable GUI path. LIT full output still needs an available compressor.
The [format dossiers](../file_formats/README.md) record narrower reader, writer,
malformed-input and product contracts.

A future application receipt should distinguish success, recoverable loss,
unsupported capability, cancellation and failure. It should identify the provider,
options and artifact verification without exposing credentials or relying on a
log line as the only explanation. See [next tasks](next_tasks.md).

## Validation

Start with one format's compact fixtures and assert the produced artifact,
preserved content and loss diagnostics. For cross-format changes,
`scripts/run_file_formats_lane.py` offers `fast`, `heavy` and `all` lanes; inspect
its help and the [test-stream guide](../hacking/test-streams.md) before a larger run.
Optional tools and their skips must be visible in results.

The [sign-off matrix](conversion_pipeline_signoff.md) preserves dated evidence.
The [earlier work plan](conversion_pipeline_todo.md) explains the campaign's
direction, not a replacement for current source or a claim that all edges work.

Virus inspection may become a separate processing step with a quarantine and
reporting contract. It is not currently a general conversion or repair guarantee.

Return to the [conversion index](README.md).
