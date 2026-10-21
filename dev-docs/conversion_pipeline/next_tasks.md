# Next conversion tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

Keep format mechanics in file_formats and orchestration in Core/jobs. The [pipeline guide](<00 - Conversion Pipeline.md>) records the present limits.

## CONV01. Carry the conversion report through jobs and Core

**Next; architecture A13.** Return a versioned result containing the artifact, selected provider/edge, warnings and loss events. Extend `run_conversion_job` and transport decoding together.

Done when a known lossy TXT/PML conversion retains its report through worker, job view and remote Core result, and an unsupported provider cannot look like successful output.

## CONV02. Isolate each conversion's directory and cleanup

**First for concurrent use.** Remove process-CWD dependence or execute conversions in isolated worker processes. Track scratch ownership and separate temporary products from final publication.

Done when two conversions run concurrently without cross-reading or deleting each other's files; cancellation and failure preserve caller-owned directories and existing output.

## CONV03. Give the edge registry a real consumer

**Following CONV01.** Select one direct or external edge worth supporting, with explicit tool availability, options, timeout and fallback policy. Keep the current OEB path as a named option.

Done when a production workflow selects the edge deterministically and records every attempt, failure and fallback with product checks; no empty planner abstraction is added.

## CONV04. Turn sign-off into reproducible capability evidence

**Following.** For each changed capability, record the fixture scope, tool/dependency versions, expected skips and current limitations alongside product assertions.

Done when a reviewer can reproduce the selected reader/writer case without confusing historical counts or mocked missing dependencies with a supported real installation.

## Validation and routing

Use focused conversion report/top-level tests and the relevant format dossier. Coordinate durable execution with [Jobs](../jobs/README.md) and provider availability with [Plugins](../plugins/next_tasks.md).

Return to [conversion](README.md).
