# Next file format tasks

Date: **2026-10-20**. Proposed work; this list does not implement the changes.

The dossiers retain useful format-specific evidence. Prioritize live failures and real product checks before broad new format support.

## FMT01. Resolve the DOCX EMF failure explicitly

**First; architecture A13.** Move optional-capability detection into a controlled boundary. Either supply a supported EMF raster extractor or report the unsupported image without an unguarded import.

Done when a real EMF-bearing fixture exercises the installed path, the resulting document and diagnostics are checked, and tests do not inject a fake missing module.

## FMT02. Make unavailable providers visible

**Next.** Decide whether HTML2ZIP should have a resource-preserving headless implementation or be exposed as unavailable. Apply the same availability contract to LIT compressor support and optional external archive tools.

Done when discovery, invocation and user-facing results agree on availability and its reason; an installed name alone is never the success criterion.

## FMT03. Close format-specific product gaps

**Following; measured corpus cases.** Use the existing MOBI/KF8 skeleton/div/NCX, PDB output, comic RAR and LIT output gaps to choose bounded fixtures. Preserve archive budgets and explicit trusted overrides.

Done when the chosen real product is readable with expected content and metadata, and malformed/budget-exhausted inputs have named failures without partial publication.

## FMT04. Narrow contracts where tests establish behaviour

**Following.** Keep OPF utilities in file_formats and generic metadata helpers format-neutral. Replace broad Any annotations at selected parser/plugin boundaries with meaningful types, preserving actual optional-tool and bytes/text behaviour.

Done when the annotation gate and focused format tests pass, and new types describe more than the concrete fixture used by one test.

## Validation and routing

Use each dossier's focused tests, [format typing](file-formats-typing.md), and [conversion tasks](../conversion_pipeline/next_tasks.md) for shared receipts and scratch isolation. Do not interpret a historical format sign-off as a claim of complete hostile-input coverage.

Return to [file format](README.md).
