# Metadata Writer Coverage Contract

This document defines the local quality bar for metadata writer tests. It is a
coverage plan and a safety contract: writer tests should prove that metadata
updates do not corrupt the target file or silently emit malformed metadata.

## Scope

The writer lane covers metadata mutation paths in:

- metadata file-source modules with `set_metadata` or equivalent write helpers
- metadata reader/writer plugin adapters
- OPF serialization and embedded OPF replacement
- archive/container formats such as EPUB, EXTZ, DOCX, ZIP-like wrappers
- binary formats such as MOBI, PDF, PDB/eReader, Topaz, and RTF

## Core Invariants

Every writer test should prove as many of these invariants as the format allows:

- Round-trip readability: write metadata, then read it back with the local
  reader and assert the expected fields changed.
- Container validity: archive/container files still open after write, and
  required members remain present.
- Payload preservation: non-metadata content is unchanged unless the writer is
  explicitly expected to replace it.
- Encoding safety: hostile strings do not create invalid XML, broken RTF/PDF
  escapes, embedded NUL damage, unpaired surrogate output, or truncated files.
- Clean failure behavior: unsupported or unsafe writes raise or return cleanly
  without leaving a partially corrupted file.
- Explicit unsupported behavior: if a format cannot safely support a metadata
  field, the writer should ignore or reject it deliberately, not half-write it.

Local tests cannot prove that every external application accepts a rewritten
file. They should prove local structural validity using available parsers such
as `zipfile`, XML parsers, the project reader, and optional PDF tooling when it
is installed.

## Unicode And Escaping Threat Set

Writer tests should use a shared hostile metadata matrix including:

- precomposed and combining forms
- CJK, Greek, Cyrillic, Arabic or right-to-left text
- emoji and astral-plane characters
- smart punctuation and full-width separators
- embedded NULs and C0 control characters
- unpaired surrogate code points
- XML-invalid characters
- RTF-sensitive braces, backslashes, and `\uNNNN?` escape edges
- PDF-sensitive parentheses, backslashes, hex strings, and name escapes
- very long title, author, tag, and comment fields

The expected policy is format-specific:

- escape when the target format has a clear safe representation
- sanitize only when the format requires it and the loss is documented
- reject or no-op cleanly when preserving the value would corrupt the file

## Test Strategy

Use small synthetic fixtures first:

- `BytesIO` for stream-safe formats
- temporary files for path-only writers
- inline ZIP/container builders for archive formats
- byte-for-byte comparisons for unaffected payloads
- read-after-write assertions through the project metadata readers

Avoid mutating checked-in binary fixtures directly. When a binary fixture is
required, copy it to a temp path before writing.

## Work Order

1. OPF and XML-backed writers: easiest to validate with XML reparsing and
   read-after-write checks.
2. RTF, PDB/eReader, and Topaz: local `BytesIO` round trips are practical and
   already have synthetic fixture helpers.
3. EPUB, EXTZ, DOCX, and ZIP-like containers: validate archive integrity, member
   preservation, embedded OPF replacement, and cover behavior.
4. MOBI and PDF: treat as the hard binary lane. Use compact builders where
   possible, optional dependencies where useful, and copied fixtures where local
   builders would become more complex than the behavior under test.

## Acceptance

The writer lane is in good shape when common writer paths have tests for:

- simple field updates
- unicode torture metadata
- malformed/unsupported values
- stream and path inputs
- unchanged non-metadata payload
- clean failures on invalid files
- read-after-write verification
