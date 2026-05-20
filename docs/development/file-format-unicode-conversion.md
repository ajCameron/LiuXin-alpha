# File Format Unicode Conversion Hardening

This note records the current direction for the file-format unicode and
malformed-input hardening work. The tests should keep using reusable fixtures
and helpers so the same corpus can later drive conversion-matrix coverage.

## Current Test Contract

- Keep shared unicode and conversion helpers under `tests/support/`.
- Exercise real conversion code where possible, not only parser leaf helpers.
- Cover foreign-language text, combining marks, control-like content, invalid
  bytes, malformed markup, and deterministic output bytes.
- Make known lossy boundaries explicit in tests instead of hiding them behind
  broad "contains text" assertions.
- Prefer small, format-specific tests that reuse common corpora over large
  one-off payloads.

## PML Status

The PML pass currently captures two boundaries:

- `PMLMLizer` and `PMLOutput` can serialize supported characters with PML
  escapes and deterministic `.pmlz` output.
- PML cannot represent the full unicode corpus. Unsupported characters are
  currently replaced with `?`.

The replacement behavior is acceptable as an output fallback, but it should not
be silent. Lossy conversion must become visible and reportable before the
broader conversion work depends on it.

## Loss Reporting Direction

Future conversion reporting should expose:

- format and phase, for example `pml-output` or `pml-input-decode`
- number of replaced or unrepresentable characters
- a small sample of affected code points or fragments
- source location when cheaply available
- log/report integration without aborting otherwise recoverable conversion

Tests for that work should assert both sides of the contract: output remains
recoverable, and the loss is recorded in a user-visible or machine-readable
conversion report.
