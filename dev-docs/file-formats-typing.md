# File-format typing

`LiuXin_alpha.file_formats` contains both application code and substantial
legacy/vendor-derived format implementations. Its typing migration therefore
uses two explicit gates.

## Complete signature coverage

Every Python module uses postponed annotation evaluation, and every callable
parameter and return value has an annotation. Verify that invariant with:

```bash
.venv/bin/python scripts/annotate_file_formats.py --check
```

The check is also part of `scripts/run_type_checks.sh`.

The migration tool has a write mode:

```bash
.venv/bin/python scripts/annotate_file_formats.py --write
```

Write mode preserves existing annotations. It infers only syntax-level facts
such as literal default types, `None` returns, generator returns, and simple
literal/container returns. Unresolved dynamic boundaries become explicit
`typing.Any` annotations instead of remaining silently untyped.

## Narrowing policy

An explicit `Any` is a migration boundary, not an assertion that the value is
permanently unknowable. Narrow it when the format contract is understood:

- use structural protocols for plugins, archive members, loggers, and similar
  duck-typed collaborators;
- use `str | os.PathLike[str]` for filesystem inputs where both are accepted;
- distinguish text from bytes at parser and compressor boundaries;
- use concrete collection element types where ordering and mutation semantics
  are known; and
- preserve a broad annotation when runtime behaviour genuinely accepts
  heterogeneous legacy values.

Runtime traces and tests can inform a proposed type, but a single observed test
value is not sufficient evidence for a public contract. In particular, fixed
tuple shapes and concrete fixture classes should not replace a more general
sequence or protocol merely because that is what one test exercised.

## Validation

The annotation gate proves coverage, not behavioural correctness. Changes to
these annotations must also run:

```bash
.venv/bin/python -m compileall -q src/LiuXin_alpha/file_formats
.venv/bin/python -m pytest -q tests/file_formats
```

Package-focused `basedpyright` runs are used while broad `Any` boundaries are
narrowed. The existing repository-wide strict checker configuration remains
limited to packages which have completed semantic narrowing; adding all legacy
format internals to that strict set before resolving their historical dynamic
behaviour would obscure the useful signal with unrelated pre-existing errors.
