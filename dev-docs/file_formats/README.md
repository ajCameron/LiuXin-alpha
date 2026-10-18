# File Format Development Notes

This directory holds durable, format-specific notes for conversion and reader
hardening. Keep cross-format strategy in the parent development docs, and keep
format dossiers here when a format grows enough edge cases to need its own
contract.

Each format folder should record:

- converter and reader entry points
- current conversion contract
- unicode and foreign-language coverage
- malformed, hostile, and wrong-format corpus coverage
- loss reporting and recovery boundaries
- archive or container safety limits, when applicable
- guarded override policy for trusted input, when applicable
- external tool adapters or direct conversion edges, when applicable
- validation commands and high-value focused tests
- open follow-ups that should survive branch handoff

Current dossiers:

- [Comic](comic/README.md)
- [DOCX](docx/README.md)
- [EPUB](epub/README.md)
- [FB2](fb2/README.md)
- [HTMLZ](htmlz/README.md)
- [LIT](lit/README.md)
- [MOBI](mobi/README.md)
- [ODT](odt/README.md)
- [PDB](pdb/README.md)

Cross-format composition policy is recorded in
[`plugin-composition.md`](plugin-composition.md). In particular, importing the
`customize.builtins` namespace must not discover the complete converter fleet.
New conversion providers subclass the canonical contracts in
`LiuXin_alpha.file_formats.conversion.plugin`; `customize.conversion` exists only
for historical import compatibility.
OPF parsing and package-mutation ownership is recorded in
[`opf-ownership.md`](opf-ownership.md); generic metadata utilities must not
import OEB/OPF implementations.

Format notes should describe stable behavior and intended policy. Short-lived
branch state still belongs in `working-memory/`.

Shared guidance: [conversion pipeline](../conversion_pipeline/README.md),
[Unicode conversion](file-format-unicode-conversion.md) and
[format typing](file-formats-typing.md). Validation commands are indexed by
[test streams](../hacking/test-streams.md).

Return to the [developer index](../README.md).
