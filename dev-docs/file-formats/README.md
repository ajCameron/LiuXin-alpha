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

- `comic/`
- `docx/`
- `epub/`
- `fb2/`
- `htmlz/`
- `lit/`
- `mobi/`
- `odt/`
- `pdb/`

Cross-format composition policy is recorded in
[`plugin-composition.md`](plugin-composition.md). In particular, importing the
`customize.builtins` namespace must not discover the complete converter fleet.
OPF parsing and package-mutation ownership is recorded in
[`opf-ownership.md`](opf-ownership.md); generic metadata utilities must not
import OEB/OPF implementations.

Format notes should describe stable behavior and intended policy. Short-lived
branch state still belongs in `working-memory/`.
