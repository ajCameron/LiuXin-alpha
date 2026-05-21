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

Format notes should describe stable behavior and intended policy. Short-lived
branch state still belongs in `working-memory/`.
