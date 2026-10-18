"""
Acquisition and ingest pipelines. Import reports from models and operations from their owning modules.

Use ordinary submodule imports; importing this namespace loads no implementations.

Separation of concerns: ingest discovers, extracts, and proposes candidates,
provenance, and acquisition results. Input remains untrusted until metadata value
rules and Catalog mutation policy accept it. Ingest does not turn a parsed
candidate into an implicitly valid database row or bypass Catalog for ordinary
metadata persistence; Library/Core workflows coordinate any accompanying file
storage.

See ``dev-docs/04 - Seperation of Concerns.md``.
"""
