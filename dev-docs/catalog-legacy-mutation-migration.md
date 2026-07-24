# Catalog legacy mutation migration

Status: complete, 2026-07-23.

## Outcome

Production mutation code enters through `Catalog`: repositories own ordinary
entity persistence, coordinated mutations own operations spanning several
entities or tables, and normalized writers own declared relationships.

`catalog_macros` and `metadata_tools` are preserved in place as frozen
reference implementations. They contain useful direct-SQL approaches that may
later inform faster implementations. Preserving them does not make them an
alternative public API: new production callers and new behavior are forbidden.

No production caller outside the preserved legacy packages now imports them or
obtains their `Add`, `Ensure`, `Apply`, or `Intralinker` helpers indirectly.
The reference code and its characterisation tests remain in place.

## Boundaries

- Legacy code is reference and compatibility code, not the owner of new
  catalog semantics.
- Existing behavior is migrated behind a repository, coordinated mutation, or
  normalized writer before its caller changes.
- A migration must preserve transaction ownership: repository and mutation
  operations nest inside a caller transaction, and only the outermost
  portable transaction commits.
- A future pure-SQL fast path belongs behind the same catalog contract. It
  needs semantic parity tests and performance evidence; callers must not be
  switched back to a legacy helper.
- Legacy characterisation tests may import the preserved modules. Production
  import-boundary tests distinguish those tests from runtime dependencies.
- Driver version reporting is bookkeeping, not permission to instantiate the
  legacy facade. It uses a stable compatibility-version constant without
  importing runtime mutation code.

## Migration map

| Callers | Replacement owner | Status and notes |
| --- | --- | --- |
| Terminal `new_work`, `new_expression`, `new_manifestation`, and `new_item` | WEMI repositories | Complete. Single-entity creation normalizes input at the command boundary. |
| Terminal `new_tag` | Tags and Labels repositories | Complete. Exact value creation retains description in the repository payload. |
| Terminal creator, organisation, and publisher commands | Agent repository aggregate operations | Complete. Core Agent, sidecar, identifiers, and organisation relationships are coordinated atomically. |
| Terminal `new_title` | Coordinated WEMI-stack mutation | Complete. Creates and links Work, preferred Expression, Manifestation, and optional Items as one semantic operation. |
| `library.library_metadata` and `library.backend` | Catalog repositories and coordinated mutations | Complete. Metadata import uses Catalog owners; the database object no longer injects legacy facades. |
| Cache writers | Normalized writers or cache-owned compatibility adapters | Complete. Generic Catalog repositories remain independent of Calibre cache conventions. |
| `metadata_sql` identifier helpers | Portable database macro contract | Complete. The database compatibility layer preserves identifier assignment and conflict semantics without importing Catalog facades. |
| SQLite driver version strings | Stable compatibility metadata | Complete. Drivers use an inert version constant without loading runtime mutation code. |

## Order of work

1. Freeze and document the boundary; add a guard that rejects new production
   dependencies while allowing an explicit shrinking allowlist.
2. Migrate the terminal commands, adding missing Agent and WEMI-stack catalog
   operations where an operation spans tables.
3. Migrate cache writers in terms of the normalized link-writer contracts.
4. Migrate library and metadata-SQL operations in coherent transactional
   slices, retaining characterization coverage throughout.
5. Remove runtime helper injection from the database/library objects and empty
   the production allowlist.
6. Retain the legacy sources and tests as an explicitly non-live reference
   area for subsequent SQL optimization work.

All six stages are complete. Future SQL optimization is a new task behind the
current Catalog contracts, not a continuation of this migration.

## Verification

Each slice must prove:

- the migrated caller imports no legacy mutation facade;
- success, validation failure, ambiguity, and rollback behavior are covered as
  appropriate to the operation;
- SQLite and SQLite/APSW real-database behavior remains equivalent where both
  drivers are supported;
- the import allowlist only shrinks; and
- the preserved legacy files are still present and importable by their
  characterization tests.

Completion evidence:

- the production direct-import and indirect-facade allowlists are both empty;
- the full Catalog suite passes (`396 passed`);
- the full storage-cache suite passes (`115 passed`, with six explicitly
  disabled legacy-Calibre suites skipped);
- the boundary and preserved legacy characterization slice passes
  (`21 passed`);
- all terminal `new_*` command cases pass (`94 passed`); and
- strict Catalog type checking passes across `74` source files.
