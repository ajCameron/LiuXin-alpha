# Catalog legacy mutation migration

Reviewed **2026-10-20**. Current repositories, relationship ownership and retrieval boundaries were checked. See [next tasks](next_tasks.md) for remaining work; older run counts retain their original dates.

Caller migration completed: **2026-07-23**. Current boundary reviewed:
**2026-10-20**, against `fc5de7af`.

Status: **Production caller migration complete; Row compatibility remains live.**

## Current outcome

Ordinary semantic persistence enters through Catalog repositories, coordinated
mutations or schema-driven writers. The current code distinguishes three cases:

| Code | Status | Production boundary |
| --- | --- | --- |
| `catalog.catalog_macros` | Frozen reference implementation | No production caller may import it; characterization tests retain access. |
| `catalog.metadata_tools` | Live Row-oriented compatibility | Only `catalog/catalog.py` may import it in production and compose `add`, `ensure`, `apply` and `intralink`. |
| `library.legacy` and `library.backend` | Retired on 2026-10-17 | Modules and their old APIs are absent; the supported facade is `library.library.Library`. |

The [import-boundary test](../../tests/catalog/test_legacy_mutation_boundary.py)
has exactly one direct-import allowance: Catalog importing `metadata_tools`.
Its indirect database/library-facade allowance is empty. It does not prohibit
supported callers from using `catalog.add` and the other Row-shaped adapters.

For Catalog-owned tables, [row_mutations](../../src/LiuXin_alpha/catalog/row_mutations.py)
adapts requests and receipts to shared repositories and `CatalogRelationships`.
Those adapters are deprecated for new code, but are not inactive reference files.
Non-Catalog tables retain low-level fallback paths, outside the Catalog semantic
validity guarantee. See the [write contract](catalog-write-validation-contract.md).

## Boundaries

- New semantic behaviour belongs in metadata value rules, Catalog repositories,
  coordinated mutations or declared writers. An adapter may delegate that behaviour
  while preserving its supported Row-shaped interface.
- Portable macro transactions compose nested operations under the caller's outer
  transaction. Compatibility workflows use that capability when available;
  legacy handles without it do not gain atomicity from the adapter alone.
- Any future pure-SQL fast path belongs behind the same Catalog contract and needs
  semantic parity plus performance evidence. Do not restore callers to frozen macros.
- Characterization tests can import retained reference code. Runtime callers cannot
  expand the exact import allowance without a separate ownership decision.
- Driver version reporting uses inert compatibility metadata rather than constructing
  a mutation facade.

## Caller map

The July migration moved these caller families. Later ownership work changed their
internal delegation without restoring direct dependencies on legacy helpers.

| Callers | Owner now | Status |
| --- | --- | --- |
| Terminal WEMI commands | Core application operations and Catalog WEMI repositories | Creation and relationship policy resides below the command. |
| Terminal tag, genre, subject and series commands | Core/Catalog entity operations | Lookups share declared full-text identities and parent scope where applicable. |
| Terminal creator, organisation and publisher commands | Catalog Agent aggregate operations | Core rows, sidecars, identifiers and links compose inside the portable transaction. |
| Terminal title creation | Coordinated WEMI-stack mutation | Creates and links the requested bibliographic path as a semantic operation. |
| Supported Library metadata workflows | Catalog repositories and coordinated mutations | The old `library.legacy` / `library.backend` facades were subsequently removed. |
| Modern cache writers | Catalog writers plus cache-owned reconciliation | Old Calibre map-writing adapters remain a separate compatibility area. |
| `metadata_sql` identifier helpers | Portable database macro contract | Low-level persistence does not import Catalog mutation facades. |
| SQLite driver version strings | Stable compatibility metadata | Reporting does not load mutation implementations. |

## What was completed, and what was not

The completed migration established the import boundary, migrated terminal,
Library and cache callers, removed database/library helper injection, and retained
reference implementations for characterization. It did **not** retire Catalog's
Row-facing compatibility API.

The October consolidation routed those Row adapters, schema writers, containers
and semantic operations through shared repository preparation and relationships.
The legacy Library retirement removed two unusable APIs and migrated the remaining
fixture caller. Neither change promises semantic validation for fields without
rules, eliminates raw database APIs, or removes all compatibility caches.

Further validators, compatibility owner decisions and read-model work belong to
the [current Catalog roadmap](catalog-roadmap.md). They are separate from the
completed migration of production imports.

## Verification and historical evidence

Current boundaries are exercised by:

- [Legacy import boundary](../../tests/catalog/test_legacy_mutation_boundary.py).
- [Write-route parity](../../tests/catalog/test_write_boundary_parity.py).
- [Write-path normalization](../../tests/catalog/test_write_path_normalization.py).
- [Public API/documentation boundary](../../tests/scripts/test_public_documentation_boundaries.py).

The July completion record reported 396 Catalog tests, 115 storage-cache tests
(with six disabled legacy-Calibre suites skipped), 21 boundary/characterization
cases, 94 terminal creation cases, and strict Catalog typing over 74 files.
Those are historical results, not a run against this review checkpoint.
