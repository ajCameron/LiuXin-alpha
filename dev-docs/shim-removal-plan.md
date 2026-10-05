# Import shim removal

Requested: 2026-09-15; completed 2026-09-16. The user asked to check for and remove
the remaining shims. This is a functional cleanup separate from the paused
documentation campaign.

Work proceeds by owner, with before-source snapshots and test command/exit records
under working-memory/test-results/shim-removal-2026-09-15/.

| Group | Scope | Checkpoint |
| --- | --- | --- |
| S1 | Storage forwarding modules and lazy utils/SquashFS exports | Focused tests passed |
| S2 | Backend-specific aliases of Location/FileInfo | 194 selected tests passed |
| S3 | Manager/value aliases, redundant factory and library forwarder | 69 selected tests passed |
| S4 | SQLite compatibility subclass | 38 selected tests passed |
| S5 | Cache, metadata, utility and SQL leaf forwarders | 327 passed, 14 skipped |
| S6 | Remaining lazy package exports and terminal/CLI forwarders | Corrected regressions passed; obsolete-facade fixtures updated |
| S7 | Remaining compatibility functions and import injection | 288 selected tests passed, including S6 corrections |
| S8 | Broad regressions, configured quality checks, documentation inventory | Complete: quality, entry points, 32 final checks, inventory and documentation validation passed |

For each group: identify owners and all callers, migrate source/tests/examples
and current guides, remove forwarding code, verify focused behavior, then record
the result. Preserve existing unrelated work and historical evidence.

Persisted ingest journal type identifiers remain stable through an explicit codec
mapping. Their classes now have their real Python module identity. Backend kind
strings and database schemas are data contracts, not Python import shims.

Contract API packages, functional adapters, fallback implementations and domain
type annotations need a concrete reason for removal. A keyword match alone does
not justify deleting behavior. Record retained candidates and their purpose in
the final audit.

The documentation campaign remains paused at D095. Its live inventory has been
reconciled after this functional change; older documentation-only proofs and
baseline snapshots remain historical.


## Audit decisions

The cleanup removes 61 forwarding/stub modules, all module-level lazy export hooks,
the storage backend/value aliases, the mixed-case database-driver loader, deprecated
crawl/SQL/PDF worker entry points, and fake calibre namespace installation.
Consumers import concrete owners. The installed liuxin entry point is now
LiuXin_alpha.surfaces.cli.app:main; the editable installation has been refreshed.

These candidates remain because they provide behavior or define a current contract:

- Contract API packages and ordinary imports used by implementation modules:
  these are explicit public contracts or local dependencies, not old-path dispatch.
- Store-facing error names and ID type annotations: domain vocabulary over a
  shared exception/type hierarchy; removing these would redesign the API.
- Ingest journal wire tags, backend-kind aliases and schema adapters:
  persisted-data contracts remain valid without exporting old Python names.
- IPC workers: construct job requests, handle cancellation, and translate
  result/error/log data; this is a functional adapter.
- Optional CHM, clint, tqdm, regex and compiled-plugin fallbacks, plus bundled
  Python-version compatibility libraries: these supply real fallback behavior
  or a dependency API and were not deleted merely for containing 'shim'.
- Calibre metadata classes and diagnostics: implementations remain under their
  real LiuXin modules. No fake calibre package installer remains.

The former GUI conversion stub is gone. HTML2ZIP reports its existing unsupported
headless operation at the caller; this change does not add GUI conversion.
The no-op GUI locale helper is also gone from the spelling command.

## Verification and inventory

Each group has before-source snapshots, exact commands, logs and terminal exit
records in the evidence directory. Failed first attempts are retained alongside
their corrections. Test selections overlap and must not be summed as unique tests.
The complete configured quality runner passed. Repository-wide pytest collection
succeeded with 7,117 tests; this is collection coverage, not a full-suite run.

The documentation inventory now has 2,671 live files, 804 reviewed and 1,867
remaining. Removed scopes are archived, and every surviving queued declaration
still has one assignment within the existing four-file/forty-declaration limits.
Affected files have explicit new functional baselines; historical documentation-only
proofs and original hashes remain unchanged. D096 remains paused until requested.

[Final evidence](../working-memory/test-results/shim-removal-2026-09-15/final-observations.json) records exact commands, exit results, current source hashes and retained failed attempts.
