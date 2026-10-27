# Import shim removal

Raw evidence paths marked "local-only" refer to ignored outputs from the dated
runs. They are not distributed with a clean checkout; the recorded results and
tracked handoffs remain the portable evidence.

Requested: 2026-09-15; completed 2026-09-16. The user asked to check for and remove
the remaining shims. This was a functional cleanup separate from the then-paused
documentation campaign. Reviewed for current status **2026-10-20** against the
working tree based on `fc5de7af`; the results below remain September evidence.

Work proceeded by owner, with before-source snapshots and test command/exit records
under the local-only `working-memory/test-results/shim-removal-2026-09-15/`.

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

At this campaign checkpoint the documentation work was paused at D095. It later
completed on **2026-10-05**; see the
[completion record](../../working-memory/project-docstrings-l001-l037-2026-10-05.md).
Do not use the September pause or inventory as a current work queue.


## Audit decisions

The September cleanup removed 61 forwarding/stub modules, the module-level lazy
export hooks identified in that campaign, the storage backend/value aliases,
the mixed-case database-driver loader, deprecated
crawl/SQL/PDF worker entry points, and fake calibre namespace installation.
Consumers migrated to concrete owners. The entry point was set to
`LiuXin_alpha.surfaces.cli.app:main`, and the editable installation was refreshed
at that checkpoint. A new checkout still needs its own installation.

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

At September close-out the documentation inventory had 2,671 live files, 804
reviewed and 1,867 remaining. Removed scopes were archived, and every surviving
queued declaration retained one assignment within the existing four-file/forty-
declaration limits. Affected files received explicit new functional baselines;
historical documentation-only
proofs and original hashes were preserved. The recorded D096 pause was later
superseded by the completed documentation campaign.

Final evidence (local-only output: `working-memory/test-results/shim-removal-2026-09-15/final-observations.json`) records exact commands, exit results, current source hashes and retained failed attempts.


## Current interpretation — 2026-10-20

This completed campaign is not a ban on every lazy import, facade or adapter.
Later work deliberately uses lazy imports to keep passive APIs small; cold-import
budgets and named ownership rules govern that behaviour.
`storage.store_manager` is a retained forwarding module at this checkpoint, so
“all shims are gone” would be an inaccurate whole-repository claim.

The separate October plugin migration retired both `customize` and
`plugin_system`; callers now use `plugins`. See the
[plugin import migration](../plugins/plugin-composition.md#import-migration).
Persisted ingest tags and backend-kind strings remain data contracts, not
permission to resurrect removed Python namespaces.

Before removing another adapter, identify its callers, external/data contracts,
behaviour and replacement owner. Keep installed-wheel and focused caller tests
with the change. The September collection count and quality result above have
not been rerun by this documentation review.
