# Configuration ownership

Reviewed **2026-11-23**; configuration closure follows the Core contract corrections in `7dc7d426`.

Core selects a deployment in `core/settings.py` and composes application settings
before opening a local database, storage manager or cache. Surfaces parse arguments
and pass the resolved paths into `create_core`. A remote surface leaves application
preferences with the remote Core; a local profile cannot redirect the server's files.

`ConfigurationPaths` is an immutable pair of absolute paths. Resolving it does not
create directories or load settings. A component object captures its path at
construction; opening another profile cannot redirect an existing object.

## Diagnostic redaction

`core.settings.redacted_manifest` returns an independent diagnostic copy. It
traverses nested mappings and arrays, masks secret-labelled fields, URL passwords,
sensitive query parameters (including encoded keys and nested URLs), and URL
fragments. Malformed URLs, cycles, excessive depth and opaque objects are replaced
with `<redacted>`. Safe connection context such as host, IPv6 brackets, path and
non-secret options is retained. The actual connection values are never modified.
This recognizes structured credential forms; it is not arbitrary-secret detection
in free text. Configuration diagnostics must use this projection before display.

## Selection and path precedence

Connection selection retains its existing order: explicit connection or profile,
then `LIUXIN_SYSTEM_ROOT`/`LIUXIN_PROFILE`, then the persisted connection pointer.
Conflicting explicit selectors are rejected. Named profiles and connection pointers
share the absolute `XDG_CONFIG_HOME/liuxin` root, falling back to `~/.config/liuxin`.
Those pointers remain user-wide so that changing deployment does not hide them.

A manifest may specify `preferences_directory` and `config_directory`; relative
paths resolve against the final manifest, including through a named-profile pointer.
For each path, an explicit manifest/API value wins over environment overrides.
`LIUXIN_SCRATCH_DIR` overrides the native temporary-file directory. The test
harness supplies it for the session and each test, including child processes;
without it the existing `<base>/LiuXin_scratch` default applies. The directory is
created on use, not by importing path constants.

`LIUXIN_PREFS_DIR` overrides the default preference directory. `LIUXIN_CONFIG_DIR`
wins over `CALIBRE_CONFIG_DIRECTORY` for independent component configuration.

Without overrides, a selected system uses `SYSTEM_ROOT/LiuXin_prefs`, with component
files in its `calibre_config` child. Without a selected system, `LIUXIN_BASE_DIR`
retains the development-tree layout when explicitly set; installed applications
use `XDG_CONFIG_HOME/liuxin` or `~/.config/liuxin`. No installed default depends on
the package installation directory or current working directory.

## One application store

`Preferences` owns the application mapping and its `LiuXin_prefs_file.ini`.
`preferences.preferences`, `config_base.prefs` and `config_base.tweaks` expose the
same lazy view. There is no separate global JSON cache or tweak mapping. The view
resolves the selected store at use time, so modules imported before profile
selection still see that profile during Core work.

Read precedence, from lowest to highest:

1. Application defaults, common packaged tweak defaults, and explicit LiuXin
   differences in `configuration_defaults.py`.
2. Literal assignments in the selected component directory's `tweaks.py`.
3. Values in its `global.py.json`.
4. Values in the application INI.

The former JSON/tweak files are read inputs. All ordinary setting writes go to the
application INI. They do not rewrite those inputs or maintain several live writers.
A full saved INI therefore takes precedence over later edits to an input file.
Existing LiuXin defaults for shared keys are preserved, including `language`,
`library_path`, `saved_searches`, `user_categories` and import policy. Common tweak
defaults are declared once; only actual LiuXin differences are retained separately.

Tweak inputs accept literal Python assignments, collections and documentation
strings. Imports, calls and control flow are rejected with the input location;
settings loading never executes their code. Configuration imports do not open the
application store. Fresh imports of configuration leaves, Core's factory, `utils.paths` and
`utils.ptempfiles` neither load the application store nor create operational
folders or install legacy startup hooks. `ptempfiles` uses the passive utility
path owner; the import-direction gate forbids `utils` importing `startup_scripts`.
Constructing/loading a store with `upgrade_on_load=False` reads
without creating or upgrading files. Explicit initialization retains the existing
INI repair behavior; ordinary setting writes persist through that same store.

Raw tweak read/write helpers are explicit file tools. Refresh the application
store to re-read those inputs. A temporary `Tweak` override changes memory and
restores it on exit; it does not persist the temporary value. Tweak reset restores
declared tweak keys and retains unrelated application settings.

## Scope and consumers

Core captures a concrete application store before bootstrap. Explicit injection
wins over the borrowed database's store, then the current selection. An empty
mapping remains a valid injected store. A `PreferenceView` is resolved once at
construction; retaining the proxy would let a later caller redirect an existing
Core. Owned databases and their services receive the same concrete store.
Borrowed databases retain their caller-owned state; an explicit service override
does not rewrite their preference attribute.

Library graph construction, lazy service construction and cleanup run in the
captured context. Entering a `LibraryServices` context binds its store until exit,
including cleanup and exception paths. Context bookkeeping is reset on exit so
repeatedly reopened graphs do not accumulate ContextVar keys in a worker. Core
command/query dispatch scopes that
same store and nested dispatch restores the outer selection. Returned collaborators
retain their own operation contracts; accessing an object through CoreServices
does not wrap every later call to that object.

The managed executor captures the submitting store even when it is the unbound
process default. Queued work and retries retain that store; the profile active at
retry time cannot redirect the original work. Other ContextVars still travel with
the submitting thread context. Serial jobs share the selected object in process.
Spawned jobs receive a serialized copy when the worker starts, rebuild codecs
without re-reading files, and retain the selected component paths. This is not an
immutable snapshot of values at enqueue time or live cross-process synchronization.
Injected stores must be serializable for process execution. Serialization/backend
startup failures produce terminal failed jobs and close their pipe endpoints;
they do not leave jobs permanently marked running.

Standalone consumers can construct `Preferences(paths=...)` and use
`use_preferences(store)` around work. The process-default view is lazy; changing
environment variables after that store has loaded does not reconfigure it.
`Preferences.refresh()` reloads values from the same fixed paths. Select another
store explicitly to change deployment.

## Connection ownership and worker reopening

`core.connections` resolves the private opening options before resources open and
captures the effective native driver target/schema for a CoreServices lifetime.
SQLite and APSW use absolute paths; changing working directory does not redirect
a queued worker. Built-in driver aliases are normalized. PostgreSQL accepts URL
and service selectors as well as the driver's metadata aliases. An explicit
selector wins over competing metadata target aliases. The schema is resolved
once, including an environment-selected schema, and carried with the target.

Named ingest/sync/backup workers receive independent copies of the complete
connection metadata. One-shot registration helpers also accept backend metadata
and preserve URL/service selectors instead of converting them to filesystem paths. Durable workers use the same captured options, with fresh
owned database connections and the captured application store. No live connection
crosses a thread/process boundary. Mutating the original mapping or a returned
copy cannot change subsequent submissions. The explicit alternate-target form of
`sync.store.start` resolves independently and does not inherit the current
library's credentials or schema. Reopening always leaves creation disabled.

PostgreSQL service-file contents and authentication remain driver-owned inputs
read when opening a connection; this does not freeze service files, password
rotation or every libpq environment variable. Borrowed custom facades without
connection metadata support synchronous operations but cannot supply a worker
reopening target. Job summaries expose argument names rather than connection
values; diagnostic rendering must use the redaction projection described above.

## Component lifetime inventory

Application settings, library `DBPrefs` and component files remain separate domains.
The following inventory makes the existing capture/reload policy explicit; it
does not imply every process singleton is independently configurable per Core.

| Consumer | Capture and reload policy | Multiple profiles |
| --- | --- | --- |
| `ConfigurationPaths` | Immutable absolute paths; resolution creates no directories | Resolve a new value for each deployment |
| `Preferences` | Paths fixed at construction; `refresh()` reloads those same files | Construct one store per deployment |
| Global preference/tweak views | Resolve the current ContextVar at use time; process fallback captures paths on first use | Core captures the concrete store before handing it to long-lived components |
| `CoreServices` / `LibraryServices` | Store and reopening options captured at construction; lazy construction/cleanup bind that store | Separate graphs keep their selections; rebuild a graph to change its connection |
| Managed jobs | Store selected at submission and retained on retry; process values copied at spawn | Queued jobs keep their original store and component paths |
| `DBPrefs` | Bound to its database; library persistence remains database-local | One facade per library; application updates do not rewrite it |
| `Config` / `ConfigProxy` | Component directory captured when `Config` is constructed; proxy refresh rereads that path | Construct within the intended context or supply `base_path` |
| `DynamicConfig` / `JSONConfig` / `XMLConfig` | File path captured at construction; refresh does not relocate it | Existing objects remain pinned when another profile is selected |
| Conversion configuration files | Path resolved from the active component directory on each operation | Enter the intended preference context around the operation |
| `plugins.configuration.config` | Process singleton captures the component directory at module import; refresh rereads the same plugin configuration | Separate plugin registries/configurations require separate processes; opening another Core does not retarget an imported registry |
| `file_formats.covers.cprefs` | Process singleton captures its JSON path at import; defaults remain shared declarations | Pass explicit `prefs` to cover generation for another profile |
| `constants.paths`, `constants.cache_dir`, `ptempfiles` | Process operational defaults captured at import; scratch overrides are process-wide; temporary objects retain their selected directory | Pass explicit directories for independent scratch/cache resources; do not switch global scratch roots concurrently |
| Plugin/driver registries and packaged resources | Process installation state, not application preference stores | Reconfiguration is an explicit registry/deployment operation |

Import-order, profile, retry and worker acceptance lives in
`tests/core/test_configuration_scope.py`, `test_configuration_connections.py` and
`test_configuration_workers.py`, alongside `tests/preferences/test_configuration_ownership.py`.
The worker suite covers SQLite/APSW, serial/spawned jobs, failure cleanup and
optional live PostgreSQL URL/service schemas (`LIUXIN_TEST_POSTGRES_URL`).

Connection propagation does not imply every domain workflow supports every native
schema. The live acceptance run found that unmanaged-disk ingestion still requires
a legacy `files` table; native PostgreSQL rejects it before scanning. This remains
tracked under domain completion in the [Core review](review-and-completion-plan-2026-11-23.md#configuration-closure-follow-up).
The supported named maintenance workflow covers end-to-end public dispatch and
durable reopening for PostgreSQL in this configuration slice.
