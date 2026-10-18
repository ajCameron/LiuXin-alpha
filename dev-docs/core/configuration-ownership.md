# Configuration ownership

Core selects a deployment in `core/settings.py` and composes application settings
before opening a local database, storage manager or cache. Surfaces parse arguments
and pass the resolved paths into `create_core`. A remote surface leaves application
preferences with the remote Core; a local profile cannot redirect the server's files.

`ConfigurationPaths` is an immutable pair of absolute paths. Resolving it does not
create directories or load settings. A component object captures its path at
construction; opening another profile cannot redirect an existing object.

## Selection and path precedence

Connection selection retains its existing order: explicit connection or profile,
then `LIUXIN_SYSTEM_ROOT`/`LIUXIN_PROFILE`, then the persisted connection pointer.
Conflicting explicit selectors are rejected. Named profiles and connection pointers
share the absolute `XDG_CONFIG_HOME/liuxin` root, falling back to `~/.config/liuxin`.
Those pointers remain user-wide so that changing deployment does not hide them.

A manifest may specify `preferences_directory` and `config_directory`; relative
paths resolve against the final manifest, including through a named-profile pointer.
For each path, an explicit manifest/API value wins over environment overrides.
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
application store. Constructing/loading a store with `upgrade_on_load=False` reads
without creating or upgrading files. Explicit initialization retains the existing
INI repair behavior; ordinary setting writes persist through that same store.

Raw tweak read/write helpers are explicit file tools. Refresh the application
store to re-read those inputs. A temporary `Tweak` override changes memory and
restores it on exit; it does not persist the temporary value. Tweak reset restores
declared tweak keys and retains unrelated application settings.

## Scope and consumers

Core passes the selected application store into its owned database before bootstrap.
Maintenance reinitialization preserves it. Command/query dispatch binds the store
in a `ContextVar`; nested calls restore the previous binding on success or failure.
The managed worker executor carries the submitting context, and process workers
receive the selected store explicitly, including under spawn. Spawn rebuilds codecs
without re-reading settings or changing paths. Injected custom stores must be
serializable if used with a process backend. Worker values have normal process-copy
semantics; this is not cross-process live cache synchronization.

Standalone library consumers can construct `Preferences(paths=...)` and use
`use_preferences(store)` around work. The process-default view is lazy; changing
environment variables after that store has loaded does not reconfigure it. Use a
new explicitly resolved store for another deployment rather than rebinding globals.

Database-local `DBPrefs`, plugin-specific `JSONConfig`, dynamic component files and
conversion option files retain distinct domains. Generic component stores capture
the selected component directory or an explicit base path. Installation resources
and plugin registries are separate from mutable application preferences. This
change does not consolidate the job lifecycle, ingest coordinators or registries.
