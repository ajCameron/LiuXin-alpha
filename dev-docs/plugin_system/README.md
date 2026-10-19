# Plugin system

`LiuXin_alpha.plugin_system` owns plugin mechanics and application plugin
composition. The former `customize` package was removed in `78a01b61` on
2026-10-19. Import contracts and operations from their owning modules; the
package root is passive and does not re-export them.

Start with the [composition contract](plugin-composition.md) for startup,
provider ordering, refresh and failure handling. Its
[import migration table](plugin-composition.md#import-migration) covers repository
and external ZIP-plugin callers of the retired namespace.

## Where changes belong

| Change | Owner |
| --- | --- |
| Generic lifecycle, resources and preferences contract | `plugin_system.base` |
| File-hook and metadata-stream contracts | `plugin_system.file_types` |
| Persisted plugin customization and enabled state | `plugin_system.configuration` |
| ZIP loading and shared initialization/ordering policy | `plugin_system.zipplugin`, `plugin_system.loading` |
| Application startup, lookups and installation/removal | `plugin_system.application` |
| Application plugin families and bundled catalogue | `plugin_system.contracts`, `plugin_system.builtins` |
| Conversion contracts, implementations and standalone dispatch | `file_formats.conversion`, `file_formats.plugin_runtime` |
| File metadata readers and writers | `metadata.file_sources` |

Sharing one package does not give generic mechanics permission to import the
application host or concrete providers. Internal dependency checks enforce that
direction. The application host extends the standalone file-provider catalogue
and binds its initialized instances into the format runtime; both use the same
loading and priority policy.

Conversion-specific guidance remains in [file formats](../file_formats/README.md)
and the [conversion pipeline](../conversion_pipeline/README.md). The persisted
plugin settings are a component namespace within the broader
[configuration model](../core/configuration-ownership.md).

## Remaining work

The moved `cache`, `folder_store_driver` and abstract `api` modules retain legacy
contracts. Unsupported device providers still refer to the removed device
package. Their relocation does not restore those integrations. Provider failure
reporting and process-wide registry/conversion state also need follow-up.

Database, storage and terminal registries remain with their domain owners;
this merger does not unify every registry in the project. The
[architecture review](../architectural_reviews/architecture-review-2026-09-26.md)
tracks those separate decisions.

## Checks

From the repository root, use the active environment and an absolute source path:

```bash
PYTHONPATH="$PWD/src" python -m pytest -q tests/plugin_system tests/file_formats/test_plugin_runtime.py -o tmp_path_retention_policy=none
PYTHONPATH="$PWD/src" python scripts/check_layer_direction.py
PYTHONPATH="$PWD/src" python scripts/check_import_footprint.py
```

Package-structure tests reject the retired namespace; the
[installed-wheel checks](../hacking/packaging.md) verify the distributed boundary.
For resource telemetry and broader lanes, see [test streams](../hacking/test-streams.md).

Return to the [developer index](../README.md).
