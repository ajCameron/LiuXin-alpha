"""Own plugin foundations, application composition and retained plugin families.

Importing this package does not discover providers or assemble registries. Generic
contracts and loading live in base, file_types, configuration, loading and zipplugin;
application startup and management live in application. The former customize and
plugin_system packages are retired, with application families in contracts and builtins.

Separation of concerns: neutral plugin leaves must not import application families
or provider assembly. Application composition may use domain providers; conversion
contracts and standalone dispatch remain owned by file_formats. See
``dev-docs/04 - Seperation of Concerns.md`` and the package guide at
``dev-docs/plugins/README.md``.
"""

__all__: tuple[str, ...] = ()
