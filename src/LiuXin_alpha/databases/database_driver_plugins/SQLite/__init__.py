"""
Expose the stdlib SQLite plugin version and its legacy composite version helper.

This initializer defines ``__driver_version__`` without loading the concrete
driver. Import ``DatabaseDriver`` from the ``databasedriver`` subpackage when a
driver class is needed. The composite helper imports its version dependencies
when called; its result describes project components, not the SQLite engine.

Example:
    >>> from LiuXin_alpha.databases.database_driver_plugins import SQLite
    >>> SQLite.__driver_version__
    (0, 6, 0)
"""

# Increment this if ANY submodule is changed
__driver_version__ = (0, 6, 0)


# Todo: I think this is being called and used in the wrong places
# Todo: Include the constants now used to determine allowable values against this as well
# Todo: My feel is the driver version and the md tools are actually different.
# Todo: Though the driver version should include the md tools version ... or not. It's catalog now.
def get_SQLite_driver_master_version() -> str:
    """
    Build the legacy SQLite component-version string from current module values.

    Read the database facade version, this plugin's ``__driver_version__``, the
    Catalog legacy metadata-tools version, metadata constants, and application
    constants on each call. Dependencies are imported inside the function; their
    import failures and missing version attributes propagate to the caller.

    Preserve the historical format: ``driver_version_SQLite-inbuilt-`` precedes
    the database facade version, while ``database_version_`` precedes the plugin
    version. The metadata-tools and metadata-constants versions follow. The
    application-constants version is read but omitted because the template has
    only four placeholders. An extra closing parenthesis precedes the literal
    ``_lx_constants`` suffix. Commas and spaces each become underscores, so tuple
    separators produce double underscores and tuple parentheses remain intact.
    The result is neither an SQLite library version nor a compatibility check.

    Example:
        >>> from LiuXin_alpha.databases import database
        >>> version = get_SQLite_driver_master_version()
        >>> database_part = str(database.__object_version__).replace(",", "_").replace(" ", "_")
        >>> driver_part = str(__driver_version__).replace(",", "_").replace(" ", "_")
        >>> version.startswith(f"driver_version_SQLite-inbuilt-{database_part}_database_version_{driver_part}")
        True
        >>> version.endswith(")_lx_constants")
        True
        >>> "," not in version and " " not in version
        True


    :return: Newly formatted legacy component-version string; no database is opened.
    """
    import LiuXin_alpha.databases.database as lx_database

    database_version = lx_database.__object_version__

    from LiuXin_alpha.catalog.legacy_versions import LEGACY_METADATA_TOOLS_VERSION

    import LiuXin_alpha.metadata.constants as md_constants

    md_constants_version = md_constants.__md_version__

    import LiuXin_alpha.constants as lx_constants

    lx_constants_version = lx_constants.__lx_constants_version__

    version_str = "driver_version_SQLite-inbuilt-{}_database_version_{}_md_tools_version_{}_md_constants_{})_lx_constants".format(
        database_version,
        __driver_version__,
        LEGACY_METADATA_TOOLS_VERSION,
        md_constants_version,
        lx_constants_version,
    )
    version_str = version_str.replace(",", "_")
    version_str = version_str.replace(" ", "_")

    return version_str
