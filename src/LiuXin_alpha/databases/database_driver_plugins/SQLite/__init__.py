# Increment this if ANY submodule is changed
__driver_version__ = (0, 4, 4)


# Todo: Include the constants now used to determine allowable values against this as well
def get_SQLite_driver_master_version():
    """
    Generate a version string from all the sub-object versions which go into the database.


    :return:
    """
    import LiuXin_alpha.databases.database as lx_database

    database_version = lx_database.__object_version__

    import LiuXin_alpha.databases.metadata_tools as lx_md_tools

    md_tools_version = lx_md_tools.__md_tools_version__

    import LiuXin_alpha.metadata.constants as md_constants

    md_constants_version = md_constants.__md_version__

    import LiuXin_alpha.constants as lx_constants

    lx_constants_version = lx_constants.__lx_constants_version__

    version_str = "driver_version_{}_database_version_{}_md_tools_version_{}_md_constants_{})_lx_constants".format(
        database_version,
        __driver_version__,
        md_tools_version,
        md_constants_version,
        lx_constants_version,
    )
    version_str = version_str.replace(",", "_")
    version_str = version_str.replace(" ", "_")

    return version_str
