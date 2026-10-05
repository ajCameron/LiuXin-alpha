# Test database with some basic, empty custom columns

"""
Expose the supported test db 6 compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
"""
from LiuXin_tests.test_databases import TestDatabaseBuilder
from LiuXin_tests.test_databases import load_data


class TestDBCustomColumns(TestDatabaseBuilder):
    """
    Construct a test database with some custom columns.

    Example:
        Exercise TestDBCustomColumns through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
    """

    def print_banner(self):
        """
        Print a welcome banner to indicate the test database which is currently being generated.

        Example:
            Exercise TestDBCustomColumns.print banner through a consuming regression::

                python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        term_width, term_height = self.get_term_size()
        info_str = [
            "-" * term_width,
            "Generating test database for plugin {}".format(self.get_plugin_name()),
            "This time with empty custom columns",
            "-" * term_width,
        ]
        if self.dump:
            info_str.append("dump not currently supported")

        self.okay_print("\n".join(info_str))

    def load_base_database(self):
        """
        Perform the load base database operation under explicit file-format and conversion rules.

        Example:
            Exercise TestDBCustomColumns.load base database through a consuming regression::

                python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return load_data(base_data=False, overwrite_db=False)

    @staticmethod
    def purge_tables(scratch_db):
        """
        Don't want any asset data - so removing it here (as bad asset data breaks the cache when we trying and load it). It also breaks the legacy library class we need up to write the custom columns.

        Example:
            Exercise TestDBCustomColumns.purge tables through a consuming regression::

                python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


        :param scratch_db: Value supplied for scratch db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        scratch_db.driver_wrapper.clear("files")
        scratch_db.driver_wrapper.clear("folders")
        scratch_db.driver_wrapper.clear("covers")

    @staticmethod
    def detail_databases(scratch_db):
        """
        Add the custom columns t

        Example:
            Exercise TestDBCustomColumns.detail databases through a consuming regression::

                python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


        :param scratch_db: Value supplied for scratch db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # Todo: Metadata should be a property
        live_database_path = scratch_db.metadata["database_path"]

        from LiuXin_alpha.library.legacy import LibraryDatabase

        live_legacy_database = LibraryDatabase(library_path=live_database_path)

        # Create the test columns
        live_legacy_database.create_custom_column(
            label="test_cc_1_label",
            name="test_cc_1_name",
            datatype="text",
            is_multiple=False,
            editable=True,
            display=None,
        )
        live_legacy_database.create_custom_column(
            label="test_cc_2_label",
            name="test_cc_2_name",
            datatype="bool",
            is_multiple=False,
            editable=True,
            display=None,
        )
        live_legacy_database.create_custom_column(
            label="test_cc_3_label",
            name="test_cc_3_name",
            datatype="comments",
            is_multiple=False,
            editable=True,
            display=None,
        )
        live_legacy_database.create_custom_column(
            label="test_cc_4_label",
            name="test_cc_4_name",
            datatype="text",
            is_multiple=True,
            editable=True,
            display=None,
        )
        live_legacy_database.create_custom_column(
            label="test_cc_5_label",
            name="test_cc_5_name",
            datatype="series",
            is_multiple=False,
            editable=True,
            display=None,
        )


def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    Construct the test database specified by this module. In this case a blank database is constructed and filled with data - before being copied into the test_databases folder.

    Example:
        Exercise build test db through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


    :param dst_file_path: Value supplied for dst file path under the utility contract.
    :param dump: Value supplied for dump under the utility contract.
    :param plugin_name: Value supplied for plugin name under the utility contract.
    :param new_db_uuid: Value supplied for new db uuid under the utility contract.
    :param test_asset_version: Value supplied for test asset version under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    test_db_builder = TestDBCustomColumns(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
