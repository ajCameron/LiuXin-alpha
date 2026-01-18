# Generates test_db_10 - a database with all the usual metadata and a number of complex series

import os

from clint.textui import puts, colored

from LiuXin_tests.test_databases import load_data
from LiuXin_tests.test_databases import TestDatabaseBuilder
from LiuXin_tests.test_utils.test_utils import BasicMetadataFramework

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB10Builder(TestDatabaseBuilder):
    """
    Constructs test_db_10 - which has some complex series data and no asset data
    """

    @staticmethod
    def purge_tables(scratch_db):
        puts(colored.green("Purging asset rows - all will be removed"))
        scratch_db.driver_wrapper.clear("files")
        scratch_db.driver_wrapper.clear("folders")
        scratch_db.driver_wrapper.clear("covers")

    def load_base_database(self):
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def detail_databases(scratch_db):
        add_complex_series_to_db(scratch_db)
        return scratch_db


# Todo: Add capacity to dump the data here after it's been built
def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    test_db_2 is intended for applications where the whole database had to be read into the cache - as such size is at a
    premium in order to speed up the tests.
    The test database has one title - it's got all the metadata associated with that title - but it only has one title.
    This method constructs the test database - starting with a regular test database and removing everything except
    title 1 (and the unknown title - if it exists).
    :param dst_file_path: Place to copy the database file to after it's been built
    :param dump: If True then the csv files compromising this database will be written into the folder where this
                 script is running.
    :return:
    """
    test_db_builder = TestDB10Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()


def add_complex_series_to_db(scratch_db):
    # Add in the complex series - a series in the Star Wars extended universe
    # Write a series tree into the database - it should be as horrible as possible
    series_tree_ids = set()

    # Make the root of the new tree - the Star Wars Universe
    root_series_row = scratch_db.get_blank_row("series")
    root_series_row["series"] = "Star Wars Universe"
    root_series_row.sync()
    sw_root_series_id = root_series_row.row_id
    series_tree_ids.add(sw_root_series_id)

    next_series_row = scratch_db.get_blank_row("series")
    next_series_row["series"] = "Star Wars Legends"
    next_series_row["series_parent"] = sw_root_series_id
    next_series_row.sync()
    sw_legends_id = next_series_row.row_id
    series_tree_ids.add(sw_legends_id)

    # Place some series in the Star Wars Legends series
    next_series_row = scratch_db.get_blank_row("series")
    next_series_row["series"] = "X-Wing"
    next_series_row["series_parent"] = sw_legends_id
    next_series_row.sync()
    xwing_id = next_series_row.row_id
    series_tree_ids.add(xwing_id)

    next_series_row = scratch_db.get_blank_row("series")
    next_series_row["series"] = "Wraith Squadron"
    next_series_row["series_parent"] = xwing_id
    next_series_row.sync()
    wraith_id = next_series_row.row_id
    series_tree_ids.add(wraith_id)

    next_series_row = scratch_db.get_blank_row("series")
    next_series_row["series"] = "The Thrawn Trilogy"
    next_series_row["series_parent"] = sw_legends_id
    next_series_row.sync()
    thrawn_id = next_series_row.row_id
    series_tree_ids.add(thrawn_id)

    next_series_row = scratch_db.get_blank_row("series")
    next_series_row["series"] = "The Corellia Trilogy"
    next_series_row["series_parent"] = sw_legends_id
    next_series_row.sync()
    corellia_id = next_series_row.row_id
    series_tree_ids.add(corellia_id)

    next_series_row = scratch_db.get_blank_row("series")
    next_series_row["series"] = "The New Jedi Order"
    next_series_row["series_parent"] = sw_legends_id
    next_series_row.sync()
    njo_id = next_series_row.row_id
    series_tree_ids.add(njo_id)

    next_series_row = scratch_db.get_blank_row("series")
    next_series_row["series"] = "Dark Tide Rising"
    next_series_row["series_parent"] = njo_id
    next_series_row.sync()
    dt_rising = next_series_row.row_id
    series_tree_ids.add(dt_rising)

    dark_tide_series_row = scratch_db.get_row_from_id("series", row_id=41)
    assert dark_tide_series_row["series"] == "Dark Tide Rising"

    # Import the metadata tools needed to do the work of adding and linking the book, creator and series
    md_framework = BasicMetadataFramework(db=scratch_db)
    title_row = md_framework.add.title(title="Onslaught")
    creator_row = md_framework.add.creator(creator="Michael A. Stackpole")

    # Link the title to the creator
    md_framework.apply.creator(resource_row=title_row, creator_row=creator_row)

    # Link the book into the series
    md_framework.apply.series(series=dark_tide_series_row, series_index=1, resource_row=title_row)
    md_framework.add.book(title_row=title_row)

    dt_title_rows = scratch_db.get_interlinked_rows(target_row=dark_tide_series_row, secondary_table="titles")
    assert len(dt_title_rows) == 1

    return scratch_db
