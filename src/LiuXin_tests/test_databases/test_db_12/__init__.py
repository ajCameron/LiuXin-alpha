# Generates test_db_12 - as db 10 - but with an additional book in one of the leaves of the complex series

import os

from clint.textui import puts, colored

from LiuXin_tests.test_databases import load_data
from LiuXin_tests.test_databases import TestDatabaseBuilder
from LiuXin_tests.test_utils.test_utils import BasicMetadataFramework

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB12Builder(TestDatabaseBuilder):
    """
    Build test_db_12 - which has all the complex series data and an additional book with multiple creators.
    """

    def load_base_database(self):
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def purge_tables(scratch_db):
        puts(colored.green("Purging asset rows - all will be removed"))
        scratch_db.driver_wrapper.clear("files")
        scratch_db.driver_wrapper.clear("folders")
        scratch_db.driver_wrapper.clear("covers")

    @staticmethod
    def detail_databases(scratch_db):
        return add_complex_series_to_db(scratch_db)


# Todo: Add capacity to dump the data here after it's been built
def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    test_db_12 - all the complex series data is present - but there's an additional book added as well.
    :param dst_file_path: Place to copy the database file to after it's been built
    :param dump: If True then the csv files compromising this database will be written into the folder where this
                 script is running.
    :return:
    """
    test_db_builder = TestDB12Builder(
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

    # Adding a book to the first of the new series -
    # Now add a title, and make a book in one of the leaf series - this will force the creation of all the series
    # folders on up to that leaf
    vp_title = scratch_db.get_blank_row("titles")
    vp_title_id = vp_title.row_id
    vp_title["title"] = "Vector Prime"
    vp_title.sync()

    # Make the creator and associate the title with it
    vp_author = "R. A. Salvatore"
    vp_creator_row = scratch_db.get_blank_row("creators")
    vp_creator_row["creator"] = vp_author
    vp_creator_row.sync()
    scratch_db.interlink_rows(primary_row=vp_title, secondary_row=vp_creator_row, type="authors")

    # Associate the title and the target series
    njo_row = scratch_db.get_row_from_id(table="series", row_id=njo_id)
    scratch_db.interlink_rows(primary_row=vp_title, secondary_row=njo_row, index=1)

    # Now, using that title, make a book and add it to the database
    scratch_db.driver_wrapper.add_row({"book_id": vp_title_id})

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
