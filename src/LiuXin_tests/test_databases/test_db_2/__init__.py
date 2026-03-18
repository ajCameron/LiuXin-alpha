# Generates test_db_2 - which is a database with a number of empty blank custom columns

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from LiuXin_tests.test_databases.test_db_1 import test_db_1_folder as __folder__
from LiuXin_tests.test_databases import TestDatabaseBuilder


class TestDB2Builder(TestDatabaseBuilder):
    """
    Executes build for the test database described here.
    """

    def detail_databases(self, scratch_db):
        """
        Delete all but the first title (and title 0 - if it exists).
        :param scratch_db:
        :return:
        """
        title_count = scratch_db.driver_wrapper.get_record_count("titles")

        # Purge the titles of all but title 0 and titles 1
        puts(colored.green("Purging titles - all but 1 (and, if present, 0) will be removed"))
        for title_id in range(2, title_count + 1):
            title_row = scratch_db.get_row_from_id("titles", title_id)
            scratch_db.delete(row=title_row)

        self.write_timestamps_books_table(scratch_db)

        return scratch_db

    @staticmethod
    def write_timestamps_books_table(scratch_db):
        """
        Update the timestamp columns of the books field to static values, freezing them after database rebuilds.
        :param scratch_db:
        :return:
        """
        # Update the books table

        theo_book_created_datestamp_dict = {1: 1652101362}
        for book_row in scratch_db.get_all_rows("books"):
            book_row["book_created_datestamp"] = theo_book_created_datestamp_dict[book_row.row_id]
            book_row.sync()


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
    test_db_builder = TestDB2Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=__folder__,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
