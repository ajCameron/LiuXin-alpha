# Generates test_db_11 - a database with complex series and some apparently valid asset data

import os

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from LiuXin_alpha.folder_stores.folderstore import FolderStore
from LiuXin_alpha.folder_stores.folderstoremanager import FolderStoreManager

from LiuXin_tests.test_databases import load_data
from LiuXin_tests.test_databases.test_db_10 import add_complex_series_to_db
from LiuXin_tests.test_databases import TestDatabaseBuilder
from tests.support.test_databases._legacy.objects import TestObjectsHandler
from LiuXin_tests.test_utils.test_utils import DatabaseValidator

# Todo: Really need a means to kill ramdisks and remove all the entries in them
from LiuXin_alpha.utils.ptempfiles import ScratchFolderManager
from LiuXin_alpha.utils.ptempfiles import get_ramdisk

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


from utils.lx_libraries.liuxin_random import LiuXinBadPseudoRandomGenerator

random = LiuXinBadPseudoRandomGenerator(seed=1194)


class TestDB11Builder(TestDatabaseBuilder):
    """
    Generates test_db_11 - which has complex series metadata and some apparently valid asset data.
    """

    def load_base_database(self):
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def purge_tables(scratch_db):
        # There is some redundancy here
        # Clear the link tables as well - should be cleared when the tables are cleared - but might not be (faulty db?)
        # Want to make sure as to the final state.
        puts(colored.green("Purging asset rows - all will be removed. Also removing links from the assets to md rows"))
        scratch_db.driver_wrapper.clear("files")
        scratch_db.driver_wrapper.clear("file_folder_links")
        scratch_db.driver_wrapper.clear("book_file_links")

        scratch_db.driver_wrapper.clear("folders")
        scratch_db.driver_wrapper.clear("book_folder_links")

        scratch_db.driver_wrapper.clear("covers")
        scratch_db.driver_wrapper.clear("book_cover_links")
        scratch_db.driver_wrapper.clear("cover_creator_links")
        scratch_db.driver_wrapper.clear("cover_series_links")

    def detail_databases(self, scratch_db):
        # Include the complex series data
        puts(colored.green("Adding complex series data"))
        scratch_db = add_complex_series_to_db(scratch_db)

        # Include the test asset data
        puts(colored.green("Writing in test asset data"))
        scratch_db = self.build_valid_asset_data(scratch_db)
        return scratch_db

    def build_valid_asset_data(
        self,
        scratch_db,
        fs_count=10,
        book_folder_count=100,
        format_count=400,
        book_cover_count=150,
        creator_cover_count=150,
        series_cover_count=150,
    ):
        """
        All asset data has been cleared from the database by this point - now creating a set of valid entries in a set
        of test folder stores.
        Assets will be removed after they've been created and added to the database - it's just easier to actually
        use the existing methods to make the test database with the rest of it.
        :param scratch_db: DB to build the asset data for
        :param fs_count: The number of folder stores to build - defaults to 10
        :param book_folder_count: The number of book folders to make - book folders will be evenly distributed among the
                                  folder stores.
        :param format_count: The number of formats to be added to the folder store. They will be added to random book
                             folders in random folder stores.
        :param book_cover_count: The total number of covers to add to books on the system.
        :param creator_cover_count: The number of covers to add to creators on the system
        :param series_cover_count: The total number of covers to add to series on the system.
        :return:
        """
        from utils.lx_libraries.liuxin_random import LiuXinBadPseudoRandomGenerator

        # Build the ramdisk - all folder store construction will be done here for efficiency
        scratch_ram_disk = get_ramdisk()
        scratch_folder_manager = ScratchFolderManager(make_in=scratch_ram_disk, only_derez_own=True)

        test_objects_handler = TestObjectsHandler(scratch_file_handler=scratch_folder_manager)
        test_objects_handler.rng = LiuXinBadPseudoRandomGenerator(seed=book_folder_count)

        # Other tools which will be used to probe an apparent problem with one of the methods
        test_db_validator = DatabaseValidator(db=scratch_db)

        # Create ten folder stores - will be used as a place to store the test assets
        fs_path_dict = dict()
        self.okay_print("About to create the folder stores")
        for i in range(1, fs_count + 1):

            self.okay_print("Generating folder store {}".format(i))

            test_fs = FolderStore(database=scratch_db, folder_store_row=None)

            new_fs_path = scratch_folder_manager.get_scratch_folder(filename=str(i + 1), pinned=False)

            new_entry_row = dict()
            new_entry_row["folder_store_path"] = new_fs_path

            test_fs.create(new_entry_row)

            fs_path_dict[i] = new_fs_path

        # Load the folder store manager - which should now have ten folder stores to operate on
        self.okay_print("Building folder store manager")
        scratch_fsm = FolderStoreManager(database=scratch_db, empty_cover_cache=True)

        # Build a number of book folders in the folder store - want a random distribution in random folder stores
        # Some of the books should have no formats in them
        self.okay_print("Building book folders")
        existing_books = set()

        # Resources will be randomly added to metadata entities - need to know the range of all of them before we can
        # randomly select from them to assign
        potential_book_count = scratch_db.driver_wrapper.get_record_count("books")
        potential_creator_count = scratch_db.driver_wrapper.get_record_count("creators")
        potential_series_count = scratch_db.driver_wrapper.get_record_count("series")

        for i in range(book_folder_count):
            # Select a random folder store and book - for which there isn't already a book
            fs_id, book_id = draw_fs_resource_id_combo(
                folder_store_count=fs_count,
                resource_count=potential_book_count,
                bad_combos=existing_books,
            )

            # Construct a folder for the book
            book_row = scratch_db.get_row_from_id("books", row_id=book_id)
            scratch_fsm.ensure_book_folder(
                book_row=book_row,
                allowed_fs_ids=frozenset(
                    [
                        fs_id,
                    ]
                ),
            )

        # Add a number of formats to the folder store - should give some files to work with
        self.okay_print("Adding formats to the test folder stores")
        for i in range(format_count):

            self.okay_print("Doing add for format {}".format(i))

            # Select a random folder store and book - might or might not already been a book folder associated with it
            fs_id, book_id = draw_fs_resource_id_combo(
                folder_store_count=fs_count,
                resource_count=potential_book_count,
                bad_combos=set(),
            )

            # Add a format to the given book
            test_book_file = test_objects_handler.get_rand_md_test_file_path()
            test_book_file_format = os.path.splitext(test_book_file)[1]
            scratch_fsm.add.format(
                book_id=book_id,
                fmt=test_book_file_format,
                stream=test_book_file,
                allowed_fs_ids=frozenset([fs_id]),
            )

        # Todo: Need to double or triple up on some of these - want explicit duplication in some cases

        # Add some covers to a number of books
        # Covers will be randomly added to the folder store - multiple copies of a cover may be added in different ways
        puts(colored.green("Adding book covers to the test folder store"))
        for i in range(book_cover_count):
            self.okay_print("Adding book cover for {}".format(i))

            # Select a random folder store and book - might or might not already been a book folder associated with it
            fs_id, book_id = draw_fs_resource_id_combo(
                folder_store_count=fs_count,
                resource_count=potential_book_count,
                bad_combos=set(),
            )

            # Add a cover to the given book
            test_book_cover = test_objects_handler.get_rand_test_cover_path()

            scratch_fsm.add.book_cover(
                book_id=book_id,
                stream=test_book_cover,
                allowed_fs_ids=frozenset(
                    [
                        fs_id,
                    ]
                ),
            )

        # Add some covers to a number of creators
        # Covers will be randomly added to the folder store - multiple copies of a cover may be added in different ways
        self.okay_print("Adding creator covers to the test folder store")
        for i in range(creator_cover_count):

            self.okay_print("Generating creator cover for {}".format(i))

            # Select a random folder store and creator - might or might not already have a series folder associated with it
            fs_id, creator_id = draw_fs_resource_id_combo(
                folder_store_count=fs_count,
                resource_count=potential_creator_count,
                bad_combos=set(),
            )

            creator_row = scratch_db.get_row_from_id("creators", row_id=creator_id)
            if creator_row is None:
                continue

            # Add a cover to the given creator
            test_creator_cover = test_objects_handler.get_rand_test_cover_path()

            scratch_fsm.add.creator_cover(
                creator_id=creator_id,
                stream=test_creator_cover,
                allowed_fs_ids=frozenset([fs_id]),
            )

        test_db_validator.validate_every_folder_has_name()

        # Add some covers to a number of series
        # Covers will be randomly added to the folder store - multiple copies of a cover may be added in different ways
        puts(colored.green("Adding series covers to the test folder store"))
        for i in range(1, series_cover_count + 1):

            self.okay_print("Adding series cover for {}".format(i))

            # Select a random folder store and series - might or might not already been a series folder associated with it
            fs_id, series_id = draw_fs_resource_id_combo(
                folder_store_count=fs_count,
                resource_count=potential_series_count,
                bad_combos=set(),
            )

            # Add a cover to the given series
            test_series_cover = test_objects_handler.get_rand_test_cover_path()

            scratch_fsm.add.series_cover(
                series_id=series_id,
                stream=test_series_cover,
                allowed_fs_ids=frozenset(
                    [
                        fs_id,
                    ]
                ),
            )

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
    test_db_builder = TestDB11Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()


# Todo: Those database which include random elements - check that they are build built deterministically
def draw_fs_resource_id_combo(folder_store_count, resource_count, bad_combos):
    """
    Return a randomly selected folder store, folder combination.
    :param folder_store_count: The total number of folder stores
    :param resource_count: The total number of books on the system
    :param bad_combos: A set of tuples - first element being the folder store, second element being the book id.
                       Only combinations not in the bad_combos will be returned.
    :return:
    """
    give_up_count = 50
    try_count = 0
    while try_count < give_up_count:
        fs_id = random.randrange(1, folder_store_count + 1)
        book_id = random.randrange(1, resource_count + 1)
        if (fs_id, book_id) not in bad_combos:
            bad_combos.add((fs_id, book_id))
            return fs_id, book_id
        try_count += 1
    raise NotImplementedError("give up count - {} - was surpassed trying to find a good combo")
