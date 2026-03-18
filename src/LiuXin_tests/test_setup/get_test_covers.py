from __future__ import unicode_literals

import Queue
import os
import time

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from LiuXin_alpha.paths import LiuXin_data_folder

from LiuXin_alpha.databases.database import Database

from LiuXin_alpha.metadata.web_sources.amazon import Amazon

from LiuXin_tests.test_databases import file_load_test_database_backup

from LiuXin_alpha.utils.logger import default_log


# Use the amazon metadata downloader to build a test set of covers

test_covers_folder = os.path.join(LiuXin_data_folder, "test_covers")


class DummyAbort(object):
    def __init__(self):
        pass

    @staticmethod
    def is_set():
        return False


def download_test_covers():
    """
    Takes a database - downloads a test cover for every book in the database.
    :param test_db:
    :return:
    """
    # Load a (hopefully) clean version of the test database to work with - put it in a scratch folder
    full_data_backup_path = file_load_test_database_backup(scratch=True)
    test_db = Database(metadata={"database_path": full_data_backup_path})

    amazon_downloader = Amazon()
    result_queue = Queue.Queue()
    dummy_abort = DummyAbort()

    puts(colored.green("About to begin cover download"))
    for title_row in test_db.get_all_rows("titles"):

        # Construct the metadata to use to call amazon
        title = title_row["title"]
        title_author_rows = test_db.get_interlinked_rows(
            target_row=title_row, secondary_table="creators", type_filter="author"
        )
        title_authors = [tar["creator"] for tar in title_author_rows]
        title_author_string = " ".join(title_authors)

        puts(colored.green("Downloading for title - {} - authors - {}".format(title, title_author_string)))

        # Do the download
        amazon_downloader.download_cover(
            log=default_log,
            result_queue=result_queue,
            abort=dummy_abort,
            title=title,
            authors=title_authors,
        )

        # Try and retrieve the result from the queue
        try:
            downloader, cdata = result_queue.get_nowait()
        except Queue.Empty:
            puts(colored.red("Nothing appeared in the Queue - trying the next cover"))
        else:
            puts(colored.green("Something seemed to download - at least calling didn't immediately throw an exception"))
            # Writing the cover data out to disk
            cover_file_name = "book_id_{}.jpg".format(title_row["title_id"])
            cover_dst_path = os.path.join(LiuXin_data_folder, cover_file_name)
            with open(cover_dst_path, "wb+") as dst_file:
                dst_file.write(cdata)

        # Wait some time to avoid flooding amazon
        time.sleep(10)


if __name__ == "__main__":

    download_test_covers()
