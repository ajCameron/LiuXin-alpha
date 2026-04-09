# Parses a calibre test library and retrieves files for LiuXin tests

import os
import pprint
import shutil
import sys
import time
from copy import deepcopy
from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from typing import Optional, Container

from LiuXin_alpha.constants.paths import LiuXin_data_folder
from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS_DOTTED

TEST_BOOKS_PATH: str = os.path.join(LiuXin_data_folder, "test_books")


def gather_files(file_root: str, target_extensions: Optional[Container[str]] = None):
    """
    Gather the files from the tree and move them into test_books.

    :param file_root:
    :param target_extensions:
    :return:
    """
    if target_extensions is None:
        target_extensions = deepcopy(BOOK_EXTENSIONS_DOTTED)

    if not os.path.exists(TEST_BOOKS_PATH):
        puts(colored.red("Cannot proceed - test_books folder not found"))
        raise NotImplementedError

    if not os.path.exists(file_root):
        puts(colored.red("Cannot proceed - test_library_path not found"))
        raise NotImplementedError

    for root, dirs, files in os.walk(file_root):

        for book_file in files:
            book_file_path = os.path.join(root, book_file)
            book_ext = os.path.splitext(book_file_path)[1]
            if book_ext in target_extensions:
                puts(colored.green('Scanned "{}" - including'.format(book_file_path)))
                book_name_ext = os.path.split(book_file_path)[1]
                final_path = os.path.join(TEST_BOOKS_PATH, book_name_ext)
                shutil.copyfile(src=book_file_path, dst=final_path)
                target_extensions.remove(book_ext)
                # Gives you time to see what you've actually got
                time.sleep(4)
            else:
                puts(colored.red('Scanned "{}" - already have'.format(book_file_path)))

        if len(target_extensions) == 0:
            break

    info_str = "Run complete - the following formats where not found \n{}".format(pprint.pformat(target_extensions))
    puts(colored.green(info_str))


if __name__ == "__main__":
    gather_files(file_root=sys.argv[1])
