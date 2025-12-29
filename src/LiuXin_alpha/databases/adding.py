#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

from typing import Optional, Union, Iterable, Callable, Iterator, Any, TypeAlias, Protocol, overload

import fnmatch
import os
import re
import time
from collections import defaultdict

from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS

from LiuXin_alpha.utils.storage.local.file_ops import local_open as lopen
from LiuXin_alpha.utils.text.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import trans as _

# Py2.Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iterkeys as iterkeys
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues
from LiuXin_alpha.utils.libraries.liuxin_six import six_map
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode

# Todo: Should be re-organized over to library

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def splitext(path: str) -> tuple[str, str]:
    """
    As for os.path.splitext, except returns the extension without a dot

    :param path:
    :return:
    """
    key, ext = os.path.splitext(path)
    return key, ext[1:].lower()


def formats_ok(formats: Union[Iterable[str], dict[str, str]]) -> bool:
    """
    Checks to see if the provided iterable formats from a folder is a book or can be ignored.

    :param formats:
    :return:
    """
    if formats and (len(formats) > 1 or tuple(iterkeys(formats)) != ("opf",)):
        return True
    return False


def path_ok(path: Union[os.PathLike[str], str]) -> bool:
    """
    Checks to see if the given path is accessible for reading and exists.

    :param path:
    :return:
    """
    return not os.path.isdir(path) and os.access(path, os.R_OK)


def compile_glob(pat: str) -> re.Pattern[str]:
    """
    Compile a glob - using fnmatch to translate the pattern to the local file system.

    :param pat:
    :return:
    """
    return re.compile(fnmatch.translate(pat), flags=re.I)


def compile_rule(rule) -> tuple[Callable[[str], bool], bool]:
    """
    Rules are used to determine which files should be added during an import

    Takes their dict form and produces a function which can be applied to the file name.
    Multiple rules (probably) form a single query.
    This will (probably) take the form of a series of functions applied to a file name.
    :param rule:
    :return:
    """
    mt = rule["match_type"]

    if "with" in mt:
        q = icu_lower(rule["query"])
        if "startswith" in mt:

            def func(filename):
                return icu_lower(filename).startswith(q)

        else:

            def func(filename):
                return icu_lower(filename).endswith(q)

    elif "glob" in mt:
        q = compile_glob(rule["query"])

        def func(filename):
            return q.match(filename) is not None

    else:
        q = re.compile(rule["query"])

        def func(filename):
            return q.match(filename) is not None

    ans = func
    if mt.startswith("not_"):

        def ans(filename):
            return not func(filename)

    return ans, rule["action"] == "add"


def filter_filename(compiled_rules: tuple[Callable[[str], bool], bool], filename: str) -> bool:
    """
    Apply the compiled rules to the filename.

    :param compiled_rules:
    :param filename:
    :return:
    """
    for q, action in compiled_rules:
        if q(filename):
            return action


_metadata_extensions = None


def metadata_extensions() -> frozenset[str]:
    """
    Set of all known book extensions + OPF (the OPF is used to read metadata, but not actually added).

    Files from which metadata can be read.
    :return:
    """
    global _metadata_extensions
    if _metadata_extensions is None:
        _metadata_extensions = frozenset(six_map(six_unicode, BOOK_EXTENSIONS)) | {"opf"}
    return _metadata_extensions


def listdir(root: Union[str, os.PathLike[str]],
            sort_by_mtime: bool = False) -> Iterator[Union[os.PathLike[str], str]]:
    """
    Yields absolute paths to all the files in the root.

    :param root:
    :param sort_by_mtime: Preform sort on the last modification time
    :type sort_by_mtime: bool
    :return:
    """
    items = (os.path.join(root, x) for x in os.listdir(root))
    if sort_by_mtime:

        def safe_mtime(x):
            try:
                return os.path.getmtime(x)
            except EnvironmentError:
                return time.time()

        items = sorted(items, key=safe_mtime)

    for path in items:
        if path_ok(path):
            yield path


def allow_path(path: Union[str, os.PathLike[str]],
               ext: str,
               compiled_rules: tuple[Callable[[str], bool], bool]) -> bool:
    """
    Check to see if the given path complies with all the rules and so can be imported.

    :param path:
    :param ext:
    :param compiled_rules:
    :return:
    """
    ans = filter_filename(compiled_rules, os.path.basename(path))
    if ans is None:
        ans = ext in metadata_extensions()
    return ans


class ListdirFn(Protocol):
    def __call__(
        self,
        root: str | os.PathLike[str],
        sort_by_mtime: bool = False,
    ) -> Iterator[str | os.PathLike[str]]: ...


def find_books_in_directory(
    dirpath: Union[str, os.PathLike[str]],
    single_book_per_directory: bool,
    compiled_rules: tuple[Callable[[str], bool], bool] = (),
    listdir_impl: ListdirFn = listdir,
    single_fmt: bool = False,
) -> Iterator[list[str]]:
    """
    Searches a directory for any valid book files.

    Yields an iterator of lists of valid files.
    Each list corresponds to a book.
    :param dirpath: THe directory to search the folder in.
    :param single_book_per_directory: If True then each directory is considered to have one and only one book in it.
    :param compiled_rules: Rules to be applied to files in the directory during the impor phase.
    :param listdir_impl: The implementation of listdir to be used during the search
    :param single_fmt: If True, then yields a single instance of each format - otherwise yields a list of formats of
                       each type.
    :return one_type_fmt_list: Yields a list of all the files of a particular fmt in the given dir
    """
    dirpath = os.path.abspath(dirpath)
    if single_book_per_directory:

        if single_fmt:
            formats = {}
            for path in listdir_impl(dirpath):
                key, ext = splitext(path)
                if allow_path(path, ext, compiled_rules):
                    formats[ext] = path

        else:
            formats = defaultdict(list)
            for path in listdir_impl(dirpath):
                key, ext = splitext(path)
                if allow_path(path, ext, compiled_rules):
                    formats[ext].append(path)

        if formats_ok(formats):
            yield list(itervalues(formats))

    else:
        if single_fmt:
            books = defaultdict(dict)
            for path in listdir_impl(dirpath, sort_by_mtime=True):
                key, ext = splitext(path)
                if allow_path(path, ext, compiled_rules):
                    books[icu_lower(key) if isinstance(key, unicode) else key.lower()][ext] = path
        else:
            books = defaultdict(dict)
            for path in listdir_impl(dirpath, sort_by_mtime=True):
                key, ext = splitext(path)
                if allow_path(path, ext, compiled_rules):
                    book_path = icu_lower(key) if isinstance(key, unicode) else key.lower()
                    if book_path in books:
                        books[book_path][ext].append(path)
                    else:
                        books[book_path][ext] = [path]

        for formats in itervalues(books):
            if formats_ok(formats):
                yield list(itervalues(formats))


def import_book_directory(db,
                          dirpath: Union[str, os.PathLike[str]],
                          callback: Optional[Callable[[str, ], None]] = None,
                          added_ids: set[int] = None,
                          compiled_rules: tuple[Callable[[str], bool], bool] = ()
                          ):
    """
    Import an entire directory - assuming that the directory only includes files associated with one book

    :param db: The database we're adding files to.
    :param dirpath: We're going to walk this directory
    :param callback:
    :param added_ids:
    :param compiled_rules:
    :return:
    """
    from LiuXin.metadata.meta import metadata_from_formats

    dirpath = os.path.abspath(dirpath)
    formats = None
    for formats in find_books_in_directory(dirpath, True, compiled_rules=compiled_rules):
        break

    if not formats:
        return

    mi = metadata_from_formats(formats)
    if mi.title is None:
        return
    if db.has_book(mi):
        return [(mi, formats)]
    book_id = db.import_book(mi, formats)
    if added_ids is not None:
        added_ids.add(book_id)

    if callable(callback):
        callback(mi.title)


def import_book_directory_multiple(
        db,
        dirpath,
        callback: Optional[Callable[[str, ], None]] = None,
        added_ids: set[int] = None,
        compiled_rules: tuple[Callable[[str], bool], bool] = ()
):
    """
    Import a book directory into the database - directory is assumed to contain multiple books.

    :param db:
    :param dirpath:
    :param callback:
    :param added_ids:
    :param compiled_rules:
    :return:
    """
    from LiuXin.metadata.meta import metadata_from_formats

    duplicates = []
    for formats in find_books_in_directory(dirpath, False):
        mi = metadata_from_formats(formats)
        if mi.title is None:
            continue
        if db.has_book(mi):
            duplicates.append((mi, formats))
            continue
        book_id = db.import_book(mi, formats)
        if added_ids is not None:
            added_ids.add(book_id)
        if callable(callback):
            if callback(mi.title):
                break

    return duplicates


def recursive_import(
    db,
    root: Union[str, os.PathLike[str]],
    single_book_per_directory: bool = True,
    callback: Optional[Callable[[str, ], None]] = None,
    added_ids: set[int] = None,
    compiled_rules: tuple[Callable[[str], bool], bool] = (),
):
    """
    Recursively import every book in an entire directory structure.
    :param db: The database to work with
    :param root: The root of the tree to walk down
    :param single_book_per_directory: Should each book map to a single dictionary?
    :param callback: Callback function to report progress
    :param added_ids: A set of the ids which have already been added to the database
    :param compiled_rules: Rules to include/exclude certain file types
    :return:
    """
    root = os.path.abspath(root)
    duplicates = []
    for dirpath in os.walk(root):
        res = (
            import_book_directory(
                db,
                dirpath[0],
                callback=callback,
                added_ids=added_ids,
                compiled_rules=compiled_rules,
            )
            if single_book_per_directory
            else import_book_directory_multiple(
                db,
                dirpath[0],
                callback=callback,
                added_ids=added_ids,
                compiled_rules=compiled_rules,
            )
        )
        if res is not None:
            duplicates.extend(res)
        if callable(callback):
            if callback(""):
                break
    return duplicates


def add_catalog(cache, path, title, dbapi=None) -> tuple[int, bool]:
    """
    Add a catalog entry to the database.

    :param cache:
    :param path:
    :param title:
    :param dbapi:
    :return:
    """
    from LiuXin.metadata.book.base import calibreMetadata as Metadata
    from LiuXin.metadata.meta import get_metadata
    from LiuXin.utils.date import utcnow

    fmt = os.path.splitext(path)[1][1:].lower()
    new_book_added = False

    with lopen(path, "rb") as stream:

        with cache.write_lock:
            matches = cache._search(
                'title:="%s" and tags:="%s"' % (title.replace('"', '\\"'), _("Catalog")),
                None,
            )
            db_id = None
            if matches:
                db_id = list(matches)[0]
            try:
                mi = get_metadata(stream, fmt)
                mi.authors = ["calibre"]
            except:
                mi = Metadata(title, ["calibre"])
            mi.title, mi.authors = title, ["calibre"]
            mi.author_sort = "calibre"  # The MOBI/AZW3 format sets author sort to date
            mi.tags = [_("Catalog")]
            mi.pubdate = mi.timestamp = utcnow()
            if fmt == "mobi":
                mi.cover, mi.cover_data = None, (None, None)
            if db_id is None:
                db_id = cache._create_book_entry(mi, apply_import_tags=False)
                new_book_added = True
            else:
                cache._set_metadata(db_id, mi)
        cache.add_format(db_id, fmt, stream, dbapi=dbapi)  # Cant keep write lock since post-import hooks might run

    return db_id, new_book_added


def add_news(cache, path, arg, dbapi=None) -> int:
    """
    Add a news entry to the database.

    :param cache:
    :param path:
    :param arg:
    :param dbapi:
    :return:
    """
    from LiuXin.metadata.meta import get_metadata
    from LiuXin.utils.date import utcnow

    fmt = os.path.splitext(getattr(path, "name", path))[1][1:].lower()
    stream = path if hasattr(path, "read") else lopen(path, "rb")
    stream.seek(0)
    mi = get_metadata(stream, fmt, use_libprs_metadata=False, force_read_metadata=True)

    # Force the author to calibre as the auto delete of old news checks for both the author==calibre and the tag News
    mi.authors = ["calibre"]
    stream.seek(0)
    with cache.write_lock:
        if mi.series_index is None:
            mi.series_index = cache._get_next_series_num_for(mi.series)
        mi.tags = [_("News")]
        if arg["add_title_tag"]:
            mi.tags += [arg["title"]]
        if arg["custom_tags"]:
            mi.tags += arg["custom_tags"]
        if mi.pubdate is None:
            mi.pubdate = utcnow()
        if mi.timestamp is None:
            mi.timestamp = utcnow()

        db_id = cache._create_book_entry(mi, apply_import_tags=False)
    cache.add_format(db_id, fmt, stream, dbapi=dbapi)  # Cant keep write lock since post-import hooks might run

    if not hasattr(path, "read"):
        stream.close()
    return db_id
