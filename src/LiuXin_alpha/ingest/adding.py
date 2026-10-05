#!/usr/bin/env python
# vim:fileencoding=utf-8
"""
Retain legacy filename discovery, grouping, and Calibre-style import adapters.

Discovery groups readable nondirectory paths by stem/extension without validating
ebook contents. Rule precedence, nested per-format path lists, and ignored
single_fmt compatibility behavior are preserved. Database/news/catalog import
adapters depend on legacy metadata APIs, including LiuXin_alpha.metadata.meta,
which is absent from this checkout; importing these helpers is not proof those
integration paths are operational. No adapter provides all-run rollback.
"""


from __future__ import unicode_literals, division, absolute_import, print_function

from typing import Optional, Union, Iterable, Callable, Iterator, Any, TypeAlias, Protocol, overload

import fnmatch
import os
import re
import time
import warnings
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
    Split the final filename suffix and return it lowercased without its leading dot.

    Example:
        >>> splitext('books/title.EPUB')
        ('books/title', 'epub')


    :param path: Path text interpreted by the platform's os.path.splitext.
    :return: Unchanged stem and lowercase dotless final suffix, possibly empty.
    """
    key, ext = os.path.splitext(path)
    return key, ext[1:].lower()


def formats_ok(formats: Union[Iterable[str], dict[str, str]]) -> bool:
    """
    Accept a nonempty format group unless its sole mapping key is opf.

    This checks grouping keys, not ebook bytes or allowed extensions. Despite
    the broad annotation, singleton inputs require mapping-style key iteration;
    arbitrary iterables are not generally supported.

    Example:
        >>> formats_ok({'opf': ['metadata.opf']})
        False
        >>> formats_ok({'epub': ['book.epub'], 'opf': ['metadata.opf']})
        True


    :param formats: Sized format-key collection, normally the discovery grouping mapping.
    :return: False for an empty group or singleton opf mapping; otherwise True.
    """
    if formats and (len(formats) > 1 or tuple(iterkeys(formats)) != ("opf",)):
        return True
    return False


def path_ok(path: Union[os.PathLike[str], str]) -> bool:
    """
    Test current read access while excluding paths observed as directories.

    Normal filesystem lookup follows symlinks. This is neither a regular-file
    check nor a reservation guaranteeing a later open succeeds.

    Example:
        >>> path_ok(existing_book_path)  # doctest: +SKIP
        True


    :param path: Filesystem path checked by isdir and access(R_OK).
    :return: True for a readable path not currently observed as a directory.
    """
    return not os.path.isdir(path) and os.access(path, os.R_OK)


def compile_glob(pat: str) -> re.Pattern[str]:
    """
    Translate a shell-style glob into a case-insensitive regular expression.

    This compiles matching syntax; it does not enumerate or inspect the filesystem.

    Example:
        >>> compile_glob('*.epub').match('BOOK.EPUB') is not None
        True


    :param pat: fnmatch glob pattern interpreted as text.
    :return: Compiled re.I pattern using fnmatch.translate semantics.
    """
    return re.compile(fnmatch.translate(pat), flags=re.I)


def compile_rule(rule) -> tuple[Callable[[str], bool], bool]:
    """
    Compile a filename predicate and its add-versus-reject action from a rule mapping.

    match_type containing 'with' selects ICU-lowercased startswith or endswith;
    containing 'glob' otherwise selects a case-insensitive glob; all other values
    compile a case-sensitive regex using match, not search. A not_ prefix negates
    the selected predicate. Only action equal to 'add' produces True.

    Example:
        >>> predicate, add = compile_rule({'match_type': 'startswith', 'query': 'BOOK', 'action': 'add'})
        >>> predicate('book.epub'), add
        (True, True)


    :param rule: Mapping with match_type, query, and action keys; no schema validation is added.
    :return: Filename predicate paired with its boolean inclusion action.
    :raises KeyError: A required rule key is absent.
    :raises re.error: The selected regular-expression syntax is invalid.
    """
    mt = rule["match_type"]

    if "with" in mt:
        q = icu_lower(rule["query"])
        if "startswith" in mt:

            def func(filename):
                """
                Match the captured prefix against an ICU-lowercased filename.

                Example:
                    >>> predicate, _ = compile_rule({'match_type': 'startswith', 'query': 'book', 'action': 'add'})
                    >>> predicate('BOOK.epub')
                    True


                :param filename: Filename text to lowercase before prefix matching.
                :return: Whether the normalized name starts with the captured query.
                """
                return icu_lower(filename).startswith(q)

        else:

            def func(filename):
                """
                Match the captured suffix against an ICU-lowercased filename.

                Example:
                    >>> predicate, _ = compile_rule({'match_type': 'endswith', 'query': '.epub', 'action': 'add'})
                    >>> predicate('book.EPUB')
                    True


                :param filename: Filename text to lowercase before suffix matching.
                :return: Whether the normalized name ends with the captured query.
                """
                return icu_lower(filename).endswith(q)

    elif "glob" in mt:
        q = compile_glob(rule["query"])

        def func(filename):
            """
            Apply the captured case-insensitive translated glob to a filename.

            Example:
                >>> predicate, _ = compile_rule({'match_type': 'glob', 'query': '*.epub', 'action': 'add'})
                >>> predicate('BOOK.EPUB')
                True


            :param filename: Text tested directly against the compiled glob expression.
            :return: Whether the glob pattern matches the name.
            """
            return q.match(filename) is not None

    else:
        q = re.compile(rule["query"])

        def func(filename):
            """
            Match the captured regular expression at the beginning of filename text.

            Example:
                >>> predicate, _ = compile_rule({'match_type': 'regex', 'query': 'Book', 'action': 'add'})
                >>> predicate('Book.epub'), predicate('my Book.epub')
                (True, False)


            :param filename: Text tested with regex.match without case normalization.
            :return: Whether a match begins at position zero.
            """
            return q.match(filename) is not None

    ans = func
    if mt.startswith("not_"):

        def ans(filename):
            """
            Negate the compiled predicate for a not_-prefixed rule.

            Example:
                >>> predicate, _ = compile_rule({'match_type': 'not_glob', 'query': '*.tmp', 'action': 'add'})
                >>> predicate('book.epub')
                True


            :param filename: Text passed unchanged to the captured inner predicate.
            :return: Boolean inverse of that predicate's result.
            """
            return not func(filename)

    return ans, rule["action"] == "add"


def filter_filename(compiled_rules: tuple[Callable[[str], bool], bool], filename: str) -> bool:
    """
    Return the first matching rule's action, or implicitly None if no rule matches.

    The input is an iterable of predicate/action pairs, despite the historical
    single-pair annotation. None distinguishes no decision from explicit rejection.

    Example:
        >>> filter_filename([compile_rule({'match_type': 'glob', 'query': '*.epub', 'action': 'add'})], 'book.epub')
        True
        >>> filter_filename((), 'book.epub') is None
        True


    :param compiled_rules: Ordered predicate/action pairs; later matches do not override the first.
    :param filename: Filename text passed to each predicate until one succeeds.
    :return: First matching action, or None for extension-based fallback.
    """
    for q, action in compiled_rules:
        if q(filename):
            return action


_metadata_extensions = None


def metadata_extensions() -> frozenset[str]:
    """
    Lazily cache recognized book suffixes plus opf for metadata-sidecar discovery.

    This set is not a runtime decoder-availability check. OPF may accompany a
    book but a group containing only OPF is rejected separately by formats_ok.

    Example:
        >>> 'opf' in metadata_extensions()
        True


    :return: Cached immutable suffix set, initialized on the first call.
    """
    global _metadata_extensions
    if _metadata_extensions is None:
        _metadata_extensions = frozenset(six_map(six_unicode, BOOK_EXTENSIONS)) | {"opf"}
    return _metadata_extensions


def listdir(root: Union[str, os.PathLike[str]],
            sort_by_mtime: bool = False) -> Iterator[Union[os.PathLike[str], str]]:
    """
    Yield readable nondirectory children, optionally ordered by ascending mtime.

    Joined paths preserve the root's relativity; this helper does not make them
    absolute. Without sorting, filesystem enumeration order is retained. The
    access filter follows links and can include readable special files.

    Example:
        >>> paths = list(listdir(existing_directory, sort_by_mtime=True))  # doctest: +SKIP


    :param root: Directory passed to os.listdir and joined to each returned name.
    :param sort_by_mtime: Sort by observed modification time, substituting current time on errors.
    :return: Iterator of accessible nondirectory child paths, without recursing.
    :type sort_by_mtime: bool
    """
    items = (os.path.join(root, x) for x in os.listdir(root))
    if sort_by_mtime:

        def safe_mtime(x):
            """
            Read a sort key from mtime, falling back to current wall time on OSError.

            Example:
                >>> timestamp = safe_mtime(existing_path)  # doctest: +SKIP


            :param x: Child path whose last modification time is requested.
            :return: Observed epoch seconds, or time.time() after an access failure.
            """
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
    Apply first-match basename rules, falling back to recognized metadata suffixes.

    No filesystem access or content validation is performed. The supplied ext
    is used literally and should already be normalized by splitext.

    Example:
        >>> allow_path('book.epub', 'epub', ())
        True


    :param path: Path whose basename is passed to compiled predicates.
    :param ext: Lowercase dotless suffix used only when rules make no decision.
    :param compiled_rules: Ordered predicate/action pairs, not an all-rules conjunction.
    :return: First matching rule action, or suffix-membership result.
    """
    ans = filter_filename(compiled_rules, os.path.basename(path))
    if ans is None:
        ans = ext in metadata_extensions()
    return ans


class ListdirFn(Protocol):
    """
    Describe the injectable readable-path iterator used by directory discovery.

    Implementations must accept sort_by_mtime by keyword for per-stem grouping.
    Discovery trusts the supplied iterator rather than rechecking path access.

    Example:
        >>> listing: ListdirFn = listdir
        >>> listing is listdir
        True
    """

    def __call__(
        self,
        root: str | os.PathLike[str],
        sort_by_mtime: bool = False,
    ) -> Iterator[str | os.PathLike[str]]:
        """
        Iterate readable candidate paths with optional modification-time ordering.

        Example:
            >>> paths = list(listing(root, sort_by_mtime=True))  # doctest: +SKIP


        :param root: Directory whose immediate candidate children are requested.
        :param sort_by_mtime: Whether discovery requests oldest-first path order.
        :return: Iterator of filesystem path values for grouping, not parsed books.
        """
        ...


def find_books_in_directory(
    dirpath: Union[str, os.PathLike[str]],
    single_book_per_directory: bool = False,
    compiled_rules: tuple[Callable[[str], bool], bool] = (),
    listdir_impl: ListdirFn = listdir,
    single_fmt: bool = False,
) -> Iterator[list[str]]:
    """
    Group permitted paths into per-format lists for one directory or each filename stem.

    Normalize the directory to absolute, not symlink-resolved, form. Default
    mode requests mtime ordering and groups by ICU-lowercased full stem; single-
    book mode makes one directory-wide group. Preserve all same-format paths,
    and suppress OPF-only groups. Each yielded value is a list of path lists,
    despite the historical flatter return annotation. Contents are not parsed.
    single_fmt=True warns on iteration and is reset to False before grouping.

    Example:
        >>> groups = list(find_books_in_directory(existing_directory))  # doctest: +SKIP


    :param dirpath: Directory whose immediate paths are grouped; this function does not recurse.
    :param single_book_per_directory: Whether to combine all accepted stems into one group.
    :param compiled_rules: Ordered basename predicate/action pairs used before suffix fallback.
    :param listdir_impl: Injectable path iterator accepting optional sort_by_mtime.
    :param single_fmt: Deprecated and ignored. All discovered files are retained.
    :return: Iterator of candidate groups, each containing one path list per extension.
    """
    if single_fmt:
        warnings.warn(
            "find_books_in_directory(single_fmt=True) is deprecated and ignored; "
            "all discovered files are retained.",
            DeprecationWarning,
            stacklevel=2,
        )
        single_fmt = False

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
                    # Keep one list per extension for each discovered book key.
                    # setdefault avoids a KeyError when we see a *new* extension
                    # for a book key that is already present.
                    books[book_path].setdefault(ext, []).append(path)

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
    Import the first directory-wide candidate through the legacy metadata/database APIs.

    The lazy metadata.meta import currently requires an absent legacy module.
    When available, missing groups or titles return None; an existing book
    returns a duplicate pair instead of importing. After import, update added_ids
    before invoking a callable callback; its return value is ignored. Errors can
    follow a completed database effect, without rollback here.

    Example:
        >>> duplicates = import_book_directory(legacy_db, directory)  # doctest: +SKIP


    :param db: Legacy object providing has_book and import_book.
    :param dirpath: Directory combined into one grouped candidate.
    :param callback: Optional callable receiving the imported metadata title.
    :param added_ids: Optional mutable set to receive the new book identifier.
    :param compiled_rules: Basename rules forwarded to single-book discovery.
    :return: One-element duplicate list for an existing book, otherwise None.
    """
    from LiuXin_alpha.metadata.meta import metadata_from_formats

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
    Import per-stem candidates and collect duplicates through legacy database methods.

    metadata.meta is an absent legacy dependency in this checkout. If provided,
    candidates without titles are skipped; duplicates are retained without
    invoking the callback. A truthy callback result stops this directory after
    the just-imported book and added_ids update. compiled_rules is accepted but
    not forwarded to discovery in this implementation. No import rollback occurs.

    Example:
        >>> duplicates = import_book_directory_multiple(legacy_db, directory)  # doctest: +SKIP


    :param db: Legacy has_book/import_book service.
    :param dirpath: Directory grouped by normalized stem rather than treated as one book.
    :param callback: Optional post-import title callback; truthy return ends this directory.
    :param added_ids: Optional mutable set receiving each successful import identifier.
    :param compiled_rules: Historical argument currently ignored by this multi-book adapter.
    :return: List of metadata/group pairs found to be duplicates before the loop ended.
    """
    from LiuXin_alpha.metadata.meta import metadata_from_formats

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
    single_book_per_directory: bool = False,
    callback: Optional[Callable[[str, ], None]] = None,
    added_ids: set[int] = None,
    compiled_rules: tuple[Callable[[str], bool], bool] = (),
):
    """
    Walk directories and delegate each to the selected legacy import adapter.

    os.walk uses its default order, error handling, and no symlink-directory
    following. The same callback receives imported titles from child adapters
    and an empty string after each directory; only a truthy empty-string call
    stops the outer walk. Earlier imports survive cancellation/errors. Default
    multi-book mode currently ignores rules inside its child adapter.

    Example:
        >>> duplicates = recursive_import(legacy_db, library_root)  # doctest: +SKIP


    :param db: Legacy database object passed to each directory adapter.
    :param root: Tree root converted to an absolute path before walking.
    :param single_book_per_directory: Select directory-wide grouping instead of per-stem grouping.
    :param callback: Optional title/directory-progress callback with branch-dependent stop semantics.
    :param added_ids: Optional mutable set shared across all child import calls.
    :param compiled_rules: Forwarded rule pairs; only the single-book child currently applies them.
    :return: Concatenated duplicate pairs from directories processed before termination.
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
    Create or update a legacy Catalog-tagged book, then attach its format outside the lock.

    Requires the legacy metadata imports, including absent metadata.meta. Under
    the cache write lock, use the first title/tag search match or create a book;
    metadata read failures of any BaseException fall back to minimal metadata.
    Force calibre authorship/timestamps and clear MOBI cover data. add_format
    runs after releasing the lock with the same, not explicitly rewound stream.
    A later format failure does not roll back metadata changes here; the local
    input stream closes on exit.

    Example:
        >>> book_id, created = add_catalog(legacy_cache, path, 'Library catalog')  # doctest: +SKIP


    :param cache: Legacy search/create/set-metadata/format service exposing write_lock.
    :param path: Catalogue file opened in binary-read mode for metadata and format import.
    :param title: Forced book title and escaped exact-title search text.
    :param dbapi: Optional database API forwarded only to add_format.
    :return: Selected/new book identifier and whether this call created its book row.
    """
    from LiuXin_alpha.metadata.book.base import calibreMetadata as Metadata
    from LiuXin_alpha.metadata.meta import get_metadata
    from LiuXin_alpha.utils.date import utcnow

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
    Create a legacy News-tagged book and attach its format after releasing the write lock.

    Requires absent metadata.meta. Accept a path or seekable readable object,
    seek to zero around metadata extraction, then force calibre authorship and
    News tags with requested additions. Fill missing series index/date fields
    before creating a book row. A later add_format failure leaves prior effects.
    Caller streams remain open; internally opened streams close only on normal
    completion, not through a finally block.

    Example:
        >>> book_id = add_news(legacy_cache, path, {'add_title_tag': False, 'custom_tags': []})  # doctest: +SKIP


    :param cache: Legacy metadata/create/format service with a write lock.
    :param path: Filename or seekable stream whose name supplies the format suffix.
    :param arg: Options with add_title_tag, custom_tags, and title when title-tagging is enabled.
    :param dbapi: Optional database API forwarded to add_format.
    :return: Created book identifier after successful format attachment.
    """
    from LiuXin_alpha.metadata.meta import get_metadata
    from LiuXin_alpha.utils.date import utcnow

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
