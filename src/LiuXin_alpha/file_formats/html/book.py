"""
The HTML intermediate: a set of HTML pages and their resources in one folder.

An :class:`HTMLBook` describes a folder and never changes it. It names the
entry page, the pages in reading order, the resources they use, a table of
contents, and an optional cover and metadata sidecar. Every member is given by
a path relative to the root folder.

This module holds the data model and the invariants that need no filesystem
access. A value that breaks one of them cannot be constructed. Loading a
folder, saving one, and the invariants that need the disk are specified in
``dev-docs/file_formats/html-book-spec.md`` and are not implemented here.

The module imports only the standard library.
"""

from __future__ import annotations

import codecs
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Self, cast


class HTMLBookError(ValueError):
    """
    Report a value that cannot be an HTML book.

    Catch this to handle every problem this module raises.

    Example:
        >>> issubclass(HTMLBookPathError, HTMLBookError)
        True
    """


class HTMLBookPathError(HTMLBookError):
    """
    Report a member path that could name a file outside the root folder.

    Example:
        >>> try:
        ...     member_path("../secret.txt")
        ... except HTMLBookPathError as error:
        ...     print(error)
        member path '../secret.txt' has a '..' segment
    """


class HTMLBookStructureError(HTMLBookError):
    """
    Report a book whose parts do not fit together.

    Example:
        >>> try:
        ...     HTMLTocEntry(label="  ", page=PurePosixPath("index.html"))
        ... except HTMLBookStructureError as error:
        ...     print(error)
        a table-of-contents label must contain a visible character, got '  '
    """


def member_path(text: str) -> PurePosixPath:
    """
    Turn text into a member path, refusing anything that is not one.

    Use this for text that comes from a file, an archive or a manifest. A
    member path is relative, uses ``/`` between segments, and has no empty,
    ``.`` or ``..`` segment, no drive, no backslash and no NUL character. The
    text is checked as written, before any tidying could hide a problem.

    Example:
        >>> member_path("images/cover.jpg")
        PurePosixPath('images/cover.jpg')
        >>> try:
        ...     member_path("text//page.html")
        ... except HTMLBookPathError as error:
        ...     print(error)
        member path 'text//page.html' has an empty segment


    :param text: The path as written in its source.
    :return: The same path as a :class:`~pathlib.PurePosixPath`.
    :raises HTMLBookPathError: If the text is not a member path.
    """
    if not text:
        raise HTMLBookPathError("a member path must not be empty")
    segments = text.split("/")
    if text.startswith("/"):
        raise HTMLBookPathError(f"member path {text!r} is not relative")
    if "" in segments:
        raise HTMLBookPathError(f"member path {text!r} has an empty segment")
    if "." in segments:
        raise HTMLBookPathError(f"member path {text!r} has a '.' segment")
    return checked_member_path(PurePosixPath(text), "member path")


def checked_member_path(value: object, what: str) -> PurePosixPath:
    """
    Check that a value already held as a path is a member path.

    A :class:`~pathlib.PurePosixPath` has already dropped empty and ``.``
    segments, so this checks what can survive that: an absolute path, a
    ``..`` segment, a drive, a backslash or a NUL character. A concrete
    :class:`~pathlib.Path` is refused even where it is a ``PurePosixPath``
    subclass, so that the same value is accepted on every platform.

    Example:
        >>> checked_member_path(PurePosixPath("text/page.html"), "page")
        PurePosixPath('text/page.html')
        >>> try:
        ...     checked_member_path(PurePosixPath("C:/page.html"), "page")
        ... except HTMLBookPathError as error:
        ...     print(error)
        page 'C:/page.html' has a drive


    :param value: The candidate, of any type.
    :param what: What the path is for, used to start the error message.
    :return: The value, known to be a member path.
    :raises HTMLBookPathError: If the value is not a member path.
    """
    if not isinstance(value, PurePosixPath) or isinstance(value, Path):
        raise HTMLBookPathError(f"{what} must be a PurePosixPath, got {value!r}")
    text = str(value)
    if value.is_absolute():
        raise HTMLBookPathError(f"{what} {text!r} is not relative")
    if not value.parts:
        raise HTMLBookPathError(f"{what} must name a file, got {text!r}")
    if ".." in value.parts:
        raise HTMLBookPathError(f"{what} {text!r} has a '..' segment")
    if "\\" in text:
        raise HTMLBookPathError(f"{what} {text!r} has a backslash")
    if "\x00" in text:
        raise HTMLBookPathError(f"{what} {text!r} has a NUL character")
    if PureWindowsPath(text).drive:
        raise HTMLBookPathError(f"{what} {text!r} has a drive")
    return value


def has_visible_character(text: str) -> bool:
    """
    Say whether text would show something when displayed.

    Example:
        >>> has_visible_character("Chapter 1"), has_visible_character(" \\t\\u200b")
        (True, False)


    :param text: The text to test.
    :return: ``True`` if any character is printable and is not white space.
    """
    return any(char.isprintable() and not char.isspace() for char in text)


def _text(value: object, what: str) -> str:
    """
    Check that a value is text.

    Example:
        >>> _text("utf-8", "page encoding")
        'utf-8'


    :param value: The candidate, of any type.
    :param what: The name of the field, used in the error message.
    :return: The value, known to be a string.
    :raises HTMLBookStructureError: If it is not a string.
    """
    if not isinstance(value, str):
        raise HTMLBookStructureError(f"{what} must be text, got {value!r}")
    return value


def _absolute_folder(value: object) -> Path:
    """
    Check that a value is an absolute filesystem path.

    Example:
        >>> _absolute_folder(Path.cwd()) == Path.cwd()
        True


    :param value: The candidate, of any type.
    :return: The value, known to be an absolute path.
    :raises HTMLBookStructureError: If it is not a path or is relative.
    """
    if not isinstance(value, Path) or not value.is_absolute():
        raise HTMLBookStructureError(f"root must be an absolute Path, got {value!r}")
    return value


def _tuple_of[T](value: object, item_type: type[T], what: str) -> tuple[T, ...]:
    """
    Check that a value is a tuple holding only one type of item.

    Example:
        >>> _tuple_of(("a", "b"), str, "names")
        ('a', 'b')


    :param value: The candidate, of any type.
    :param item_type: The type every item must have.
    :param what: The name of the field, used in the error message.
    :return: The value, known to be a tuple of ``item_type``.
    :raises HTMLBookStructureError: If it is not a tuple or holds another type.
    """
    if not isinstance(value, tuple):
        raise HTMLBookStructureError(f"{what} must be a tuple, got {value!r}")
    items = cast("tuple[object, ...]", value)
    for item in items:
        if not isinstance(item, item_type):
            raise HTMLBookStructureError(
                f"{what} must hold only {item_type.__name__} values, got {item!r}"
            )
    return cast("tuple[T, ...]", items)


@dataclass(frozen=True, slots=True)
class HTMLPage:
    """
    Describe one HTML page of a book.

    The encoding is stored under the name Python's codec registry gives it,
    so two pages in the same encoding always compare equal on that field.

    Example:
        >>> page = HTMLPage(PurePosixPath("index.html"), "utf-8", "Contents")
        >>> page.title
        'Contents'
        >>> try:
        ...     HTMLPage(PurePosixPath("index.html"), "UTF8")
        ... except HTMLBookStructureError as error:
        ...     print(error)
        page encoding 'UTF8' must be written as 'utf-8'


    :param path: The page's path relative to the book's root.
    :param encoding: The character encoding of the page's bytes, as a Python
        codec name.
    :param title: The page's title, or ``None`` if it has none.
    """

    path: PurePosixPath
    encoding: str
    title: str | None = None

    def __post_init__(self: Self) -> None:
        """
        Refuse a page whose path or encoding breaks the book's invariants.

        Example:
            >>> try:
            ...     HTMLPage(PurePosixPath("index.html"), "no-such-codec")
            ... except HTMLBookStructureError as error:
            ...     print(error)
            page encoding 'no-such-codec' is not a known codec


        :return: ``None``.
        :raises HTMLBookPathError: If ``path`` is not a member path.
        :raises HTMLBookStructureError: If ``encoding`` is unknown or is not
            written under its registry name.
        """
        checked_member_path(self.path, "page path")
        encoding = _text(self.encoding, "page encoding")
        try:
            registered = codecs.lookup(encoding).name
        except LookupError:
            raise HTMLBookStructureError(
                f"page encoding {encoding!r} is not a known codec"
            ) from None
        if registered != encoding:
            raise HTMLBookStructureError(
                f"page encoding {encoding!r} must be written as {registered!r}"
            )


@dataclass(frozen=True, slots=True)
class HTMLTocEntry:
    """
    Describe one entry in a book's table of contents.

    Example:
        >>> part = HTMLTocEntry(
        ...     label="Part One",
        ...     page=PurePosixPath("part1.html"),
        ...     children=(
        ...         HTMLTocEntry("Chapter 1", PurePosixPath("part1.html"), "ch1"),
        ...     ),
        ... )
        >>> part.children[0].fragment
        'ch1'


    :param label: The text shown for the entry.
    :param page: The page the entry points at.
    :param fragment: An anchor within that page, or ``None`` for its start.
    :param children: Entries nested under this one.
    """

    label: str
    page: PurePosixPath
    fragment: str | None = None
    children: tuple[HTMLTocEntry, ...] = ()

    def __post_init__(self: Self) -> None:
        """
        Refuse an entry with nothing to show or with a bad target.

        Example:
            >>> try:
            ...     HTMLTocEntry("Start", PurePosixPath("/index.html"))
            ... except HTMLBookPathError as error:
            ...     print(error)
            table-of-contents page '/index.html' is not relative


        :return: ``None``.
        :raises HTMLBookPathError: If ``page`` is not a member path.
        :raises HTMLBookStructureError: If ``label`` shows nothing or
            ``children`` is not a tuple of entries.
        """
        label = _text(self.label, "a table-of-contents label")
        if not has_visible_character(label):
            raise HTMLBookStructureError(
                "a table-of-contents label must contain a visible character, "
                f"got {label!r}"
            )
        checked_member_path(self.page, "table-of-contents page")
        _tuple_of(self.children, HTMLTocEntry, "table-of-contents children")

    def walk(self: Self) -> tuple[HTMLTocEntry, ...]:
        """
        List this entry and every entry nested under it, in reading order.

        Example:
            >>> page = PurePosixPath("index.html")
            >>> entry = HTMLTocEntry("A", page, children=(HTMLTocEntry("B", page),))
            >>> [item.label for item in entry.walk()]
            ['A', 'B']


        :return: This entry first, then its descendants depth first.
        """
        found: list[HTMLTocEntry] = [self]
        for child in self.children:
            found.extend(child.walk())
        return tuple(found)


@dataclass(frozen=True, slots=True)
class HTMLBook:
    """
    Describe a book held as HTML pages and resources in one folder.

    The value is a description. Constructing one reads nothing from disk, and
    nothing that takes one may change the folder it describes; a step that
    alters a book writes a new folder and returns a new value.

    Example:
        >>> from pathlib import Path
        >>> book = HTMLBook(
        ...     root=Path.cwd(),
        ...     entry=PurePosixPath("index.html"),
        ...     pages=(
        ...         HTMLPage(PurePosixPath("index.html"), "utf-8", "A Book"),
        ...         HTMLPage(PurePosixPath("chapter1.html"), "utf-8"),
        ...     ),
        ...     resources=(PurePosixPath("images/cover.jpg"),),
        ...     cover=PurePosixPath("images/cover.jpg"),
        ... )
        >>> book.title
        'A Book'
        >>> [str(path) for path in book.member_paths]
        ['index.html', 'chapter1.html', 'images/cover.jpg']


    :param root: The folder that contains every member; an absolute path.
    :param entry: The path of the first page.
    :param pages: The pages in reading order, starting with the entry.
    :param resources: The files the pages use, in a stable order.
    :param toc: The table of contents.
    :param toc_is_derived: ``True`` when the table of contents was built from
        the pages by rule, ``False`` when a provider or manifest supplied it.
    :param cover: The cover image, which must be one of ``resources``.
    :param metadata: An OPF metadata sidecar, which is neither a page nor a
        resource.
    """

    root: Path
    entry: PurePosixPath
    pages: tuple[HTMLPage, ...]
    resources: tuple[PurePosixPath, ...] = ()
    toc: tuple[HTMLTocEntry, ...] = ()
    toc_is_derived: bool = True
    cover: PurePosixPath | None = None
    metadata: PurePosixPath | None = None

    def __post_init__(self: Self) -> None:
        """
        Refuse a book that breaks a structural invariant.

        The invariants are S1 to S6 of the HTMLBook specification. None of
        them needs the filesystem.

        Example:
            >>> from pathlib import Path
            >>> try:
            ...     HTMLBook(Path.cwd(), PurePosixPath("index.html"), pages=())
            ... except HTMLBookStructureError as error:
            ...     print(error)
            a book must have at least one page


        :return: ``None``.
        :raises HTMLBookPathError: If a member path is not a member path.
        :raises HTMLBookStructureError: If any other invariant is broken.
        """
        _absolute_folder(self.root)
        entry = checked_member_path(self.entry, "entry")
        pages = _tuple_of(self.pages, HTMLPage, "pages")
        if not pages:
            raise HTMLBookStructureError("a book must have at least one page")
        if pages[0].path != entry:
            raise HTMLBookStructureError(
                f"the first page must be the entry {str(entry)!r}, "
                f"got {str(pages[0].path)!r}"
            )
        resources = _tuple_of(self.resources, PurePosixPath, "resources")
        for resource in resources:
            checked_member_path(resource, "resource")
        self._check_members_are_distinct()
        self._check_cover_and_metadata()
        self._check_table_of_contents()

    def _check_members_are_distinct(self: Self) -> None:
        """
        Refuse a book that lists the same path twice among its members.

        Example:
            >>> from pathlib import Path
            >>> page = HTMLPage(PurePosixPath("index.html"), "utf-8")
            >>> try:
            ...     HTMLBook(Path.cwd(), page.path, (page, page))
            ... except HTMLBookStructureError as error:
            ...     print(error)
            'index.html' is listed more than once


        :return: ``None``.
        :raises HTMLBookStructureError: If a path appears more than once.
        """
        seen: set[PurePosixPath] = set()
        for path in (*(page.path for page in self.pages), *self.resources):
            if path in seen:
                raise HTMLBookStructureError(f"{str(path)!r} is listed more than once")
            seen.add(path)

    def _check_cover_and_metadata(self: Self) -> None:
        """
        Refuse a cover that is not a resource or a sidecar that is a member.

        Example:
            >>> from pathlib import Path
            >>> page = HTMLPage(PurePosixPath("index.html"), "utf-8")
            >>> try:
            ...     HTMLBook(Path.cwd(), page.path, (page,), metadata=page.path)
            ... except HTMLBookStructureError as error:
            ...     print(error)
            metadata 'index.html' must not also be a page or a resource


        :return: ``None``.
        :raises HTMLBookPathError: If either path is not a member path.
        :raises HTMLBookStructureError: If either is in the wrong place.
        """
        if self.cover is not None:
            cover = checked_member_path(self.cover, "cover")
            if cover not in self.resources:
                raise HTMLBookStructureError(
                    f"cover {str(cover)!r} must be one of the resources"
                )
        if self.metadata is not None:
            metadata = checked_member_path(self.metadata, "metadata")
            if metadata in self.resources or metadata in self.page_paths:
                raise HTMLBookStructureError(
                    f"metadata {str(metadata)!r} must not also be a page or a resource"
                )

    def _check_table_of_contents(self: Self) -> None:
        """
        Refuse a table of contents that points at something that is not a page.

        Example:
            >>> from pathlib import Path
            >>> page = HTMLPage(PurePosixPath("index.html"), "utf-8")
            >>> entry = HTMLTocEntry("Notes", PurePosixPath("notes.html"))
            >>> try:
            ...     HTMLBook(Path.cwd(), page.path, (page,), toc=(entry,))
            ... except HTMLBookStructureError as error:
            ...     print(error)
            table-of-contents entry 'Notes' points at 'notes.html', which is not a page


        :return: ``None``.
        :raises HTMLBookStructureError: If the table is not a tuple of entries
            or an entry's page is not one of the book's pages.
        """
        page_paths = frozenset(self.page_paths)
        for top in _tuple_of(self.toc, HTMLTocEntry, "toc"):
            for entry in top.walk():
                if entry.page not in page_paths:
                    raise HTMLBookStructureError(
                        f"table-of-contents entry {entry.label!r} points at "
                        f"{str(entry.page)!r}, which is not a page"
                    )

    @property
    def title(self: Self) -> str | None:
        """
        Give the book's title, which is the entry page's title.

        Example:
            >>> from pathlib import Path
            >>> page = HTMLPage(PurePosixPath("index.html"), "utf-8")
            >>> HTMLBook(Path.cwd(), page.path, (page,)).title is None
            True


        :return: The entry page's title, or ``None`` if it has none.
        """
        return self.pages[0].title

    @property
    def page_paths(self: Self) -> tuple[PurePosixPath, ...]:
        """
        List the paths of the pages in reading order.

        Example:
            >>> from pathlib import Path
            >>> page = HTMLPage(PurePosixPath("index.html"), "utf-8")
            >>> HTMLBook(Path.cwd(), page.path, (page,)).page_paths
            (PurePosixPath('index.html'),)


        :return: One path per page, starting with the entry.
        """
        return tuple(page.path for page in self.pages)

    @property
    def member_paths(self: Self) -> tuple[PurePosixPath, ...]:
        """
        List every file the book consists of.

        Example:
            >>> from pathlib import Path
            >>> page = HTMLPage(PurePosixPath("index.html"), "utf-8")
            >>> sidecar = PurePosixPath("metadata.opf")
            >>> book = HTMLBook(Path.cwd(), page.path, (page,), metadata=sidecar)
            >>> [str(path) for path in book.member_paths]
            ['index.html', 'metadata.opf']


        :return: The pages, then the resources, then the metadata sidecar if
            there is one.
        """
        sidecar = () if self.metadata is None else (self.metadata,)
        return (*self.page_paths, *self.resources, *sidecar)
