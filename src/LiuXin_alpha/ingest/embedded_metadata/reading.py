"""
Choose registered files, read their embedded metadata, and normalise it for planning.

Reading goes through the file's Store, so the same code serves local disks and
remote read-only Stores; a remote Store downloads each file through its own rate
limits. Each file is read in isolation: a reader failure becomes a
:class:`FileFailure` and never stops the run. Nothing is written anywhere.

Normalisation applies the agreed catalogue-boundary rules: calibre's undefined
date is dropped, placeholder titles and authors are removed, and a missing
title falls back to the file name, with a flag either way.
"""

from __future__ import annotations

import re
import tempfile
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, BinaryIO

from LiuXin_alpha.ingest.embedded_metadata.models import (
    FLAG_NO_AUTHORS,
    FLAG_PARTIAL_DATE,
    FLAG_TITLE_FROM_FILENAME,
    FLAG_UNDEFINED_DATE,
    FileFacts,
    FileFailure,
    IngestFile,
)

DEFAULT_MAX_BYTES = 512 * 1024 * 1024
_SPOOL_IN_MEMORY_BYTES = 32 * 1024 * 1024
_COPY_CHUNK_BYTES = 1024 * 1024
_PLACEHOLDER_NAMES = frozenset({"", "unknown"})
# Book-record identifiers say which file or calibre record this was, not which
# Work it is, so they never count as Work identity evidence.
_RECORD_IDENTIFIER_SCHEMES = frozenset({"calibre", "uri", "uuid"})
_UNDETERMINED_LANGUAGES = frozenset({"", "und", "zxx"})
_WHITESPACE = re.compile(r"\s+")

MetadataReader = Callable[..., Any]
StreamOpener = Callable[[str], BinaryIO]


class _TooLargeError(Exception):
    """
    Signal that a stream grew past the configured size limit while copying.

    Example:
        >>> isinstance(_TooLargeError("x"), Exception)
        True
    """


def select_files(
    rows: Iterable[Mapping[str, Any]],
    *,
    readable: Collection[str],
    extensions: Collection[str] | None = None,
    limit: int | None = None,
) -> tuple[list[IngestFile], dict[str, int]]:
    """
    Choose the registered files a dry run should read, in Store-key order.

    Files already attached to an Item were ingested before and are skipped, as
    are files outside the extension filter, files with no metadata reader, and
    files past the limit. Skips are counted by reason, not listed.

    Example:
        >>> rows = [
        ...     {"file_id": 1, "file_storage_key": "b.epub", "file_extension": "epub", "file_item_id": None, "file_size_bytes": 5},
        ...     {"file_id": 2, "file_storage_key": "a.bin", "file_extension": "bin", "file_item_id": None, "file_size_bytes": 5},
        ... ]
        >>> files, skipped = select_files(rows, readable={"epub"})
        >>> [f.storage_key for f in files], skipped
        (['b.epub'], {'no_metadata_reader': 1})


    :param rows: ``files`` rows with id, storage key, extension, item id and size.
    :param readable: Extensions that have a metadata reader.
    :param extensions: Optional extensions to keep; None keeps every readable one.
    :param limit: Optional maximum number of files to choose.
    :return: Chosen files and per-reason skip counts.
    """
    wanted = None if extensions is None else {_extension(item) for item in extensions}
    chosen: list[IngestFile] = []
    skipped: dict[str, int] = {}
    for row in sorted(rows, key=lambda item: str(item.get("file_storage_key") or "")):
        reason = _skip_reason(row, readable=readable, wanted=wanted)
        if reason is None and limit is not None and len(chosen) >= limit:
            reason = "over_limit"
        if reason is not None:
            skipped[reason] = skipped.get(reason, 0) + 1
            continue
        chosen.append(_ingest_file(row))
    return chosen, skipped


def read_file(
    open_stream: StreamOpener,
    file: IngestFile,
    *,
    reader: MetadataReader,
    max_bytes: int = DEFAULT_MAX_BYTES,
    today: date | None = None,
) -> FileFacts | FileFailure:
    """
    Read one file's embedded metadata through its Store and normalise it.

    Files recorded as larger than ``max_bytes`` are not opened. Streams that
    cannot seek are copied to a temporary file first, still within the limit.
    Any exception from opening or reading becomes a failure for this file only.

    Example:
        >>> import io
        >>> file = IngestFile(1, "a.epub", "epub", 3)
        >>> facts = read_file(lambda key: io.BytesIO(b"abc"), file, reader=lambda stream, force_type: None)
        >>> facts.title, facts.notes
        ('a', ('title_from_filename', 'no_authors'))


    :param open_stream: Callable returning a binary stream for a storage key.
    :param file: File to read.
    :param reader: Metadata reader called as ``reader(stream, force_type=extension)``.
    :param max_bytes: Largest file to read.
    :param today: Date used to spot dates whose month and day were filled in; defaults to today.
    :return: Normalised facts, or the failure that stopped this file.
    """
    if file.size_bytes is not None and file.size_bytes > max_bytes:
        return FileFailure(
            file, "too_large", _limit_message(file.size_bytes, max_bytes)
        )
    try:
        source = open_stream(file.storage_key)
    except Exception as exc:  # A Store may fail in many backend-specific ways.
        return FileFailure(file, "open", _describe(exc))
    try:
        with source, _seekable(source, max_bytes) as stream:
            metadata = reader(stream, force_type=file.extension)
    except _TooLargeError as exc:
        return FileFailure(file, "too_large", str(exc))
    except Exception as exc:  # Readers raise many unrelated error types.
        return FileFailure(file, "read", _describe(exc))
    return normalise(metadata, file, today=today)


def normalise(
    metadata: Any, file: IngestFile, *, today: date | None = None
) -> FileFacts:
    """
    Turn a reader's metadata object into catalogue-ready facts with review notes.

    Both LiuXin's calibre-like containers and plain calibre ``Metadata`` objects
    are accepted; a missing object (a reader that found nothing) yields a title
    from the file name and no other values.

    Example:
        >>> from types import SimpleNamespace
        >>> facts = normalise(SimpleNamespace(title=" Frankenstein ", authors=["Unknown"]), IngestFile(1, "f.epub", "epub"))
        >>> facts.title, facts.authors, facts.notes
        ('Frankenstein', (), ('no_authors',))


    :param metadata: Reader result, or None.
    :param file: File the metadata came from.
    :param today: Date used to spot filled-in months and days; defaults to today.
    :return: Normalised facts.
    """
    book = _calibre_view(metadata)
    notes: list[str] = []
    title = _clean(_field(book, "title"))
    if title is None or title.casefold() in _PLACEHOLDER_NAMES:
        title = _title_from_file_name(file)
        notes.append(FLAG_TITLE_FROM_FILENAME)
    authors = _names(_field(book, "authors"))
    if not authors:
        notes.append(FLAG_NO_AUTHORS)
    pubdate, date_notes = calendar_date(_field(book, "pubdate"), today=today)
    notes.extend(date_notes)
    series = _clean(_field(book, "series"))
    return FileFacts(
        file=file,
        title=title,
        title_sort=_clean(_field(book, "title_sort")),
        authors=authors,
        author_sort=_clean(_field(book, "author_sort")),
        languages=_languages(_field(book, "languages")),
        pubdate=pubdate,
        publisher=_publisher(_field(book, "publisher")),
        identifiers=_identifiers(_field(book, "identifiers")),
        tags=_names(_field(book, "tags")),
        series=series,
        series_index=_series_index(_field(book, "series_index")) if series else None,
        has_comments=_clean(_field(book, "comments")) is not None,
        cover=_cover(_field(book, "cover_data")),
        notes=tuple(notes),
    )


def calendar_date(
    value: Any, *, today: date | None = None
) -> tuple[str | None, tuple[str, ...]]:
    """
    Convert a reader's datetime to a calendar date, applying the boundary rules.

    Calibre's undefined date (year 101 or earlier) is dropped. Readers parse a
    partial date such as ``1818`` by filling the month and day from the day of
    reading, so a date falling on today's month and day in another year is
    flagged as possibly partial. Aware datetimes are read back in the zone the
    readers assumed for dates written without one.

    Example:
        >>> from datetime import date, datetime, timezone
        >>> calendar_date(datetime(101, 1, 1, tzinfo=timezone.utc))
        (None, ('undefined_date_dropped',))
        >>> calendar_date(datetime(1818, 11, 27), today=date(2026, 11, 27))
        ('1818-11-27', ('date_may_be_partial',))


    :param value: Reader value; anything but a datetime yields no date.
    :param today: Date of reading; defaults to today.
    :return: ISO date or None, and any review notes.
    """
    if not isinstance(value, datetime):
        return None, ()
    if value.year <= 101:
        return None, (FLAG_UNDEFINED_DATE,)
    day = _local_date(value)
    reference = today or date.today()
    if (day.month, day.day) == (
        reference.month,
        reference.day,
    ) and day.year != reference.year:
        return day.isoformat(), (FLAG_PARTIAL_DATE,)
    return day.isoformat(), ()


def _skip_reason(
    row: Mapping[str, Any], *, readable: Collection[str], wanted: set[str] | None
) -> str | None:
    """
    Return why a file row is not read, or None when it should be.

    Example:
        >>> _skip_reason({"file_item_id": 3}, readable={"epub"}, wanted=None)
        'already_attached'


    :param row: ``files`` row.
    :param readable: Extensions with a metadata reader.
    :param wanted: Optional extension filter.
    :return: A skip reason, or None.
    """
    if row.get("file_item_id") is not None:
        return "already_attached"
    extension = _row_extension(row)
    if wanted is not None and extension not in wanted:
        return "extension_filtered"
    if extension not in readable:
        return "no_metadata_reader"
    return None


def _ingest_file(row: Mapping[str, Any]) -> IngestFile:
    """
    Build the file identity from a ``files`` row.

    Example:
        >>> _ingest_file({"file_id": "4", "file_storage_key": "a.EPUB", "file_extension": None, "file_size_bytes": None})
        IngestFile(file_id=4, storage_key='a.EPUB', extension='epub', size_bytes=None)


    :param row: ``files`` row.
    :return: The file's id, key, extension and recorded size.
    """
    size = row.get("file_size_bytes")
    return IngestFile(
        file_id=int(row["file_id"]),
        storage_key=str(row.get("file_storage_key") or ""),
        extension=_row_extension(row),
        size_bytes=None if size is None else int(size),
    )


def _row_extension(row: Mapping[str, Any]) -> str:
    """
    Return a row's extension, falling back to the storage key's suffix.

    Example:
        >>> _row_extension({"file_extension": "", "file_storage_key": "x/b.Mobi"})
        'mobi'


    :param row: ``files`` row.
    :return: Lower-case extension without a dot, or an empty string.
    """
    recorded = _extension(row.get("file_extension") or "")
    if recorded:
        return recorded
    name = str(row.get("file_storage_key") or "").rpartition("/")[2]
    base, dot, suffix = name.rpartition(".")
    return _extension(suffix) if dot and base else ""


def _extension(value: object) -> str:
    """
    Normalise an extension to lower case without leading dots.

    Example:
        >>> _extension(".EPUB")
        'epub'


    :param value: Extension text.
    :return: Normalised extension.
    """
    return str(value).strip().lstrip(".").lower()


@contextmanager
def _seekable(source: BinaryIO, max_bytes: int) -> Iterator[BinaryIO]:
    """
    Yield a seekable stream with the source's bytes, copying only when needed.

    Example:
        >>> import io
        >>> with _seekable(io.BytesIO(b"abc"), 10) as stream:
        ...     stream.read()
        b'abc'


    :param source: Stream opened by the Store; left open for the caller to close.
    :param max_bytes: Largest copy allowed.
    :return: The source itself when seekable, otherwise a spooled copy.
    :raises _TooLargeError: If the copy grows past ``max_bytes``.
    """
    seekable = getattr(source, "seekable", None)
    if callable(seekable) and seekable():
        yield source
        return
    with tempfile.SpooledTemporaryFile(max_size=_SPOOL_IN_MEMORY_BYTES) as copy:
        _copy_within_limit(source, copy, max_bytes)
        copy.seek(0)
        yield copy  # type: ignore[misc]


def _copy_within_limit(source: BinaryIO, target: Any, max_bytes: int) -> None:
    """
    Copy a stream in chunks, stopping once it passes the size limit.

    Example:
        >>> import io
        >>> target = io.BytesIO()
        >>> _copy_within_limit(io.BytesIO(b"abc"), target, 10)
        >>> target.getvalue()
        b'abc'


    :param source: Stream to read.
    :param target: Writable file object.
    :param max_bytes: Largest copy allowed.
    :return: None.
    :raises _TooLargeError: If the stream holds more than ``max_bytes`` bytes.
    """
    copied = 0
    while chunk := source.read(_COPY_CHUNK_BYTES):
        copied += len(chunk)
        if copied > max_bytes:
            raise _TooLargeError(_limit_message(copied, max_bytes, at_least=True))
        target.write(chunk)


def _limit_message(size: int, max_bytes: int, *, at_least: bool = False) -> str:
    """
    Describe a file that is over the size limit.

    Example:
        >>> _limit_message(10, 5)
        'file is 10 bytes; the limit is 5'


    :param size: Size seen so far, or recorded size.
    :param max_bytes: The limit.
    :param at_least: Whether the size is a lower bound.
    :return: Human-readable message.
    """
    prefix = "at least " if at_least else ""
    return f"file is {prefix}{size} bytes; the limit is {max_bytes}"


def _describe(exc: BaseException) -> str:
    """
    Summarise an exception as its type and first message line.

    Example:
        >>> _describe(ValueError("bad\\nmore"))
        'ValueError: bad'


    :param exc: Exception to describe.
    :return: ``Type: message`` with the message cut to one line of 300 characters.
    """
    message = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    text = type(exc).__name__ + (f": {message}" if message else "")
    return text[:300]


def _calibre_view(metadata: Any) -> Any:
    """
    Return a calibre-shaped view of a reader result.

    Example:
        >>> _calibre_view(None) is None
        True


    :param metadata: LiuXin container, calibre ``Metadata``, or None.
    :return: The container's calibre projection, or the object itself.
    """
    convert = getattr(metadata, "to_calibre", None)
    return convert() if callable(convert) else metadata


def _field(book: Any, name: str) -> Any:
    """
    Read a field, treating absent fields as None.

    Example:
        >>> _field(None, "title") is None
        True


    :param book: Calibre-shaped metadata or None.
    :param name: Field name.
    :return: The value, or None.
    """
    if book is None:
        return None
    return getattr(book, name, None)


def _clean(value: Any) -> str | None:
    """
    Collapse whitespace in text, returning None for empty values.

    Example:
        >>> _clean("  A   b ")
        'A b'


    :param value: Text or None.
    :return: Cleaned text, or None.
    """
    if value is None:
        return None
    text = _WHITESPACE.sub(" ", str(value)).strip()
    return text or None


def _names(values: Any) -> tuple[str, ...]:
    """
    Clean a list of names, dropping placeholders and case-insensitive repeats.

    Example:
        >>> _names(["Ann", "ann", "Unknown", " Bo "])
        ('Ann', 'Bo')


    :param values: Iterable of names, a single name, or None.
    :return: Names in first-seen order.
    """
    if values is None:
        return ()
    items = [values] if isinstance(values, str) else list(values)
    seen: set[str] = set()
    names: list[str] = []
    for item in items:
        name = _clean(item)
        if (
            name is None
            or name.casefold() in _PLACEHOLDER_NAMES
            or name.casefold() in seen
        ):
            continue
        seen.add(name.casefold())
        names.append(name)
    return tuple(names)


def _languages(values: Any) -> tuple[str, ...]:
    """
    Clean language codes, dropping undetermined codes and repeats.

    Example:
        >>> _languages(["eng", "und", "ENG", "fr"])
        ('eng', 'fr')


    :param values: Iterable of codes or None.
    :return: Lower-case codes in first-seen order.
    """
    codes: list[str] = []
    for value in _names(values):
        code = value.lower()
        if code not in _UNDETERMINED_LANGUAGES and code not in codes:
            codes.append(code)
    return tuple(codes)


def _publisher(value: Any) -> str | None:
    """
    Clean a publisher name, dropping placeholders.

    Example:
        >>> _publisher("Unknown") is None
        True


    :param value: Publisher text or None.
    :return: Cleaned name, or None.
    """
    names = _names(value)
    return names[0] if names else None


def _identifiers(value: Any) -> tuple[tuple[str, str], ...]:
    """
    Keep identifiers that could identify a Work, sorted by scheme.

    Example:
        >>> _identifiers({"ISBN": "978-0-306-40615-7", "uri": "urn:uuid:1", "oclc": ""})
        (('isbn', '978-0-306-40615-7'),)


    :param value: Mapping of scheme to value, or None.
    :return: ``(scheme, value)`` pairs with lower-case schemes.
    """
    if not isinstance(value, Mapping):
        return ()
    pairs = []
    for scheme, raw in value.items():
        key = _clean(scheme)
        text = _clean(raw)
        if key and text and key.lower() not in _RECORD_IDENTIFIER_SCHEMES:
            pairs.append((key.lower(), text))
    return tuple(sorted(pairs))


def _series_index(value: Any) -> float | None:
    """
    Read a series position as a number.

    Example:
        >>> _series_index("2.5"), _series_index("x")
        (2.5, None)


    :param value: Reader value.
    :return: The position, or None when absent or not numeric.
    """
    try:
        return None if value is None else float(value)
    except TypeError, ValueError:
        return None


def _cover(value: Any) -> tuple[str, int] | None:
    """
    Describe an embedded cover by format and size, without keeping its bytes.

    Example:
        >>> _cover(("jpeg", b"1234")), _cover((None, None))
        (('jpeg', 4), None)


    :param value: Calibre ``cover_data`` pair or None.
    :return: ``(format, byte count)``, or None when there is no cover.
    """
    if not isinstance(value, tuple) or len(value) != 2 or not value[1]:
        return None
    return (str(value[0] or "unknown").lower(), len(value[1]))


def _title_from_file_name(file: IngestFile) -> str:
    """
    Derive a title from a file name, as calibre does when none is embedded.

    Example:
        >>> _title_from_file_name(IngestFile(1, "x/The_Last_Man.epub", "epub"))
        'The Last Man'


    :param file: File whose name to use.
    :return: The stem with underscores read as spaces.
    """
    return _clean(file.stem.replace("_", " ")) or file.storage_key


def _local_date(value: datetime) -> date:
    """
    Return a datetime's calendar date in the zone the readers used to parse it.

    Readers turn a date written without a zone into local midnight using
    LiuXin's safe local zone, which treats dates before 1970 as UTC. Converting
    back through the same zone recovers the written date; Python's own local
    conversion would apply historical mean time and can move the day.

    Example:
        >>> from datetime import datetime, timezone
        >>> _local_date(datetime(1818, 1, 1, tzinfo=timezone.utc))
        datetime.date(1818, 1, 1)


    :param value: Datetime from a reader.
    :return: Its calendar date; the plain date if conversion is impossible.
    """
    from LiuXin_alpha.utils.date import as_local_time

    if value.tzinfo is None:
        return value.date()
    try:
        return as_local_time(value).date()
    except OverflowError, OSError, ValueError:
        return value.date()


__all__ = [
    "DEFAULT_MAX_BYTES",
    "MetadataReader",
    "StreamOpener",
    "calendar_date",
    "normalise",
    "read_file",
    "select_files",
]
