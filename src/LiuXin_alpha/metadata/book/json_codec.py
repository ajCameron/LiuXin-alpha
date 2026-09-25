"""
Encode device book lists as JSON-compatible metadata and restore their typed fields.

The codec preserves Unicode text, encodes thumbnail bytes, serializes dates, and
migrates custom-column multiplicity descriptors. Decode failures are logged and
contained; caller-owned files remain open.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/book/test_json_codec.py
"""
from __future__ import print_function

from base64 import b64encode, b64decode
import json
import logging
from datetime import datetime, time

from LiuXin_alpha.constants import filesystem_encoding, preferred_encoding

from LiuXin_alpha.catalog.field_metadata import FieldMetadata

from LiuXin_alpha.metadata.book import SERIALIZABLE_FIELDS

from LiuXin_alpha.utils.calibre import isbytestring

"""
Created on 4 Jun 2010
"""

logger = logging.getLogger(__name__)


# Translate datetimes to and from strings. The string form is the datetime in
# UTC. The returned date is also UTC
def string_to_datetime(src):
    """
    Parse a serialized date, mapping the None sentinel and parse failures to None.

    Example:
        >>> string_to_datetime('None') is None
        True


    :param src: Date string accepted by parse_date or the literal None string.
    :return: Parsed datetime or None.
    """
    from LiuXin_alpha.utils.date import parse_date

    if src != "None":
        try:
            return parse_date(src)
        except Exception:
            pass
    return None


def datetime_to_string(dateval):
    """
    Convert a date or datetime to the shared ISO representation.

    Dates become midnight and naive datetimes receive the local timezone. None and
    values at or before UNDEFINED_DATE become the literal None string; aware timezone
    formatting follows isoformat.

    Example:
        >>> datetime_to_string(None)
        'None'


    :param dateval: Date/datetime value or None.
    :return: ISO date string or the literal None string.
    """
    from LiuXin_alpha.utils.date import isoformat, UNDEFINED_DATE, local_tz

    if dateval is None:
        return "None"
    if not isinstance(dateval, datetime):
        dateval = datetime.combine(dateval, time())
    if hasattr(dateval, "tzinfo") and dateval.tzinfo is None:
        dateval = dateval.replace(tzinfo=local_tz)
    if dateval <= UNDEFINED_DATE:
        return "None"
    return isoformat(dateval)


def encode_thumbnail(thumbnail):
    """
    Encode thumbnail bytes as base64 while retaining image dimensions.

    A tuple/list supplies dimensions directly. Other inputs are loaded through the
    optional image wrapper to find their size; failure there returns None. Non-byte
    payloads are stringified using preferred_encoding with replacement.

    Example:
        >>> encode_thumbnail((2, 3, b'abc'))
        (2, 3, 'YWJj')


    :param thumbnail: None, (width, height, data), or image payload accepted by
        Image.load.
    :return: Width, height, ASCII base64 tuple, or None.
    """
    if thumbnail is None:
        return None
    if not isinstance(thumbnail, (tuple, list)):
        try:
            from LiuXin_alpha.utils.magick import Image

            img = Image()
            img.load(thumbnail)
            width, height = img.size
            thumbnail = (width, height, thumbnail)
        except:
            return None
    data = thumbnail[2]
    if not isinstance(data, (bytes, bytearray)):
        data = str(data).encode(preferred_encoding, "replace")
    return (thumbnail[0], thumbnail[1], b64encode(data).decode("ascii"))


def decode_thumbnail(tup):
    """
    Decode the data component of an encoded thumbnail tuple.

    Example:
        >>> decode_thumbnail((2, 3, 'YWJj'))
        (2, 3, b'abc')


    :param tup: Encoded three-part thumbnail or None; malformed inputs propagate errors.
    :return: Width, height, bytes tuple, or None.
    """
    if tup is None:
        return None
    return (tup[0], tup[1], b64decode(tup[2]))


def object_to_unicode(obj, enc=preferred_encoding):
    """
    Recursively decode bytes and bytearrays in metadata values.

    Tuples and lists become new lists; dictionaries have both keys and values converted.
    Other values are retained. Invalid encoded bytes use replacement characters.

    Example:
        >>> object_to_unicode({b'tags': (b'one', b'two')}, enc='utf-8')
        {'tags': ['one', 'two']}


    :param obj: Value or nested list, tuple, or dictionary.
    :param enc: Encoding for byte-like values, defaulting to preferred_encoding.
    :return: Converted structure or unchanged scalar.
    """

    def dec(x):
        """
        Decode one byte-like value using the captured encoding with replacement.

        Example:
            >>> object_to_unicode(b'caf' + bytes([233]), enc='latin-1')
            'café'


        :param x: Bytes or bytearray supporting decode.
        :return: Decoded string.
        """
        return x.decode(enc, "replace")

    if isinstance(obj, (bytes, bytearray)):
        return dec(obj)
    if isinstance(obj, (list, tuple)):
        return [object_to_unicode(x, enc=enc) for x in obj]
    if isinstance(obj, dict):
        ans = {}
        for k, v in obj.items():
            k = object_to_unicode(k, enc=enc)
            v = object_to_unicode(v, enc=enc)
            ans[k] = v
        return ans
    return obj


def encode_is_multiple(fm):
    """
    Add a legacy separator while retaining the modern multiplicity descriptor.

    Mutate the field mapping: truthy is_multiple moves by reference to is_multiple2,
    while is_multiple becomes comma for composites or vertical bar otherwise. False
    multiplicity becomes None plus an empty is_multiple2 dictionary.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_json_codec.py


    :param fm: Mutable custom-field descriptor.
    :return: None.
    """
    if fm.get("is_multiple", None):
        # migrate is_multiple back to a character
        fm["is_multiple2"] = fm.get("is_multiple", {})
        dt = fm.get("datatype", None)
        if dt == "composite":
            fm["is_multiple"] = ","
        else:
            fm["is_multiple"] = "|"
    else:
        fm["is_multiple"] = None
        fm["is_multiple2"] = {}


def decode_is_multiple(fm):
    """
    Restore modern multiplicity metadata in a decoded field descriptor.

    Prefer a truthy is_multiple2 and remove that key. Otherwise migrate a legacy
    separator using datatype and display.is_names, or turn None into an empty
    dictionary.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_json_codec.py


    :param fm: Mutable custom-field descriptor from JSON.
    :return: None.
    """
    im = fm.get("is_multiple2", None)
    if im:
        fm["is_multiple"] = im
        del fm["is_multiple2"]
    else:
        # Must migrate the is_multiple from char to dict
        im = fm.get("is_multiple", {})
        if im:
            dt = fm.get("datatype", None)
            if dt == "composite":
                im = {"cache_to_list": ",", "ui_to_list": ",", "list_to_ui": ", "}
            elif fm.get("display", {}).get("is_names", False):
                im = {"cache_to_list": "|", "ui_to_list": "&", "list_to_ui": ", "}
            else:
                im = {"cache_to_list": "|", "ui_to_list": ",", "list_to_ui": ", "}
        elif im is None:
            im = {}
        fm["is_multiple"] = im


class JsonCodec(object):
    """
    Translate device book metadata to JSON values and reconstruct caller-selected book classes.

    Encoding uses SERIALIZABLE_FIELDS. Decoding accepts the legacy classifiers name for
    identifiers and logs malformed book/file failures without rolling back books already
    appended.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_json_codec.py
    """
    def __init__(self):
        """
        Create a FieldMetadata registry for typed field conversion.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_json_codec.py


        :return: None.
        """
        self.field_metadata = FieldMetadata()

    def encode_to_file(self, file_, booklist):
        """
        Write the encoded book list as indented Unicode JSON.

        The stream is not flushed or closed here; serialization and write errors propagate.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_json_codec.py


        :param file_: Caller-owned writable text stream.
        :param booklist: Iterable of books supporting metadata accessors.
        :return: None.
        """
        file_.write(json.dumps(self.encode_booklist_metadata(booklist), indent=2, ensure_ascii=False))

    def encode_booklist_metadata(self, booklist):
        """
        Encode each book in order using the single-book codec.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_json_codec.py


        :param booklist: Iterable of metadata books.
        :return: New list of metadata dictionaries.
        """
        result = []
        for book in booklist:
            result.append(self.encode_book_metadata(book))
        return result

    def encode_book_metadata(self, book):
        """
        Encode every SERIALIZABLE_FIELDS entry from one book.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_json_codec.py


        :param book: Book exposing get and get_all_user_metadata.
        :return: Dictionary containing encoded values, including nulls.
        """
        result = {}
        for key in SERIALIZABLE_FIELDS:
            result[key] = self.encode_metadata_attr(book, key)
        return result

    def encode_metadata_attr(self, book, key):
        """
        Encode one metadata field using its descriptor and special field rules.

        Custom metadata is deep-copied before date and multiplicity conversion. Thumbnails
        become base64 triples, lpath bytes use filesystem_encoding, and remaining
        bytes/containers use preferred_encoding.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_json_codec.py


        :param book: Book exposing get and get_all_user_metadata.
        :param key: Metadata field name to encode.
        :return: JSON-compatible field value where supported; unrelated scalar objects pass
            through.
        """
        if key == "user_metadata":
            meta = book.get_all_user_metadata(make_copy=True)
            for fm in meta.values():
                if fm["datatype"] == "datetime":
                    fm["#value#"] = datetime_to_string(fm["#value#"])
                encode_is_multiple(fm)
            return meta
        if key in self.field_metadata:
            datatype = self.field_metadata[key]["datatype"]
        else:
            datatype = None
        value = book.get(key)
        if key == "thumbnail":
            return encode_thumbnail(value)
        elif isbytestring(value):  # str includes bytes
            enc = filesystem_encoding if key == "lpath" else preferred_encoding
            return object_to_unicode(value, enc=enc)
        elif datatype == "datetime":
            return datetime_to_string(value)
        else:
            return object_to_unicode(value)

    def decode_from_file(self, file_, booklist, book_class, prefix):
        """
        Load JSON and append each successfully reconstructed book to the supplied list.

        Malformed books are logged by raw_to_book and skipped. An outer read, parse,
        iteration, or append failure is logged and ends decoding; earlier appends are
        retained. The stream stays open.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_json_codec.py


        :param file_: Caller-owned readable text stream containing a JSON book list.
        :param booklist: Mutable destination supporting append.
        :param book_class: Book constructor accepting (prefix, lpath).
        :param prefix: Root/device prefix forwarded to the book constructor.
        :return: None.
        """
        js = []
        try:
            js = json.load(file_)
            for item in js:
                entry = self.raw_to_book(item, book_class, prefix)
                if entry is not None:
                    booklist.append(entry)
        except Exception:
            logger.exception("Exception during JSON decode_from_file")

    def raw_to_book(self, json_book, book_class, prefix):
        """
        Construct a book and populate its decoded fields, translating classifiers to identifiers.

        Custom metadata is installed with set_all_user_metadata. Construction or assignment
        failures are logged and return None; custom descriptors in the input may be mutated
        by decoding.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_json_codec.py


        :param json_book: Decoded JSON metadata mapping.
        :param book_class: Constructor called with prefix and the stored lpath or None.
        :param prefix: Root/device prefix passed to the constructor.
        :return: Populated book or None after a logged error.
        """
        try:
            book = book_class(prefix, json_book.get("lpath", None))
            for key, val in json_book.items():
                meta = self.decode_metadata(key, val)
                if key == "user_metadata":
                    book.set_all_user_metadata(meta)
                else:
                    if key == "classifiers":
                        key = "identifiers"
                    setattr(book, key, meta)
            return book
        except Exception:
            logger.exception("Exception during JSON decoding")

    def decode_metadata(self, key, value):
        """
        Restore custom descriptors, known date fields, and thumbnail bytes.

        Treat classifiers as identifiers for field dispatch. Custom metadata is mutated in
        place while dates and multiplicity descriptors are restored; other values pass
        through.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_json_codec.py


        :param key: Metadata field name, including the legacy classifiers alias.
        :param value: JSON-decoded field value.
        :return: Decoded field value, potentially the mutated input mapping.
        """
        if key == "classifiers":
            key = "identifiers"
        if key == "user_metadata":
            for fm in value.values():
                if fm["datatype"] == "datetime":
                    fm["#value#"] = string_to_datetime(fm["#value#"])
                decode_is_multiple(fm)
            return value
        elif key in self.field_metadata:
            if self.field_metadata[key]["datatype"] == "datetime":
                return string_to_datetime(value)
        if key == "thumbnail":
            return decode_thumbnail(value)
        return value
