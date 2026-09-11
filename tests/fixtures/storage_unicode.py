"""
Define exact Unicode keys, metadata text, and binary payloads for storage contracts.

The eight path cases retain normalization variants, case/script distinctions,
bidirectional controls, emoji/variation selectors, unusual scalars, significant
spaces, URL punctuation, and combining marks. Their flat-identifier counterparts
retain the same filenames and payloads without directory prefixes. These constants
are test data, not filenames normalized or validated for every host filesystem.

Shared title/author/path values combine several scripts and supplementary-plane
characters. UNICODE_PAYLOAD includes UTF-8 text followed by binary marker bytes.
POSIX_BAD_BYTES_FILENAME is decoded from raw directory-entry bytes with os.fsdecode
at import time; its representation depends on the host filesystem codec and error
handler. StoragePathCase.url_key uses strict URL encoding rather than the separate
surrogate-preserving file-URI reconstruction path in production storage.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote


# Keep NFC and NFD spellings in the same component so the test remains useful
# even on filesystems that normalize names instead of allowing two aliases.
UNICODE_FILENAME = (
    "Caf\u00e9-Cafe\u0301-\u6771\u4eac-\u4e66-\u0645\u0631\u062d\u0628\u0627-\u05e9\u05dc\u05d5\u05dd-"
    "\U0001f469\u200d\U0001f4bb-\U0001f4da-\U0001d11e-\U00010437.epub"
)
UNICODE_DIRECTORY = "Biblioth\u00e8que-\u56fe\u4e66\u9986-\u0645\u0643\u062a\u0628\u0629-\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4-\U0001f4da"
UNICODE_KEY = f"{UNICODE_DIRECTORY}/{UNICODE_FILENAME}"
UNICODE_URL_KEY = quote(UNICODE_KEY, safe="/")

UNICODE_TITLE = "Caf\u00e9 Cafe\u0301 \u6771\u4eac \u4e66 \u0645\u0631\u062d\u0628\u0627 \u05e9\u05dc\u05d5\u05dd \U0001f469\u200d\U0001f4bb \U0001f4da \U0001d11e \U00010437"
UNICODE_AUTHORS = ("Zo\u00eb \u674e\u96f7", "\u0623\u062d\u0645\u062f \u05e9\u05d9\u05e8\u05d4 \U0001f469\u200d\U0001f4bb")
UNICODE_PAYLOAD = (
    "LiuXin \u2014 Caf\u00e9 / Cafe\u0301 / \u6771\u4eac / \u4e66 / \u0645\u0631\u062d\u0628\u0627 / \u05e9\u05dc\u05d5\u05dd / "
    "\U0001f469\u200d\U0001f4bb / \U0001f4da / \U0001d11e / \U00010437"
).encode("utf-8") + b"\x00\xff\x80"


@dataclass(frozen=True, slots=True)
class StoragePathCase:
    """
    Hold a case label, exact opaque key, and expected bytes without normalization. This frozen
    dataclass performs no type, path-safety, encoding, or filename validation. Filename and URL
    properties are computed on access; retained fields are not deep-copied and the case does not
    create backend objects.

    Example:
        >>> case = StoragePathCase("spaces", "books/my book.epub", b"payload")
        >>> case.filename, case.url_key
        ('my book.epub', 'books/my%20book.epub')


    :ivar case_id: Descriptive pytest case label, retained without uniqueness validation.
    :ivar key: Exact Store key text, including significant Unicode distinctions and whitespace.
    :ivar payload: Expected bytes used by seeding, size, full-read, and range assertions.
    """

    case_id: str
    key: str
    payload: bytes

    @property
    def filename(self) -> str:
        """
        Take the final slash-delimited component of the retained key without path normalization.
        Empty keys or trailing slashes yield an empty filename; backslashes and whitespace remain
        literal characters.

        Example:
            >>> StoragePathCase("trailing", "books/", b"").filename
            ''


        :return: The substring after the last forward slash, unchanged.
        """
        return self.key.rsplit("/", 1)[-1]

    @property
    def url_key(self) -> str:
        """
        Percent-encode the key with slash separators left safe and quote's default UTF-8 strict
        encoding. No Unicode normalization occurs; literal percent signs and URI punctuation are
        escaped. Lone surrogate values can raise UnicodeEncodeError.

        Example:
            >>> StoragePathCase("punctuation", "books/100%#?.epub", b"").url_key
            'books/100%25%23%3F.epub'


        :return: URL-quoted key text preserving slash separators; strict encoding failures propagate.
        """
        return quote(self.key, safe="/")


# These values are all valid Unicode, but deliberately exercise distinctions
# that storage implementations are prone to erase or mis-encode. In
# particular, NFC and NFD spellings, case, bidi/format controls, variation
# selectors, non-BMP code points, and spaces are part of the opaque key.
TORTURED_UNICODE_PATH_CASES = (
    StoragePathCase(
        "normalization",
        "normalization/Caf\u00e9-Cafe\u0301-\u00c5-A\u030a.epub",
        b"normalization-payload",
    ),
    StoragePathCase(
        "case-and-scripts",
        "Case/\u0130-\u0131-\u00df-\u1e9e-\u03a3-\u03c2-\u0416-\u6771\u4eac.epub",
        b"case-and-scripts-payload",
    ),
    StoragePathCase(
        "bidi-and-format",
        "bidi/\u2067RTL-\u05e9\u05dc\u05d5\u05dd-\u2069-\u200eLTR-\u200f.epub",
        b"bidi-and-format-payload",
    ),
    StoragePathCase(
        "astral-emoji-variation",
        "astral/\U00020000-\U0001f469\u200d\U0001f4bb-\u2764\ufe0f-\U0001d11e.epub",
        b"astral-emoji-variation-payload",
    ),
    StoragePathCase(
        "unusual-scalars",
        "unusual/\ufdd0-\ufffc-\ufffd-\ue000.epub",
        b"unusual-scalars-payload",
    ),
    StoragePathCase(
        "significant-spacing",
        "spacing/ leading\u00a0name .epub ",
        b"significant-spacing-payload",
    ),
    StoragePathCase(
        "url-punctuation",
        "punctuation/100%-#-[draft]-semi;-question?.epub",
        b"url-punctuation-payload",
    ),
    StoragePathCase(
        "combining-storm",
        "combining/Z\u0351\u0357\u0300\u0316\u0342\u035c\u0321\u0349\u033d.epub",
        b"combining-storm-payload",
    ),
)

# SQLite keys are opaque but deliberately flat, so give it the same hostile
# filename components without implying hierarchical semantics.
TORTURED_UNICODE_IDENTIFIERS = tuple(
    StoragePathCase(case.case_id, case.filename, case.payload)
    for case in TORTURED_UNICODE_PATH_CASES
)

# POSIX presents undecodable directory-entry bytes as low surrogates via the
# surrogateescape handler. This is the representation storage APIs must retain
# when ingesting an old or incorrectly encoded local filesystem.
POSIX_BAD_BYTES_FILENAME_BYTES = b"bad-utf8-\xff-\x80-\xfe.epub"
POSIX_BAD_BYTES_FILENAME = os.fsdecode(POSIX_BAD_BYTES_FILENAME_BYTES)
POSIX_BAD_BYTES_PAYLOAD = b"payload from a filename containing invalid UTF-8"


__all__ = [
    "UNICODE_AUTHORS",
    "UNICODE_DIRECTORY",
    "UNICODE_FILENAME",
    "UNICODE_KEY",
    "UNICODE_PAYLOAD",
    "UNICODE_TITLE",
    "UNICODE_URL_KEY",
    "POSIX_BAD_BYTES_FILENAME",
    "POSIX_BAD_BYTES_FILENAME_BYTES",
    "POSIX_BAD_BYTES_PAYLOAD",
    "StoragePathCase",
    "TORTURED_UNICODE_IDENTIFIERS",
    "TORTURED_UNICODE_PATH_CASES",
]
