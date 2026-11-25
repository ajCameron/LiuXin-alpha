"""
Choose how a MOBI book's cover is read: as the book holds it, or as calibre gives it.

calibre re-encodes every MOBI cover it reads, through Qt, as a JPEG no larger than
1650x2200 (its ``maximum_cover_size`` tweak), flattening any transparency onto
white. That suits older devices, but it loses quality and replaces the image the
book holds. The ``read_mobi_covers_as`` preference chooses:

``original`` (the default)
    The image the book holds, byte for byte, in its own format: what an archive
    keeps.
``calibre``
    calibre's cover. A JPEG already within 1650x2200 with no transparency is left
    as it is; anything else is re-encoded as a JPEG at quality 90, scaled down to
    fit. This needs Qt or Pillow; without them, or for an image neither can
    decode, the original is kept.

Both LiuXin's MOBI reader and the calibre port follow it.

Example:
    >>> png = b"\\x89PNG\\r\\n\\x1a\\n" + bytes(16)
    >>> read_mobi_cover(png, "original")
    ('png', b'\\x89PNG\\r\\n\\x1a\\n\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00')
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.utils.logging import default_log

__all__ = [
    "CALIBRE_MAXIMUM_COVER_SIZE",
    "DEFAULT_MOBI_COVER_READING",
    "MOBI_COVERS_PREFERENCE",
    "MOBI_COVER_READINGS",
    "mobi_cover_reading",
    "read_mobi_cover",
]

#: The preference that chooses the reading.
MOBI_COVERS_PREFERENCE = "read_mobi_covers_as"
#: The readings offered, with what each means.
MOBI_COVER_READINGS: Mapping[str, str] = {
    "original": "the image the book holds, unchanged",
    "calibre": "a JPEG no larger than 1650x2200, as calibre gives it",
}
DEFAULT_MOBI_COVER_READING = "original"
#: calibre's ``maximum_cover_size`` tweak.
CALIBRE_MAXIMUM_COVER_SIZE = (1650, 2200)


def mobi_cover_reading(explicit: str | None = None) -> str:
    """
    Choose a reading: the one asked for, else the preference, else ``original``.

    Example:
        >>> mobi_cover_reading("calibre"), mobi_cover_reading("sideways")
        ('calibre', 'original')


    :param explicit: A reading named by the caller.
    :return: A key of :data:`MOBI_COVER_READINGS`.
    """
    chosen: Any = explicit
    if chosen is None:
        try:
            from LiuXin_alpha.configuration import preferences

            chosen = preferences.get(MOBI_COVERS_PREFERENCE)
        except Exception:  # an unreadable store leaves the default
            chosen = None
    chosen = str(chosen or "").strip().lower()
    return chosen if chosen in MOBI_COVER_READINGS else DEFAULT_MOBI_COVER_READING


def read_mobi_cover(data: bytes, how: str | None = None) -> tuple[str, bytes]:
    """
    Give a MOBI book's cover image as the reading chooses, with its format.

    Example:
        See the module.


    :param data: The cover record's bytes.
    :param how: A key of :data:`MOBI_COVER_READINGS`; the preference when ``None``.
    :return: ``(format, bytes)`` for the record's ``cover_data``: ``jpg``, ``png``,
        ``gif`` and so on for the original, ``jpg`` for calibre's.
    """
    if mobi_cover_reading(how) == "calibre":
        try:
            from LiuXin_alpha.utils.image_tools.img import save_cover_data_to

            return "jpg", bytes(save_cover_data_to(data, minify_to=CALIBRE_MAXIMUM_COVER_SIZE))
        except Exception as err:  # no image library, or an image it cannot decode
            default_log.log_exception("Kept a MOBI cover as the book holds it.", err, "DEBUG")
    return _format_of(data), data


def _format_of(data: bytes) -> str:
    """
    Name an image's format from its bytes, as the record's ``cover_data`` names it.

    Example:
        >>> _format_of(b"\\xff\\xd8\\xff\\xe0" + bytes(8)), _format_of(b"GIF89a" + bytes(8)), _format_of(b"???")
        ('jpg', 'gif', 'jpg')


    :param data: The image.
    :return: ``jpg``, ``png``, ``gif``, ``bmp`` or ``webp``; ``jpg`` when the bytes say nothing.
    """
    from LiuXin_alpha.utils.image_tools.imghdr import what

    found = what(None, data)
    return "jpg" if found in (None, "jpeg") else str(found)
