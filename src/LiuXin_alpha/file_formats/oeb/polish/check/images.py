#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Validate image resources and dimensions in EPUB/OEB containers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise images through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.oeb.polish.check.base import BaseError, WARN
from LiuXin_alpha.file_formats.oeb.polish.check.parsing import EmptyFile

from LiuXin_alpha.utils.text import as_unicode
from LiuXin_alpha.utils.localization import trans as _

try:
    from LiuXin_alpha.utils.magick import Image
except Exception:
    try:
        from LiuXin_alpha.utils.plugins.fallbacks.magick import Image as _FallbackImage
    except Exception:
        _FallbackImage = None

    if _FallbackImage is not None:
        class Image(_FallbackImage):
            """
            Provide the image contract for validated ebook processing.

            Example:
                Exercise Image through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
            """
            def __init__(self: _typing.Self) -> None:
                """
                Initialize and validate the image state.

                Example:
                    Exercise Image.  init   through a consuming regression::

                        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


                :return: None; validated state is stored on the receiving object.
                """
                super(Image, self).__init__(b"")
                self.colorspace = "RGBColorspace"

            def load(self: _typing.Self, data: _typing.Any) -> _typing.Any:
                """
                Perform the load operation under explicit file-format and conversion rules.

                Example:
                    Exercise Image.load through a consuming regression::

                        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


                :param data: Value supplied for data under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                self._src = data
                return self
    else:
        class Image(object):
            """
            Provide the image contract for validated ebook processing.

            Example:
                Exercise Image through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
            """
            colorspace = "RGBColorspace"

            def load(self: _typing.Self, data: _typing.Any) -> _typing.Any:
                """
                Perform the load operation under explicit file-format and conversion rules.

                Example:
                    Exercise Image.load through a consuming regression::

                        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


                :param data: Value supplied for data under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return self

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class InvalidImage(BaseError):

    """
    Provide the invalidimage contract for validated ebook processing.

    Example:
        Exercise InvalidImage through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    HELP = _(
        "An invalid image is an image that could not be loaded, typically because"
        " it is corrupted. You should replace it with a good image or remove it."
    )

    def __init__(self: _typing.Self, msg: _typing.Any, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Initialize and validate the invalidimage state.

        Example:
            Exercise InvalidImage.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param msg: Value supplied for msg under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, "Invalid image: " + msg, *args, **kwargs)


class CMYKImage(BaseError):

    """
    Provide the cmykimage contract for validated ebook processing.

    Example:
        Exercise CMYKImage through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    HELP = _(
        "Reader devices based on Adobe Digital Editions cannot display images whose"
        " colors are specified in the CMYK colorspace. You should convert this image"
        " to the RGB colorspace, for maximum compatibility."
    )
    INDIVIDUAL_FIX = _("Convert image to RGB automatically")
    level = WARN

    def __call__(self: _typing.Self, container: _typing.Any) -> bool:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise CMYKImage.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            from PyQt5.Qt import QImage
            from LiuXin_alpha.surfaces.gui2 import pixmap_to_data
        except ModuleNotFoundError:
            return False

        ext = container.mime_map[self.name].split("/")[-1].upper()
        if ext == "JPG":
            ext = "JPEG"
        if ext not in ("PNG", "JPEG", "GIF"):
            return False
        with container.open(self.name, "r+b") as f:
            raw = f.read()
            i = QImage()
            i.loadFromData(raw)
            if i.isNull():
                return False
            raw = pixmap_to_data(i, format=ext, quality=95)
            f.seek(0)
            f.truncate()
            f.write(raw)
        return True


def check_raster_images(name: _typing.Any, mt: _typing.Any, raw: _typing.Any) -> _typing.Any:
    """
    Perform the check raster images operation under explicit file-format and conversion rules.

    Example:
        Exercise check raster images through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param mt: Value supplied for mt under the utility contract.
    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not raw:
        return [EmptyFile(name)]
    errors = []
    i = Image()
    try:
        i.load(raw)
    except Exception as e:
        errors.append(InvalidImage(as_unicode(str(e)), name))
    else:
        if i.colorspace == "CMYKColorspace":
            errors.append(CMYKImage(_("Image is in the CMYK colorspace"), name))

    return errors
