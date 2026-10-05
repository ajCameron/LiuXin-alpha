#!/usr/bin/env python2
# vim:fileencoding=utf-8
# License: GPLv3 Copyright: 2015, Kovid Goyal <kovid at kovidgoyal.net>

"""
Provide Qt-backed image decoding, scaling, conversion and cover-processing helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise img through a consuming regression::

        python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function

import os
import subprocess
import errno
import shutil
import tempfile
import sys
from io import BytesIO
from threading import Thread

from PyQt5.Qt import (
    QImage,
    QByteArray,
    QBuffer,
    Qt,
    QImageReader,
    QColor,
    QImageWriter,
    QTransform,
)

from LiuXin_alpha.constants import iswindows, get_version

from LiuXin_alpha.preferences import preferences as tweaks

from LiuXin_alpha.utils.calibre import fit_image, force_unicode
from LiuXin_alpha.utils.filenames import atomic_rename
from LiuXin_alpha.utils.file_ops.file_ops import local_open as lopen
from LiuXin_alpha.utils.plugins import plugins

from past.builtins import basestring

# Utilities {{{
imageops, imageops_err = plugins["imageops"]
if imageops is None:
    if "*" in get_version():
        raise RuntimeError(
            "You are running from source, which requires the new binary module, imageops. You can"
            " get it by installing the betas from: "
            "http://www.mobileread.com/forums/showthread.php?t=274030"
        )
    raise RuntimeError(imageops_err)


class NotImage(ValueError):
    """
    Provide the NotImage utility contract with explicit state and cleanup behavior.

    Example:
        Exercise NotImage through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py
    """
    pass


def normalize_format_name(fmt):
    """
    Returns the format name, lowercased, and standardizes jpg & jpeg to jpeg

    Example:
        Exercise normalize format name through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fmt = fmt.lower()
    if fmt == "jpg":
        fmt = "jpeg"
    return fmt


def get_exe_path(name):
    """
    Returns the path to an executable for the given name.

    Example:
        Exercise get exe path through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.pdf.pdftohtml import PDFTOHTML

    base = os.path.dirname(PDFTOHTML)
    if iswindows:
        name += "-calibre.exe"
    if not base:
        return name
    return os.path.join(base, name)


# }}}

# Loading images {{{


def null_image():
    """
    Create an invalid image. For internal use.

    Example:
        Exercise null image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return QImage()


def image_from_data(data):
    """
    Create an image object from data, which should be a bytestring.

    Example:
        Exercise image from data through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(data, QImage):
        return data
    i = QImage()
    if not i.loadFromData(data):
        raise NotImage("Not a valid image")
    return i


def image_from_path(path):
    """
    Load an image from the specified path.

    Example:
        Exercise image from path through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    with lopen(path, "rb") as f:
        return image_from_data(f.read())


def image_from_x(x):
    """
    Create an image from a bytestring or a path or a file like object.

    Example:
        Exercise image from x through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, type("")):
        return image_from_path(x)
    if hasattr(x, "read"):
        return image_from_data(x.read())
    if isinstance(x, (bytes, QImage)):
        return image_from_data(x)
    if isinstance(x, bytearray):
        return image_from_data(bytes(x))
    raise TypeError("Unknown image src type: %s" % type(x))


def image_and_format_from_data(data):
    """
    Create an image object from the specified data which should be a bytsestring. Also return the format of the image

    Example:
        Exercise image and format from data through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ba = QByteArray(data)
    buf = QBuffer(ba)
    buf.open(QBuffer.ReadOnly)
    r = QImageReader(buf)
    fmt = bytes(r.format()).decode("utf-8")
    return r.read(), fmt


# }}}

# Saving images {{{


def image_to_data(
    img,
    compression_quality=95,
    fmt="JPEG",
    png_compression_level=9,
    jpeg_optimized=True,
    jpeg_progressive=False,
):
    """
    Serialize image to bytestring in the specified format.

    Example:
        Exercise image to data through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param compression_quality: Value supplied for compression quality under the utility
        contract.
    :param fmt: Date, number or template format specification.
    :param png_compression_level: Value supplied for png compression level under the
        utility contract.
    :param jpeg_optimized: Value supplied for jpeg optimized under the utility contract.
    :param jpeg_progressive: Value supplied for jpeg progressive under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fmt = fmt.upper()
    ba = QByteArray()
    buf = QBuffer(ba)
    buf.open(QBuffer.WriteOnly)
    if fmt == "GIF":
        w = QImageWriter(buf, b"PNG")
        w.setQuality(90)
        if not w.write(img):
            raise ValueError("Failed to export image as " + fmt + " with error: " + w.errorString())
        from PIL import Image

        im = Image.open(BytesIO(ba.data()))
        buf = BytesIO()
        im.save(buf, "gif")
        return buf.getvalue()
    is_jpeg = fmt in ("JPG", "JPEG")
    w = QImageWriter(buf, fmt.encode("ascii"))

    if is_jpeg:
        if img.hasAlphaChannel():
            img = blend_image(img)
        # QImageWriter only gained the following options in Qt 5.5
        if jpeg_optimized and hasattr(QImageWriter, "setOptimizedWrite"):
            w.setOptimizedWrite(True)
        if jpeg_progressive and hasattr(QImageWriter, "setProgressiveScanWrite"):
            w.setProgressiveScanWrite(True)
        w.setQuality(compression_quality)
    elif fmt == "PNG":
        cl = min(9, max(0, png_compression_level))
        w.setQuality(10 * (9 - cl))
    if not w.write(img):
        raise ValueError("Failed to export image as " + fmt + " with error: " + w.errorString())
    return ba.data()


def save_image(img, path, **kw):
    """
    Save image to the specified path. Image format is taken from the file extension. You can pass the same keyword arguments as for the `image_to_data()` function.

    Example:
        Exercise save image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param kw: Value supplied for kw under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fmt = path.rpartition(".")[-1]
    kw["fmt"] = kw.get("fmt", fmt)
    with lopen(path, "wb") as f:
        f.write(image_to_data(image_from_data(img), **kw))


def save_cover_data_to(
    data,
    path=None,
    bgcolor="#ffffff",
    resize_to=None,
    compression_quality=90,
    minify_to=None,
    grayscale=False,
    data_fmt="jpeg",
):
    """
    Saves image in data to path, in the format specified by the path extension. Removes any transparency. If there is no transparency and no resize and the input and output image formats are the same, no changes are made.

    Example:
        Exercise save cover data to through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :param resize_to: Value supplied for resize to under the utility contract.
    :param compression_quality: Value supplied for compression quality under the utility
        contract.
    :param minify_to: Value supplied for minify to under the utility contract.
    :param grayscale: Value supplied for grayscale under the utility contract.
    :param data_fmt: Value supplied for data fmt under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    img, fmt = image_and_format_from_data(data)
    orig_fmt = normalize_format_name(fmt)
    fmt = normalize_format_name(data_fmt if path is None else os.path.splitext(path)[1][1:])
    changed = fmt != orig_fmt
    if resize_to is not None:
        changed = True
        img = img.scaled(resize_to[0], resize_to[1], Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
    owidth, oheight = img.width(), img.height()
    nwidth, nheight = tweaks["maximum_cover_size"] if minify_to is None else minify_to
    scaled, nwidth, nheight = fit_image(owidth, oheight, nwidth, nheight)
    if scaled:
        changed = True
        img = img.scaled(nwidth, nheight, Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
    if img.hasAlphaChannel():
        changed = True
        img = blend_image(img, bgcolor)
    if grayscale:
        if not img.allGray():
            changed = True
            img = grayscale_image(img)
    if path is None:
        return image_to_data(img, compression_quality, fmt) if changed else data
    with lopen(path, "wb") as f:
        f.write(image_to_data(img, compression_quality, fmt) if changed else data)


# }}}

# Overlaying images {{{


def blend_on_canvas(img, width, height, bgcolor="#ffffff"):
    """
    Blend the `img` onto a canvas with the specified background color and size

    Example:
        Exercise blend on canvas through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    w, h = img.width(), img.height()
    scaled, nw, nh = fit_image(w, h, width, height)
    if scaled:
        img = img.scaled(nw, nh, Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
        w, h = nw, nh
    canvas = QImage(width, height, QImage.Format_RGB32)
    canvas.fill(QColor(bgcolor))
    overlay_image(img, canvas, (width - w) // 2, (height - h) // 2)
    return canvas


class Canvas(object):
    """
    Provide the Canvas utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Canvas through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py
    """
    def __init__(self, width, height, bgcolor="#ffffff"):
        """
        Initialize and validate the Canvas state.

        Example:
            Exercise Canvas.  init   through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param bgcolor: Value supplied for bgcolor under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.img = QImage(width, height, QImage.Format_RGB32)
        self.img.fill(QColor(bgcolor))

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise Canvas.  enter   through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    def __exit__(self, *args):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise Canvas.  exit   through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def compose(self, img, x=0, y=0):
        """
        Perform the compose utility operation under explicit compatibility rules.

        Example:
            Exercise Canvas.compose through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param img: Value supplied for img under the utility contract.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        img = image_from_data(img)
        overlay_image(img, self.img, x, y)

    def export(self, fmt="JPEG", compression_quality=95):
        """
        Perform the export utility operation under explicit compatibility rules.

        Example:
            Exercise Canvas.export through a consuming regression::

                python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


        :param fmt: Date, number or template format specification.
        :param compression_quality: Value supplied for compression quality under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return image_to_data(self.img, compression_quality=compression_quality, fmt=fmt)


def create_canvas(width, height, bgcolor="#ffffff"):
    """
    Create a blank canvas of the specified size and color.

    Example:
        Exercise create canvas through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    img = QImage(width, height, QImage.Format_RGB32)
    img.fill(QColor(bgcolor))
    return img


def overlay_image(img, canvas=None, left=0, top=0):
    """
    Overlay the `img` onto the canvas at the specified position.

    Example:
        Exercise overlay image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param canvas: Value supplied for canvas under the utility contract.
    :param left: Value supplied for left under the utility contract.
    :param top: Value supplied for top under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if canvas is None:
        canvas = QImage(img.size(), QImage.Format_RGB32)
        canvas.fill(Qt.white)
    left, top = int(left), int(top)
    imageops.overlay(img, canvas, left, top)
    return canvas


def texture_image(canvas, texture):
    """
    Repeatedly tile the image `texture` across and down the image `canvas`

    Example:
        Exercise texture image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param canvas: Value supplied for canvas under the utility contract.
    :param texture: Value supplied for texture under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if canvas.hasAlphaChannel():
        canvas = blend_image(canvas)
    return imageops.texture_image(canvas, texture)


def blend_image(img, bgcolor="#ffffff"):
    """
    Used to convert images that have semi-transparent pixels to opaque by blending with the specified color

    Example:
        Exercise blend image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param bgcolor: Value supplied for bgcolor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    canvas = QImage(img.size(), QImage.Format_RGB32)
    canvas.fill(QColor(bgcolor))
    overlay_image(img, canvas)
    return canvas


# }}}

# Image borders {{{


def add_borders_to_image(img, left=0, top=0, right=0, bottom=0, border_color="#ffffff"):
    """
    Add a border around an image. Border will be a solid colour.

    Example:
        Exercise add borders to image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param left: Value supplied for left under the utility contract.
    :param top: Value supplied for top under the utility contract.
    :param right: Value supplied for right under the utility contract.
    :param bottom: Value supplied for bottom under the utility contract.
    :param border_color: Value supplied for border color under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    img = image_from_data(img)
    if not (left > 0 or right > 0 or top > 0 or bottom > 0):
        return img
    canvas = QImage(img.width() + left + right, img.height() + top + bottom, QImage.Format_RGB32)
    canvas.fill(QColor(border_color))
    overlay_image(img, canvas, left, top)
    return canvas


def remove_borders_from_image(img, fuzz=None):
    """
    Try to auto-detect and remove any borders from the image. Returns the image itself if no borders could be removed. `fuzz` is a measure of what colors are considered identical (must be a number between 0 and 255 in absolute intensity units). Default is from a tweak whose default value is 10.

    Example:
        Exercise remove borders from image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param fuzz: Value supplied for fuzz under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        fuzz = tweaks["cover_trim_fuzz_value"] if fuzz is None else fuzz
    except KeyError:
        fuzz = 10
    img = image_from_data(img)
    ans = imageops.remove_borders(img, max(0, fuzz))
    return ans if ans.size() != img.size() else img


# }}}

# Cropping/scaling of images {{{


def resize_image(img, width, height):
    """
    Resize an image to the given width and height.

    Example:
        Exercise resize image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return img.scaled(int(width), int(height), Qt.IgnoreAspectRatio, Qt.SmoothTransformation)


def resize_to_fit(img, width, height):
    """
    Perform the resize to fit utility operation under explicit compatibility rules.

    Example:
        Exercise resize to fit through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    img = image_from_data(img)
    resize_needed, nw, nh = fit_image(img.width(), img.height(), width, height)
    if resize_needed:
        resize_image(img, nw, nh)
    return resize_needed, img


def clone_image(img):
    """
    Returns a shallow copy of the image. However, the underlying data buffer will be automatically copied-on-write

    Example:
        Exercise clone image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return QImage(img)


def scale_image(
    data,
    width=60,
    height=80,
    compression_quality=70,
    as_png=False,
    preserve_aspect_ratio=True,
):
    """
    Scale an image, returning it as either JPEG or PNG data (bytestring). Transparency is alpha blended with white when converting to JPEG. Is thread safe and does not require a QApplication.

    Example:
        Exercise scale image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param data: Value supplied for data under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param compression_quality: Value supplied for compression quality under the utility
        contract.
    :param as_png: Value supplied for as png under the utility contract.
    :param preserve_aspect_ratio: Value supplied for preserve aspect ratio under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    # We use Qt instead of ImageMagick here because ImageMagick seems to use
    # some kind of memory pool, causing memory consumption to sky rocket.
    img = image_from_data(data)
    if preserve_aspect_ratio:
        scaled, nwidth, nheight = fit_image(img.width(), img.height(), width, height)
        if scaled:
            img = img.scaled(nwidth, nheight, Qt.KeepAspectRatio, Qt.SmoothTransformation)
    else:
        if img.width() != width or img.height() != height:
            img = img.scaled(width, height, Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
    fmt = "PNG" if as_png else "JPEG"
    w, h = img.width(), img.height()
    return w, h, image_to_data(img, compression_quality=compression_quality, fmt=fmt)


def crop_image(img, x, y, width, height):
    """
    Return the specified section of the image.

    Example:
        Exercise crop image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param x: Value supplied for x under the utility contract.
    :param y: Value supplied for y under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    img = image_from_data(img)
    width = min(width, img.width() - x)
    height = min(height, img.height() - y)
    return img.copy(x, y, width, height)


# }}}

# Image transformations {{{


def grayscale_image(img):
    """
    Return an image as a greyscale (useful for compatability with some older ebook readers).

    Example:
        Exercise grayscale image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return imageops.grayscale(image_from_data(img))


def set_image_opacity(img, alpha=0.5):
    """
    Change the opacity of `img`. Note that the alpha value is multiplied to any existing alpha values, so you cannot use this function to convert a semi-transparent image to an opaque one. For that use `blend_image()`

    Example:
        Exercise set image opacity through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param alpha: Value supplied for alpha under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return imageops.set_opacity(image_from_data(img), alpha)


def flip_image(img, horizontal=False, vertical=False):
    """
    Flip an image through horizontal and/or verticle.

    Example:
        Exercise flip image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param horizontal: Value supplied for horizontal under the utility contract.
    :param vertical: Value supplied for vertical under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return image_from_data(img).mirrored(horizontal, vertical)


def image_has_transparent_pixels(img):
    """
    Return True iff the image has at least one semi-transparent pixel

    Example:
        Exercise image has transparent pixels through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    img = image_from_data(img)
    if img.isNull():
        return False
    return imageops.has_transparent_pixels(img)


def rotate_image(img, degrees):
    """
    Use a QTransform method to rotate the image.

    Example:
        Exercise rotate image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param degrees: Value supplied for degrees under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    t = QTransform()
    t.rotate(degrees)
    return image_from_data(img).transformed(t)


def gaussian_sharpen_image(img, radius=0, sigma=3, high_quality=True):
    """
    Preform a Gaussian sharpen

    Example:
        Exercise gaussian sharpen image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param sigma: Value supplied for sigma under the utility contract.
    :param high_quality: Value supplied for high quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return imageops.gaussian_sharpen(image_from_data(img), max(0, radius), sigma, high_quality)


def gaussian_blur_image(img, radius=-1, sigma=3):
    """
    Preform a Gaussian blur on the image.

    Example:
        Exercise gaussian blur image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param sigma: Value supplied for sigma under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return imageops.gaussian_blur(image_from_data(img), max(0, radius), sigma)


def despeckle_image(img):
    """
    Do noise reduction on the image.

    Example:
        Exercise despeckle image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return imageops.despeckle(image_from_data(img))


def oil_paint_image(img, radius=-1, high_quality=True):
    """
    Perform the oil paint image utility operation under explicit compatibility rules.

    Example:
        Exercise oil paint image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param radius: Value supplied for radius under the utility contract.
    :param high_quality: Value supplied for high quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return imageops.oil_paint(image_from_data(img), radius, high_quality)


def normalize_image(img):
    """
    Normalize image under the documented compatibility and safety rules.

    Example:
        Exercise normalize image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return imageops.normalize(image_from_data(img))


def quantize_image(img, max_colors=256, dither=True, palette=""):
    """
    Quantize the image to contain a maximum of `max_colors` colors. By default a palette is chosen automatically, if you want to use a fixed palette, then pass in a list of color names in the `palette` variable. If you, specify a palette `max_colors` is ignored. Note that it is possible for the actual number of colors used to be less than max_colors.

    Example:
        Exercise quantize image through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param img: Value supplied for img under the utility contract.
    :param max_colors: Value supplied for max colors under the utility contract.
    :param dither: Value supplied for dither under the utility contract.
    :param palette: Value supplied for palette under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    img = image_from_data(img)
    if img.hasAlphaChannel():
        img = blend_image(img)
    if palette and isinstance(palette, basestring):
        palette = palette.split()
    return imageops.quantize(img, max_colors, dither, [QColor(x).rgb() for x in palette])


# }}}

# Optimization of images {{{


def run_optimizer(file_path, cmd, as_filter=False, input_data=None):
    """
    Backend for the optimizer which runs the command on the actaul program. DO NOT USE UNLESS YOU KNOW WHAT YOU'RE DOING.

    Example:
        Exercise run optimizer through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param file_path: Value supplied for file path under the utility contract.
    :param cmd: Value supplied for cmd under the utility contract.
    :param as_filter: Value supplied for as filter under the utility contract.
    :param input_data: Value supplied for input data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    file_path = os.path.abspath(file_path)
    cwd = os.path.dirname(file_path)
    ext = os.path.splitext(file_path)[1]
    if not ext or len(ext) > 10 or not ext.startswith("."):
        ext = ".jpg"
    fd, outfile = tempfile.mkstemp(dir=cwd, suffix=ext)
    try:
        if as_filter:
            outf = os.fdopen(fd, "wb")
        else:
            os.close(fd)
        iname, oname = os.path.basename(file_path), os.path.basename(outfile)

        def repl(q, r):
            """
            Perform the repl utility operation under explicit compatibility rules.

            Example:
                Exercise run optimizer.repl through a consuming regression::

                    python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


            :param q: Value supplied for q under the utility contract.
            :param r: Value supplied for r under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            cmd[cmd.index(q)] = r

        if not as_filter:
            repl(True, iname), repl(False, oname)
        if iswindows:
            # subprocess in python 2 cannot handle unicode strings that are not
            # encodeable in mbcs, so we fail here, where it is more explicit,
            # instead.
            cmd = [x.encode("mbcs") if isinstance(x, type("")) else x for x in cmd]
            if isinstance(cwd, type("")):
                cwd = cwd.encode("mbcs")
        stdin = subprocess.PIPE if as_filter else None
        stderr = subprocess.PIPE if as_filter else subprocess.STDOUT
        creationflags = 0x08 if iswindows else 0
        p = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=stderr,
            stdin=stdin,
            creationflags=creationflags,
        )
        stderr = p.stderr if as_filter else p.stdout
        if as_filter:
            src = input_data or open(file_path, "rb")

            def copy(src, dest):
                """
                Perform the copy utility operation under explicit compatibility rules.

                Example:
                    Exercise run optimizer.copy through a consuming regression::

                        python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


                :param src: Value supplied for src under the utility contract.
                :param dest: Value supplied for dest under the utility contract.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
                try:
                    shutil.copyfileobj(src, dest)
                finally:
                    src.close(), dest.close()

            inw = Thread(name="CopyInput", target=copy, args=(src, p.stdin))
            inw.daemon = True
            inw.start()
            outw = Thread(name="CopyOutput", target=copy, args=(p.stdout, outf))
            outw.daemon = True
            outw.start()
        raw = force_unicode(stderr.read())
        if p.wait() != 0:
            return raw
        else:
            if as_filter:
                outw.join(60.0), inw.join(60.0)
            try:
                sz = os.path.getsize(outfile)
            except EnvironmentError:
                sz = 0
            if sz < 1:
                return "%s returned a zero size image" % cmd[0]
            shutil.copystat(file_path, outfile)
            atomic_rename(outfile, file_path)
    finally:
        try:
            os.remove(outfile)
        except EnvironmentError as err:
            if err.errno != errno.ENOENT:
                raise
        try:
            os.remove(outfile + ".bak")  # optipng creates these files
        except EnvironmentError as err:
            if err.errno != errno.ENOENT:
                raise


def optimize_jpeg(file_path):
    """
    Perform the optimize jpeg utility operation under explicit compatibility rules.

    Example:
        Exercise optimize jpeg through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    exe = get_exe_path("jpegtran")
    cmd = [exe] + "-copy none -optimize -progressive -maxmemory 100M -outfile".split() + [False, True]
    return run_optimizer(file_path, cmd)


def optimize_png(file_path):
    """
    Perform the optimize png utility operation under explicit compatibility rules.

    Example:
        Exercise optimize png through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param file_path: Value supplied for file path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    exe = get_exe_path("optipng")
    cmd = [exe] + "-fix -clobber -strip all -o7 -out".split() + [False, True]
    return run_optimizer(file_path, cmd)


def encode_jpeg(file_path, quality=80):
    """
    Perform the encode jpeg utility operation under explicit compatibility rules.

    Example:
        Exercise encode jpeg through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :param file_path: Value supplied for file path under the utility contract.
    :param quality: Value supplied for quality under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from calibre.utils.speedups import ReadOnlyFileBuffer

    quality = max(0, min(100, int(quality)))
    exe = get_exe_path("cjpeg")
    cmd = [exe] + "-optimize -progressive -maxmemory 100M -quality".split() + [str(quality)]
    img = QImage()
    if not img.load(file_path):
        raise ValueError("%s is not a valid image file" % file_path)
    ba = QByteArray()
    buf = QBuffer(ba)
    buf.open(QBuffer.WriteOnly)
    if not img.save(buf, "PPM"):
        raise ValueError("Failed to export image to PPM")
    return run_optimizer(file_path, cmd, as_filter=True, input_data=ReadOnlyFileBuffer(ba.data()))


# }}}


def test():  # {{{
    """
    Perform the test utility operation under explicit compatibility rules.

    Example:
        Exercise test through a consuming regression::

            python -m pytest -q tests/utils/image_tools/test_img_pillow_fallback.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from calibre.ptempfile import TemporaryDirectory
    from calibre import CurrentDir
    from glob import glob

    with TemporaryDirectory() as tdir, CurrentDir(tdir):
        shutil.copyfile(I("devices/kindle.jpg", allow_user_override=False), "test.jpg")
        ret = optimize_jpeg("test.jpg")
        if ret is not None:
            raise SystemExit("optimize_jpeg failed: %s" % ret)
        ret = encode_jpeg("test.jpg")
        if ret is not None:
            raise SystemExit("encode_jpeg failed: %s" % ret)
        shutil.copyfile(I("lt.png"), "test.png")
        ret = optimize_png("test.png")
        if ret is not None:
            raise SystemExit("optimize_png failed: %s" % ret)
        if glob("*.bak"):
            raise SystemExit("Spurious .bak files left behind")
    img = image_from_data(I("devices/kindle.jpg", data=True, allow_user_override=False))
    quantize_image(img)
    oil_paint_image(img)
    gaussian_sharpen_image(img)
    gaussian_blur_image(img)
    despeckle_image(img)
    remove_borders_from_image(img)
    image_to_data(img, fmt="GIF")


# }}}


if __name__ == "__main__":  # {{{
    args = sys.argv[1:]
    infile = args.pop(0)
    img = image_from_data(lopen(infile, "rb").read())
    func = globals()[args[0]]
    kw = {}
    args.pop(0)
    outf = None
    while args:
        k = args.pop(0)
        if "=" in k:
            n, v = k.partition("=")[::2]
            if v in ("True", "False"):
                v = True if v == "True" else False
            try:
                v = int(v)
            except Exception:
                try:
                    v = float(v)
                except Exception:
                    pass
            kw[n] = v
        else:
            outf = k
    if outf is None:
        bn = os.path.basename(infile)
        outf = bn.rpartition(".")[0] + "." + "-output" + bn.rpartition(".")[-1]
    img = func(img, **kw)
    with lopen(outf, "wb") as f:
        f.write(image_to_data(img, fmt=outf.rpartition(".")[-1]))
# }}}
