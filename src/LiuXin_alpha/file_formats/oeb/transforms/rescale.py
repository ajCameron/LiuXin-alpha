#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import with_statement
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.utils.calibre import fit_image

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class RescaleImages(object):
    """
    Rescale all images to fit inside given screen size
    """

    def __init__(self: _typing.Self, check_colorspaces: bool = False) -> None:
        self.check_colorspaces = check_colorspaces

    def __call__(self: _typing.Self, oeb: _typing.Any, opts: _typing.Any) -> None:
        self.oeb, self.opts, self.log = oeb, opts, oeb.log
        self.rescale()

    def rescale(self: _typing.Self) -> None:
        try:
            from LiuXin_alpha.utils.magick.draw import Image
        except ImportError:
            self.log.warn("ImageMagick bindings unavailable, skipping image rescale transform")
            return

        is_image_collection = getattr(self.opts, "is_image_collection", False)

        if is_image_collection:
            page_width, page_height = self.opts.dest.comic_screen_size
        else:
            page_width, page_height = self.opts.dest.width, self.opts.dest.height
            page_width -= (self.opts.margin_left + self.opts.margin_right) * self.opts.dest.dpi / 72.0
            page_height -= (self.opts.margin_top + self.opts.margin_bottom) * self.opts.dest.dpi / 72.0

        for item in self.oeb.manifest:
            if item.media_type.startswith("image"):
                ext = item.media_type.split("/")[-1].upper()
                if ext == "JPG":
                    ext = "JPEG"
                if ext not in ("PNG", "JPEG", "GIF"):
                    ext = "JPEG"

                raw = item.data
                if hasattr(raw, "xpath") or not raw:
                    # Probably an svg image
                    continue
                try:
                    img = Image()
                    img.load(raw)
                except:
                    continue
                width, height = img.size

                try:
                    if self.check_colorspaces and img.colorspace == "CMYKColorspace":
                        # We cannot do an imagemagick conversion of CMYK to RGB as
                        # ImageMagick inverts colors if you just set the colorspace
                        # to rgb. See for example: https://bugs.launchpad.net/bugs/1246710
                        from PyQt5.Qt import QImage
                        from LiuXin_alpha.surfaces.gui2 import pixmap_to_data

                        qimg = QImage()
                        qimg.loadFromData(raw)
                        if not qimg.isNull():
                            raw = item.data = pixmap_to_data(qimg, format=ext, quality=95)
                            img = Image()
                            img.load(raw)
                            self.log.warn(
                                "The image %s is in the CMYK colorspace, converting it "
                                "to RGB as Adobe Digital Editions cannot display CMYK" % item.href
                            )
                        else:
                            self.log.warn(
                                "The image %s is in the CMYK colorspace, you should convert"
                                " it to sRGB as Adobe Digital Editions cannot render CMYK" % item.href
                            )
                except Exception:
                    pass

                scaled, new_width, new_height = fit_image(width, height, page_width, page_height)
                if scaled:
                    new_width = max(1, new_width)
                    new_height = max(1, new_height)
                    self.log(
                        "Rescaling image from %dx%d to %dx%d" % (width, height, new_width, new_height),
                        item.href,
                    )
                    try:
                        img.size = (new_width, new_height)
                        data = img.export(ext.lower())
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        self.log.exception("Failed to rescale image - exception message: %s", str(e))
                    else:
                        item.data = data
                        item.unload_data_from_memory()
