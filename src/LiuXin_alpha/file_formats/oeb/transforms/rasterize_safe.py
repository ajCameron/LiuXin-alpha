from __future__ import with_statement

import re
from copy import deepcopy

try:
    import wand.color
    import wand.image
    from wand.api import library

    _HAS_WAND = True
except ModuleNotFoundError as err:
    wand = None
    library = None
    _HAS_WAND = False
    _WAND_IMPORT_ERROR = err

from LiuXin_alpha.file_formats.oeb.base import xml2str
from LiuXin_alpha.file_formats.oeb.transforms.rasterize import SVGRasterizer, Unavailable


# Todo: Get access to micorsoft word and make a docx file with a bunch of svgs in it. To test this mess.
class SVGRasterizerSafe(SVGRasterizer):
    """
    SVGRasterizer - without the reliance on PyQt
    """

    def __init__(self):
        if not _HAS_WAND:
            raise Unavailable("wand is unavailable for safe SVG rasterization")

    def rasterize_svg(self, elem, width=0, height=0, format="PNG"):
        """
        Do the actual work of rasterizing an svg into a sensible format.
        :param elem:
        :param width:
        :param height:
        :param format:
        :return:
        """
        # Handle the possibility that the SVG is contained within a viewbox... whatever that is
        view_box = elem.get("viewBox", elem.get("viewbox", None))
        sizes = None
        logger = self.oeb.logger

        if view_box is not None:
            try:
                box = [float(x) for x in filter(None, re.split("[, ]", view_box))]
                sizes = [box[2] - box[0], box[3] - box[1]]
            except (TypeError, ValueError, IndexError):
                logger.warn('SVG image has invalid viewBox="%s", ignoring the viewBox' % view_box)
            else:
                for image in elem.xpath(
                    'descendant::*[local-name()="image" and ' '@height and contains(@height, "%")]'
                ):
                    logger.info("Found SVG image height in %, trying to convert...")
                    try:
                        h = float(image.get("height").replace("%", "")) / 100.0
                        image.set("height", str(h * sizes[1]))
                    except:
                        logger.exception("Failed to convert percentage height:", image.get("height"))

        # https://stackoverflow.com/questions/6589358/convert-svg-to-png-in-python
        # Load the image - not sure if accounted for the background color correctly - try 'transparent' or 'white'
        if not _HAS_WAND:
            raise Unavailable("wand is unavailable for safe SVG rasterization")

        with wand.image.Image() as image:
            with wand.color.Color("white") as background_color:
                library.MagickSetBackgroundColor(image.wand, background_color.resource)
            image.read(blob=xml2str(elem, with_tail=False), format="svg")

            current_width = image.width
            current_height = image.height

            # If the size is 100 x 100 - and we're in a view box - scale the image to the size of the view box - which
            # is where it should be, instead of the default size set when it was put in the box
            if current_width == 100 and current_height == 100 and sizes:
                image.resize(sizes[0], sizes[1])
                current_width = sizes[0]
                current_height = sizes[1]

            if width or height:
                new_width, new_height = self.new_width_and_height(width, height, current_width, current_height)
                image.resize(new_width, new_height)
                current_width = new_width
                current_height = new_height

            logger.info("Rasterizing %r to %dx%d" % (elem, current_width, current_height))

            return str(image.make_blob(format.lower()))

    def new_width_and_height(self, width, height, old_width, old_height):
        """
        Given a desired final width or height works out the new width and height and returns them
        :param width:
        :param height:
        :param old_width:
        :param old_height:
        :return:
        """
        if width and height:
            return width, height

        elif width and not height:
            scale = float(width) / float(old_width)
            new_height = float(old_height) * scale
            return int(width), int(new_height)

        elif not width and height:

            scale = float(height) / float(old_height)
            new_width = float(old_width) * scale
            return int(new_width), int(height)

        elif not width and not height:
            return old_width, old_height

        else:
            raise NotImplementedError("This position should never be reached")

    def rasterize_external(self, elem, style, item, svgitem):
        raise NotImplementedError("No current way of dealing with an incoming svgitem")
