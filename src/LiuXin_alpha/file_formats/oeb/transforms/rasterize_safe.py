"""
Rasterize supported resources with guarded fallbacks.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise rasterize safe through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

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

    Example:
        Exercise SVGRasterizerSafe through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the svgrasterizersafe state.

        Example:
            Exercise SVGRasterizerSafe.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        if not _HAS_WAND:
            raise Unavailable("wand is unavailable for safe SVG rasterization")

    def rasterize_svg(self: _typing.Self, elem: _typing.Any, width: int = 0, height: int = 0, format: str = "PNG") -> _typing.Any:
        """
        Do the actual work of rasterizing an svg into a sensible format.

        Example:
            Exercise SVGRasterizerSafe.rasterize svg through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param elem: Value supplied for elem under the utility contract.
        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param format: Value supplied for format under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

    def new_width_and_height(self: _typing.Self, width: _typing.Any, height: _typing.Any, old_width: _typing.Any, old_height: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Given a desired final width or height works out the new width and height and returns them

        Example:
            Exercise SVGRasterizerSafe.new width and height through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param old_width: Value supplied for old width under the utility contract.
        :param old_height: Value supplied for old height under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

    def rasterize_external(self: _typing.Self, elem: _typing.Any, style: _typing.Any, item: _typing.Any, svgitem: _typing.Any) -> None:
        """
        Perform the rasterize external operation under explicit file-format and conversion rules.

        Example:
            Exercise SVGRasterizerSafe.rasterize external through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param elem: Value supplied for elem under the utility contract.
        :param style: Value supplied for style under the utility contract.
        :param item: Value supplied for item under the utility contract.
        :param svgitem: Value supplied for svgitem under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("No current way of dealing with an incoming svgitem")
