"""
The fixed stages that transforms run in.

A transform declares one stage. Stages run in the order they are defined
here, and transforms within a stage run by priority. The lists are closed:
adding a stage is a change to this module, so the order of a conversion can
always be read from one place.

The OEB stages are read from what the conversion pipeline does today. The
first five are the order ``Plumber.run`` already uses; the sixth collects work
the output plugins currently do themselves.
"""

from __future__ import annotations

from enum import StrEnum


class OEBStage(StrEnum):
    """
    Name the stages an OEB book passes through between import and export.

    Example:
        >>> [stage.value for stage in OEBStage]
        ['normalise', 'structure', 'content', 'style', 'resources', 'package']
    """

    NORMALISE = "normalise"
    """Make the book consistent after import."""
    STRUCTURE = "structure"
    """Establish what the book is and how it is organised."""
    CONTENT = "content"
    """Rewrite text and markup."""
    STYLE = "style"
    """Resolve styling for the target."""
    RESOURCES = "resources"
    """Settle fonts, images and the manifest."""
    PACKAGE = "package"
    """Shape the book for one output format."""


class HTMLStage(StrEnum):
    """
    Name the stages an HTML book passes through between import and export.

    There is one stage to start with. More are added when a transform exists
    that does not fit.

    Example:
        >>> [stage.value for stage in HTMLStage]
        ['normalise']
    """

    NORMALISE = "normalise"
    """Make the book consistent after import, for example by re-encoding."""


def stage_position(stage: OEBStage | HTMLStage) -> int:
    """
    Give a stage's place in the run order of its own intermediate.

    Example:
        >>> stage_position(OEBStage.NORMALISE), stage_position(OEBStage.PACKAGE)
        (0, 5)


    :param stage: A stage of either intermediate.
    :return: Zero for the first stage to run, counting up in definition order.
    """
    return list(type(stage)).index(stage)
