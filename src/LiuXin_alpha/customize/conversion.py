"""Compatibility surface for conversion contracts now owned by file formats.

New code imports :mod:`LiuXin_alpha.file_formats.conversion.plugin` directly.
This module preserves historical external imports without making format plugins
depend on the customization composition package.
"""

from LiuXin_alpha.file_formats.conversion.plugin import (
    ConversionOption,
    DummyReporter,
    InputFormatPlugin,
    OptionRecommendation,
    OutputFormatPlugin,
)

__all__ = (
    "ConversionOption",
    "DummyReporter",
    "InputFormatPlugin",
    "OptionRecommendation",
    "OutputFormatPlugin",
)
