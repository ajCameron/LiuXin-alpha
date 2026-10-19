"""Protect conversion-plugin ownership and option contracts."""

from __future__ import annotations


def test_concrete_converter_uses_file_format_contract() -> None:
    """A representative converter subclasses the new owner and generic base."""
    from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput
    from LiuXin_alpha.plugin_system.base import Plugin

    assert issubclass(EPUBInput, InputFormatPlugin)
    assert issubclass(EPUBInput, Plugin)


def test_conversion_option_behavior_survives_ownership_move() -> None:
    """Option validation, cloning, and recommendations retain their behavior."""
    import pytest

    from LiuXin_alpha.file_formats.conversion.plugin import (
        ConversionOption,
        OptionRecommendation,
    )

    option = ConversionOption(
        name="input_encoding",
        option_help="Select the source encoding",
        choices=("utf-8", "utf-16"),
    )
    cloned = option.clone()
    recommendation = OptionRecommendation(
        option=option,
        recommended_value="utf-8",
        level=OptionRecommendation.HIGH,
    )

    assert cloned is not option
    assert cloned == option
    assert recommendation.option is option
    assert recommendation.option_help == option.option_help

    with pytest.raises(ValueError, match="valid Python identifier"):
        ConversionOption(name="1invalid", option_help="Invalid")
