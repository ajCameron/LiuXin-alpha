"""Protect conversion-plugin ownership and compatibility boundaries."""

from __future__ import annotations


def test_generic_plugin_compatibility_export_preserves_identity() -> None:
    """The historical customize export is the extracted lifecycle class itself."""
    from LiuXin_alpha.customize import Plugin as CompatibilityPlugin
    from LiuXin_alpha.customize import PluginPreferences as CompatibilityPreferences
    from LiuXin_alpha.plugin_system.base import Plugin, PluginPreferences

    assert CompatibilityPlugin is Plugin
    assert CompatibilityPreferences is PluginPreferences


def test_conversion_compatibility_exports_preserve_identities() -> None:
    """Historical conversion imports resolve to the new canonical classes."""
    from LiuXin_alpha.customize.conversion import (
        ConversionOption as CompatibilityOption,
    )
    from LiuXin_alpha.customize.conversion import DummyReporter as CompatibilityReporter
    from LiuXin_alpha.customize.conversion import (
        InputFormatPlugin as CompatibilityInput,
    )
    from LiuXin_alpha.customize.conversion import (
        OptionRecommendation as CompatibilityRecommendation,
    )
    from LiuXin_alpha.customize.conversion import (
        OutputFormatPlugin as CompatibilityOutput,
    )
    from LiuXin_alpha.file_formats.conversion.plugin import (
        ConversionOption,
        DummyReporter,
        InputFormatPlugin,
        OptionRecommendation,
        OutputFormatPlugin,
    )

    assert CompatibilityOption is ConversionOption
    assert CompatibilityReporter is DummyReporter
    assert CompatibilityInput is InputFormatPlugin
    assert CompatibilityOutput is OutputFormatPlugin
    assert CompatibilityRecommendation is OptionRecommendation


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
