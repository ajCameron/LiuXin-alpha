"""Unit coverage for immutable column presentation options."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from LiuXin_alpha.databases.column_metadata import (
    column_options_from_json,
    column_options_to_json,
    default_column_metadata,
    freeze_column_options,
)


def test_column_metadata_presentation_options_are_deeply_immutable() -> None:
    metadata = replace(
        default_column_metadata("works", "work_title"),
        formatting_options={
            "template": "{value}",
            "fallbacks": ["plain", "compact"],
            "nested": {"empty_value": "—"},
        },
        display_options={
            "label": "Tïtle 🚀",
            "visible": True,
            "width": 42,
        },
    )

    assert metadata.formatting_options["fallbacks"] == ("plain", "compact")
    assert metadata.formatting_options["nested"] == {"empty_value": "—"}
    assert metadata.display_options == {
        "label": "Tïtle 🚀",
        "visible": True,
        "width": 42,
    }

    with pytest.raises(TypeError):
        cast_options = metadata.formatting_options  # keep the mutation explicit
        cast_options["template"] = "changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        nested = metadata.formatting_options["nested"]
        assert isinstance(nested, dict) is False
        nested["empty_value"] = "changed"  # type: ignore[index]


def test_column_options_json_roundtrip_is_stable_and_unicode_safe() -> None:
    options = {
        "label": "Tïtle 🚀",
        "rules": [{"visible": True}, {"width": 42}],
        "empty_value": None,
    }

    encoded = column_options_to_json(options)
    decoded = column_options_from_json(encoded, field_name="display_options")

    assert encoded == (
        '{"empty_value":null,"label":"Tïtle 🚀",'
        '"rules":[{"visible":true},{"width":42}]}'
    )
    assert column_options_to_json(decoded) == encoded
    assert decoded["label"] == options["label"]
    assert decoded["empty_value"] is None
    assert decoded["rules"] == (
        {"visible": True},
        {"width": 42},
    )


@pytest.mark.parametrize(
    "options, error",
    [
        ({1: "bad"}, "keys must be strings"),
        ({"bad": {1, 2}}, "unsupported value type"),
        ({"bad": float("nan")}, "NaN or infinity"),
        ({"bad": float("inf")}, "NaN or infinity"),
    ],
)
def test_column_options_reject_non_json_values(
    options: Any,
    error: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=error):
        freeze_column_options(options, field_name="display_options")


def test_column_options_json_requires_an_object_root() -> None:
    with pytest.raises(ValueError, match="must contain an object"):
        column_options_from_json('["not", "an", "object"]')
