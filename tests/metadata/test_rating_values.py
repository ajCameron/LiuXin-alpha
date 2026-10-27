"""Keep rating validation pure and explicit about nulls, fractions and scales."""

from decimal import Decimal

import pytest

from LiuXin_alpha.metadata.ratings import normalise_rating_pair


@pytest.mark.parametrize(
    ("value", "scale", "expected"),
    (
        (None, None, (None, None)),
        (None, 5, (None, 5)),
        (None, 2**63 - 1, (None, 2**63 - 1)),
        (0, 5, (0.0, 5)),
        (5, 5, (5.0, 5)),
        (" 4.25 ", "5.0", (4.25, 5)),
        (Decimal("3.9"), Decimal("10"), (3.9, 10)),
        (20, None, (20.0, None)),
    ),
)
def test_nullable_rating_pair(value, scale, expected):
    assert normalise_rating_pair(value, scale) == expected


@pytest.mark.parametrize(
    ("value", "scale"),
    (
        (-1, None),
        ("-1e-1000", None),
        ("5.0000000000000001", 5),
        (True, None),
        (float("nan"), 5),
        (float("inf"), None),
        ("NaN", None),
        ("1e1000000", None),
        ("", None),
        (object(), None),
        (6, 5),
        (None, True),
        (None, 0),
        (None, -1),
        (None, 5.5),
        (None, "5.0000000000000001"),
        (None, float("nan")),
        (None, float("inf")),
        (None, ""),
        (None, "1e1000000"),
        (None, 2**63),
    ),
)
def test_invalid_rating_pair(value, scale):
    with pytest.raises(ValueError):
        normalise_rating_pair(value, scale)
