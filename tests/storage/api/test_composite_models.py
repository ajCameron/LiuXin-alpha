"""Validate Composite domain values at their construction boundary."""

from __future__ import annotations

import pytest

import LiuXin_alpha.storage.api as api


def _member() -> api.CompositeDigitalAssetMembership:
    """Return one valid required member for declaration and record tests."""
    return api.CompositeDigitalAssetMembership(api.DigitalAssetID(1), 0)


@pytest.mark.parametrize(
    ("arguments", "error_type", "message"),
    (
        ((True, 0), TypeError, "digital_asset_id"),
        ((1, 0.5), TypeError, "sequence_number"),
        ((1, 0, None, None, None, None, 1), TypeError, "required"),
        ((1, 0, 7), TypeError, "role"),
    ),
)
def test_composite_membership_rejects_malformed_scalar_values(
    arguments,
    error_type,
    message,
) -> None:
    """Reject values that previously passed comparison/truthiness-only checks."""
    with pytest.raises(error_type, match=message):
        api.CompositeDigitalAssetMembership(*arguments)


def test_composite_declaration_and_record_validate_attributes_consistently() -> None:
    """Apply the same name and attribute invariants to input and persisted records."""
    declaration = api.CompositeDigitalAssetDeclaration(
        (_member(),),
        name="book set",
        attributes=(("edition", "first"),),
    )
    record = api.CompositeDigitalAssetRecord(
        api.CompositeDigitalAssetID(1),
        declaration.members,
        name=declaration.name,
        attributes=declaration.attributes,
    )
    assert record.attributes == (("edition", "first"),)

    for constructor in (
        lambda: api.CompositeDigitalAssetDeclaration(
            (_member(),), attributes=(("edition", "one"), ("edition", "two"))
        ),
        lambda: api.CompositeDigitalAssetRecord(
            api.CompositeDigitalAssetID(1),
            (_member(),),
            attributes=(("", "value"),),
        ),
    ):
        with pytest.raises(ValueError):
            constructor()


@pytest.mark.parametrize(
    ("expected", "resolved", "readable", "error_type", "message"),
    (
        (-1, 0, 0, ValueError, "expected_members"),
        (1, 2, 1, ValueError, "resolved_members"),
        (2, 1, 2, ValueError, "readable_members"),
        (True, 0, 0, TypeError, "expected_members"),
    ),
)
def test_composite_availability_rejects_impossible_counts(
    expected,
    resolved,
    readable,
    error_type,
    message,
) -> None:
    """Ensure availability summaries cannot encode impossible count relationships."""
    with pytest.raises(error_type, match=message):
        api.CompositeDigitalAssetAvailabilityAssessment(
            api.CompositeDigitalAssetID(1),
            expected,
            resolved,
            readable,
        )


def test_composite_availability_validates_diagnostic_elements() -> None:
    """Require attributable positive missing IDs and useful nonblank error text."""
    with pytest.raises(ValueError, match="missing Digital Asset IDs"):
        api.CompositeDigitalAssetAvailabilityAssessment(
            api.CompositeDigitalAssetID(1),
            1,
            0,
            0,
            missing_digital_asset_ids=(api.DigitalAssetID(0),),
        )
    with pytest.raises(ValueError, match="must not be blank"):
        api.CompositeDigitalAssetAvailabilityAssessment(
            api.CompositeDigitalAssetID(1),
            1,
            1,
            0,
            errors=(" ",),
        )

    valid = api.CompositeDigitalAssetAvailabilityAssessment(
        api.CompositeDigitalAssetID(1),
        2,
        1,
        1,
        missing_digital_asset_ids=(api.DigitalAssetID(2),),
        errors=("one required member is unavailable",),
    )
    assert not valid.readable
