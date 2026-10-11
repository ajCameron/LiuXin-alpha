"""Exercise metadata-owned identifier normalization policy."""

from __future__ import annotations

import pytest

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.metadata.identifiers import (
    NormalisedIdentifier,
    check_isbn10,
    check_isbn13,
    clean_id_key,
    clean_id_value,
    clean_identifier,
    clean_identifier_key,
    clean_identifier_mapping,
    clean_identifier_value,
    normalise_identifier_pair,
    prepare_legacy_identifier_mapping,
    prepare_legacy_single_identifier,
    validate_legacy_identifier,
)


def test_legacy_checksum_helpers_preserve_uncleaned_contract() -> None:
    assert check_isbn10("0261103571") == "0261103571"
    assert check_isbn10("0261103571suffix") == "0261103571suffix"
    assert check_isbn10("097522980X") == "097522980X"
    assert check_isbn10("097522980x") is None
    assert check_isbn10("not-isbn") is None

    assert check_isbn13("9780261103573") == "9780261103573"
    assert check_isbn13("9780261103573suffix") == "9780261103573suffix"
    assert check_isbn13("9780000000040") == "9780000000040"
    assert check_isbn13("bad") is None


def test_legacy_single_identifier_contract_remains_isbn_only() -> None:
    assert prepare_legacy_single_identifier(" ISBN: ", "0261103571") == (
        "isbn",
        "0261103571",
    )
    assert prepare_legacy_single_identifier("", "value") == ("", "value")

    validate_legacy_identifier("isbn", "9780261103573")
    with pytest.raises(InputIntegrityError, match="did not pass validation"):
        prepare_legacy_single_identifier("isbn", "0-261-10357-1")
    with pytest.raises(NotImplementedError):
        prepare_legacy_single_identifier("doi", "10.1234/example")


def test_legacy_bulk_identifier_contract_remains_permissive() -> None:
    prepared = prepare_legacy_identifier_mapping(
        {
            " ISBN: ": "not-an-isbn",
            "DOI": [" 10.1234/Example ", ""],
            "": "value",
        }
    )

    assert prepared == {
        "isbn": {"not-an-isbn"},
        "doi": {"10.1234/Example", ""},
        "": {"value"},
    }


def test_legacy_bulk_scalar_replaces_but_iterables_merge_after_scheme_cleaning() -> (
    None
):
    assert prepare_legacy_identifier_mapping(
        {
            " ISBN: ": ["first", "second"],
            "isbn": "replacement",
        }
    ) == {"isbn": {"replacement"}}
    assert prepare_legacy_identifier_mapping(
        {
            " ISBN: ": ["first"],
            "isbn": ["second", "first"],
        }
    ) == {"isbn": {"first", "second"}}


def test_curated_identifier_normalization_is_scheme_specific() -> None:
    isbn = normalise_identifier_pair("ISBN-13", "978-0-306-40615-7")
    doi = normalise_identifier_pair("DOI", "https://doi.org/10.1000/ABC")
    generic = normalise_identifier_pair("publisher_code", " AbC/Def ")

    assert isbn == NormalisedIdentifier(
        identifier_type="isbn13",
        value="978-0-306-40615-7",
        normalised_value="9780306406157",
    )
    assert doi.normalised_value == "10.1000/abc"
    assert generic == NormalisedIdentifier(
        identifier_type="publisher_code",
        value="AbC/Def",
        normalised_value="AbC/Def",
    )

    with pytest.raises(ValueError, match="invalid ISBN"):
        normalise_identifier_pair("isbn13", "978-0-306-40615-8")


@pytest.mark.parametrize(
    ("supplied", "canonical"),
    [
        ("Archive ID", "archive-id"),
        ("asset_id", "asset-id"),
        ("IMDB-ID", "imdb_id"),
        ("Local Call", "local-call"),
        ("Publisher PHash", "publisher_phash"),
        ("UUID-ish", "uuid-ish"),
        ("Wikipedia URL", "wikipedia_url"),
        ("Publisher Code", "publisher_code"),
    ],
)
def test_curated_identifier_scheme_aliases_preserve_unknown_schemes(
    supplied: str,
    canonical: str,
) -> None:
    assert normalise_identifier_pair(supplied, "Value").identifier_type == canonical


def test_curated_identifier_normalization_preserves_special_scheme_rules() -> None:
    assert normalise_identifier_pair("isbn", "0-306-40615-2").normalised_value == (
        "0306406152"
    )
    calibre_uuid = normalise_identifier_pair(
        "calibre_uuid",
        "F47AC10B-58CC-4372-A567-0E02B2C3D479",
    )
    assert calibre_uuid.identifier_type == "calibre_uuid"
    assert calibre_uuid.normalised_value == "f47ac10b-58cc-4372-a567-0e02b2c3d479"
    assert normalise_identifier_pair("oclc", "OCN 12345").normalised_value == "12345"
    assert normalise_identifier_pair("oclc", "oclc").normalised_value == ""


def test_curated_identifier_normalization_preserves_override_contract() -> None:
    overridden = normalise_identifier_pair(
        "doi",
        "original-value",
        normalised_value=" DOI:10.1000/OVERRIDE ",
    )
    fallback = normalise_identifier_pair(
        "publisher_code",
        " Original Value ",
        normalised_value="",
    )

    assert overridden.value == "original-value"
    assert overridden.normalised_value == "10.1000/override"
    assert fallback.value == "Original Value"
    assert fallback.normalised_value == "Original Value"

    with pytest.raises(ValueError, match="must be non-empty"):
        normalise_identifier_pair("doi", "10.1000/original", normalised_value=" ")


@pytest.mark.parametrize(
    ("scheme", "value", "message"),
    [
        ("isbn10", "978-0-306-40615-7", "does not contain an ISBN-10"),
        ("uuid", "not-a-uuid", "invalid UUID"),
        ("doi", "not-a-doi", "invalid DOI"),
        ("", "value", "must be non-empty"),
        ("scheme", "", "must be non-empty"),
    ],
)
def test_curated_identifier_normalization_rejects_known_invalid_pairs(
    scheme: str,
    value: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        normalise_identifier_pair(scheme, value)


@pytest.mark.parametrize(("scheme", "value"), [(None, "value"), ("scheme", None)])
def test_curated_identifier_normalization_does_not_coerce_inputs(
    scheme: object,
    value: object,
) -> None:
    with pytest.raises(TypeError, match="scheme and value must be strings"):
        normalise_identifier_pair(scheme, value)  # type: ignore[arg-type]


def test_clean_identifier_preserves_legacy_scheme_and_value_rules() -> None:
    assert clean_identifier("isbn:", "1234567890") == ("isbn", "1234567890")
    assert clean_identifier("my,type", "value") == ("mytype", "value")
    assert clean_identifier("key", "val,ue") == ("key", "val|ue")
    assert clean_identifier("ISBN", "9780000000000") == (
        "isbn",
        "9780000000000",
    )


def test_identifier_cleaners_preserve_empty_value_policy() -> None:
    assert clean_identifier("", "") == ("", "")
    assert clean_identifier(None, "value") == ("", "value")
    assert clean_identifier("isbn", None) == ("isbn", "")
    assert clean_identifier_key(None) == ""
    assert clean_identifier_value(None) == ""


def test_strict_container_cleaners_still_require_strings() -> None:
    assert clean_id_key(" : ,") == ""
    assert clean_id_value(" a,b ") == "a|b"

    with pytest.raises(AttributeError):
        clean_id_key(None)
    with pytest.raises(AttributeError):
        clean_id_value(None)


def test_clean_identifier_mapping_filters_empty_pairs_without_mutation() -> None:
    source = {" ISBN: ": " a,b ", "empty": "", "": "value"}

    assert clean_identifier_mapping(source) == {"isbn": "a|b"}
    assert source == {" ISBN: ": " a,b ", "empty": "", "": "value"}


def test_clean_identifier_mapping_uses_last_normalized_scheme() -> None:
    assert clean_identifier_mapping({"ISBN": "first", "isbn:": "second"}) == {
        "isbn": "second"
    }
