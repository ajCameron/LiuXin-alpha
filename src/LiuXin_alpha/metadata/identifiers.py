"""Normalize and validate identifier values without Catalog or storage knowledge.

The ``clean_*`` helpers preserve historical Calibre-compatible punctuation rules;
they do not validate whether a scheme is known or a value is valid for that scheme.
``normalise_identifier_pair`` owns the stricter curated-identifier policy shared by
Catalog matching and writes. The explicitly named ``prepare_legacy_*`` helpers
contain the intentionally different single and bulk compatibility contracts without
making them current policy. Unknown nonblank Catalog schemes remain valid by design.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.text.icu import lower as icu_lower

_DOI_PREFIX = re.compile(
    r"^(?:doi\s*:\s*|https?://(?:dx\.)?doi\.org/)",
    flags=re.IGNORECASE,
)
_ISBN_SCHEMES = frozenset({"isbn", "isbn10", "isbn13"})


@dataclass(frozen=True, slots=True)
class NormalisedIdentifier:
    """Hold one metadata-owned canonical identifier result.

    This passive value deliberately contains no Catalog candidate, provenance, or
    persistence concepts. ``value`` is the stripped original input, while
    ``normalised_value`` is the scheme-specific comparison form.

    :ivar identifier_type: Canonical scheme or normalized unknown scheme.
    :ivar value: Original identifier value with outer whitespace removed.
    :ivar normalised_value: Scheme-specific value used for equality comparisons.
    """

    identifier_type: str
    value: str
    normalised_value: str


def check_isbn10(isbn: str) -> str | None:
    """Apply the historical uncleaned ISBN-10 checksum contract.

    Only the first ten positions participate. Uppercase ``X`` is accepted as the
    final check character; lowercase ``x`` is not. The original input, including
    unchecked suffix text, is returned on success. Ordinary checksum exceptions are
    logged and converted to ``None``.

    :param isbn: Candidate whose first ten positions are checked without cleanup.
    :return: The original input on success, otherwise ``None``.
    """

    try:
        digits = list(map(int, isbn[:9]))
        products = [(index + 1) * digits[index] for index in range(9)]
        check = sum(products) % 11
        if (check == 10 and isbn[9] == "X") or check == int(isbn[9]):
            return isbn
    except Exception as error:
        default_log.log_exception(
            "Unable to check for ISBN-10.",
            error,
            "INFO",
        )
    return None


def check_isbn13(isbn: str) -> str | None:
    """Apply the historical uncleaned ISBN-13 checksum contract.

    Only the first thirteen positions participate and no ISBN prefix is required.
    The original input, including unchecked suffix text, is returned on success.
    Ordinary checksum exceptions are logged and converted to ``None``.

    :param isbn: Candidate whose first thirteen positions are checked without cleanup.
    :return: The original input on success, otherwise ``None``.
    """

    try:
        digits = list(map(int, isbn[:12]))
        products = [(1 if index % 2 == 0 else 3) * digits[index] for index in range(12)]
        check = 10 - (sum(products) % 10)
        if check == 10:
            check = 0
        if str(check) == isbn[12]:
            return isbn
    except Exception as error:
        default_log.log_exception(
            "Unable to check for ISBN-13.",
            error,
            "INFO",
        )
    return None


def validate_legacy_identifier(identifier_type: str, identifier_value: str) -> None:
    """Preserve the legacy single-writer validation contract exactly.

    Only the exact lowercase scheme ``isbn`` is supported. Its value must be exactly
    ten or thirteen characters and pass the corresponding uncleaned checksum helper.
    Other schemes raise ``NotImplementedError``; invalid ISBN values raise
    ``InputIntegrityError``.

    This deliberately differs from :func:`normalise_identifier_pair` and from the
    permissive legacy bulk writer. It exists to contain compatibility behaviour, not
    to define the current Catalog write contract.

    :param identifier_type: Already-cleaned legacy scheme.
    :param identifier_value: Already-cleaned legacy value, without ISBN punctuation cleanup.
    :return: None when the exact legacy ISBN contract accepts the value.
    :raises NotImplementedError: If the scheme is anything other than ``isbn``.
    :raises InputIntegrityError: If the ISBN length or checksum is invalid.
    """

    status: str | None = None
    if identifier_type == "isbn":
        if len(identifier_value) == 10:
            status = check_isbn10(identifier_value)
        elif len(identifier_value) == 13:
            status = check_isbn13(identifier_value)
    else:
        raise NotImplementedError

    if not status:
        raise InputIntegrityError("Identifier did not pass validation")


def _normalise_scheme(value: str) -> str:
    """Return a canonical known scheme or normalized unknown scheme."""

    scheme = re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")
    compact = scheme.replace("_", "")
    aliases = {
        "archiveid": "archive-id",
        "assetid": "asset-id",
        "imdbid": "imdb_id",
        "isbn": "isbn",
        "isbn10": "isbn10",
        "isbn13": "isbn13",
        "calibreuuid": "calibre_uuid",
        "localcall": "local-call",
        "publisherphash": "publisher_phash",
        "uuidish": "uuid-ish",
        "wikipediaurl": "wikipedia_url",
    }
    return aliases.get(compact, scheme)


def _check_isbn(value: str) -> str | None:
    """Return a compact checksum-valid ISBN-10/13, or ``None``."""

    compact = re.sub(r"[^0-9Xx]", "", value)
    if (
        len(compact) == 10
        and compact[:9].isdigit()
        and (compact[-1].isdigit() or compact[-1] in "Xx")
    ):
        total = sum(
            (10 - index) * (10 if digit in "Xx" else int(digit))
            for index, digit in enumerate(compact)
        )
        return compact.upper() if total % 11 == 0 else None
    if len(compact) == 13 and compact.isdigit():
        total = sum(
            int(digit) * (1 if index % 2 == 0 else 3)
            for index, digit in enumerate(compact)
        )
        return compact if total % 10 == 0 else None
    return None


def normalise_identifier_pair(
    identifier_type: str,
    identifier_value: str,
    *,
    normalised_value: str | None = None,
) -> NormalisedIdentifier:
    """Canonicalize and validate one curated identifier pair.

    A truthy ``normalised_value`` takes precedence for comparison processing; an
    empty override falls back to the original value. ISBN values receive checksum
    and explicit-length checks, UUID schemes use UUID parsing, DOI values lose one
    recognized prefix and case-fold, and OCLC values lose one recognized leading
    token. Other schemes retain stripped, case-sensitive values. Unknown nonblank
    schemes are supported.

    The returned ``value`` always comes from the stripped original input, even when
    an override supplies the comparison form. No Catalog object, database, network
    lookup, or input mutation is involved.

    Example:
        >>> result = normalise_identifier_pair("DOI", " https://doi.org/10.1000/ABC ")
        >>> result.identifier_type, result.value, result.normalised_value
        ('doi', 'https://doi.org/10.1000/ABC', '10.1000/abc')

    :param identifier_type: Scheme text to canonicalize.
    :param identifier_value: Original identifier text to preserve after stripping.
    :param normalised_value: Optional pre-normalized comparison input.
    :return: Metadata-owned canonical scheme, original text, and comparison value.
    :raises TypeError: If the original scheme or value is not text.
    :raises ValueError: If required text is empty or a known scheme is invalid.
    :raises AttributeError: If a truthy override does not support string operations.
    """

    if not isinstance(identifier_type, str) or not isinstance(identifier_value, str):
        raise TypeError("identifier scheme and value must be strings")

    scheme = _normalise_scheme(identifier_type)
    raw_value = normalised_value or identifier_value
    value = raw_value.strip()
    if not scheme or not value:
        raise ValueError("identifier scheme and value must be non-empty")

    if scheme in _ISBN_SCHEMES:
        checked = _check_isbn(value)
        if checked is None:
            raise ValueError(f"invalid ISBN: {identifier_value!r}")
        expected_length = (
            10 if scheme == "isbn10" else 13 if scheme == "isbn13" else None
        )
        if expected_length is not None and len(checked) != expected_length:
            raise ValueError(
                f"{identifier_type!r} does not contain an ISBN-{expected_length}"
            )
        scheme = f"isbn{len(checked)}"
        comparison_value = checked
    elif scheme in {"uuid", "calibre_uuid"}:
        try:
            comparison_value = str(UUID(value))
        except ValueError as error:
            raise ValueError(f"invalid UUID: {identifier_value!r}") from error
    elif scheme == "doi":
        comparison_value = _DOI_PREFIX.sub("", value).strip().casefold()
        if not comparison_value.startswith("10.") or "/" not in comparison_value:
            raise ValueError(f"invalid DOI: {identifier_value!r}")
    elif scheme == "oclc":
        comparison_value = re.sub(
            r"^(?:oclc|ocm|ocn|on)",
            "",
            value,
            flags=re.IGNORECASE,
        )
        comparison_value = comparison_value.strip().casefold()
    else:
        comparison_value = value

    return NormalisedIdentifier(
        identifier_type=scheme,
        value=identifier_value.strip(),
        normalised_value=comparison_value,
    )


def clean_id_key(identifier_type: str) -> str:
    """Apply the strict historical metadata-container scheme cleaner.

    The caller must supply text. Built-in lowercase is used, punctuation is removed,
    and the result is stripped again. This intentionally retains the pre-existing
    metadata helper contract; use :func:`clean_identifier_key` for the false-safe ICU
    variant historically used at the database and legacy-library boundary.

    Example:
        >>> clean_id_key(" ISBN:, ")
        'isbn'

    :param identifier_type: Identifier scheme string.
    :return: Cleaned scheme, possibly empty.
    """

    return identifier_type.lower().strip().replace(":", "").replace(",", "").strip()


def clean_id_value(identifier_value: str) -> str:
    """Apply the strict historical metadata-container value cleaner.

    The caller must supply text. Leading and trailing whitespace is removed and commas
    become vertical bars; case and colons are preserved.

    Example:
        >>> clean_id_value(" a,b ")
        'a|b'

    :param identifier_value: Identifier value string.
    :return: Cleaned value, possibly empty.
    """

    return identifier_value.strip().replace(",", "|")


def clean_identifier_key(identifier_type: object) -> str:
    """Apply the false-safe ICU scheme policy used by legacy database callers.

    Punctuation removal deliberately has no final strip, preserving the exact legacy
    ordering of operations.

    :param identifier_type: Identifier scheme or a false value.
    :return: Cleaned scheme, possibly empty.
    """

    return icu_lower(identifier_type or "").strip().replace(":", "").replace(",", "")


def clean_identifier_value(identifier_value: object) -> str:
    """Apply the false-safe value policy used by legacy database callers.

    :param identifier_value: Identifier value or a false value.
    :return: Cleaned value, possibly empty.
    """

    return (identifier_value or "").strip().replace(",", "|")


def clean_identifier(
    identifier_type: object,
    identifier_value: object,
) -> tuple[str, str]:
    """Normalize one identifier scheme/value pair.

    Example:
        >>> clean_identifier(" ISBN: ", " a,b ")
        ('isbn', 'a|b')

    :param identifier_type: Identifier scheme or a false value.
    :param identifier_value: Identifier value or a false value.
    :return: Cleaned scheme and value; either may be empty.
    """

    return clean_identifier_key(identifier_type), clean_identifier_value(
        identifier_value
    )


def prepare_legacy_single_identifier(
    identifier_type: object,
    identifier_value: object,
) -> tuple[str, str]:
    """Clean and apply the legacy single-writer validation contract.

    A scheme that cleans to empty bypasses validation and is returned for the caller
    to ignore. Every nonempty scheme is passed to
    :func:`validate_legacy_identifier`, meaning only exact cleaned ``isbn`` is
    accepted. This preserves the legacy writer's historical asymmetry with its bulk
    counterpart.

    :param identifier_type: Legacy scheme input accepted by :func:`clean_identifier`.
    :param identifier_value: Legacy value input accepted by :func:`clean_identifier`.
    :return: The cleaned pair after any required legacy validation.
    :raises NotImplementedError: If a nonempty cleaned scheme is not ``isbn``.
    :raises InputIntegrityError: If an ISBN fails the legacy validation contract.
    """

    clean_type, clean_value = clean_identifier(identifier_type, identifier_value)
    if clean_type:
        validate_legacy_identifier(clean_type, clean_value)
    return clean_type, clean_value


def prepare_legacy_identifier_mapping(
    identifiers: Mapping[object, object],
) -> dict[str, set[str]]:
    """Clean values for the permissive legacy bulk writer without validation.

    String and bytes values become one-element sets; other values are iterated. A
    scalar entry replaces an earlier entry whose scheme cleans to the same key, while
    iterable entries merge with an existing set. Empty cleaned schemes and values are
    retained. ISBN checks and known-scheme checks are intentionally absent.

    This is a compatibility codec only. It does not provide the semantic guarantee of
    :func:`normalise_identifier_pair` and must not be used by current Catalog writes.

    :param identifiers: Legacy scheme-to-scalar-or-iterable mapping.
    :return: Cleaned scheme keys mapped to sets of cleaned values.
    :raises TypeError: If an input cannot support its historical cleaning/iteration operations.
    """

    cleaned: dict[str, set[str]] = {}
    for identifier_type, values in identifiers.items():
        if isinstance(values, (str, bytes)):
            clean_type, clean_value = clean_identifier(identifier_type, values)
            cleaned[clean_type] = {clean_value}
            continue

        for value in values:  # type: ignore[union-attr]
            clean_type, clean_value = clean_identifier(identifier_type, value)
            cleaned.setdefault(clean_type, set()).add(clean_value)
    return cleaned


def clean_identifier_mapping(
    identifiers: Mapping[object, object],
) -> dict[str, str]:
    """Return the nonempty normalized pairs from an identifier mapping.

    Later source entries whose schemes normalize to the same key replace earlier
    entries. The input mapping is not mutated.

    Example:
        >>> clean_identifier_mapping({" ISBN: ": " a,b ", "empty": ""})
        {'isbn': 'a|b'}

    :param identifiers: Scheme-to-value mapping to normalize.
    :return: New mapping containing only pairs with nonempty schemes and values.
    """

    cleaned: dict[str, str] = {}
    for identifier_type, identifier_value in identifiers.items():
        clean_type, clean_value = clean_identifier(identifier_type, identifier_value)
        if clean_type and clean_value:
            cleaned[clean_type] = clean_value
    return cleaned


__all__ = [
    "NormalisedIdentifier",
    "check_isbn10",
    "check_isbn13",
    "clean_id_key",
    "clean_id_value",
    "clean_identifier",
    "clean_identifier_key",
    "clean_identifier_mapping",
    "clean_identifier_value",
    "normalise_identifier_pair",
    "prepare_legacy_identifier_mapping",
    "prepare_legacy_single_identifier",
    "validate_legacy_identifier",
]
