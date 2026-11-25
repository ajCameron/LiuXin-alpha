"""Produce the same bundled language constants for every native database backend."""

from __future__ import annotations


def common_variants() -> dict[str, list[str]]:
    """
    Return a small fresh mapping of common regional/script language variants.

    This is a convenience seed, not a complete BCP-47 registry or validator.

    Example:
        >>> "en-GB" in common_variants()["en"]
        True


    :return: Primary-language-to-variant-list mapping.
    """
    return {
        # English
        "en": ["en-GB", "en-US", "en-CA", "en-AU"],
        # French
        "fr": ["fr-FR", "fr-CA", "fr-BE", "fr-CH"],
        # German
        "de": ["de-DE", "de-AT", "de-CH"],
        # Spanish
        "es": ["es-ES", "es-MX", "es-AR"],
        # Portuguese
        "pt": ["pt-PT", "pt-BR"],
        # Dutch
        "nl": ["nl-NL", "nl-BE"],
        # Chinese
        "zh": ["zh-Hans", "zh-Hant", "zh-Hans-CN", "zh-Hant-TW"],
        # Serbian
        "sr": ["sr-Cyrl", "sr-Latn"],
    }


def language_rows() -> tuple[
    tuple[int, str, str, str | None, str, str | None, str, str | None], ...
]:
    """Return stable bibliographic-code ordered IDs and their public language fields."""
    from LiuXin_alpha.utils.libraries.iso639 import data

    variants = common_variants()
    rows = []
    for index, entry in enumerate(
        sorted(data, key=lambda value: str(value.get("iso639_2_b") or ""))
    ):
        code = (entry.get("iso639_2_b") or "").strip().lower()
        if not code:
            continue
        code_t = (entry.get("iso639_2_t") or "").strip().lower()
        code_t = code_t if code_t and code_t != code else None
        code_1 = (entry.get("iso639_1") or "").strip().lower() or None
        primary = code_1 or code_t or code
        regional = " ".join(sorted(set(variants.get(primary, ())))) or None
        rows.append(
            (
                index + 1,
                (entry.get("name") or "").strip() or code,
                code,
                code_1,
                code,
                code_t,
                primary,
                regional,
            )
        )
    return tuple(rows)
