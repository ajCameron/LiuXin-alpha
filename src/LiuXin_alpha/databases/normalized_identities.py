"""Declared normalized identities for human-facing database values.

Case-insensitive comparison and normalized identity are deliberately separate
concepts.  A title can be searched case-insensitively without every work with
that title becoming the same work.  The declarations in this module are only
for relation rows whose display value is itself an identity.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
import json
import unicodedata
from typing import Any

from LiuXin_alpha.databases.column_metadata import ColumnNormalizationProfile
from LiuXin_alpha.errors import InputIntegrityError


NORMALIZED_IDENTITIES_TABLE = "normalized_identities"


@dataclass(frozen=True, slots=True)
class NormalizedIdentitySpec:
    """Describe one display value whose normalized form identifies its row.

    ``scope_columns`` narrows identity to a parent or other owning relation.
    For example, a genre name is unique within its parent, not across the
    entire genre table.
    """

    table: str
    value_column: str
    identity_column: str
    normalization_profile: ColumnNormalizationProfile
    scope_columns: tuple[str, ...] = ()
    unique: bool = True

    def __post_init__(self) -> None:
        fields = (self.table, self.value_column, self.identity_column, *self.scope_columns)
        if any(not str(value).strip() for value in fields):
            raise ValueError("Normalized identity names cannot be blank.")
        if len(set(self.scope_columns)) != len(self.scope_columns):
            raise ValueError("Normalized identity scope columns cannot repeat.")
        if self.value_column == self.identity_column:
            raise ValueError("The display and derived identity columns must differ.")


# These declarations are schema policy, not a list of every case-insensitive
# display column.  Titles, agent names, and other non-identity display text are
# intentionally absent.
NORMALIZED_IDENTITY_DEFAULTS: tuple[NormalizedIdentitySpec, ...] = (
    NormalizedIdentitySpec(
        table="backup_policies",
        value_column="backup_policy_name",
        identity_column="backup_policy_name_norm",
        normalization_profile=ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
    ),
    NormalizedIdentitySpec(
        table="custom_columns",
        value_column="custom_column_label",
        identity_column="custom_column_label_norm",
        normalization_profile=ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
    ),
    NormalizedIdentitySpec(
        table="custom_columns",
        value_column="custom_column_name",
        identity_column="custom_column_name_norm",
        normalization_profile=ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
    ),
    NormalizedIdentitySpec(
        table="genres",
        value_column="genre",
        identity_column="genre_phash",
        normalization_profile=ColumnNormalizationProfile.TITLE_SEARCH_TERM,
        scope_columns=("genre_parent_id",),
    ),
    NormalizedIdentitySpec(
        table="labels",
        value_column="label_text",
        identity_column="label_text_norm",
        normalization_profile=ColumnNormalizationProfile.TAG_SEARCH_TERM,
    ),
    NormalizedIdentitySpec(
        table="replication_policies",
        value_column="replication_policy_name",
        identity_column="replication_policy_name_norm",
        normalization_profile=ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
    ),
    NormalizedIdentitySpec(
        table="series",
        value_column="series",
        identity_column="series_name_norm",
        normalization_profile=ColumnNormalizationProfile.TITLE_SEARCH_TERM,
    ),
    NormalizedIdentitySpec(
        table="subjects",
        value_column="subject",
        identity_column="subject_phash",
        normalization_profile=ColumnNormalizationProfile.TITLE_SEARCH_TERM,
        scope_columns=("subject_parent_id",),
    ),
    NormalizedIdentitySpec(
        table="tags",
        value_column="tag",
        identity_column="tag_phash",
        normalization_profile=ColumnNormalizationProfile.TAG_SEARCH_TERM,
    ),
)

_DEFAULTS_BY_VALUE_COLUMN = {
    (spec.table, spec.value_column): spec
    for spec in NORMALIZED_IDENTITY_DEFAULTS
}
_DEFAULTS_BY_TABLE: dict[str, tuple[NormalizedIdentitySpec, ...]] = {}
for _spec in NORMALIZED_IDENTITY_DEFAULTS:
    _DEFAULTS_BY_TABLE[_spec.table] = (
        *_DEFAULTS_BY_TABLE.get(_spec.table, ()),
        _spec,
    )


def default_normalized_identity_spec(
    table: str,
    value_column: str,
) -> NormalizedIdentitySpec | None:
    """Return the built-in declaration for a display column, if it has one."""

    return _DEFAULTS_BY_VALUE_COLUMN.get((str(table), str(value_column)))


def iter_normalized_identity_defaults() -> Iterator[NormalizedIdentitySpec]:
    """Yield the built-in declarations in deterministic order."""

    yield from NORMALIZED_IDENTITY_DEFAULTS


def normalized_identity_defaults_for_table(
    table: str,
) -> tuple[NormalizedIdentitySpec, ...]:
    """Return the built-in identity declarations for one table."""

    return _DEFAULTS_BY_TABLE.get(str(table), ())


def normalize_identity_value(
    value: Any,
    profile: ColumnNormalizationProfile,
) -> Any:
    """Derive the stable comparison value for a declared identity."""

    if not isinstance(profile, ColumnNormalizationProfile):
        try:
            profile = ColumnNormalizationProfile(str(profile))
        except (TypeError, ValueError) as exc:
            raise InputIntegrityError(
                f"Unsupported normalization profile: {profile!r}"
            ) from exc
    if not isinstance(value, str):
        return value
    if profile is ColumnNormalizationProfile.NONE:
        return value
    if profile is ColumnNormalizationProfile.UNICODE_NFC:
        return unicodedata.normalize("NFC", value)
    if profile is ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD:
        return unicodedata.normalize("NFC", value).strip().casefold()
    if profile is ColumnNormalizationProfile.TAG_SEARCH_TERM:
        from LiuXin_alpha.metadata.standardization import make_tag_search_term

        return make_tag_search_term(unicodedata.normalize("NFC", value))
    if profile is ColumnNormalizationProfile.TITLE_SEARCH_TERM:
        from LiuXin_alpha.metadata.standardization import make_title_search_term

        return make_title_search_term(unicodedata.normalize("NFC", value))
    raise InputIntegrityError(f"Unsupported normalization profile: {profile!r}")


def add_derived_identity_values(
    table: str,
    row: Mapping[str, Any],
    *,
    overwrite: bool = True,
    available_columns: set[str] | frozenset[str] | None = None,
) -> dict[str, Any]:
    """Return a row payload with derived identity columns kept in sync.

    A derived column is touched only when its display column is present.  This
    makes the helper safe for partial updates.  ``None`` remains ``None`` so
    the existing blank-row mechanism continues to work.
    """

    prepared = dict(row)
    for spec in NORMALIZED_IDENTITY_DEFAULTS:
        if spec.table != table or spec.value_column not in prepared:
            continue
        if (
            available_columns is not None
            and spec.identity_column not in available_columns
        ):
            continue
        if not overwrite and spec.identity_column in prepared:
            continue
        value = prepared[spec.value_column]
        prepared[spec.identity_column] = (
            None
            if value is None
            else normalize_identity_value(value, spec.normalization_profile)
        )
    return prepared


def normalized_identity_db_values(
    spec: NormalizedIdentitySpec,
) -> tuple[str, str, str, str, str, int]:
    """Serialize a declaration for the database-side catalog."""

    return (
        spec.table,
        spec.value_column,
        spec.identity_column,
        spec.normalization_profile.value,
        json.dumps(spec.scope_columns, separators=(",", ":")),
        int(spec.unique),
    )


def normalized_identity_from_db_values(
    table: Any,
    value_column: Any,
    identity_column: Any,
    normalization_profile: Any,
    scope_columns_json: Any,
    unique: Any,
) -> NormalizedIdentitySpec:
    """Deserialize one database-side identity declaration."""

    try:
        raw_scope = json.loads(str(scope_columns_json or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise InputIntegrityError(
            f"Invalid normalized identity scope JSON: {scope_columns_json!r}"
        ) from exc
    if not isinstance(raw_scope, list) or not all(
        isinstance(column, str) for column in raw_scope
    ):
        raise InputIntegrityError(
            f"Normalized identity scope must be a JSON list of column names: {raw_scope!r}"
        )
    return NormalizedIdentitySpec(
        table=str(table),
        value_column=str(value_column),
        identity_column=str(identity_column),
        normalization_profile=ColumnNormalizationProfile(str(normalization_profile)),
        scope_columns=tuple(raw_scope),
        unique=bool(unique),
    )


__all__ = [
    "NORMALIZED_IDENTITIES_TABLE",
    "NORMALIZED_IDENTITY_DEFAULTS",
    "NormalizedIdentitySpec",
    "add_derived_identity_values",
    "default_normalized_identity_spec",
    "iter_normalized_identity_defaults",
    "normalize_identity_value",
    "normalized_identity_defaults_for_table",
    "normalized_identity_db_values",
    "normalized_identity_from_db_values",
]
