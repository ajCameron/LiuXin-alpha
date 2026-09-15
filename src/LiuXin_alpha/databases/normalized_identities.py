"""
Declare and derive normalized identities for selected database display values.

Identity policy is narrower than case-insensitive search. Built-in declarations cover relation values such as tags, scoped genres/subjects and named policies; Work titles and Agent names are not identities merely because they support normalized search. Helpers prepare payloads and catalog representations without reading or writing a database.
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
    """
    Describe a display column whose derived key identifies rows within a scope.

    table/value_column select stored display data; identity_column names its derived key and normalization_profile chooses the transformation. scope_columns narrows equality, for example to a genre parent. unique requests uniqueness from schema/macro consumers rather than enforcing it during construction. The frozen slotted dataclass validates basic name relationships only; supplied containers are not copied or deep-frozen.

    Example:
        >>> spec = default_normalized_identity_spec("genres", "genre")
        >>> spec.scope_columns
        ('genre_parent_id',)
    """

    table: str
    value_column: str
    identity_column: str
    normalization_profile: ColumnNormalizationProfile
    scope_columns: tuple[str, ...] = ()
    unique: bool = True

    def __post_init__(self) -> None:
        """
        Reject blank name representations, repeated scopes and identical value/key columns.

        Blank checks use str(value).strip(), so they do not enforce actual string types or strip surrounding whitespace in the stored names. Scope duplicate checks use set membership; the profile and unique flag are not validated here.

        Example:
            >>> spec = NormalizedIdentitySpec("tags", "tag", "tag_key", ColumnNormalizationProfile.TAG_SEARCH_TERM)
            >>> spec.unique
            True


        :return: None; validation does not alter or normalize stored fields.
        :raises ValueError: A name representation is blank, scope columns repeat, or value and identity columns compare equal.
        :raises TypeError: A supplied scope entry cannot be hashed for duplicate checking.
        """

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
    """
    Look up one built-in table/display-column identity declaration.

    No case normalization or database override lookup occurs.

    Example:
        >>> default_normalized_identity_spec("tags", "tag").identity_column
        'tag_phash'
        >>> default_normalized_identity_spec("works", "work_title") is None
        True


    :param table: Table name stringified for exact lookup.
    :param value_column: Display-column name stringified for exact lookup.
    :return: Existing built-in spec, or None when the pair has no declared identity.
    """

    return _DEFAULTS_BY_VALUE_COLUMN.get((str(table), str(value_column)))


def iter_normalized_identity_defaults() -> Iterator[NormalizedIdentitySpec]:
    """
    Yield built-in identity declarations in their configured tuple order.

    This is the source registry order rather than an on-demand database enumeration or independent sort.

    Example:
        >>> tuple(iter_normalized_identity_defaults()) == NORMALIZED_IDENTITY_DEFAULTS
        True


    :return: Iterator of the existing NormalizedIdentitySpec objects, without copying them.
    """

    yield from NORMALIZED_IDENTITY_DEFAULTS


def normalized_identity_defaults_for_table(
    table: str,
) -> tuple[NormalizedIdentitySpec, ...]:
    """
    Return every built-in identity declaration for one exact table name.

    Example:
        >>> tuple(spec.value_column for spec in normalized_identity_defaults_for_table("custom_columns"))
        ('custom_column_label', 'custom_column_name')


    :param table: Table name stringified for the precomputed registry lookup.
    :return: Tuple of existing specs in configured order, or an empty tuple.
    """

    return _DEFAULTS_BY_TABLE.get(str(table), ())


def normalize_identity_value(
    value: Any,
    profile: ColumnNormalizationProfile,
) -> Any:
    """
    Apply one supported normalization profile while preserving non-string values.

    NONE preserves text; UNICODE_NFC normalizes composition; NFC/trim/casefold applies those steps in order without a second NFC pass. Tag search removes whitespace and lowercases after NFC. Title search delegates to the legacy simpler-title helper, including its hyphen truncation, character/stop-word removal and underscore joining. No database comparison or uniqueness check occurs.

    Example:
        >>> normalize_identity_value("  Straße  ", ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD)
        'strasse'
        >>> normalize_identity_value("Science Fiction", ColumnNormalizationProfile.TAG_SEARCH_TERM)
        'sciencefiction'
        >>> normalize_identity_value(None, ColumnNormalizationProfile.NONE) is None
        True


    :param value: Value to transform; non-strings are returned unchanged after profile validation.
    :param profile: ColumnNormalizationProfile member or its exact string value.
    :return: Normalized string, or the original non-string value.
    :raises InputIntegrityError: profile cannot be resolved to a supported normalization profile.
    """

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
    """
    Return a shallow row copy with eligible built-in identity keys derived.

    Leave the input mapping untouched. A present display value of None yields None for its key; missing display fields never trigger an update. overwrite=False preserves even an explicit None identity value. Scope fields and uniqueness constraints are neither supplied nor checked here.

    Example:
        >>> add_derived_identity_values("tags", {"tag": "Science Fiction"})
        {'tag': 'Science Fiction', 'tag_phash': 'sciencefiction'}
        >>> add_derived_identity_values("tags", {"tag": None}, available_columns={"tag"})
        {'tag': None}


    :param table: Exact table name compared against the built-in registry without string coercion.
    :param row: Mapping of supplied row fields; only present display fields trigger derivation.
    :param overwrite: Whether to replace an explicitly supplied identity field, default True.
    :param available_columns: Optional set of schema columns; absent derived columns are skipped when this filter is supplied.
    :return: New dictionary preserving unrelated values and adding or replacing eligible derived keys.
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
    """
    Serialize a declaration into the normalized-identities catalog column order.

    No database write occurs. Scope order is retained; normal specs encode their scope tuple as a JSON array. The profile must expose .value, because dataclass construction itself does not enforce its type.

    Example:
        >>> spec = default_normalized_identity_spec("tags", "tag")
        >>> normalized_identity_db_values(spec)
        ('tags', 'tag', 'tag_phash', 'tag_search_term', '[]', 1)


    :param spec: Spec with a profile enum, JSON-serializable scope columns and an int-convertible unique flag.
    :return: Tuple of table, value column, identity column, profile value, compact scope JSON and integer uniqueness.
    """

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
    """
    Decode a catalog row and construct a normalized identity declaration.

    Parse str(scope_columns_json or "[]") and reject non-list or non-string members. Nonempty bytes are stringified rather than decoded and therefore generally fail JSON parsing. Spec name/duplicate validation and enum lookup errors propagate separately from scope-JSON errors.

    Example:
        >>> spec = default_normalized_identity_spec("genres", "genre")
        >>> normalized_identity_from_db_values(*normalized_identity_db_values(spec)) == spec
        True


    :param table: Table name converted with str.
    :param value_column: Display-column name converted with str.
    :param identity_column: Derived-column name converted with str.
    :param normalization_profile: Profile value converted with str before exact enum lookup; use the stored value, not an enum repr.
    :param scope_columns_json: JSON text representing a list of strings; false input is treated as an empty list.
    :param unique: Flag converted with bool, so nonempty text such as "0" is true.
    :return: New NormalizedIdentitySpec with tuple scope columns and boolean uniqueness.
    :raises InputIntegrityError: Scope JSON cannot be parsed or is not a list of strings.
    :raises ValueError: Profile lookup or NormalizedIdentitySpec name/scope validation fails.
    """

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
