"""
Type-aware row-dict conversion for SQLite-backed drivers.

This exists to prevent the historical behaviour of coercing *all* DB values via
`force_unicode`, which converts integers like 999 into strings like '999'.

Casting is conservative: only coerce to numeric when the DB driver already
returned a numeric type.
Otherwise, keep malformed values visible as text.
The intent is convenient - not to obscure problems.
"""

# Todo: Might be an idea to add a as_value to row - so we can be sure we're e.g. getting an int

from __future__ import annotations

from dataclasses import replace
import re

from typing import Any, Dict, Iterator, Optional, Sequence, Union

from LiuXin_alpha.databases.column_metadata import (
    COLUMN_METADATA_TABLE,
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
    default_column_metadata,
    infer_column_metadata,
)
from LiuXin_alpha.databases.normalized_identities import (
    NORMALIZED_IDENTITIES_TABLE,
    NormalizedIdentitySpec,
    default_normalized_identity_spec,
    iter_normalized_identity_defaults,
    normalized_identity_from_db_values,
)
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode


class ValueCastingMixin:
    """
    Add type-aware row-to-dict conversion for SQLite-backed drivers.
    """

    _DECLARED_TYPES_CACHE_ATTR = "_declared_types_cache"

    def direct_get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Return the database-native declared datatype for one column.

        This deliberately returns the declaration reported by the backend,
        rather than a normalized datatype or SQLite affinity. SQLite columns
        that were declared without a datatype therefore return ``""``.

        :param table:
        :param column:
        :return:
        """
        table_name = self._canonicalise_table_name_for_cache(table)
        column_name = str(column)
        headings = self.direct_get_column_headings(table_name)
        if column_name not in headings:
            raise InputIntegrityError(f"column {column_name!r} not found in table {table_name!r}")

        declared_types = self.direct_get_declared_types_for_table(table_name)
        try:
            return declared_types[column_name]
        except KeyError as exc:
            raise DatabaseIntegrityError(
                f"column {column_name!r} exists in table {table_name!r} "
                "but has no declared-datatype catalog entry"
            ) from exc

    def direct_get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Return database-owned text equality policy for one column.

        Older databases without the column metadata catalog use the canonical
        built-in defaults.

        :param table:
        :param column:
        :return:
        """
        return self.direct_get_column_metadata(table, column).case_sensitive

    def direct_get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """Return the complete database-owned policy for one physical column."""

        table_name, column_name = self._validated_column_metadata_target(table, column)
        try:
            declared_type = self.direct_get_declared_column_datatype(
                table_name,
                column_name,
            )
        except DatabaseIntegrityError:
            declared_type = None
        fallback = infer_column_metadata(
            table_name,
            column_name,
            declared_type,
        )
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            return fallback

        catalog_columns = set(self.direct_get_column_headings(COLUMN_METADATA_TABLE))
        expanded_columns = {
            "column_metadata_semantic_role",
            "column_metadata_normalization_profile",
            "column_metadata_comparison_column",
            "column_metadata_empty_value_policy",
            "column_metadata_merge_policy",
            "column_metadata_validation_profile",
        }
        conn = self.get_connection()
        try:
            if expanded_columns <= catalog_columns:
                row = conn.execute(
                    """
                    SELECT
                      column_metadata_case_sensitive,
                      column_metadata_semantic_role,
                      column_metadata_normalization_profile,
                      column_metadata_comparison_column,
                      column_metadata_empty_value_policy,
                      column_metadata_merge_policy,
                      column_metadata_validation_profile
                    FROM column_metadata
                    WHERE column_metadata_table_name = ?
                      AND column_metadata_column_name = ?
                    LIMIT 1;
                    """,
                    (table_name, column_name),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT column_metadata_case_sensitive
                    FROM column_metadata
                    WHERE column_metadata_table_name = ?
                      AND column_metadata_column_name = ?
                    LIMIT 1;
                    """,
                    (table_name, column_name),
                ).fetchone()
        finally:
            conn.close()
        if row is None:
            return fallback
        if not expanded_columns <= catalog_columns:
            return ColumnMetadata(
                table=fallback.table,
                column=fallback.column,
                case_sensitive=self._coerce_column_case_sensitivity(
                    row[0],
                    table_name,
                    column_name,
                ),
                semantic_role=fallback.semantic_role,
                normalization_profile=fallback.normalization_profile,
                comparison_column=fallback.comparison_column,
                empty_value_policy=fallback.empty_value_policy,
                merge_policy=fallback.merge_policy,
                validation_profile=fallback.validation_profile,
            )
        return self._column_metadata_from_values(
            table_name,
            column_name,
            *row,
        )

    def direct_set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """Persist the complete database-owned policy for one physical column."""

        metadata = self._validated_column_metadata_input(metadata)
        self._validate_normalized_identity_metadata(metadata)
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            raise DatabaseIntegrityError(
                "database has no column_metadata table; migrate the schema before storing column policy"
            )
        required_columns = {
            "column_metadata_semantic_role",
            "column_metadata_normalization_profile",
            "column_metadata_comparison_column",
            "column_metadata_empty_value_policy",
            "column_metadata_merge_policy",
            "column_metadata_validation_profile",
        }
        if not required_columns <= set(self.direct_get_column_headings(COLUMN_METADATA_TABLE)):
            raise DatabaseIntegrityError(
                "column_metadata schema is outdated; migrate it before storing expanded policy"
            )

        conn = self.get_connection()
        try:
            conn.execute(
                """
                INSERT INTO column_metadata (
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive,
                  column_metadata_semantic_role,
                  column_metadata_normalization_profile,
                  column_metadata_comparison_column,
                  column_metadata_empty_value_policy,
                  column_metadata_merge_policy,
                  column_metadata_validation_profile
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                  column_metadata_table_name,
                  column_metadata_column_name
                ) DO UPDATE SET
                  column_metadata_case_sensitive = excluded.column_metadata_case_sensitive,
                  column_metadata_semantic_role = excluded.column_metadata_semantic_role,
                  column_metadata_normalization_profile = excluded.column_metadata_normalization_profile,
                  column_metadata_comparison_column = excluded.column_metadata_comparison_column,
                  column_metadata_empty_value_policy = excluded.column_metadata_empty_value_policy,
                  column_metadata_merge_policy = excluded.column_metadata_merge_policy,
                  column_metadata_validation_profile = excluded.column_metadata_validation_profile;
                """,
                self._column_metadata_db_values(metadata),
            )
            conn.commit()
        finally:
            conn.close()

    def _validate_normalized_identity_metadata(
        self,
        metadata: ColumnMetadata,
    ) -> None:
        """Keep column policy coherent with an immutable identity declaration."""

        identity_spec = self.direct_get_normalized_identity_spec(
            metadata.table,
            metadata.column,
        )
        if identity_spec is None:
            return
        if metadata.comparison_column != identity_spec.identity_column:
            raise InputIntegrityError(
                f"{metadata.table}.{metadata.column} is a normalized identity; "
                f"its comparison column must remain "
                f"{identity_spec.identity_column!r}."
            )
        if metadata.normalization_profile is not identity_spec.normalization_profile:
            raise InputIntegrityError(
                f"{metadata.table}.{metadata.column} is a normalized identity; "
                f"its normalization profile must remain "
                f"{identity_spec.normalization_profile.value!r}."
            )
        expected_case_sensitive = identity_spec.normalization_profile in {
            ColumnNormalizationProfile.NONE,
            ColumnNormalizationProfile.UNICODE_NFC,
        }
        if metadata.case_sensitive is not expected_case_sensitive:
            raise InputIntegrityError(
                f"{metadata.table}.{metadata.column} is a normalized identity; "
                f"case sensitivity must remain {expected_case_sensitive!r}."
            )

    def direct_get_semantic_role(
        self,
        table: str,
        column: str,
    ) -> ColumnSemanticRole:
        """Return the semantic role for one physical column."""

        return self.direct_get_column_metadata(table, column).semantic_role

    def direct_set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """Persist only the semantic role for one physical column."""

        self._direct_replace_column_metadata(
            table,
            column,
            semantic_role=semantic_role,
        )

    def direct_get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """Return the comparison-normalization profile for one column."""

        return self.direct_get_column_metadata(table, column).normalization_profile

    def direct_set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """Persist only the comparison-normalization profile for one column."""

        self._direct_replace_column_metadata(
            table,
            column,
            normalization_profile=normalization_profile,
        )

    def direct_get_comparison_column(
        self,
        table: str,
        column: str,
    ) -> str | None:
        """Return the derived comparison column, if any."""

        return self.direct_get_column_metadata(table, column).comparison_column

    def _default_normalized_identity_if_supported(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        spec = default_normalized_identity_spec(table, value_column)
        if spec is None:
            return None
        available = set(self.direct_get_column_headings(table))
        required = {
            spec.value_column,
            spec.identity_column,
            *spec.scope_columns,
        }
        return spec if required <= available else None

    def direct_get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """Return the database declaration for one normalized identity."""

        table_name, column_name = self._validated_column_metadata_target(
            table,
            value_column,
        )
        if NORMALIZED_IDENTITIES_TABLE not in set(self.direct_get_tables()):
            return self._default_normalized_identity_if_supported(
                table_name,
                column_name,
            )

        conn = self.get_connection()
        try:
            row = conn.execute(
                """
                SELECT
                  normalized_identity_table_name,
                  normalized_identity_value_column,
                  normalized_identity_key_column,
                  normalized_identity_normalization_profile,
                  normalized_identity_scope_columns_json,
                  normalized_identity_unique
                FROM normalized_identities
                WHERE normalized_identity_table_name = ?
                  AND normalized_identity_value_column = ?
                LIMIT 1;
                """,
                (table_name, column_name),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        spec = normalized_identity_from_db_values(*row)
        available = set(self.direct_get_column_headings(table_name))
        required = {
            spec.value_column,
            spec.identity_column,
            *spec.scope_columns,
        }
        if not required <= available:
            raise DatabaseIntegrityError(
                f"Normalized identity declaration for {table_name}.{column_name} "
                "references missing physical columns."
            )
        return spec

    def direct_iter_normalized_identity_specs(
        self,
    ) -> Iterator[NormalizedIdentitySpec]:
        """Yield every normalized identity supported by this database."""

        if NORMALIZED_IDENTITIES_TABLE not in set(self.direct_get_tables()):
            for spec in iter_normalized_identity_defaults():
                if spec.table not in set(self.direct_get_tables()):
                    continue
                supported = self._default_normalized_identity_if_supported(
                    spec.table,
                    spec.value_column,
                )
                if supported is not None:
                    yield supported
            return

        conn = self.get_connection()
        try:
            rows = list(
                conn.execute(
                    """
                    SELECT
                      normalized_identity_table_name,
                      normalized_identity_value_column,
                      normalized_identity_key_column,
                      normalized_identity_normalization_profile,
                      normalized_identity_scope_columns_json,
                      normalized_identity_unique
                    FROM normalized_identities
                    ORDER BY normalized_identity_table_name,
                             normalized_identity_value_column;
                    """
                )
            )
        finally:
            conn.close()
        for row in rows:
            spec = normalized_identity_from_db_values(*row)
            available = set(self.direct_get_column_headings(spec.table))
            required = {
                spec.value_column,
                spec.identity_column,
                *spec.scope_columns,
            }
            if not required <= available:
                raise DatabaseIntegrityError(
                    f"Normalized identity declaration for "
                    f"{spec.table}.{spec.value_column} references missing physical columns."
                )
            yield spec

    def direct_set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """Persist only the derived comparison column for one column."""

        self._direct_replace_column_metadata(
            table,
            column,
            comparison_column=comparison_column,
        )

    def direct_get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """Return the empty-value policy for one physical column."""

        return self.direct_get_column_metadata(table, column).empty_value_policy

    def direct_set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """Persist only the empty-value policy for one physical column."""

        self._direct_replace_column_metadata(
            table,
            column,
            empty_value_policy=empty_value_policy,
        )

    def direct_get_merge_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnMergePolicy:
        """Return the merge policy for one physical column."""

        return self.direct_get_column_metadata(table, column).merge_policy

    def direct_set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """Persist only the merge policy for one physical column."""

        self._direct_replace_column_metadata(
            table,
            column,
            merge_policy=merge_policy,
        )

    def direct_get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """Return the validation profile for one physical column."""

        return self.direct_get_column_metadata(table, column).validation_profile

    def direct_set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """Persist only the validation profile for one physical column."""

        self._direct_replace_column_metadata(
            table,
            column,
            validation_profile=validation_profile,
        )

    def _direct_replace_column_metadata(
        self,
        table: str,
        column: str,
        **changes: Any,
    ) -> None:
        """Replace selected policy fields while preserving the complete record."""

        current = self.direct_get_column_metadata(table, column)
        self.direct_set_column_metadata(replace(current, **changes))

    def direct_set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Persist text equality policy for one column.

        :param table:
        :param column:
        :param case_sensitive:
        :return:
        """
        table_name, column_name = self._validated_column_metadata_target(table, column)
        if type(case_sensitive) is not bool:
            raise InputIntegrityError("case_sensitive must be a bool")
        self._validate_normalized_identity_metadata(
            replace(
                self.direct_get_column_metadata(table_name, column_name),
                case_sensitive=case_sensitive,
            )
        )
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            raise DatabaseIntegrityError(
                "database has no column_metadata table; migrate the schema before storing column policy"
            )

        conn = self.get_connection()
        try:
            conn.execute(
                """
                INSERT INTO column_metadata (
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive
                ) VALUES (?, ?, ?)
                ON CONFLICT (
                  column_metadata_table_name,
                  column_metadata_column_name
                ) DO UPDATE SET
                  column_metadata_case_sensitive = excluded.column_metadata_case_sensitive;
                """,
                (table_name, column_name, int(case_sensitive)),
            )
            conn.commit()
        finally:
            conn.close()

    def direct_is_column_case_sensitive(self, table: str, column: str) -> bool:
        """Compatibility alias for :meth:`direct_get_case_sensitivity`."""

        return self.direct_get_case_sensitivity(table, column)

    def direct_set_column_case_sensitive(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """Compatibility alias for :meth:`direct_set_case_sensitivity`."""

        self.direct_set_case_sensitivity(table, column, case_sensitive)

    def _validated_column_metadata_target(self, table: str, column: str) -> tuple[str, str]:
        table_name = self._canonicalise_table_name_for_cache(table)
        column_name = str(column)
        headings = self.direct_get_column_headings(table_name)
        if column_name not in headings:
            raise InputIntegrityError(f"column {column_name!r} not found in table {table_name!r}")
        return table_name, column_name

    def _validated_column_metadata_input(self, metadata: ColumnMetadata) -> ColumnMetadata:
        if not isinstance(metadata, ColumnMetadata):
            raise InputIntegrityError("metadata must be a ColumnMetadata instance")
        table_name, column_name = self._validated_column_metadata_target(
            metadata.table,
            metadata.column,
        )
        if type(metadata.case_sensitive) is not bool:
            raise InputIntegrityError("ColumnMetadata.case_sensitive must be a bool")
        comparison_column = metadata.comparison_column
        if comparison_column is not None:
            comparison_column = str(comparison_column)
            if comparison_column not in set(self.direct_get_column_headings(table_name)):
                raise InputIntegrityError(
                    f"comparison column {comparison_column!r} not found in table {table_name!r}"
                )
        try:
            return ColumnMetadata(
                table=table_name,
                column=column_name,
                case_sensitive=metadata.case_sensitive,
                semantic_role=ColumnSemanticRole(metadata.semantic_role),
                normalization_profile=ColumnNormalizationProfile(
                    metadata.normalization_profile
                ),
                comparison_column=comparison_column,
                empty_value_policy=ColumnEmptyValuePolicy(metadata.empty_value_policy),
                merge_policy=ColumnMergePolicy(metadata.merge_policy),
                validation_profile=ColumnValidationProfile(
                    metadata.validation_profile
                ),
            )
        except ValueError as exc:
            raise InputIntegrityError(f"invalid column metadata policy: {exc}") from exc

    def _column_metadata_from_values(
        self,
        table: str,
        column: str,
        case_sensitive: Any,
        semantic_role: Any,
        normalization_profile: Any,
        comparison_column: Any,
        empty_value_policy: Any,
        merge_policy: Any,
        validation_profile: Any,
    ) -> ColumnMetadata:
        try:
            metadata = ColumnMetadata(
                table=table,
                column=column,
                case_sensitive=self._coerce_column_case_sensitivity(
                    case_sensitive,
                    table,
                    column,
                ),
                semantic_role=ColumnSemanticRole(str(semantic_role)),
                normalization_profile=ColumnNormalizationProfile(
                    str(normalization_profile)
                ),
                comparison_column=(
                    str(comparison_column) if comparison_column is not None else None
                ),
                empty_value_policy=ColumnEmptyValuePolicy(str(empty_value_policy)),
                merge_policy=ColumnMergePolicy(str(merge_policy)),
                validation_profile=(
                    ColumnValidationProfile(str(validation_profile))
                    if validation_profile is not None
                    else default_column_metadata(table, column).validation_profile
                ),
            )
        except ValueError as exc:
            raise DatabaseIntegrityError(
                f"invalid column metadata for {table}.{column}: {exc}"
            ) from exc
        return self._validated_column_metadata_input(metadata)

    @staticmethod
    def _column_metadata_db_values(metadata: ColumnMetadata) -> tuple[Any, ...]:
        return (
            metadata.table,
            metadata.column,
            int(metadata.case_sensitive),
            metadata.semantic_role.value,
            metadata.normalization_profile.value,
            metadata.comparison_column,
            metadata.empty_value_policy.value,
            metadata.merge_policy.value,
            metadata.validation_profile.value,
        )

    @staticmethod
    def _coerce_column_case_sensitivity(value: Any, table: str, column: str) -> bool:
        if value in (0, False, "0"):
            return False
        if value in (1, True, "1"):
            return True
        raise DatabaseIntegrityError(
            f"invalid case-sensitivity metadata for {table}.{column}: {value!r}"
        )

    def direct_get_declared_types_for_table(self, table: str) -> Dict[str, str]:
        """
        Return a mapping of column name -> declared type string for a table.

        :param table:
        :return:
        """
        cache = getattr(self, self._DECLARED_TYPES_CACHE_ATTR, None)
        if cache is None:
            cache = {}
            setattr(self, self._DECLARED_TYPES_CACHE_ATTR, cache)

        if table in cache:
            return cache[table]

        stmt = f"PRAGMA table_info({table})"
        conn = self.get_connection()
        c = conn.cursor()
        types: Dict[str, str] = {}
        for row in c.execute(stmt):
            # row: (cid, name, type, notnull, dflt_value, pk)
            name = row[1]
            decl = row[2] or ""
            types[name] = decl
        conn.close()

        cache[table] = types
        return types

    @staticmethod
    def _normalize_declared_type(declared_type: Any) -> str:
        """
        Bring the declared type into normal - comparable - form.

        :param declared_type:
        :return:
        """
        if declared_type is None:
            return ""
        dt = str(declared_type).strip().upper()
        if not dt:
            return ""
        # Strip constraints / extras and size spec (e.g. VARCHAR(255))
        parts = dt.split()
        if not parts:
            return ""
        dt = parts[0]
        dt = dt.split("(", 1)[0]
        return dt

    @classmethod
    def _sqlite_affinity(cls, declared_type: Any) -> str:
        """
        Return SQLite affinity bucket from a declared type string.

        :param declared_type:
        :return:
        """
        dt = cls._normalize_declared_type(declared_type)

        # SQLite affinity rules (simplified)
        if "INT" in dt:
            return "INTEGER"
        if any(x in dt for x in ("CHAR", "CLOB", "TEXT")):
            return "TEXT"
        if "BLOB" in dt:
            return "BLOB"
        if any(x in dt for x in ("REAL", "FLOA", "DOUB")):
            return "REAL"
        return "NUMERIC"

    # Todo: We can... possibly make this better with some protocol work
    def _coerce_db_value(
            self,
            value: Any,
            declared_type: Any) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Coerce a DB value based on declared type, conservatively.

        :param value:
        :param declared_type:
        :return:
        """
        if value is None:
            return None

        affinity = self._sqlite_affinity(declared_type)

        if affinity == "INTEGER":
            # Coerce common string/bytes representations of integers.
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return int(value)
            # SQLite is dynamically typed: even an INTEGER-affinity column may
            # legitimately contain REAL values (e.g. priority=2.25). Preserve
            # non-integer floats rather than stringifying them.
            if isinstance(value, float):
                return int(value) if value.is_integer() else float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?\d+", s2):
                        try:
                            return int(s2)
                        except Exception:
                            pass
                    # Preserve float-ish numeric strings in INTEGER columns
                    # (SQLite allows this; callers often expect numeric back).
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?\d+", s2):
                    try:
                        return int(s2)
                    except Exception:
                        pass
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        if affinity == "REAL":
            if isinstance(value, bool):
                return float(int(value))
            if isinstance(value, (int, float)):
                return float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        if affinity == "BLOB":
            if isinstance(value, memoryview):
                return bytes(value)
            if isinstance(value, (bytes, bytearray)):
                return bytes(value)
            # If the driver hands us something odd, keep it visible as text
            return force_unicode(value)

        if affinity == "NUMERIC":
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return int(value)
            if isinstance(value, float):
                return float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?\d+", s2):
                        try:
                            return int(s2)
                        except Exception:
                            pass
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?\d+", s2):
                    try:
                        return int(s2)
                    except Exception:
                        pass
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        # TEXT
        return force_unicode(value)

    @staticmethod
    def _coerce_untyped_value(value: Any) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Best-effort coercion when no declared type information is available.

        This is intentionally conservative:
        - Preserve ints/floats/bools as-is.
        - Decode bytes-like to text for visibility.
        - Convert memoryview to bytes.
        - Otherwise fall back to :func:`force_unicode`.

        Unlike :meth:`_coerce_db_value`, we do **not** parse numeric strings into
        numbers, because we have no declared affinity to justify doing so.

        :param value:
        :return:
        """
        if value is None:
            return None

        if isinstance(value, bool):
            # bool is a subtype of int; preserve intent
            return bool(value)

        if isinstance(value, int):
            return int(value)

        if isinstance(value, float):
            return float(value)

        if isinstance(value, memoryview):
            return bytes(value)

        if isinstance(value, (bytes, bytearray)):
            return force_unicode(bytes(value))

        return force_unicode(value)

    def _row_to_dict(
        self,
        *,
        table: Optional[str] = None,
        headings: Sequence[Any],
        row: Sequence[Any],
    ) -> Dict[Any, Any]:
        """
        Convert a DB row tuple into a dict.

        If ``table`` is provided, declared types are used for conservative
        casting (INTEGER stays int, etc.). If ``table`` is ``None``, a conservative
        best-effort conversion is applied that preserves numeric types.

        :param table:
        :param headings:
        :param row:
        :return:
        """
        declared_types = self.direct_get_declared_types_for_table(table) if table else {}
        result: Dict[Any, Any] = {}
        for i, head in enumerate(headings):
            val = row[i]
            # Preserve set-valued cells used by legacy 'set column' code paths
            if isinstance(val, set):
                result[head] = val
                continue
            # Some legacy code can yield non-string headings (e.g. set markers for set columns).
            if isinstance(head, set):
                result[head] = val
                continue
            if table:
                result[head] = self._coerce_db_value(val, declared_types.get(head, ""))
            else:
                result[head] = self._coerce_untyped_value(val)
        return result
