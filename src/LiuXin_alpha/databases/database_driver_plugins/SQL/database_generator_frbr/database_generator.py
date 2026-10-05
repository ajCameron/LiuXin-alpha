
"""
Build FRBR SQLite schemas from packaged table/trigger SQL and TOML link requests.

The staged builder creates tables, links and views, seeds column/identity/language
metadata and locks reference tables. Individual stages commit; the complete build
is neither atomic nor safely repeatable on a populated database.
"""

import sqlite3

# When viewing the database certain information needs to be all present and correct in one place. There are two options
# for this
# 1) Views - the sane, professional and reasonable solution. Views execute queries to generate the data requested on the
#            fly - ensuring it is always up to date and accurate. However as the queries need to be executed at run time
#            there will be a performance hit - especially when using slower storage.
# or there is the other way.
# 2) aggregate_tables - put all the information needed in one table which updates itself from other tables using
#                       quite a lot of triggers. Much faster. Needs a lot more code and results in a bloated database
# Hopefully you will have the option to choose.
from typing import Any

try:
    import tomllib  # py3.11+
except ImportError:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore
    except ImportError:  # pragma: no cover
        tomllib = None  # type: ignore

import difflib
import os
import pathlib
import pprint
import re
import sys
from copy import deepcopy
from typing import Optional

from LiuXin_alpha.constants import VERBOSE_DEBUG
from LiuXin_alpha.constants.paths import LiuXin_database_folder as __database_folder__
from LiuXin_alpha.databases.api import DatabaseGeneratorAPI
from LiuXin_alpha.databases.column_metadata import (
    COLUMN_METADATA_TABLE,
    column_options_to_json,
    infer_column_metadata,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.constants import (
    __ALLOWED_INTERLINK_TYPE_VAL_DICT__,
    __ALLOWED_INTRALINK_TYPE_VAL_DICT__,
    __INTERLINK_TABLE_CONSTRAINTS__,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import (
    SQLiteTableLinkingMixin,
)
from LiuXin_alpha.databases.db_types import (
    ALL_IDENTIFIER_ENTITY_TYPES,
    ENTITY_IDENTIFIER_SCHEMES_BY_TYPE,
    OBSERVED_ITEM_IDENTIFIER_SCHEMES,
    IdentifierEntityType,
)
from LiuXin_alpha.databases.normalized_identities import (
    NORMALIZED_IDENTITIES_TABLE,
    iter_normalized_identity_defaults,
    normalized_identity_db_values,
)
from LiuXin_alpha.metadata.constants.container_vocabularies import (
    GenreKind,
    IdentifierStatus,
    LabelKind,
    NoteFormat,
    NoteKind,
    NoteVisibility,
    SubjectKind,
    TitleKind,
)
from LiuXin_alpha.utils.language_tools import (
    plural_singular_mapper,
    singular_plural_mapper,
)
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_warning_print

# ---------------------------------------------------------------------------
# Interlink TOML helpers (types expansion / SQL emission)
# ---------------------------------------------------------------------------

def _parse_toml_bool(value: Any, *, default: bool = False) -> bool:
    """
    Accept bools, integers and common true/false text spellings.

    None returns default; nonzero integers are True. Unsupported objects/text raise
    TypeError. This permissive helper is used for symmetry, not strict nullability.

    Example:
        >>> _parse_toml_bool(" off ")
        False


    :param value: Parsed TOML value or supported convenience representation.
    :param default: Value returned unchanged for None.
    :return: Parsed boolean or the supplied default.
    """
    if value is None:
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return bool(value)

    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"true", "t", "yes", "y", "1", "on"}:
            return True
        if s in {"false", "f", "no", "n", "0", "off", ""}:
            return False

    raise TypeError(f"TOML boolean value is not parseable: {value!r}")


def _require_toml_bool(value: Any, *, context: str) -> bool:
    """
    Require an actual bool for strict TOML settings and identify invalid input.

    Example:
        >>> _require_toml_bool(False, context="nullable")
        False


    :param value: Parsed configuration value; integers and strings are rejected.
    :param context: Configuration location included in the TypeError message.
    :return: Original bool; non-bools raise TypeError.
    """
    if isinstance(value, bool):
        return value

    raise TypeError(
        f"{context} must be a TOML boolean (true/false), not {type(value).__name__}: {value!r}"
    )


def _sql_quote_literal(value: str) -> str:
    """
    Surround text with apostrophes and double embedded apostrophes.

    Example:
        >>> _sql_quote_literal("author")
        "'author'"


    :param value: String to embed in controlled generated SQL.
    :return: Escaped SQL text literal.
    """
    return "'" + value.replace("'", "''") + "'"


def _sql_in_list(values: list[str] | tuple[str, ...] | set[str]) -> str:
    """
    Sort and quote strings for a comma-separated SQL IN-list body.

    Example:
        >>> _sql_in_list(["b", "a"])
        "'a', 'b'"


    :param values: String collection; duplicates are retained for list/tuple inputs.
    :return: Joined quoted values without surrounding parentheses; empty input gives empty text.
    """
    return ", ".join(_sql_quote_literal(value) for value in sorted(values))


def _build_entity_identifier_type_check_sql() -> str:
    """
    Build a nullable CHECK constraint from the canonical identifier entity types.

    Example:
        The generated constraint accepts NULL or a known entity-type spelling.


    :return: Constraint fragment ending in a comma for insertion into a CREATE TABLE body.
    """
    allowed_entity_types = _sql_in_list(list(ALL_IDENTIFIER_ENTITY_TYPES))
    return f"""CONSTRAINT `entity_identifier_entity_type_valid`
    CHECK (
      `entity_identifier_entity_type` IS NULL
      OR `entity_identifier_entity_type` IN ({allowed_entity_types})
    ),"""


def _build_entity_identifier_scheme_check_sql() -> str:
    """
    Build the entity-type-dependent identifier scheme CHECK constraint.

    A null entity type or null scheme passes; otherwise a matching canonical pair is required.

    Example:
        An item-specific scheme must be paired with an entity type whose allowed set includes it.


    :return: Constraint fragment without a trailing comma.
    """
    clauses: list[str] = []

    for entity_type in IdentifierEntityType:
        allowed_schemes = ENTITY_IDENTIFIER_SCHEMES_BY_TYPE[entity_type]
        allowed_schemes_sql = _sql_in_list([scheme.value for scheme in allowed_schemes])
        clauses.append(
            f"(`entity_identifier_entity_type` = {_sql_quote_literal(entity_type.value)} "
            f"AND `entity_identifier_scheme` IN ({allowed_schemes_sql}))"
        )

    joined_clauses = "\n        OR ".join(clauses)

    return f"""CONSTRAINT `entity_identifier_scheme_valid_for_entity_type`
    CHECK (
      `entity_identifier_entity_type` IS NULL
      OR `entity_identifier_scheme` IS NULL
      OR (
        {joined_clauses}
      )
    )"""


def _build_observed_item_identifier_scheme_check_sql() -> str:
    """
    Build a nullable observed-item identifier scheme constraint.

    Example:
        The emitted IN-list uses OBSERVED_ITEM_IDENTIFIER_SCHEMES enum values.


    :return: CHECK fragment ending in a comma.
    """
    allowed_schemes_sql = _sql_in_list([scheme.value for scheme in OBSERVED_ITEM_IDENTIFIER_SCHEMES])
    return f"""CONSTRAINT `item_identifier_scheme_valid`
    CHECK (
      `item_identifier_scheme` IS NULL
      OR `item_identifier_scheme` IN ({allowed_schemes_sql})
    ),"""


def _build_simple_text_check_sql(
    *,
    constraint_name: str,
    column_name: str,
    allowed_values: list[str] | tuple[str, ...] | set[str],
) -> str:
    """
    Build a named nullable text CHECK from a sorted canonical vocabulary.

    Example:
        A title-kind constraint accepts NULL or one of the supplied title-kind values.


    :param constraint_name: Trusted unescaped constraint identifier.
    :param column_name: Trusted unescaped column identifier.
    :param allowed_values: Canonical strings quoted into the IN-list.
    :return: CHECK fragment ending in a comma.
    """
    allowed_values_sql = _sql_in_list(allowed_values)
    return f"""CONSTRAINT `{constraint_name}`
    CHECK (
      `{column_name}` IS NULL
      OR `{column_name}` IN ({allowed_values_sql})
    ),"""


def _substitute_canonical_vocabulary_placeholders(sql_text: str) -> str:
    """
    Replace known identifier/metadata vocabulary markers throughout SQL text.

    Unknown markers are left untouched; replacements are textual and not SQL-aware.

    Example:
        SQL without any known marker passes through unchanged.


    :param sql_text: Packaged SQL containing supported placeholder tokens.
    :return: SQL text containing canonical CHECK fragments.
    """
    replacements = {
        "__ENTITY_IDENTIFIER_ENTITY_TYPE_CHECK__": _build_entity_identifier_type_check_sql(),
        "__ENTITY_IDENTIFIER_SCHEME_BY_TYPE_CHECK__": _build_entity_identifier_scheme_check_sql(),
        "__ITEM_IDENTIFIER_SCHEME_CHECK__": _build_observed_item_identifier_scheme_check_sql(),
        "__TITLE_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="title_kind_valid",
            column_name="title_kind",
            allowed_values=tuple(member.value for member in TitleKind),
        ),
        "__NOTE_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="note_kind_valid",
            column_name="note_kind",
            allowed_values=tuple(member.value for member in NoteKind),
        ),
        "__NOTE_FORMAT_CHECK__": _build_simple_text_check_sql(
            constraint_name="note_format_valid",
            column_name="note_format",
            allowed_values=tuple(member.value for member in NoteFormat),
        ),
        "__NOTE_VISIBILITY_CHECK__": _build_simple_text_check_sql(
            constraint_name="note_visibility_valid",
            column_name="note_visibility",
            allowed_values=tuple(member.value for member in NoteVisibility),
        ),
        "__LABEL_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="label_kind_valid",
            column_name="label_kind",
            allowed_values=tuple(member.value for member in LabelKind),
        ),
        "__GENRE_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="genre_kind_valid",
            column_name="genre_kind",
            allowed_values=tuple(member.value for member in GenreKind),
        ),
        "__SUBJECT_KIND_CHECK__": _build_simple_text_check_sql(
            constraint_name="subject_kind_valid",
            column_name="subject_kind",
            allowed_values=tuple(member.value for member in SubjectKind),
        ),
        "__IDENTIFIER_STATUS_CHECK__": _build_simple_text_check_sql(
            constraint_name="identifier_status_valid",
            column_name="identifier_status",
            allowed_values=tuple(member.value for member in IdentifierStatus),
        ),
    }

    for placeholder, replacement in replacements.items():
        sql_text = sql_text.replace(placeholder, replacement)

    return sql_text


def collect_type_tables(allowed_types_by_link_table: dict[str, Optional[list[str]]]) -> dict[str, set[str]]:
    """
    Collect nonempty link type lists as deduplicated __types table values.

    Example:
        >>> collect_type_tables({"agent_work_links": ["author", "author"]})
        {'agent_work_links__types': {'author'}}


    :param allowed_types_by_link_table: Link table names mapped to optional label lists; falsy lists are omitted.
    :return: Reference-table-to-label-set mapping.
    """
    out: dict[str, set[str]] = {}
    for link_table, types in allowed_types_by_link_table.items():
        if not types:
            continue
        types_table = f"{link_table}__types"
        bucket = out.setdefault(types_table, set())
        bucket.update(types)
    return out


def emit_types_tables_sql(types_map: dict[str, set[str]]) -> list[str]:
    """
    Generate sorted, repeatable reference-table creation and seed inserts.

    Labels are escaped literals; table names are trusted internal identifiers.

    Example:
        One table with two labels produces one CREATE TABLE and two INSERT OR IGNORE statements.


    :param types_map: Reference-table names mapped to label sets.
    :return: Ordered SQL statement list.
    """
    stmts: list[str] = []
    for types_table in sorted(types_map):
        stmts.append(
            f"""CREATE TABLE IF NOT EXISTS `{types_table}` (
  `type` TEXT PRIMARY KEY
);"""
        )
        for t in sorted(types_map[types_table]):
            stmts.append(
                f"INSERT OR IGNORE INTO `{types_table}` (`type`) VALUES ({_sql_quote_literal(t)});"
            )
    return stmts


def _bcp47_common_variants() -> dict[str, list[str]]:
    """
    Return a small fresh mapping of common regional/script language variants.

    This is a convenience seed, not a complete BCP-47 registry or validator.

    Example:
        >>> "en-GB" in _bcp47_common_variants()["en"]
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



# Constraints on the interlink tables - DO NOT IMPORT - dynamically modified at run time


# http://stackoverflow.com/questions/4060221/how-to-reliably-open-a-file-in-the-same-directory-as-a-python-script

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
__database_file_path__ = os.path.join(__database_folder__, "LiuXin_main_database.db")

# Todo: rename comments to reviews

# Not all columns are needed in all interlink tables - this dictionary provides an easy way to specify the columns
# needed

# Todo: Identify and note the custom columns in database startup
# Todo: How do you want to handle story reviews of other works
# Todo: Need to handle deleting custom tables
# Todo: Might want to make a characters table - possibly as an example? Or an option you can turn on
# Todo: These would make a lot of sense to move to db constants or something like that
# See the docs - interlink_table_explanation for what each of these links should do

# See the docs - explanations for what these are


def create_new_database(connection: sqlite3.Connection) -> None:
    """
    Run the complete FRBR builder on the supplied empty SQLite connection.

    The caller chooses its database target and retains connection ownership. Build
    stages may commit before later failure.

    Example:
        Create an empty connection, call create_new_database(conn), then close it in
        the caller after any required inspection.


    :param connection: Open SQLite connection used and committed but not closed here.
    :return: None; builds and seeds schema objects.
    """
    conn = connection

    builder = SQLiteDatabaseGenerator(conn=conn)
    builder.run()



def get_main_table_sql_files() -> list[pathlib.Path]:
    """
    Discover lowercase .sql resources recursively under table_sql in deterministic path order.

    Raise NotADirectoryError if the required resource directory is absent.

    Example:
        The returned list can be stored on a builder before executing its schema stage.


    :return: Sorted pathlib.Path list; no SQL is read or executed here.
    """
    all_sql_files = []

    table_sql_folder = pathlib.Path(__folder__) / "table_sql"

    if not table_sql_folder.is_dir():
        raise NotADirectoryError(f"Expected 'table_sql' folder to be a directory: {table_sql_folder!s}")

    # pathlib.Path.walk() is only available on Python 3.12+; use os.walk for compatibility.
    for root, dirs, files in os.walk(table_sql_folder):
        dirs.sort()
        files.sort()

        for file in files:

            if os.path.splitext(file)[1] == ".sql":

                all_sql_files.append(pathlib.Path(root) / file)

    # Ensure deterministic application order across platforms/filesystems.
    return sorted(all_sql_files)




def get_trigger_sql_files() -> list[pathlib.Path]:
    """
    Discover lowercase .sql resources recursively under trigger_sql in deterministic path order.

    Raise NotADirectoryError if the required resource directory is absent.

    Example:
        The returned list can be stored on a builder before executing its schema stage.


    :return: Sorted pathlib.Path list; no SQL is read or executed here.
    """
    all_sql_files = []

    table_sql_folder = pathlib.Path(__folder__) / "trigger_sql"
    if not table_sql_folder.is_dir():
        raise NotADirectoryError(f"Expected 'trigger_sql' folder to be a directory: {table_sql_folder!s}")

    # pathlib.Path.walk() is only available on Python 3.12+; use os.walk for compatibility.
    for root, dirs, files in os.walk(table_sql_folder):
        dirs.sort()
        files.sort()

        for file in files:

            if os.path.splitext(file)[1] == ".sql":

                all_sql_files.append(pathlib.Path(root) / file)

    # Ensure deterministic application order across platforms/filesystems.
    return sorted(all_sql_files)



class SQLiteDatabaseGenerator(SQLiteTableLinkingMixin, DatabaseGeneratorAPI):
    """
    Own per-run build configuration while using a caller-owned SQLite connection.

    TOML supplies current relationships and enumerations. Per-instance copies avoid
    mutating legacy constant dictionaries; staged commits and final lock triggers mean
    run() is intended for new databases.

    Example:
        Instantiate with an empty connection, then run() to create the full FRBR schema.
    """

    ALLOWED_INTERLINK_TYPE_VAL_DICT = __ALLOWED_INTERLINK_TYPE_VAL_DICT__

    # Interlink constraints are derived entirely from the TOML spec at build time.
    INTERLINK_TABLE_CONSTRAINTS: dict[str, dict[str, str]] = {}

    # Additional optional columns permitted on interlink tables via TOML requested_columns
    INTERLINK_TABLE_COLUMN_NAME_DICT: dict[str, tuple] = {
        'priority': ('priority', 'INTEGER', 'DEFAULT 0'),
        'primary': ('primary', 'INTEGER', 'NULL DEFAULT 0'),
        'type': ('type', 'TEXT', 'NULL'),
        'origin': ('origin', 'TEXT', 'NULL'),
        'source': ('source', 'TEXT', 'NULL'),
        'policy': ('policy', 'TEXT', 'NULL'),
        'data': ('data', 'TEXT', 'NULL'),
        'index': ('index', 'TEXT', 'NULL'),
        'sequence_number': ('sequence_number', 'INTEGER', 'NULL'),
        'is_required': ('is_required', 'INTEGER', 'DEFAULT 1'),
    }

    def __init__(self, conn: sqlite3.Connection) -> None:
        """
        Retain the connection and initialize per-run schema/link configuration.

        Copy legacy constraints, but clear the allowed interlink type fallback so current
        FRBR builds use TOML declarations.

        Example:
            Construction alone neither queries the database nor loads SQL/TOML resources.


        :param conn: Open connection expected to target an empty database for run().
        :return: None; initializes builder state.
        """
        self.conn = conn

        self.main_tables = set()
        self.main_tables_sql_files: list[pathlib.Path] = []

        self.triggers_sql_files: list[pathlib.Path] = []

        self.interlink_tables = set()
        self.interlink_table_pairs = set()

        # Per-run interlink spec metadata (direction + declared link type)
        self.interlink_specs_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
        self.interlink_default_link_type: str = "many_to_many"

        # Per-run interlink column/type/nullable configuration derived from TOML
        self.interlink_requested_cols_by_table: dict[str, Any] = {}
        self.interlink_allowed_types_by_table: dict[str, Optional[list[str]]] = {}
        self.interlink_nullable_fks_by_table: dict[str, bool] = {}
        self.intralink_allowed_types_by_table: dict[str, Optional[list[str]]] = {}
        self.intralink_requested_cols_by_table: dict[str, Any] = {}
        self.intralink_nullable_fks_by_table: dict[str, bool] = {}
        self.intralink_symmetric_by_table: dict[str, bool] = {}
        self.intralink_symmetric_types_by_table: dict[str, Optional[list[str]]] = {}

        # Per-instance copies so we can add dynamic constraints based on TOML
        self.ALLOWED_INTERLINK_TYPE_VAL_DICT = deepcopy(__ALLOWED_INTERLINK_TYPE_VAL_DICT__)
        self.INTERLINK_TABLE_CONSTRAINTS = deepcopy(__INTERLINK_TABLE_CONSTRAINTS__)

        # FRBR generator is TOML-first: do not fall back to legacy hard-coded type enums.
        self.ALLOWED_INTERLINK_TYPE_VAL_DICT = {}
        self.intralink_tables = set()

    def run(self) -> None:
        """
        Execute the staged build, seed metadata and install reference-table locks.

        Load resources and preliminary link validation first; create main tables/triggers
        and languages before parsing/materializing links. Add configured aggregates, seed
        column/identity policies, store the version, then lock constants. Stages commit
        independently and failures leave earlier work present.

        Example:
            A successful run finishes with languages and database_version protected by
            write-blocking triggers.


        :return: None; leaves the caller connection open.
        """
        # 0) Load resources
        self.main_tables_sql_files = get_main_table_sql_files()
        self.triggers_sql_files = get_trigger_sql_files()

        # Todo: This is clearly not gonna be true at the moment
        #  - Check what we're being commanded to do is, in fact, sane
        self.sanity_check_interlink_inputs()

        # 1) Building the main tables from SQLite - the main tables have to be created by direct SQL execution
        self.create_main_tables()

        # 2) Build the triggers
        self.create_main_triggers()

        # 2.5) Seed constant tables (languages, etc.)
        self.seed_constant_tables()

        # Sanity: ensure we can introspect tables after main DDL
        _ = self.direct_get_tables()

        # 2) Creates the interlink tables - these are sufficiently similar that they are amenable to automated creation
        self.interlink_tables_pairs = self.get_requested_interlink_tables()
        for link_pair in self.interlink_tables_pairs:
            self.interlink_tables.add(self.get_interlink_name(link_pair))

        # Populate/override link-table constraints from the interlink spec (incl. link_type / direction)
        self.apply_interlink_constraints_from_spec()

        # 3) Validate the constraints which will be applied to the interlink tables
        self.validate_interlink_table_constraints()

        # 4) Validate that the allowed types request is valid
        self.validate_allowed_type_val_dict()

        # 5) Validate the table column requests - the columns that we want added to each of the link tables
        self.validate_interlink_table_column_requests()

        # 6) Build the interlink tables
        for table in self.interlink_tables_pairs:
            self.create_interlink_table(table[0], table[1], connection=self.conn)

        # 7) Read the intralink tables
        self.intralink_tables = self.get_requested_intralink_tables()

        # 8) Build the intralink tables
        self.sanity_check_intralink_inputs()
        for table in self.intralink_tables:
            self.create_intralink_table(table, connection=self.conn)

        # 9) Add the aggregate tables (here mostly views)
        self.create_aggregate_tables()

        # 10) Persist a complete policy for every physical column, including
        # dynamically generated interlink and intralink tables.
        self.seed_column_metadata()
        self.seed_normalized_identities()

        # 11) Set the version - so we can check the database and driver version used to build this database
        self.set_database_version()

        # 12) Lock constant tables so they are stable reference data.
        self.lock_constant_tables()


    # ------------------------------------------------------------------
    # Constant tables (seed + lock)
    # ------------------------------------------------------------------

    def seed_constant_tables(self) -> None:
        """
        Seed the currently supported constant reference table, languages.

        Example:
            This stage populates languages before lock_constant_tables installs its guards.


        :return: None; delegates language seeding and its commit.
        """
        self.seed_languages_table()

    def seed_column_metadata(self) -> None:
        """
        Infer and insert missing policies for every noninternal physical column.

        Use SQLite declared types, primary-key flags and FK metadata. Existing policy rows
        are preserved. If table enumeration fails or column_metadata is absent, return
        without writes; otherwise commit the insert batch.

        Example:
            Generated link columns receive policy rows alongside main-table columns.


        :return: None; seeds missing metadata when its catalog exists.
        """

        try:
            tables = self.direct_get_tables()
        except Exception:
            tables = set()
        if COLUMN_METADATA_TABLE not in tables:
            return

        rows: list[tuple[Any, ...]] = []
        for table in sorted(table for table in tables if not table.startswith("sqlite_")):
            quoted_table = table.replace("`", "``")
            table_info = list(
                self.conn.execute(f"PRAGMA table_info(`{quoted_table}`);")
            )
            foreign_keys = {
                str(row[3])
                for row in self.conn.execute(
                    f"PRAGMA foreign_key_list(`{quoted_table}`);"
                )
            }
            for column_info in table_info:
                column = str(column_info[1])
                metadata = infer_column_metadata(
                    table,
                    column,
                    str(column_info[2] or ""),
                    is_primary_key=bool(column_info[5]),
                    is_foreign_key=column in foreign_keys,
                )
                rows.append(
                    (
                        metadata.table,
                        metadata.column,
                        int(metadata.case_sensitive),
                        metadata.semantic_role.value,
                        metadata.normalization_profile.value,
                        metadata.comparison_column,
                        metadata.empty_value_policy.value,
                        metadata.merge_policy.value,
                        metadata.validation_profile.value,
                        column_options_to_json(metadata.formatting_options),
                        column_options_to_json(metadata.display_options),
                    )
                )

        self.conn.executemany(
            """
            INSERT OR IGNORE INTO column_metadata (
              column_metadata_table_name,
              column_metadata_column_name,
              column_metadata_case_sensitive,
              column_metadata_semantic_role,
              column_metadata_normalization_profile,
              column_metadata_comparison_column,
              column_metadata_empty_value_policy,
              column_metadata_merge_policy,
              column_metadata_validation_profile,
              column_metadata_formatting_options_json,
              column_metadata_display_options_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """.strip(),
            rows,
        )
        self.conn.commit()

    def seed_normalized_identities(self) -> None:
        """
        Insert default identity policies supported by all required physical columns.

        Skip absent policy tables or failed table discovery. Preserve existing records
        with INSERT OR IGNORE and commit when the table is available.

        Example:
            A declaration whose identity-key or scope column is missing is omitted.


        :return: None; seeds supported identity declarations.
        """

        try:
            tables = set(self.direct_get_tables())
        except Exception:
            tables = set()
        if NORMALIZED_IDENTITIES_TABLE not in tables:
            return

        table_columns = {
            table: {
                str(row[1])
                for row in self.conn.execute(
                    f"PRAGMA table_info(`{table.replace('`', '``')}`);"
                )
            }
            for table in tables
            if not table.startswith("sqlite_")
        }
        rows = [
            normalized_identity_db_values(spec)
            for spec in iter_normalized_identity_defaults()
            if spec.table in table_columns
            and {
                spec.value_column,
                spec.identity_column,
                *spec.scope_columns,
            }
            <= table_columns[spec.table]
        ]
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO normalized_identities (
              normalized_identity_table_name,
              normalized_identity_value_column,
              normalized_identity_key_column,
              normalized_identity_normalization_profile,
              normalized_identity_scope_columns_json,
              normalized_identity_unique
            ) VALUES (?, ?, ?, ?, ?, ?);
            """.strip(),
            rows,
        )
        self.conn.commit()


    def seed_languages_table(self) -> None:
        """
        Seed bundled ISO-639 language data with stable sorted IDs and convenience variants.

        Sort by bibliographic code; retain legacy language_code as that code. Choose the
        BCP-47 primary from two-letter, terminologic then bibliographic codes. Missing
        table/discovery/data imports become a no-op; nonempty seed batches are committed.

        Example:
            English is seeded with language_code eng and primary en before the table is locked.


        :return: None; preserves existing conflicting rows with INSERT OR IGNORE.
        """
        try:
            tables = self.direct_get_tables()
        except Exception:
            tables = set()
        if "languages" not in tables:
            return

        try:
            from LiuXin_alpha.utils.libraries.iso639 import data as iso639_data
        except Exception:
            iso639_data = []  # pragma: no cover

        variants_map = _bcp47_common_variants()

        # Deterministic IDs: stable ordering by ISO-639-2/B.
        rows: list[tuple] = []
        for idx, entry in enumerate(sorted(iso639_data or [], key=lambda d: str(d.get("iso639_2_b") or ""))):
            code_b = (entry.get("iso639_2_b") or "").strip().lower()
            if not code_b:
                continue

            code_t_raw = (entry.get("iso639_2_t") or "").strip().lower()
            code_t = code_t_raw if code_t_raw and code_t_raw != code_b else None

            code_1_raw = (entry.get("iso639_1") or "").strip().lower()
            code_1 = code_1_raw if code_1_raw else None

            name = (entry.get("name") or "").strip()
            if not name:
                name = code_b

            bcp47_primary = (code_1 or code_t or code_b).lower()

            variants = variants_map.get(bcp47_primary)
            variants_str = " ".join(sorted(set(variants))) if variants else None

            # Keep legacy columns coherent.
            # language_code == iso639_2_b (3-letter bibliographic)
            rows.append(
                (
                    int(idx) + 1,
                    name,
                    code_b,
                    code_1,
                    code_b,
                    code_t,
                    bcp47_primary,
                    variants_str,
                )
            )

        if not rows:
            return

        self.conn.executemany(
            """
            INSERT OR IGNORE INTO languages (
              language_id,
              language,
              language_code,
              language_iso639_1,
              language_iso639_2_b,
              language_iso639_2_t,
              language_bcp47_primary,
              language_bcp47_variants
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """.strip(),
            rows,
        )
        self.conn.commit()


    def lock_constant_tables(self) -> None:
        """
        Install write-blocking triggers on languages through the shared lock helper.

        Example:
            After this stage, inserting into languages aborts with the read-only message.


        :return: None; creates guards and commits.
        """
        self._lock_table_read_only("languages", message="languages is read-only")


    def _lock_table_read_only(self, table: str, *, message: str) -> None:
        """
        Install BEFORE INSERT/UPDATE/DELETE ABORT triggers and commit.

        IF NOT EXISTS preserves preexisting guards rather than replacing their message.

        Example:
            Locking languages uses three action-specific trigger names.


        :param table: Trusted existing table identifier interpolated into trigger DDL.
        :param message: Failure text escaped as a SQL literal.
        :return: None; leaves the connection open.
        """
        msg = _sql_quote_literal(message)
        for action in ("INSERT", "UPDATE", "DELETE"):
            trig = f"block_{action.lower()}_on_{table}"
            stmt = (
                f"""
                CREATE TRIGGER IF NOT EXISTS `{trig}`
                BEFORE {action} ON `{table}`
                BEGIN
                    SELECT RAISE(ABORT, {msg});
                END;
                """
            )
            self.conn.execute(stmt)
        self.conn.commit()


    # Todo: Add annotations table?
    # Todo: Add a unified tasks table?

    def direct_get_tables(self) -> set[str]:
        """
        Read all SQLite main-schema table names, including internal tables and excluding views.

        Example:
            sqlite_sequence may be present in this set when AUTOINCREMENT tables exist.


        :return: Set of table-name strings from sqlite_master.
        """

        stmt = "SELECT name FROM sqlite_master WHERE type = 'table';"
        processed_return = []
        for row in self.conn.execute(stmt):
            processed_return.append(row[0])

        return set(processed_return)

    def sanity_check_interlink_inputs(self) -> None:
        """
        Perform preliminary shape/column checks on the required interlink TOML file.

        Require a list of entries and a TOML parser. Incomplete/nondict entries and absent
        or all column selections are skipped here; later parsing performs fuller validation.
        For list selections, accept known or safe bespoke suffixes and check explicit bool
        nullability. The later parser rejects bespoke interlink suffixes.

        Example:
            A string nullable value paired with a list of columns raises TypeError here.


        :return: None; validates without creating database objects.
        """
        spec_path_toml = os.path.join(__folder__, "interlink_table_requests.toml")
        if not os.path.exists(spec_path_toml):
            raise FileNotFoundError(
                "Missing interlink spec: expected `interlink_table_requests.toml` in database_generator_frbr"
            )
        if tomllib is None:  # pragma: no cover
            raise RuntimeError(
                "interlink_table_requests.toml present but tomllib/tomli is unavailable in this Python runtime."
            )

        with open(spec_path_toml, "rb") as fh:
            data: dict[str, Any] = tomllib.load(fh)

        interlinks = data.get("interlinks", [])
        if not isinstance(interlinks, list):
            raise TypeError("TOML key `interlinks` must be a list")

        allowed_req_cols = {"priority", "primary", "type", "origin", "source", "policy", "data", "index", "sequence_number", "is_required", "nullable", "all"}

        for idx, entry in enumerate(interlinks):
            if not isinstance(entry, dict):
                continue
            if not (entry.get("left_table") or entry.get("left")):
                continue
            if not (entry.get("right_table") or entry.get("right")):
                continue

            requested_columns = entry.get("requested_columns")
            if requested_columns is None:
                requested_columns = entry.get("requested_cols") or entry.get("columns")

            if requested_columns is None:
                continue

            if isinstance(requested_columns, str):
                if requested_columns.strip().lower() != "all":
                    raise TypeError(
                        f"requested_columns for interlink {idx} must be 'all' or a list; got: {requested_columns!r}"
                    )
                continue

            if not isinstance(requested_columns, list):
                raise TypeError(f"requested_columns for interlink {idx} must be a list or string")

            for rc in requested_columns:
                rcs = str(rc).strip().lower()
                if rcs not in allowed_req_cols:
                    # Permit bespoke per-link metadata columns as long as the name is safe.
                    if not re.fullmatch(r"[a-z][a-z0-9_]*", rcs):
                        raise TypeError(f"Unknown requested_columns entry {rcs!r} in interlink {idx}")


            # Validate optional per-interlink nullable flag (must be a *real* TOML bool).
            if 'nullable' in entry and entry.get('nullable') is not None:
                _require_toml_bool(entry.get('nullable'), context=f"interlinks[{idx}].nullable")

    def sanity_check_intralink_inputs(self) -> None:
        """
        Require every requested self-link target to be in the known main-table set.

        Example:
            A requested self-link for an unknown table raises ValueError.


        :return: None when all targets are known.
        """
        for intralink_table in self.intralink_tables:
            if intralink_table not in self.main_tables:
                raise ValueError(f"Unknown intralink main table: {intralink_table!r}")

    def create_main_tables(self) -> None:
        """
        Read and execute configured table SQL files, committing completed work.

        Files without -- BREAK markers run as scripts; marked files accumulate chunks
        between pairs of markers for later single-statement execution. Unmatched trailing
        chunks are ignored. I/O errors become FileNotFoundError; SQLite operational/programming
        errors become TypeError containing the failing SQL. Known vocabulary placeholders are expanded before execution.

        Example:
            Populate main_tables_sql_files before this stage; multi-statement unmarked files use executescript.


        :return: None; creates objects and commits per script/marked statement.
        """
        conn = self.conn
        c = conn.cursor()

        statements = []

        for main_table_sql_file in self.main_tables_sql_files:

            try:
                sql_text = main_table_sql_file.read_text(encoding="utf-8")
            except OSError as e:
                raise FileNotFoundError(
                    f"Unable to read main-table SQL file: {main_table_sql_file!s}"
                ) from e

            sql_text = _substitute_canonical_vocabulary_placeholders(sql_text)
            test = sql_text.splitlines(keepends=True)

            # If the table file uses no BREAK markers, execute it as a script (supports multi-statement SQL).
            # This avoids silently skipping tables like metadata_additional/annotations.sql.
            if not any(line.startswith("-- BREAK") for line in test):
                statement = "".join(test)
                if statement.strip():
                    try:
                        conn.executescript(statement)
                    except sqlite3.OperationalError as e:
                        raise TypeError(f"\n{statement}\n: {e}")
                    except sqlite3.ProgrammingError as e:
                        raise TypeError(f"\n{statement}\n: {e}")
                    conn.commit()
                continue

            break_count = 0  # counting the number of break statements so far

            current_statement = """ """

            for line in test:

                if line[0:8] == "-- BREAK":
                    break_count += 1

                current_statement += line

                if break_count == 2:
                    break_count = 0
                    statements.append(current_statement)
                    current_statement = """ """

        for statement in statements:
            if VERBOSE_DEBUG:
                LiuXin_print(statement)
            try:
                c.execute(statement)
            except sqlite3.OperationalError as e:
                raise TypeError(f"\n{statement}\n: {e}")
            except sqlite3.ProgrammingError as e:
                raise TypeError(f"\n{statement}\n: {e}")

            conn.commit()

    def create_main_triggers(self) -> None:
        """
        Read and execute configured trigger SQL files, committing completed work.

        Files without -- BREAK markers run as scripts; marked files accumulate chunks
        between pairs of markers for later single-statement execution. Unmatched trailing
        chunks are ignored. I/O errors become FileNotFoundError; SQLite operational/programming
        errors become TypeError containing the failing SQL.

        Example:
            Populate triggers_sql_files before this stage; multi-statement unmarked files use executescript.


        :return: None; creates objects and commits per script/marked statement.
        """
        conn = self.conn
        c = conn.cursor()

        statements = []

        for main_table_sql_file in self.triggers_sql_files:

            try:
                with main_table_sql_file.open("r", encoding="utf-8") as main_tables_sqlite_file:
                    test = main_tables_sqlite_file.readlines()
            except OSError as e:
                raise FileNotFoundError(
                    f"Unable to read trigger SQL file: {main_table_sql_file!s}"
                ) from e

            # If the trigger file uses no BREAK markers, execute it as a script.
            # This is important for large trigger bundles (e.g. timestamp triggers) that are authored as multi-statement SQL.
            if not any(line.startswith("-- BREAK") for line in test):
                statement = "".join(test)
                if statement.strip():
                    try:
                        conn.executescript(statement)
                    except sqlite3.OperationalError as e:
                        raise TypeError(f"\n{statement}\n: {e}")
                    except sqlite3.ProgrammingError as e:
                        raise TypeError(f"\n{statement}\n: {e}")
                    conn.commit()
                continue

            break_count = 0  # counting the number of break statements so far

            current_statement = """ """

            for line in test:

                if line[0:8] == "-- BREAK":
                    break_count += 1

                current_statement += line

                if break_count == 2:
                    break_count = 0
                    statements.append(current_statement)
                    current_statement = """ """

        for statement in statements:
            if VERBOSE_DEBUG:
                LiuXin_print(statement)
            try:
                c.execute(statement)
            except sqlite3.OperationalError as e:
                raise TypeError(f"\n{statement}\n: {e}")
            except sqlite3.ProgrammingError as e:
                raise TypeError(f"\n{statement}\n: {e}")

            conn.commit()

    def get_requested_interlink_tables(self) -> set[tuple[str, str]]:
        """
        Parse required TOML relationships and record canonical pairs with per-pair options.

        Refresh known tables, normalize cardinality aliases and expand MARC/hash type labels.
        Defaults select priority and strict requested nullability, though the shared DDL
        builder currently keeps interlink FKs nullable. Reject unknown tables, duplicate pairs,
        invalid columns and strict bool settings. Malformed entries/self-links are warned
        and skipped. Forbidden warnings skip pairs; forbidden errors abort. A redundant FK
        pair is added to the returned set before allow_redundant_links=False skips its metadata,
        so that flag does not remove the returned pair.

        Example:
            A typed many_to_many entry records direction, columns and labels for later
            constraint materialization; no link table is created by parsing.


        :return: Set of sorted table-name pairs; also mutates main_tables and per-run spec maps.
        """
        c = self.conn.cursor()

        # Reset per-run spec metadata
        self.interlink_specs_by_pair = {}
        self.forbidden_interlink_pairs = {}

        # Refresh known tables
        stmt = "SELECT name FROM sqlite_master WHERE type='table';"
        current_tables = self.main_tables
        for row in c.execute(stmt):
            current_tables.add(row[0])

        spec_path_toml = os.path.join(__folder__, "interlink_table_requests.toml")
        if not os.path.exists(spec_path_toml):
            raise FileNotFoundError(
                "Missing interlink spec: expected `interlink_table_requests.toml` in database_generator_frbr"
            )
        if tomllib is None:  # pragma: no cover
            raise RuntimeError(
                "interlink_table_requests.toml present but tomllib/tomli is unavailable in this Python runtime."
            )

        with open(spec_path_toml, "rb") as fh:
            data: dict[str, Any] = tomllib.load(fh)

        if "allow_redundant_links" in data:
            allow_redundant_links = _require_toml_bool(
                data.get("allow_redundant_links"),
                context="allow_redundant_links",
            )
        else:
            allow_redundant_links = True

        if "warn_on_redundant_links" in data:
            warn_on_redundant_links = _require_toml_bool(
                data.get("warn_on_redundant_links"),
                context="warn_on_redundant_links",
            )
        else:
            warn_on_redundant_links = True

        # Forbidden interlinks: explicit pairs that must never be created
        forbidden = data.get("forbidden_interlinks", [])
        if forbidden is None:
            forbidden = []
        if not isinstance(forbidden, list):
            raise TypeError("TOML key `forbidden_interlinks` must be a list")

        for fent in forbidden:
            if not isinstance(fent, dict):
                LiuXin_warning_print("Warning - forbidden_interlinks entry is not a dict: " + repr(fent))
                continue
            fleft = fent.get("left_table") or fent.get("left") or fent.get("a")
            fright = fent.get("right_table") or fent.get("right") or fent.get("b")

            if not fleft or not fright:
                LiuXin_warning_print("Warning - forbidden_interlinks entry missing left_table/right_table: " + repr(fent))
                continue

            cf1 = self.match_to_table_name(str(fleft))
            cf2 = self.match_to_table_name(str(fright))
            if cf1 is None or cf2 is None:
                LiuXin_warning_print("Warning - forbidden_interlinks references unknown table: " + repr((fleft, fright)))
                continue

            if cf1 == cf2:
                LiuXin_warning_print("Warning - forbidden_interlinks contains self-link (ignored): " + repr((cf1, cf2)))
                continue

            fpair = tuple(sorted((cf1, cf2)))

            reason = str(fent.get("rationale") or fent.get("reason", ""))

            severity = str(fent.get("severity", "error")).lower().strip()

            if fpair in self.forbidden_interlink_pairs:
                prev = self.forbidden_interlink_pairs[fpair]
                if (prev.get("reason"), prev.get("severity")) != (reason, severity):
                    LiuXin_warning_print("Warning - duplicate forbidden_interlinks entry differs (keeping first): " + repr(fpair))
                continue

            self.forbidden_interlink_pairs[fpair] = {"reason": reason, "severity": severity}

        default_link_type_raw = data.get("default_link_type", "many_to_many")
        default_link_type = self._canonicalize_link_type(str(default_link_type_raw))
        allowed_link_types = {
            "many_to_many",
            "many_to_many_non_exclusive",
            "one_to_many",
            "many_to_one",
            "one_to_one",
        }
        if default_link_type not in allowed_link_types:
            raise TypeError(
                f"Unknown default_link_type {default_link_type_raw!r} (canonical: {default_link_type!r}). "
                f"Allowed: {sorted(allowed_link_types)!r}"
            )
        self.interlink_default_link_type = default_link_type

        interlinks = data.get("interlinks", [])
        if not isinstance(interlinks, list):
            raise TypeError("TOML key `interlinks` must be a list")

        allowed_req_cols = {"priority", "primary", "type", "origin", "source", "policy", "data", "index", "sequence_number", "is_required", "nullable", "all"}

        # Build a set of unordered FK edges to warn about redundant interlinks
        fk_pairs: set[tuple[str, str]] = set()
        for table in sorted(current_tables):
            try:
                for fk in c.execute(f"PRAGMA foreign_key_list(`{table}`);"):
                    ref_table = fk[2]
                    if isinstance(ref_table, str) and ref_table in current_tables:
                        fk_pairs.add(tuple(sorted((table, ref_table))))
            except sqlite3.OperationalError:
                continue

        link_tables: set[tuple[str, str]] = set()

        # Track canonical unordered interlink pairs seen in the TOML spec; duplicates are always an error.
        seen_interlink_pairs: dict[tuple[str, str], int] = {}

        for idx, entry in enumerate(interlinks):
            if not isinstance(entry, dict):
                LiuXin_warning_print(f"Warning - interlink spec entry {idx} is not a table (dict): {entry!r}")
                continue

            left = entry.get("left_table") or entry.get("left") or entry.get("table1") or entry.get("a")
            right = entry.get("right_table") or entry.get("right") or entry.get("table2") or entry.get("b")

            if not left or not right:
                LiuXin_warning_print(f"Warning - interlink spec entry {idx} missing left_table/right_table: {entry!r}")
                continue

            link_type = entry.get("link_type") or entry.get("link") or entry.get("cardinality") or default_link_type

            link_type_canon = self._canonicalize_link_type(str(link_type))
            allowed_link_types = {
                "many_to_many",
                "many_to_many_non_exclusive",
                "one_to_many",
                "many_to_one",
                "one_to_one",
            }
            if link_type_canon not in allowed_link_types:
                raise TypeError(
                    f"Unknown link_type {link_type!r} (canonical: {link_type_canon!r}) in interlinks[{idx}]. "
                    f"Allowed: {sorted(allowed_link_types)!r}"
                )

            # requested_columns
            requested_columns = entry.get("requested_columns")
            if requested_columns is None:
                requested_columns = entry.get("requested_cols") or entry.get("columns")

            nullable_fks = False
            requested_cols: Any = {"priority"}  # default

            if requested_columns is not None:
                if isinstance(requested_columns, str):
                    if requested_columns.strip().lower() == "all":
                        requested_cols = "all"
                    else:
                        raise TypeError(f"requested_columns must be 'all' or a list; got: {requested_columns!r}")
                elif isinstance(requested_columns, list):
                    lowered = [str(x).strip().lower() for x in requested_columns]
                    for rc in lowered:
                        if rc not in allowed_req_cols:
                            raise TypeError(f"Unknown requested_columns entry {rc!r} in interlink {idx}")
                    if "nullable" in lowered:
                        nullable_fks = True
                        lowered = [x for x in lowered if x != "nullable"]
                    if "all" in lowered:
                        requested_cols = "all"
                    else:
                        requested_cols = set(lowered) if lowered else set()
                else:
                    raise TypeError(f"requested_columns must be a list or string; got: {type(requested_columns)}")

            # Support an explicit per-interlink `nullable` key (preferred), in addition to the
            # legacy `requested_columns = [..., 'nullable', ...]` sentinel.
            if 'nullable' in entry and entry.get('nullable') is not None:
                nullable_fks = _require_toml_bool(entry.get('nullable'), context=f"interlinks[{idx}].nullable")
            # allowed_types: optional explicit allowed values for the type column
            allowed_types = entry.get("allowed_types") or entry.get("types")
            allowed_types_list: Optional[list[str]] = None
            if allowed_types is not None:
                if not isinstance(allowed_types, list):
                    raise TypeError(f"allowed_types must be a list of strings; got: {allowed_types!r}")

                raw_items = [str(x).strip() for x in allowed_types if str(x).strip()]

                # Expand special placeholders.
                expanded: list[str] = []
                for item in raw_items:
                    key = item.strip()
                    if key.lower() == "insert_marc_roles":
                        try:
                            from LiuXin_alpha.constants.marc_relator_dicts import (
                                MARC_ROLE_DESC,
                            )
                        except Exception as e:  # pragma: no cover
                            raise RuntimeError("Unable to import MARC_ROLE_DESC for insert_marc_roles expansion") from e
                        expanded.extend(sorted(MARC_ROLE_DESC.keys()))
                        continue
                    if key.lower() == "insert_known_hash_types":
                        import hashlib
                        expanded.extend(sorted(hashlib.algorithms_guaranteed))
                        continue
                    if key.lower().startswith("insert_"):
                        raise TypeError(f"Unknown types placeholder {key!r} in interlink {idx}")
                    expanded.append(key)

                # De-duplicate while preserving order (deterministic for MARC roles because we sort that expansion).
                seen: set[str] = set()
                allowed_types_list = []
                for v in expanded:
                    if v in seen:
                        continue
                    seen.add(v)
                    allowed_types_list.append(v)

                if not allowed_types_list:
                    raise TypeError(f"allowed_types list is empty after expansion in interlink {idx}")

            # If the spec defines an explicit enumeration for the type column, ensure the type column is present.
            if allowed_types_list is not None and requested_cols != "all":
                try:
                    if isinstance(requested_cols, set) and "type" not in requested_cols:
                        requested_cols.add("type")
                except Exception:
                    pass

            # Strict guardrail: if non-exclusive M2M explicitly declares requested columns,
            # `type` must be included by the user (or use requested_columns='all').
            if link_type_canon == "many_to_many_non_exclusive" and requested_columns is not None:
                has_type = requested_cols == "all" or (
                    isinstance(requested_cols, (set, list, tuple)) and "type" in requested_cols
                )
                if not has_type:
                    raise TypeError(
                        "many_to_many_non_exclusive requires requested_columns to include 'type' "
                        "(or set requested_columns='all')."
                    )


            c_table1 = self.match_to_table_name(str(left))
            c_table2 = self.match_to_table_name(str(right))

            if c_table1 is None:
                raw_left = str(left).strip()
                if raw_left in current_tables:
                    c_table1 = raw_left
            if c_table2 is None:
                raw_right = str(right).strip()
                if raw_right in current_tables:
                    c_table2 = raw_right

            if c_table1 is None or c_table2 is None:
                raw_left = str(left).strip()
                raw_right = str(right).strip()
                missing = []
                if c_table1 is None:
                    missing.append(raw_left)
                if c_table2 is None:
                    missing.append(raw_right)

                known = sorted(current_tables)
                sugg_lines = []
                for miss in missing:
                    matches = difflib.get_close_matches(miss, known, n=5, cutoff=0.6)
                    if matches:
                        sugg_lines.append(f"  - {miss!r} -> {matches!r}")
                msg = (
                    f"Unknown table referenced in interlinks[{idx}] in interlink_table_requests.toml: "
                    f"{raw_left!r} ↔ {raw_right!r}. Missing: {missing!r}."
                )
                if sugg_lines:
                    msg += "\nDid you mean one of:\n" + "\n".join(sugg_lines)
                raise ValueError(msg)

            if c_table1 == c_table2:
                LiuXin_warning_print("Warning - self-link requested (ignored): " + repr((c_table1, c_table2)))
                continue

            current_pair = tuple(sorted((c_table1, c_table2)))

            # Hard fail if the pair is explicitly forbidden
            forbid = getattr(self, "forbidden_interlink_pairs", {}).get(current_pair)
            if forbid is not None:
                severity = str(forbid.get("severity", "error")).lower().strip()
                reason = str(forbid.get("reason", ""))
                msg = ("Forbidden interlink requested: " + repr(current_pair))
                if reason:
                    msg += "\nReason: " + reason
                if severity in ("warn", "warning"):
                    LiuXin_warning_print("Warning - " + msg)
                    continue
                raise TypeError(msg)

            first_idx = seen_interlink_pairs.get(current_pair)
            if first_idx is not None:
                raise ValueError(
                    f"Duplicate interlink pair {current_pair!r} in interlink_table_requests.toml: "
                    f"interlinks[{first_idx}] and interlinks[{idx}]. "
                    f"Please merge them into a single entry."
                )
            seen_interlink_pairs[current_pair] = idx

            link_tables.add(current_pair)

            if current_pair in fk_pairs:
                if warn_on_redundant_links:
                    LiuXin_warning_print(
                        "Warning - interlink request duplicates an existing FK edge: " + repr(current_pair)
                    )
                if not allow_redundant_links:
                    continue

            if (c_table1 not in current_tables) or (c_table2 not in current_tables):
                LiuXin_warning_print("Warning - interlink request references non-main table: " + repr((left, right)))
                continue

            # Record spec metadata (direction + link_type + requested columns + allowed types + nullable flag)
            self.interlink_specs_by_pair[current_pair] = {
                "left_table": c_table1,
                "right_table": c_table2,
                "link_type": str(link_type_canon),
                "requested_cols": requested_cols,
                "nullable_fks": bool(nullable_fks),
                "allowed_types": allowed_types_list,
            }

        return link_tables

    @staticmethod
    def _canonicalize_link_type(link_type: str) -> str:
        """
        Normalize separators and recognized cardinality aliases without rejecting unknown text.

        Example:
            >>> SQLiteDatabaseGenerator._canonicalize_link_type("M2M")
            'many_to_many'


        :param link_type: Cardinality spelling to normalize.
        :return: Canonical alias, normalized unknown spelling, or many_to_many for None.
        """

        if link_type is None:
            return "many_to_many"
        s = str(link_type).strip().lower()
        s = s.replace("-", "_")
        s = re.sub(r"\s+", "_", s)

        aliases = {
            "many_many": "many_to_many",
            "many_to_many": "many_to_many",
            "m2m": "many_to_many",
            "many_many_non_exclusive": "many_to_many_non_exclusive",
            "many_to_many_non_exclusive": "many_to_many_non_exclusive",
            "m2m_non_exclusive": "many_to_many_non_exclusive",
            "one_many": "one_to_many",
            "one_to_many": "one_to_many",
            "o2m": "one_to_many",
            "many_one": "many_to_one",
            "many_to_one": "many_to_one",
            "m2o": "many_to_one",
            "one_one": "one_to_one",
            "one_to_one": "one_to_one",
            "o2o": "one_to_one",
        }
        return aliases.get(s, s)

    def apply_interlink_constraints_from_spec(self) -> None:
        """
        Translate parsed relationship direction/cardinality into shared builder settings.

        Typed many_to_many becomes nonexclusive; explicit nonexclusive mode ensures type
        is selected. Fill requested-column, allowed-type and nullability maps per link.
        Pairs lacking metadata use the configured default cardinality and priority column.

        Example:
            A typed role relationship gets internal many_many_non_exclusive constraints.


        :return: None; updates constraint and per-link option mappings.
        """
        default_link_type = getattr(self, "interlink_default_link_type", "many_to_many")

        for pair in getattr(self, "interlink_tables_pairs", set()):
            table_a, table_b = pair

            spec = getattr(self, "interlink_specs_by_pair", {}).get(pair)
            left_table = spec.get("left_table") if spec else table_a
            right_table = spec.get("right_table") if spec else table_b
            link_type_raw = spec.get("link_type") if spec else default_link_type

            link_type = self._canonicalize_link_type(link_type_raw)
            # Map spec language -> internal constraint language
            if link_type == "many_to_many":
                # If the spec requests a `type` column, this is almost always a role-style mapping.
                # Use many_many_non_exclusive so (A,B,type) is unique while allowing multiple roles.
                if spec is not None:
                    rc = spec.get("requested_cols")
                    has_type = (rc == "all") or (isinstance(rc, (set, list, tuple)) and ("type" in rc))
                else:
                    has_type = False

                if has_type:
                    primary_table, secondary_table, internal_link_type = left_table, right_table, "many_many_non_exclusive"
                else:
                    primary_table, secondary_table, internal_link_type = left_table, right_table, "many_many"


            elif link_type == "many_to_many_non_exclusive":
                # Explicit role-style many-to-many: allow multiple (A,B) links as long as `type` differs.
                # This requires a `type` column; ensure it is requested.
                primary_table, secondary_table, internal_link_type = left_table, right_table, "many_many_non_exclusive"
                if spec is not None:
                    rc = spec.get("requested_cols")
                    if rc is None:
                        spec["requested_cols"] = {"priority", "type"}
                    elif rc != "all" and isinstance(rc, (set, list, tuple)):
                        if "type" not in rc:
                            spec["requested_cols"] = set(rc) | {"type"}

            elif link_type == "one_to_many":
                primary_table, secondary_table, internal_link_type = left_table, right_table, "one_many"
            elif link_type == "many_to_one":
                primary_table, secondary_table, internal_link_type = left_table, right_table, "many_one"
            elif link_type == "one_to_one":
                primary_table, secondary_table, internal_link_type = left_table, right_table, "one_one"
            else:
                raise TypeError(
                    f"Unknown interlink link_type {link_type_raw!r} (canonical: {link_type!r}) for pair {pair!r}."
                )

            link_table_name = self.get_interlink_name([table_a, table_b])

            self.INTERLINK_TABLE_CONSTRAINTS[link_table_name] = {
                "primary": primary_table,
                "secondary": secondary_table,
                "link_type": internal_link_type,
            }

            # Materialise TOML per-link-table options
            if spec is not None:
                self.interlink_requested_cols_by_table[link_table_name] = spec.get("requested_cols", {"priority"})
                self.interlink_nullable_fks_by_table[link_table_name] = bool(spec.get("nullable_fks", False))
                self.interlink_allowed_types_by_table[link_table_name] = spec.get("allowed_types")
            else:
                self.interlink_requested_cols_by_table[link_table_name] = {"priority"}
                self.interlink_nullable_fks_by_table[link_table_name] = False
                self.interlink_allowed_types_by_table[link_table_name] = None

    def extract_main_tables(self, interlink_request: str) -> Optional[list[str]]:
        """
        Parse a legacy name-name_ prefix and resolve its two table names.

        No regex match returns None. Unresolved names are not filtered before sorting,
        so matching malformed requests may raise TypeError.

        Example:
            A matching works-agents_ request resolves and sorts those known tables.


        :param interlink_request: Legacy request string matched from its beginning.
        :return: Sorted pair of resolved names, or None for a nonmatching request.
        """
        input_pattern = re.compile(r"\s*([0-9a-zA-Z_]+)-([0-9a-zA-Z]+)_")
        tables = input_pattern.match(interlink_request)

        if tables is None:
            return None

        i_table1 = tables.group(1)
        i_table2 = tables.group(2)
        c_table1 = self.match_to_table_name(i_table1)
        c_table2 = self.match_to_table_name(i_table2)

        return sorted([c_table1, c_table2])

    def validate_interlink_table_constraints(self) -> None:
        """
        Require a constraint-map entry for every requested link-table name.

        Example:
            A missing entry raises KeyError with the known link-table listing.


        :return: None when all requested tables have entries.
        """
        for link_table in self.interlink_tables:
            if link_table not in self.INTERLINK_TABLE_CONSTRAINTS:
                raise KeyError(self.__constraint_not_found_error(link_table))


    def validate_allowed_type_val_dict(self) -> None:
        """
        Require explicit allowed-type values to be lists containing only strings.

        None means free-form type values and is accepted; this validator does not reject
        an empty list.

        Example:
            A tuple of labels fails validation even though its contents are strings.


        :return: None when all stored enumerations match the required representation.
        """
        for link_table in self.interlink_tables:
            allowed = self.interlink_allowed_types_by_table.get(link_table)
            if allowed is None:
                continue
            if not isinstance(allowed, list):
                raise TypeError(f"allowed_types for {link_table} must be a list[str], got: {type(allowed)}")
            for v in allowed:
                if not isinstance(v, str):
                    raise TypeError(f"allowed_types entry for {link_table} must be a string, got: {type(v)}: {v!r}")

    def validate_interlink_table_column_requests(self) -> None:
        """
        Validate normalized per-link column sets against the supported optional columns.

        Accept None, all, or a set; nullable is a legacy no-op marker. Unknown suffixes
        and other collection representations raise TypeError.

        Example:
            A list must be normalized to a set by parsing before this validation stage.


        :return: None when all requested column selections are valid.
        """
        allowed_cols = set(self.INTERLINK_TABLE_COLUMN_NAME_DICT.keys())
        for link_table in self.interlink_tables:
            req = self.interlink_requested_cols_by_table.get(link_table, {"priority"})
            if req is None or req == "all":
                continue
            if not isinstance(req, set):
                raise TypeError(f"requested_columns for {link_table} must be a set or 'all', got: {type(req)}")
            for cr in req:
                # Legacy no-op (nullable is now derived from TOML key `nullable`)
                if cr == "nullable":
                    continue
                if cr not in allowed_cols:
                    raise TypeError(
                        f"requested column {cr!r} not valid for {link_table} (allowed: {sorted(allowed_cols)})"
                    )
    def materialize_interlink_type_reference_tables(self) -> None:
        """
        Create and seed nonempty type-reference tables from the stored link options.

        This helper does not install link guard triggers; those are installed by the
        link-creation path. Return without a commit when no tables are requested.

        Example:
            Two configured type lists create their sorted __types tables and missing labels.


        :return: None; commits the emitted reference-table statements.
        """
        types_map = collect_type_tables(self.interlink_allowed_types_by_table)
        if not types_map:
            return
        c = self.conn.cursor()
        for stmt in emit_types_tables_sql(types_map):
            c.execute(stmt)
        self.conn.commit()

    def __constraint_not_found_error(self, link_table: str) -> str:
        """
        Describe a missing constraint entry with the current requested table set.

        Example:
            The missing link name appears before the pretty-printed known-table set.


        :param link_table: Requested link table with no constraint entry.
        :return: Multiline diagnostic text.
        """
        err_msg = [
            "{} not found in the known interlink tables".format(link_table),
            "\n{}\n".format(pprint.pformat(self.interlink_tables)),
        ]
        return "\n".join(err_msg)

    @staticmethod
    def get_interlink_name(link_pair: list[str]) -> str:
        """
        Sort table names, singularize the first two and compose the link-table name.

        Example:
            >>> SQLiteDatabaseGenerator.get_interlink_name(["works", "agents"])
            'agent_work_links'


        :param link_pair: Pair of main-table names used to derive a conventional link name.
        :return: Conventional link name; fewer than two inputs raise IndexError.
        """
        link_pair = sorted(link_pair)
        return "{}_{}_links".format(plural_singular_mapper(link_pair[0]), plural_singular_mapper(link_pair[1]))

    def get_interlink_constraint(self, link_pair: list[str]) -> dict[str, str]:
        """
        Return the stored mutable constraint mapping for a derived link name.

        Example:
            A missing generated name raises KeyError rather than a fallback constraint.


        :param link_pair: Pair of main-table names used to derive a conventional link name.
        :return: Existing constraint dictionary; no copy is made.
        """
        link_table_name = self.get_interlink_name(link_pair)
        return self.INTERLINK_TABLE_CONSTRAINTS[link_table_name]

    def match_to_table_name(self, candidate_name: str) -> Optional[str]:
        """
        Resolve a lowercase exact or pluralized name against main_tables.

        This performs no edit-distance matching or whitespace stripping.

        Example:
            With works in main_tables, the singular work can resolve through the pluralizer.


        :param candidate_name: Candidate converted to text before lookup/pluralization.
        :return: Known table name or None.
        """
        name_local = deepcopy(candidate_name)
        name_local = six_unicode(name_local)

        candidate_name = name_local.lower()

        if candidate_name in self.main_tables:
            return candidate_name

        candidate_name = singular_plural_mapper(name_local)
        candidate_name = candidate_name.lower()

        if candidate_name in self.main_tables:
            return candidate_name
        else:
            return None

    def create_interlink_table(self, table1: str, table2: str, connection: sqlite3.Connection) -> None:
        """
        Create an expected link from stored options, commit, then add optional type guards.

        Reject link names absent from interlink_tables. Type reference-table setup commits
        separately; a later guard failure can leave the link table created.

        Example:
            Creating an enumerated role link produces both the link and its __types guards.


        :param table1: First known main-table name.
        :param table2: Second known main-table name.
        :param connection: Open SQLite connection used and committed but not closed here.
        :return: None; mutates the supplied connection.
        """

        table1_l = deepcopy(table1)
        table1_l = six_unicode(table1_l)
        table2_l = deepcopy(table2)
        table2_l = six_unicode(table2_l)
        conn = connection
        c = conn.cursor()

        table_name, column_name = self.get_interlink_table_name(table1, table2)

        requested_cols: Any = self.interlink_requested_cols_by_table.get(table_name, {"priority"})

        if requested_cols is None:
            requested_cols = set()

        allowed_types = self.interlink_allowed_types_by_table.get(table_name)
        nullable_fks = self.interlink_nullable_fks_by_table.get(table_name, False)


        # Check that the table we're building is actually expected
        if table_name not in self.interlink_tables:
            raise ValueError(f"Unexpected interlink table_name {table_name!r} (not in known interlink tables)")

        # Up to two tables need to be constructed, and one needs to be populated
        # If required, an allowed_type_table will be constructed and populated from the list of statements already
        # created
        att_table_sqlite_list = self.build_interlink_table_sqlite(
            table1_l,
            table2_l,
            requested_cols=requested_cols,
            allowed_types=None,
            nullable_fks=nullable_fks,
        )
        for att_table_build_stmt in att_table_sqlite_list:
            if VERBOSE_DEBUG:
                LiuXin_print(att_table_build_stmt)
            c.execute(att_table_build_stmt)

        conn.commit()

        # If this interlink defines a permitted enumeration for the type column, materialise it into a
        # dedicated reference table `{interlink_table}__types` and enforce it via lightweight triggers.
        if allowed_types is not None:
            self.direct_create_interlink_types_reference_table(
                interlink_table_name=table_name,
                interlink_column_base=column_name,
                allowed_types=allowed_types,
                connection=conn,
            )


    
    def direct_create_interlink_types_reference_table(
        self,
        interlink_table_name: str,
        interlink_column_base: str,
        allowed_types: list[str],
        connection: sqlite3.Connection,
    ) -> None:
        """
        Use the shared __types table seeding and insert/update guard implementation.

        Example:
            Non-NULL labels absent from the reference table are rejected after guard installation.


        :param interlink_table_name: Trusted existing link-table name.
        :param interlink_column_base: Trusted prefix of the link type column.
        :param allowed_types: Label strings inserted with bound parameters.
        :param connection: Open SQLite connection used and committed but not closed here.
        :return: None; the shared helper commits without closing.
        """

        return super().direct_create_interlink_types_reference_table(
            interlink_table_name=interlink_table_name,
            interlink_column_base=interlink_column_base,
            allowed_types=allowed_types,
            connection=connection,
        )


    # this section deals with adding the intralink tables
        # examples might be authors and their pseudonames.
        # The format is always primary is type of secondary
    def create_intralink_table(self, table_name: str, connection: sqlite3.Connection) -> None:
        """
        Build one configured self-link with nullable endpoints, then optional type guards.

        The build forces nullable FKs regardless of the parsed nullable setting to support
        blank-row construction. Commit DDL before separately committed type-guard setup.

        Example:
            A configured symmetric self-link gets ordering guards but still allows blank
            endpoint placeholders.


        :param table_name: Main table whose self-link schema is requested.
        :param connection: Open SQLite connection used and committed but not closed here.
        :return: None; mutates and commits the supplied connection.
        """

        conn = connection
        c = conn.cursor()

        name_local = deepcopy(table_name)
        name_local = six_unicode(name_local)

        # TOML-derived configuration for this intralink table
        requested_cols: Any = self.intralink_requested_cols_by_table.get(name_local, {"type"})
        allowed_types = self.intralink_allowed_types_by_table.get(name_local)
        # NOTE:
        # DriverWrapper.get_blank_row() creates placeholder rows by inserting ONLY the scratch column first,
        # and then updating FK/type/etc. afterwards.
        # Until we revisit that design, intralink FK columns must stay nullable or blank-row creation breaks.
        nullable_fks = True
        symmetric = self.intralink_symmetric_by_table.get(name_local, False)
        symmetric_types = self.intralink_symmetric_types_by_table.get(name_local)

        sql_list = super().direct_build_intralink_table_sql(
            name_local,
            allowed_types=allowed_types,
            requested_cols=requested_cols,
            nullable_fks=nullable_fks,
            symmetric=symmetric,
            symmetric_types=symmetric_types,
            use_reference_types_table=True,
        )

        for stmt in sql_list:
            if VERBOSE_DEBUG:
                LiuXin_print(stmt)
            c.execute(stmt)
        conn.commit()

        # If this intralink defines a permitted enumeration for the type column, materialise it into a
        # dedicated reference table `{intralink_table}__types` and enforce it via lightweight triggers.
        if allowed_types is not None:
            target_table_name = self.match_to_table_name(name_local) or name_local
            target_row_name = plural_singular_mapper(target_table_name)
            row_name = f"{target_row_name}_{target_row_name}_intralink"
            intralink_table_name = f"{row_name}s"
            self.direct_create_interlink_types_reference_table(
                interlink_table_name=intralink_table_name,
                interlink_column_base=row_name,
                allowed_types=allowed_types,
                connection=conn,
            )

    def direct_build_intralink_table_sql(self, name: str, **kwargs: Any) -> list[str]:
        """
        Forward self-link SQL generation to the shared utility implementation.

        Example:
            Pass symmetric=True through kwargs to request canonical endpoint-order guards.


        :param name: Main-table name passed to the shared builder.
        :param kwargs: Supported shared-builder options, including columns, types, nullability and symmetry.
        :return: List of generated SQL statements without executing them.
        """

        return super().direct_build_intralink_table_sql(name, **kwargs)

    def get_requested_intralink_tables(self) -> set[str]:
        """
        Read optional self-link TOML and store normalized options for known tables.

        Missing files return an empty set. Entries may be names or mappings; unknown targets
        and invalid columns/types fail. Nullability requires real bools, symmetry allows
        permissive bool spellings. Expand MARC/hash types and require symmetric type subsets
        to be supported. Duplicate table entries overwrite stored settings; old option maps
        are not cleared first. Parsed nullability is later overridden by creation.

        Example:
            A types list automatically adds the type column when it was not otherwise requested.


        :return: Set of requested main-table names; also populates per-table option mappings.
        """
        c = self.conn.cursor()

        current_tables = self.main_tables
        stmt = "SELECT name FROM sqlite_master WHERE type='table';"
        for row in c.execute(stmt):
            current_tables.add(row[0])

        spec_path_toml = os.path.join(__folder__, "intralink_table_requests.toml")
        if not os.path.exists(spec_path_toml):
            return set()

        if tomllib is None:  # pragma: no cover
            raise RuntimeError(
                "intralink_table_requests.toml present but tomllib/tomli is unavailable in this Python runtime."
            )

        with open(spec_path_toml, "rb") as fh:
            data: dict[str, Any] = tomllib.load(fh)

        intralinks = data.get("intralinks", [])
        if not isinstance(intralinks, list):
            raise TypeError("TOML key `intralinks` must be a list")

        allowed_req_cols = {"priority", "primary", "type", "origin", "source", "policy", "data", "index", "sequence_number", "is_required", "nullable", "all"}

        intralink_tables: set[str] = set()

        for idx, entry in enumerate(intralinks):
            if isinstance(entry, str):
                entry = {"table": entry}
            if not isinstance(entry, dict):
                LiuXin_warning_print(
                    f"Warning - intralink spec entry {idx} is not a string or table (dict): {entry!r}"
                )
                continue

            name = entry.get("table") or entry.get("table_name") or entry.get("name")
            if not name:
                LiuXin_warning_print(f"Warning - intralink spec entry {idx} missing `table`: {entry!r}")
                continue

            c_table = self.match_to_table_name(str(name)) or str(name)

            if c_table not in current_tables:
                raise ValueError(
                    f"Unknown table referenced in intralinks[{idx}] in intralink_table_requests.toml: {c_table!r}"
                )

            # requested columns
            requested_columns = entry.get("requested_columns")
            if requested_columns is None:
                requested_columns = entry.get("requested_cols") or entry.get("requested_col") or entry.get("columns")

            requested_cols: Any = {"type"}
            nullable_fks: bool = False  # prefer strictness for self-links

            if requested_columns is not None:
                if isinstance(requested_columns, str):
                    if requested_columns.strip().lower() != "all":
                        raise TypeError(
                            f"requested_cols for intralink {idx} must be 'all' or a list; got: {requested_columns!r}"
                        )
                    requested_cols = "all"
                elif isinstance(requested_columns, list):
                    lowered = [str(x).strip().lower() for x in requested_columns]
                    for rc in lowered:
                        if rc not in allowed_req_cols:
                            raise TypeError(
                                f"Unknown requested_cols entry {rc!r} in intralinks[{idx}] "
                                f"(allowed: {sorted(allowed_req_cols)!r})"
                            )
                    if "nullable" in lowered:
                        nullable_fks = True
                        lowered = [x for x in lowered if x != "nullable"]
                    if "all" in lowered:
                        requested_cols = "all"
                    else:
                        requested_cols = set(lowered) if lowered else set()
                else:
                    raise TypeError(f"requested_cols must be a list or string; got: {type(requested_columns)}")

            # explicit nullable key (preferred)
            if "nullable" in entry and entry.get("nullable") is not None:
                nullable_fks = _require_toml_bool(entry.get("nullable"), context=f"intralinks[{idx}].nullable")

            # allowed types (optional)
            allowed_types = entry.get("allowed_types") or entry.get("types")
            allowed_types_list: Optional[list[str]] = None
            if allowed_types is not None:
                if not isinstance(allowed_types, list):
                    raise TypeError(f"types for intralink {c_table!r} must be a list[str]")
                raw_items = [str(x).strip() for x in allowed_types if str(x).strip()]
                expanded: list[str] = []
                for item in raw_items:
                    key = item.strip()
                    if key.lower() == "insert_marc_roles":
                        try:
                            from LiuXin_alpha.constants.marc_relator_dicts import (
                                MARC_ROLE_DESC,
                            )
                        except Exception as e:  # pragma: no cover
                            raise RuntimeError("Unable to import MARC_ROLE_DESC for insert_marc_roles expansion") from e
                        expanded.extend(sorted(MARC_ROLE_DESC.keys()))
                        continue
                    if key.lower() == "insert_known_hash_types":
                        import hashlib

                        expanded.extend(sorted(hashlib.algorithms_guaranteed))
                        continue
                    if key.lower().startswith("insert_"):
                        raise TypeError(f"Unknown types placeholder {key!r} in intralink {idx}")
                    expanded.append(key)

                seen: set[str] = set()
                allowed_types_list = []
                for v in expanded:
                    if v in seen:
                        continue
                    seen.add(v)
                    allowed_types_list.append(v)

                if not allowed_types_list:
                    raise TypeError(f"types list is empty after expansion in intralink {idx}")

            # ensure type column exists if types are declared
            if allowed_types_list is not None and requested_cols != "all":
                if isinstance(requested_cols, set) and "type" not in requested_cols:
                    requested_cols.add("type")

            # symmetric ordering enforcement
            symmetric = False
            if "symmetric" in entry and entry.get("symmetric") is not None:
                symmetric = _parse_toml_bool(entry.get("symmetric"), default=False)

            symmetric_types = entry.get("symmetric_types") or entry.get("symmetric_type")
            symmetric_types_list: Optional[list[str]] = None
            if symmetric_types is not None:
                if not isinstance(symmetric_types, list):
                    raise TypeError(f"symmetric_types for intralink {c_table!r} must be a list[str]")
                symmetric_types_list = [str(x).strip() for x in symmetric_types if str(x).strip()]
                if not symmetric_types_list:
                    symmetric_types_list = None

            # validate symmetric_types (requires type col, and should be subset of allowed types if provided)
            if symmetric_types_list is not None:
                if requested_cols != "all":
                    if isinstance(requested_cols, set) and "type" not in requested_cols:
                        raise TypeError(f"symmetric_types requires requested_cols to include 'type' in intralink {idx}")
                if allowed_types_list is not None:
                    unknown = sorted(set(symmetric_types_list) - set(allowed_types_list))
                    if unknown:
                        raise TypeError(
                            f"symmetric_types contains values not present in types list for intralink {idx}: {unknown!r}"
                        )

            # Persist per-table intralink config for the build step.
            self.intralink_allowed_types_by_table[c_table] = allowed_types_list
            self.intralink_requested_cols_by_table[c_table] = requested_cols
            self.intralink_nullable_fks_by_table[c_table] = nullable_fks
            self.intralink_symmetric_by_table[c_table] = symmetric
            self.intralink_symmetric_types_by_table[c_table] = symmetric_types_list

            intralink_tables.add(c_table)

        return intralink_tables
    def create_aggregate_tables(self) -> None:
        """
        Execute explicitly enabled aggregate SQL resources with a commit after each script.

        Missing specs or disabled settings are no-ops. enabled must be a bool. Explicit
        files precede an optional recursive folder scan; duplicate resources may run twice,
        and a missing scan folder is ignored. Paths are trusted configuration, not sandboxed.

        Example:
            An enabled specification can load WEMI views after all main/link tables exist.


        :return: None; creates configured derived objects when enabled.
        """
        spec_path_toml = os.path.join(__folder__, "aggregate_tables.toml")
        if not os.path.exists(spec_path_toml):
            return

        if tomllib is None:  # pragma: no cover
            raise RuntimeError(
                "aggregate_tables.toml present but tomllib/tomli is unavailable in this Python runtime."
            )

        with open(spec_path_toml, "rb") as fh:
            data: dict[str, Any] = tomllib.load(fh)

        enabled_raw = data.get("enabled", False)
        if "enabled" in data:
            enabled = _require_toml_bool(enabled_raw, context="aggregate_tables.enabled")
        else:
            enabled = False

        if not enabled:
            return

        sql_files = data.get("sql_files", [])
        sql_folder = data.get("sql_folder")

        scripts: list[str] = []

        # Explicit file list
        if sql_files:
            if not isinstance(sql_files, list):
                raise TypeError("aggregate_tables.toml key `sql_files` must be a list")
            for rel in sql_files:
                rel_str = str(rel)
                abs_path = os.path.join(__folder__, rel_str)
                if not os.path.exists(abs_path):
                    raise FileNotFoundError(f"Aggregate SQL file not found: {rel_str!r}")
                with open(abs_path, "r", encoding="utf-8") as f:
                    scripts.append(f.read())

        # Optional folder scan (in addition to explicit files)
        if sql_folder:
            folder_path = os.path.join(__folder__, str(sql_folder))
            if os.path.exists(folder_path):
                for dirpath, _, filenames in os.walk(folder_path):
                    for fn in sorted(filenames):
                        if fn.lower().endswith(".sql"):
                            with open(os.path.join(dirpath, fn), "r", encoding="utf-8") as f:
                                scripts.append(f.read())

        if not scripts:
            return

        c = self.conn.cursor()
        for script in scripts:
            if VERBOSE_DEBUG:
                LiuXin_print(script)
            c.executescript(script)
            self.conn.commit()


    def set_database_version(self) -> None:
        """
        Insert and verify the APSW master-version row, then block all version-table writes.

        Commit the insert and each guard separately. This is not an upsert: rerunning after
        the version row or write locks exist can fail. A read-back mismatch raises RuntimeError.

        Example:
            After success, inserts, updates and deletes on database_version abort.


        :return: None; stores the version and installs three triggers.
        """
        from LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw import (
            get_SQLite_driver_master_version,
        )

        version_str = get_SQLite_driver_master_version()

        stmt = "INSERT INTO database_version (database_version_id, database_version_version) VALUES (1, ?);"
        c = self.conn.cursor()
        c.execute(stmt, (version_str,))
        self.conn.commit()

        # Check to see if the insert has actually been written out
        version_val = None
        for row in c.execute("SELECT database_version_version FROM database_version;"):
            version_val = row[0]
        if version_val != version_str:
            raise RuntimeError(f"database_version insert mismatch: expected {version_str!r}, got {version_val!r}")

        ins_stmt_block = """
        CREATE TRIGGER IF NOT EXISTS block_insert_on_database_version_table
        BEFORE INSERT ON database_version
        BEGIN
            SELECT RAISE(ABORT, 'Cannot insert into database_version');
        END;
        """
        c = self.conn.cursor()
        c.execute(ins_stmt_block)
        self.conn.commit()

        upd_stmt_block = """
        CREATE TRIGGER IF NOT EXISTS block_update_on_database_version_table
        BEFORE UPDATE ON database_version
        BEGIN
            SELECT RAISE(ABORT, 'Cannot update on database_version');
        END;
        """
        c = self.conn.cursor()
        c.execute(upd_stmt_block)
        self.conn.commit()

        del_stmt_block = """
        CREATE TRIGGER IF NOT EXISTS block_delete_on_database_version_table
        BEFORE DELETE ON database_version
        BEGIN
            SELECT RAISE(ABORT, 'Cannot delete from database version');
        END;
        """
        c = self.conn.cursor()
        c.execute(del_stmt_block)
        self.conn.commit()
