# Generates the database from the stored SQL and instructions
# Starts from the SQL code for the main tables. Generates them.
# Some SQL files contain multiple statements; those should be executed via executescript.
# Takes the default list of interlink tables. Generates the basic SQL syntax for them.
# Does the same for the intralink tables
# Adds any additional columns which have been created by the user

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

import sys
import re
import os
import pprint
import pathlib
import difflib
from copy import deepcopy

from typing import Optional

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.constants import \
    __INTERLINK_TABLE_CONSTRAINTS__, __ALLOWED_INTERLINK_TYPE_VAL_DICT__, \
    __ALLOWED_INTRALINK_TYPE_VAL_DICT__
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.utils.logging import LiuXin_print
from LiuXin_alpha.utils.logging import LiuXin_warning_print
from LiuXin_alpha.utils.language_tools import singular_plural_mapper, plural_singular_mapper

from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import SQLiteTableLinkingMixin

from LiuXin_alpha.constants.paths import LiuXin_database_folder as __database_folder__

from LiuXin_alpha.databases.api import DatabaseBuilderAPI

from LiuXin_alpha.constants import VERBOSE_DEBUG


# ---------------------------------------------------------------------------
# Interlink TOML helpers (types expansion / SQL emission)
# ---------------------------------------------------------------------------

def _parse_toml_bool(value: Any, *, default: bool = False) -> bool:
    """Parse a permissive TOML boolean value.

    Accepts bools, ints, and common string spellings ("true"/"false", "yes"/"no", "1"/"0").
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
    """Require a *real* TOML boolean.

    TOML supports native booleans (`true`/`false`). For a few high-impact knobs (notably
    link-table FK nullability), we deliberately refuse stringly-typed values like "true".

    :param value: Parsed TOML value.
    :param context: Human-friendly location for error messages.
    :return: The boolean value.
    """
    if isinstance(value, bool):
        return value

    raise TypeError(
        f"{context} must be a TOML boolean (true/false), not {type(value).__name__}: {value!r}"
    )


def _sql_quote_literal(value: str) -> str:
    """SQL-quote a literal string for inclusion in a statement."""
    return "'" + value.replace("'", "''") + "'"


def collect_type_tables(allowed_types_by_link_table: dict[str, Optional[list[str]]]) -> dict[str, set[str]]:
    """Collect `{link_table}__types` reference tables to build, keyed by their table names."""
    out: dict[str, set[str]] = {}
    for link_table, types in allowed_types_by_link_table.items():
        if not types:
            continue
        types_table = f"{link_table}__types"
        bucket = out.setdefault(types_table, set())
        bucket.update(types)
    return out


def emit_types_tables_sql(types_map: dict[str, set[str]]) -> list[str]:
    """Emit idempotent SQL statements to create and seed all requested `__types` tables."""
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
    Creates a new blank database using the resources in the database generator folder.

    If the file path is None, then the database generates in LiuXin_data with the default name (LiuXin_main_database).
    :param connection: sqlite3.Connection:
    """
    conn = connection

    builder = SQLiteDatabaseBuilder(conn=conn)
    builder.run()



def get_main_table_sql_files() -> list[pathlib.Path]:
    """
    Walk and return the paths to all the table_sql files to load into the database.

    :return:
    """
    all_sql_files = []

    table_sql_folder = pathlib.Path(__folder__) / "table_sql"

    if not table_sql_folder.is_dir():
        raise NotADirectoryError(f"Expected 'table_sql' folder to be a directory: {table_sql_folder!s}")

    # pathlib.Path.walk() is only available on Python 3.12+; use os.walk for compatibility.
    for root, dirs, files in os.walk(table_sql_folder):

        for file in files:

            if os.path.splitext(file)[1] == ".sql":

                all_sql_files.append(pathlib.Path(root) / file)

    return all_sql_files


def get_trigger_sql_files() -> list[pathlib.Path]:
    """
    Walk and return the paths to all the table_sql files to load into the database.

    :return:
    """
    all_sql_files = []

    table_sql_folder = pathlib.Path(__folder__) / "trigger_sql"
    if not table_sql_folder.is_dir():
        raise NotADirectoryError(f"Expected 'trigger_sql' folder to be a directory: {table_sql_folder!s}")

    # pathlib.Path.walk() is only available on Python 3.12+; use os.walk for compatibility.
    for root, dirs, files in os.walk(table_sql_folder):

        for file in files:

            if os.path.splitext(file)[1] == ".sql":

                all_sql_files.append(pathlib.Path(root) / file)

    return all_sql_files



class SQLiteDatabaseBuilder(SQLiteTableLinkingMixin, DatabaseBuilderAPI):
    """
    Method to support the construction of a database.
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
        'policy': ('policy', 'TEXT', 'NULL'),
        'data': ('data', 'TEXT', 'NULL'),
        'index': ('index', 'TEXT', 'NULL'),
    }

    def __init__(self, conn: sqlite3.Connection) -> None:
        """
        A conn object pointing to an empty database.

        :param conn:
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
        Actually preforms the build on the database.

        :return:
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

        # 10) Set the version - so we can check the database and driver version used to build this database
        self.set_database_version()


    # Todo: Add annotations table?
    # Todo: Add a unified tasks table?

    def direct_get_tables(self) -> set[str]:
        """
        Returns a index of the names of all tables in the database.
        :param force_refresh: Force the driver to introspect the database again
        :return:
        """

        stmt = "SELECT name FROM sqlite_master WHERE type = 'table';"
        processed_return = []
        for row in self.conn.execute(stmt):
            processed_return.append(row[0])

        return set(processed_return)

    def sanity_check_interlink_inputs(self) -> None:
        """
        Lightweight sanity checks for TOML specs.

        We intentionally derive interlink configuration from `interlink_table_requests.toml` and do not
        support legacy `.txt` request lists.
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

        allowed_req_cols = {"priority", "primary", "type", "origin", "data", "index", "nullable", "all"}

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
        Check that requested intralink tables are valid main tables.

        Intralinks are always self-links (table ↔ table), so we only validate existence here.
        """
        for intralink_table in self.intralink_tables:
            if intralink_table not in self.main_tables:
                raise ValueError(f"Unknown intralink main table: {intralink_table!r}")

    def create_main_tables(self) -> None:
        """
        Generates and executes the SQL needed to build the main tables.

        :return:
        """
        conn = self.conn
        c = conn.cursor()

        statements = []

        for main_table_sql_file in self.main_tables_sql_files:

            try:
                with main_table_sql_file.open("r", encoding="utf-8") as main_tables_sqlite_file:
                    test = main_tables_sqlite_file.readlines()
            except OSError as e:
                raise FileNotFoundError(
                    f"Unable to read main-table SQL file: {main_table_sql_file!s}"
                ) from e


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
        Generates and executes the SQL needed to build the main tables.

        :return:
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
        Parse `interlink_table_requests.toml` and return requested interlink table pairs.

        This method is TOML-only (legacy .txt specs are intentionally unsupported).
        Alongside returning the canonical (table_a, table_b) pairs, we also store per-pair
        metadata in `self.interlink_specs_by_pair`, and per-table build options are later
        materialised by `apply_interlink_constraints_from_spec()`.

        Supported interlink entry fields:
          - left_table, right_table (required)
          - link_type (optional; defaults to top-level default_link_type or many_to_many)
          - requested_columns (optional; defaults to ["priority"])
              - allowed: priority, primary, type, index, nullable, all
              - "nullable" toggles whether the FK columns in the link table are nullable
          - allowed_types (optional; only meaningful if "type" is requested)
              - list of strings; if omitted, the type column is free-form text

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

        allowed_req_cols = {"priority", "primary", "type", "origin", "data", "index", "nullable", "all"}

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
                            # Permit bespoke per-link metadata columns as long as the name is safe.
                            if not re.fullmatch(r"[a-z][a-z0-9_]*", rc):
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
                            from LiuXin_alpha.constants.marc_relator_dicts import MARC_ROLE_DESC
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
        Normalize common link-type spellings to a small canonical set.
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
        Ensure every requested interlink table has an entry in INTERLINK_TABLE_CONSTRAINTS.

        We populate/override constraints from `interlink_table_requests.toml` (when present), so the link-table
        generator can enforce cardinality via constraints (many_many / one_many / many_one / one_one).

        If a pair has no explicit spec metadata, we default to many-to-many.
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
        Extract the main tables we're being instructed to link from the main table.

        :param interlink_request:
        :return:
        """
        input_pattern = re.compile(r"\s*([0-9a-zA-Z_]+)-([0-9a-zA-Z]+)_")
        tables = input_pattern.match(interlink_request)

        if tables is None:
            return

        i_table1 = tables.group(1)
        i_table2 = tables.group(2)
        c_table1 = self.match_to_table_name(i_table1)
        c_table2 = self.match_to_table_name(i_table2)

        return sorted([c_table1, c_table2])

    def validate_interlink_table_constraints(self) -> None:
        """
        Check that we're not trying to constrain tables that don't exist.

        :return:
        """
        for link_table in self.interlink_tables:
            if link_table not in self.INTERLINK_TABLE_CONSTRAINTS:
                raise KeyError(self.__constraint_not_found_error(link_table))


    def validate_allowed_type_val_dict(self) -> None:
        """
        Validate any explicit allowed-types configuration coming from TOML.

        FRBR interlinks may request a free-form `type` column without enumerating allowed values.
        If `allowed_types` is present for a link table, we validate it is a list[str].
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
        Validate requested_columns from TOML for each interlink table.

        Supported optional columns are those in INTERLINK_TABLE_COLUMN_NAME_DICT
        (e.g. priority, primary, type, origin, data, index) plus the legacy
        requested_columns entry "nullable" (now handled via the TOML key `nullable`).
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
        """Create and seed all `{link_table}__types` tables requested by TOML."""
        types_map = collect_type_tables(self.interlink_allowed_types_by_table)
        if not types_map:
            return
        c = self.conn.cursor()
        for stmt in emit_types_tables_sql(types_map):
            c.execute(stmt)
        self.conn.commit()

    def __constraint_not_found_error(self, link_table: str) -> str:
        err_msg = [
            "{} not found in the known interlink tables".format(link_table),
            "\n{}\n".format(pprint.pformat(self.interlink_tables)),
        ]
        return "\n".join(err_msg)

    @staticmethod
    def get_interlink_name(link_pair: list[str]) -> str:
        """
        Take the pair of tables to be linked and return the name of their interlink table.

        :param link_pair:
        :return:
        """
        link_pair = sorted(link_pair)
        return "{}_{}_links".format(plural_singular_mapper(link_pair[0]), plural_singular_mapper(link_pair[1]))

    def get_interlink_constraint(self, link_pair: list[str]) -> dict[str, str]:
        """
        Takes a pair of tables and returns a link table for it - if it exists.

        :param link_pair:
        :return:
        """
        link_table_name = self.get_interlink_name(link_pair)
        return self.INTERLINK_TABLE_CONSTRAINTS[link_table_name]

    def match_to_table_name(self, candidate_name: str) -> Optional[str]:
        """
        Attempt to fuzzy match the cand name to a known table.

        Tries to match the given string with one that is definitely the name of a table.
        Returns the name of the table - or None if no match can be found.
        :param candidate_name:
        :return:
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
        Takes the names of two tables - creates an interlink table between them.

        :param table1:
        :param table2:
        :param connection: The global connection uses throughout this extended method
        :return None: Operation is applied directly to database
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
            self.create_interlink_types_reference_table(
                interlink_table_name=table_name,
                interlink_column_base=column_name,
                allowed_types=allowed_types,
                connection=conn,
            )


    
    def create_interlink_types_reference_table(
        self,
        interlink_table_name: str,
        interlink_column_base: str,
        allowed_types: list[str],
        connection: sqlite3.Connection,
    ) -> None:
        """Delegate to the shared link-table utility mixin implementation."""

        return super().create_interlink_types_reference_table(
            interlink_table_name=interlink_table_name,
            interlink_column_base=interlink_column_base,
            allowed_types=allowed_types,
            connection=connection,
        )


    # this section deals with adding the intralink tables
        # examples might be authors and their pseudonames.
        # The format is always primary is type of secondary
    def create_intralink_table(self, table_name: str, connection: sqlite3.Connection) -> None:
        """Create the intralink (self-link) table for `table_name` from the TOML spec."""

        conn = connection
        c = conn.cursor()

        name_local = deepcopy(table_name)
        name_local = six_unicode(name_local)

        # TOML-derived configuration for this intralink table
        requested_cols: Any = self.intralink_requested_cols_by_table.get(name_local, {"type"})
        allowed_types = self.intralink_allowed_types_by_table.get(name_local)
        nullable_fks = self.intralink_nullable_fks_by_table.get(name_local, False)
        symmetric = self.intralink_symmetric_by_table.get(name_local, False)
        symmetric_types = self.intralink_symmetric_types_by_table.get(name_local)

        sql_list = super().build_intralink_table_sqlite(
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
            self.create_interlink_types_reference_table(
                interlink_table_name=intralink_table_name,
                interlink_column_base=row_name,
                allowed_types=allowed_types,
                connection=conn,
            )

    def build_intralink_table_sqlite(self, name: str, **kwargs: Any) -> list[str]:
        """Delegate intralink SQL generation to the shared utility mixin."""

        return super().build_intralink_table_sqlite(name, **kwargs)

    def get_requested_intralink_tables(self) -> set[str]:
        """Parse `intralink_table_requests.toml` and return requested intralink tables.

        Intralinks are TOML-only (legacy .txt specs are intentionally unsupported).

        Supported keys per `[[intralinks]]` entry:
          - table: str (required)
          - requested_cols / requested_columns: 'all' or list[str] (optional)
          - types / allowed_types: list[str] (optional; supports insert_marc_roles and insert_known_hash_types)
          - nullable: bool (optional; controls FK NULL vs NOT NULL)
          - symmetric: bool (optional; enforce ordering for *all* rows)
          - symmetric_types: list[str] (optional; enforce ordering only for these type values)
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

        allowed_req_cols = {"priority", "primary", "type", "origin", "policy", "data", "index", "nullable", "all"}

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
                            from LiuXin_alpha.constants.marc_relator_dicts import MARC_ROLE_DESC
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
        Execute any aggregate / derived SQL configured for this generator.

        This is controlled by `aggregate_tables.toml`. By default it is disabled so that
        stale legacy aggregate definitions can't silently pollute a new FRBR schema.
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
        Import the driver version and the database version and set it.

        :return:
        """
        from LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw import get_SQLite_driver_master_version

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
