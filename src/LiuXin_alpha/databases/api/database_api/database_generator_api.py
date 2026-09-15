"""
Declare builder hooks for schema creation, relationship SQL and specification validation.

DatabaseGeneratorAPI is abstract and contains no generation implementation. Concrete builders supply resource loading, validation, SQL execution and transaction behavior. SQL-producing helpers and helpers that execute changes are separate contracts; abstract annotations alone do not make either safe or atomic.
"""

from __future__ import annotations

import abc
import sqlite3
from typing import Any, Iterable, Optional, Union, LiteralString


class DatabaseGeneratorAPI(abc.ABC):
    """
    Specify the lifecycle and schema helpers required of a database builder.

    Implementations supply every abstract hook. The FRBR SQLite builder receives an existing connection, initializes specification state, then performs a staged run with intermediate commits. Caller code remains responsible for connection lifetime and recovery from a partly completed build.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseGeneratorAPI)
        True
    """

    @abc.abstractmethod
    def __init__(self, conn: Any) -> None:
        """
        Require initialization against a caller-supplied database connection.

        The FRBR constructor retains the connection and initializes per-instance schema/specification caches without running the build.

        Example:
            Construct a concrete SQLiteDatabaseGenerator(conn), then call run() when ready to create its schema.


        :param conn: Connection-like object used by the concrete builder.
        :return: None.
        """

    @abc.abstractmethod
    def _bad_link_type_error(self, link_type: str) -> str:
        """
        Require diagnostic text explaining an unsupported relationship type.

        Example:
            A concrete builder can include builder._bad_link_type_error("unknown") in its validation exception.


        :param link_type: Rejected cardinality/type spelling.
        :return: Error message; formatting the message does not itself raise.
        """

    @abc.abstractmethod
    def _build_allowed_types_table_interlink(self, for_table: str, allowed_types: Iterable[str]) -> str:
        """
        Require SQL to create and populate a legacy interlink allowed-values table.

        This hook produces SQL rather than executing it. Legacy generation interpolates configured values into statements, so callers must supply trusted schema configuration.

        Example:
            A concrete builder generates the allowed_types__agent_work_links statements from its configured relationship enumeration.


        :param for_table: Interlink table whose type values are being enumerated.
        :param allowed_types: Values to seed in the generated table.
        :return: Creation/insertion SQL; the shared SQLite implementation returns a list despite this abstract str annotation.
        """

    @abc.abstractmethod
    def _build_interlink_table_sqlite(self,
                                      table1: str,
                                      table2: str,
                                      requested_cols: Optional[Union[str, list[str]]]=None,
                                      allowed_types: Optional[Iterable[str]]=None,
                                      override_restriction_sql: Optional[str]=None) -> list[str]:
        """
        Require the compatibility hook for producing interlink creation statements.

        The shared wrapper delegates to build_interlink_table_sqlite and forces nullable foreign keys. Configure its restriction mapping rather than relying on this ignored override.

        Example:
            For a configured concrete builder, builder._build_interlink_table_sqlite("agents", "works") delegates to the current link SQL generator.


        :param table1: First main table.
        :param table2: Second main table.
        :param requested_cols: Requested optional columns, or the implementation default.
        :param allowed_types: Optional permitted type values.
        :param override_restriction_sql: Legacy override argument; ignored by the shared SQLite compatibility wrapper.
        :return: List of SQL statements; no execution is implied.
        """

    @staticmethod
    @abc.abstractmethod
    def _canonicalize_link_type(link_type: str) -> str:
        """
        Require normalization of accepted relationship-type spellings.

        The FRBR implementation strips/lowercases text, normalizes separators and maps aliases such as m2m to many_to_many. Canonicalization alone does not reject unknown values.

        Example:
            The concrete FRBR builder maps "many-many" to "many_to_many".


        :param link_type: Type spelling to normalize.
        :return: Canonical spelling, or a normalized unknown spelling for later validation.
        """

    @abc.abstractmethod
    def direct_get_direct_link_main_tables_sql(self,
                                            primary_table: str,
                                            secondary_table: str,
                                            link_type: str='many_many',
                                            requested_cols: str='all',
                                            index_both: bool=True,
                                            allowed_types: Optional[Iterable[str]]=None,
                                            one_link_with_one_type: bool = True,
                                            override_restriction_sql: Optional[str]=None,
                                            nullable_fks: bool=True
                                            ) -> tuple[list[str], Union[str, LiteralString]]:
        """
        Require relationship DDL and its generated table name without executing it.

        The SQLite generator may reorder endpoint names and reverse directed cardinality to preserve their meaning. Identifier and override inputs are schema configuration, not query values.

        Example:
            For a concrete builder, statements, name = builder.direct_get_direct_link_main_tables_sql("agents", "works", requested_cols="all") produces DDL for inspection.


        :param primary_table: Primary endpoint whose direction defines the requested cardinality.
        :param secondary_table: Secondary endpoint.
        :param link_type: Relationship cardinality supported by the implementation.
        :param requested_cols: Optional column selection, or all.
        :param index_both: Request indexes for both endpoint columns.
        :param allowed_types: Optional enumeration for a requested type column.
        :param one_link_with_one_type: Restrict each endpoint pair/type combination to one link when supported.
        :param override_restriction_sql: Trusted SQL replacing automatically generated restrictions.
        :param nullable_fks: Permit null endpoint foreign keys when True.
        :return: Pair of SQL-statement list and generated link-table name.
        """

    @staticmethod
    @abc.abstractmethod
    def _get_link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
        """
        Require deterministic relationship table and column-prefix names.

        The shared SQLite helper sorts endpoint names before singularizing and composing them.

        Example:
            The shared SQLite naming convention gives ("agent_work_links", "agent_work_link") for agents and works.


        :param primary_table: First endpoint table.
        :param secondary_table: Second endpoint table.
        :return: Pair of link-table name and its singular column prefix.
        """

    @abc.abstractmethod
    def _lock_table_read_only(self, table: str, *, message: str) -> None:
        """
        Require database-level guards against writes to a reference table.

        The FRBR builder installs INSERT, UPDATE and DELETE abort triggers and commits; this is not a connection mutex.

        Example:
            A concrete builder uses builder._lock_table_read_only("languages", message="languages is read-only") after seeding reference data.


        :param table: Existing table to protect.
        :param message: Diagnostic message used by write guards.
        :return: None; the concrete operation modifies schema state.
        """

    @abc.abstractmethod
    def apply_interlink_constraints_from_spec(self) -> None:
        """
        Require derivation of relationship restrictions from the parsed specification.

        This stage translates declared direction/cardinality before creating relationship tables.

        Example:
            The FRBR run invokes builder.apply_interlink_constraints_from_spec() after loading its requested endpoint pairs.


        :return: None; update the concrete builder constraint state.
        """

    @abc.abstractmethod
    def build_allowed_types_table_interlink(self,
                                            for_table: str,
                                            allowed_types: Optional[Iterable[str]]=None
                                            ) -> list[str]:
        """
        Require optional SQL for a legacy interlink type-enumeration table.

        Example:
            For a concrete SQLite builder, builder.build_allowed_types_table_interlink("agent_work_links", None) produces no statements.


        :param for_table: Interlink table name.
        :param allowed_types: Allowed values, or None to omit generation.
        :return: List of SQL statements; the shared SQLite helper returns [] when allowed_types is None.
        """

    # Todo: Biiigggg change, but we probably need many to many and one-one intralinks as well for symmetry
    # one-to-onne - two people hate each other
    # one-to-many: writing team operating under a single name (e.g. The Expanse)
    # Todo: We need links between org agents and human agents - "employed by" e.t.c
    @abc.abstractmethod
    def direct_build_allowed_types_table_intralink(self,
                                            for_table: str,
                                            allowed_types: Optional[Iterable[str]] = None) -> list[str]:
        """
        Require optional SQL for the type enumeration of a self-link table.

        The shared SQLite helper checks membership when an intralink table set is available, then uses explicit or stored values.

        Example:
            A concrete builder configured for agents can generate allowed-type statements for pseudonym relationships.


        :param for_table: Main table whose rows link to each other.
        :param allowed_types: Explicit allowed values, or None to consult implementation configuration.
        :return: Creation/seed statements, or an empty list when no values are configured.
        """

    @abc.abstractmethod
    def build_interlink_table_sqlite(self,
                                     table1: str,
                                     table2: str,
                                     requested_cols: Optional[Union[str, Iterable[str]]] = None,
                                     allowed_types: Optional[Iterable[str]]=None,
                                     nullable_fks: bool=True) -> list[str]:
        """
        Require interlink DDL using the builder constraint and type configuration.

        The shared SQLite implementation requires a restriction entry for the generated table; it accepts either restriction SQL or a direction/cardinality mapping.

        Example:
            After loading constraints, statements = builder.build_interlink_table_sqlite("agents", "works", requested_cols={"type", "priority"}) describes a typed ordered relationship.


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param requested_cols: Optional column selection.
        :param allowed_types: Optional enumeration overriding configured type values.
        :param nullable_fks: Permit null endpoint foreign keys.
        :return: List of creation/constraint SQL statements without executing them.
        """

    # Todo: Not clear why this signature is like this? Firm it up.
    @abc.abstractmethod
    def direct_build_intralink_table_sql(self, name: str, **kwargs: Any) -> list[str]:
        """
        Require SQL for relationships between rows of a single main table.

        Example:
            A concrete builder can request builder.direct_build_intralink_table_sql("agents", requested_cols={"type"}) for typed self-links.


        :param name: Main table to link back to itself.
        :param kwargs: Backend options such as requested_cols, allowed_types, nullable_fks and symmetry controls.
        :return: List of SQL statements without executing them.
        """

    @abc.abstractmethod
    def create_aggregate_tables(self) -> None:
        """
        Require creation of enabled aggregate or derived schema objects.

        The FRBR implementation consults aggregate_tables.toml and returns without work when it is absent or disabled; it does not promise materialized tables rather than views.

        Example:
            During a concrete build, builder.create_aggregate_tables() applies the enabled aggregate specification after relationships exist.


        :return: None; execute configured derived SQL when enabled.
        """

    @abc.abstractmethod
    def create_interlink_table(self,
                               table1: str,
                               table2: str,
                               connection: sqlite3.Connection) -> None:
        """
        Require creation of one configured relationship between distinct tables.

        The concrete builder resolves requested columns, type rules and direction before executing statements. Commit behavior belongs to that implementation.

        Example:
            A configured concrete builder creates the agents/works relationship with builder.create_interlink_table("agents", "works", conn).


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param connection: Connection on which to execute the generated schema changes.
        :return: None.
        """

    @abc.abstractmethod
    def direct_create_interlink_types_reference_table(
            self,
            interlink_table_name: str,
            interlink_column_base: str,
            allowed_types: list[str],
            connection: sqlite3.Connection
    ) -> None:
        """
        Require an extensible type-reference table and guards on relationship values.

        The shared SQLite implementation creates <link table>__types, inserts values with bound parameters, installs insert/update guards that allow NULL, and commits.

        Example:
            A concrete builder can seed ["author"] for agent_work_links using the agent_work_link column prefix.


        :param interlink_table_name: Existing relationship table to protect.
        :param interlink_column_base: Relationship column prefix used to identify its type column.
        :param allowed_types: Initial permitted type strings.
        :param connection: Connection used for DDL, seed inserts and guards.
        :return: None.
        """

    @abc.abstractmethod
    def create_intralink_table(
            self,
            table_name: str,
            connection: sqlite3.Connection
    ) -> None:
        """
        Require creation of the configured self-link schema for a main table.

        The FRBR implementation uses configured type/symmetry options but currently forces nullable foreign keys to support placeholder Row creation; callers should not infer strict nullability from the specification alone.

        Example:
            For a configured concrete builder, builder.create_intralink_table("agents", conn) creates its self-link schema and any type guards.


        :param table_name: Main table whose rows form relationship endpoints.
        :param connection: Connection receiving the generated statements.
        :return: None.
        """

    @abc.abstractmethod
    def create_main_tables(self) -> None:
        """
        Require execution of the main-table schema resources.

        Example:
            The concrete run loads its SQL resources before calling builder.create_main_tables().


        :return: None; populate the concrete database and builder table state.
        """

    @abc.abstractmethod
    def create_main_triggers(self) -> None:
        """
        Require installation of the loaded main-schema trigger resources.

        Example:
            The concrete run calls builder.create_main_triggers() after main tables are available.


        :return: None; schema trigger definitions are created.
        """

    @staticmethod
    @abc.abstractmethod
    def direct_get_column_base(table_name: str) -> str:
        """
        Require the singular column-name prefix for a table.

        Example:
            The shared SQLite naming helper uses "work" as the column base for "works".


        :param table_name: Table name to convert under the backend naming convention.
        :return: Column prefix string.
        """

    @abc.abstractmethod
    def direct_get_tables(self) -> set[str]:
        """
        Require discovery of physical tables currently in the database.

        The FRBR SQLite implementation queries sqlite_master for type=table, excluding views.

        Example:
            After main DDL, names = builder.direct_get_tables() lets a concrete builder inspect created tables.


        :return: Set of table-name strings.
        """

    @abc.abstractmethod
    def direct_link_main_tables(
            self,
            primary_table: str,
            secondary_table: str,
            link_type: str = 'many_many',
            requested_cols: Union[LiteralString["all"], Iterable[str]]='all',
            index_both: bool = True,
            allowed_types: Optional[Iterable[str]] = None,
            override_restriction_sql: str = None,
            nullable_fks: bool=True):
        """
        Require execution of DDL linking two main tables.

        Unlike direct_get_direct_link_main_tables_sql, this operation executes SQL and invalidates schema caches. Recovery and commits are backend-specific.

        Example:
            On a disposable configured backend, name = builder.direct_link_main_tables("agents", "works") creates the relationship schema.


        :param primary_table: Primary endpoint defining relationship direction.
        :param secondary_table: Secondary endpoint.
        :param link_type: Requested relationship cardinality.
        :param requested_cols: Optional columns, or all.
        :param index_both: Request indexing of both endpoint columns.
        :param allowed_types: Optional enumeration for type values.
        :param override_restriction_sql: Trusted replacement restriction SQL.
        :param nullable_fks: Permit null endpoint foreign keys.
        :return: Generated table name in the shared SQLite implementation.
        """

    # Todo: All the machinery for parsing the spec can be spun out into a helper class
    @abc.abstractmethod
    def extract_main_tables(self, interlink_request: str) -> Optional[list[str]]:
        """
        Require parsing of the legacy textual relationship request format.

        The FRBR helper uses a restricted legacy regex and name matching. It is separate from TOML parsing; unresolved names can fail during sorting.

        Example:
            A concrete builder may use builder.extract_main_tables("agents-works_link") only for compatible legacy request text.


        :param interlink_request: Legacy request text containing two endpoint names.
        :return: Sorted matched endpoint list, or None when the text pattern does not match.
        """

    # Todo: rename to get_allowed_types_table_name_interlinks
    @staticmethod
    @abc.abstractmethod
    def get_allowed_types_table_name(for_table: str) -> str:
        """
        Require the legacy allowed-values table name for an interlink table.

        Example:
            The shared SQLite helper maps agent_work_links to allowed_types__agent_work_links.


        :param for_table: Relationship table name.
        :return: Associated allowed-types table name.
        """

    @abc.abstractmethod
    def get_allowed_types_table_name_intralinks(self, for_table: str) -> str:
        """
        Require the legacy allowed-values table name for a self-link configuration.

        The shared SQLite convention repeats the supplied main-table name before adding the intralinks suffix.

        Example:
            For agents, the shared helper returns allowed_types__agents_agents_intralinks.


        :param for_table: Main table with an intralink definition.
        :return: Associated allowed-types table name.
        """

    @abc.abstractmethod
    def get_interlink_constraint(self, link_pair: list[str]) -> dict[str, str]:
        """
        Require the configured restriction for an endpoint pair.

        The FRBR implementation looks up the canonical relationship name directly and raises KeyError when absent; legacy entries can also be SQL strings.

        Example:
            After specification loading, builder.get_interlink_constraint(["agents", "works"]) retrieves the configured relationship restriction.


        :param link_pair: Pair of main-table names.
        :return: Stored restriction value; the abstract annotation describes the mapping form.
        """

    @staticmethod
    @abc.abstractmethod
    def get_interlink_name(link_pair: list[str]) -> str:
        """
        Require a canonical relationship table name from an endpoint pair.

        The FRBR implementation sorts and singularizes endpoint names, giving the same result for reversed input order.

        Example:
            The concrete FRBR helper names both ["agents", "works"] and ["works", "agents"] as agent_work_links.


        :param link_pair: Two main-table names.
        :return: Relationship table name.
        """

    # Todo: Replace get_interlink_name with this
    @staticmethod
    @abc.abstractmethod
    def get_interlink_table_name(table1: str, table2: str) -> tuple[str, str]:
        """
        Require both relationship table name and column-name prefix.

        Example:
            The shared SQLite helper returns ("agent_work_links", "agent_work_link") for works and agents.


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :return: Pair of table name and column prefix.
        """

    @abc.abstractmethod
    def get_requested_interlink_tables(self) -> set[tuple[str, str]]:
        """
        Require parsing of requested endpoint pairs and their relationship options.

        The FRBR parser uses TOML and records direction, requested columns, allowed types and nullability for subsequent validation and creation.

        Example:
            The concrete run stores pairs = builder.get_requested_interlink_tables() before applying link constraints.


        :return: Set of endpoint-name pairs; concrete parsers also retain option maps.
        """

    @abc.abstractmethod
    def get_requested_intralink_tables(self) -> set[str]:
        """
        Require parsing of requested self-link main tables and their options.

        The FRBR parser is TOML-only and returns an empty set when its intralink specification file is absent.

        Example:
            A concrete builder loops over builder.get_requested_intralink_tables() before creating each self-link table.


        :return: Set of main-table names; concrete parsers also retain self-link option maps.
        """

    @abc.abstractmethod
    def lock_constant_tables(self) -> None:
        """
        Require protection of seeded reference tables against ordinary writes.

        The FRBR implementation protects languages with abort triggers after seeding. This does not lock every table in the database.

        Example:
            The concrete build calls builder.lock_constant_tables() at the end of its run.


        :return: None; the implementation installs its write guards.
        """

    # Todo: Porbably a bad idea - deprecate.
    @abc.abstractmethod
    def match_to_table_name(self, candidate_name: str) -> Optional[str]:
        """
        Require matching of a candidate to a known main-table name.

        The FRBR helper tries lowercased exact and pluralized forms; it does not perform general edit-distance matching.

        Example:
            When works is known, the concrete helper can resolve "work" to "works".


        :param candidate_name: Candidate table spelling.
        :return: Matched name, or None when no supported name matches.
        """

    @abc.abstractmethod
    def materialize_interlink_type_reference_tables(self) -> None:
        """
        Require creation and seeding of configured extensible relationship type tables.

        The FRBR hook uses its parsed type map, returns immediately for an empty map and commits emitted statements. It does not itself install relationship guards.

        Example:
            A concrete builder with configured enumerations can call builder.materialize_interlink_type_reference_tables() to seed the associated reference tables.


        :return: None; configured reference tables are materialized.
        """

    @abc.abstractmethod
    def run(self) -> None:
        """
        Require orchestration of the complete configured database build.

        The FRBR sequence loads resources, validates inputs, creates main/relationship/derived structures, seeds policies and identities, writes the version and protects constants. Stages may commit independently; a failure does not imply that prior DDL or data was rolled back.

        Example:
            With a disposable caller-owned connection, builder = SQLiteDatabaseGenerator(conn); builder.run() builds the configured schema before the caller closes conn.


        :return: None; schema and reference data are written.
        """

    @abc.abstractmethod
    def sanity_check_interlink_inputs(self) -> None:
        """
        Require preliminary validation of relationship specification structure.

        The FRBR hook checks TOML availability, requested-column shapes and boolean nullability. Later parsing and validation still own endpoint names, relationship types and final column constraints.

        Example:
            The FRBR run calls builder.sanity_check_interlink_inputs() before writing its main schema.


        :return: None when preliminary checks pass.
        """

    @abc.abstractmethod
    def sanity_check_intralink_inputs(self) -> None:
        """
        Require validation that requested self-link endpoints are known main tables.

        Example:
            After loading self-link requests, a concrete builder checks builder.sanity_check_intralink_inputs() before creating their tables.


        :return: None when all requested tables exist.
        """

    @abc.abstractmethod
    def seed_constant_tables(self) -> None:
        """
        Require insertion of the builder reference datasets.

        The FRBR implementation delegates to its language seeder. This stage precedes write protection of constants.

        Example:
            The concrete run calls builder.seed_constant_tables() after creating main tables and triggers.


        :return: None; reference data is written.
        """

    @abc.abstractmethod
    def seed_languages_table(self) -> None:
        """
        Require loading of the supported language reference dataset.

        Example:
            The FRBR constant-data stage invokes builder.seed_languages_table() before languages is protected by triggers.


        :return: None; language rows and their reference metadata are stored.
        """

    @abc.abstractmethod
    def set_database_version(self) -> None:
        """
        Require persistence and protection of the generated schema version.

        The FRBR implementation inserts the driver master version, commits and verifies it, then installs write guards. Repeated invocation on an already seeded/protected version table need not be harmless.

        Example:
            The concrete builder calls builder.set_database_version() near the end of a fresh schema build.


        :return: None.
        """

    # Todo: Name it with what it's for - interlink types or intralink types
    @abc.abstractmethod
    def validate_allowed_type_val_dict(self) -> None:
        """
        Require validation of explicit relationship type enumerations.

        For each FRBR interlink, an omitted enumeration permits free-form types; a present enumeration must be a list of strings. This does not create or seed type tables.

        Example:
            After parsing TOML type options, builder.validate_allowed_type_val_dict() checks their shapes before DDL generation.


        :return: None when the configured values are acceptable.
        """

    @abc.abstractmethod
    def validate_interlink_table_column_requests(self) -> None:
        """
        Require validation of parsed optional-column selections.

        The FRBR hook accepts None or all, otherwise requires a set of known column keys; nullable is a legacy no-op token. Preliminary acceptance of a bespoke name does not bypass this later validation.

        Example:
            The concrete build validates its parsed requested-column sets before creating interlink tables.


        :return: None when all requested selections are supported.
        """

    @abc.abstractmethod
    def validate_interlink_table_constraints(self) -> None:
        """
        Require a restriction entry for every relationship selected for generation.

        The FRBR hook checks mapping membership and raises KeyError for a missing entry; it does not execute or validate the SQL content of each restriction.

        Example:
            After applying specification defaults, builder.validate_interlink_table_constraints() detects relationships lacking a restriction entry.


        :return: None when all requested interlink tables have configured constraints.
        """
