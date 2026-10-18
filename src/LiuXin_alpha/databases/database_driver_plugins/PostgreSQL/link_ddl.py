"""Share native PostgreSQL identifier quoting and relationship DDL fragments."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper


def _q(name: str) -> str:
    """
    Double embedded double quotes and delimit one SQL identifier.

    Example:
        >>> _q("works")
        '"works"'


    :param name: Identifier converted to text before escaping.
    :return: Quoted identifier.
    """
    return '"' + str(name).replace('"', '""') + '"'


def _qualified_table(schema: str, table_name: str) -> str:
    """
    Quote schema and table independently and join them with a dot.

    Example:
        >>> _qualified_table("library", "works")
        '"library"."works"'


    :param schema: Schema identifier to quote.
    :param table_name: Table name normalized to its unqualified spelling.
    :return: Quoted schema-qualified relation reference.
    """
    return f"{_q(schema)}.{_q(table_name)}"


def _assert_safe_identifier(value: str, *, kind: str = "identifier") -> str:
    """
    Validate a nonblank alphanumeric/underscore identifier without a leading digit.

    Python isalnum/isdigit predicates allow Unicode letters and digits; this is broader
    than the ASCII allowlist used by runtime grant builders.

    Example:
        >>> _assert_safe_identifier(" work_label ")
        'work_label'


    :param value: Identifier candidate converted to text and stripped.
    :param kind: Description included in validation errors.
    :return: Trimmed validated identifier; failures raise InputIntegrityError.
    """
    text = str(value).strip()
    if not text:
        raise InputIntegrityError(f"PostgreSQL {kind} cannot be blank.")
    if not all(char.isalnum() or char == "_" for char in text):
        raise InputIntegrityError(
            f"PostgreSQL {kind} contains unsupported characters: {value!r}"
        )
    if text[0].isdigit():
        raise InputIntegrityError(
            f"PostgreSQL {kind} cannot start with a digit: {value!r}"
        )
    return text


def _pg_literal(value: Any) -> str:
    """
    Quote a stringified value as a SQL text literal, doubling apostrophes.

    None is rendered as quoted text rather than SQL NULL.

    Example:
        >>> _pg_literal("author")
        "'author'"


    :param value: Value converted to text for trusted generated DDL seeds.
    :return: Escaped SQL string literal.
    """
    return "'" + str(value).replace("'", "''") + "'"


def _normalise_requested_link_columns(
    requested_cols: str | Sequence[str] | None,
) -> str | set[str]:
    """
    Normalize all/None or a suffix sequence for optional link-column selection.

    Only the string all is accepted; other scalar strings raise InputIntegrityError.
    Sequence entries are trimmed, lowercased and deduplicated without identifier validation.

    Example:
        >>> _normalise_requested_link_columns([" TYPE ", "type"])
        {'type'}


    :param requested_cols: all, None, or a sequence of optional link-column suffixes.
    :return: all or a set of normalized nonblank suffixes.
    """
    if requested_cols is None:
        return set()
    if isinstance(requested_cols, str):
        if requested_cols.strip().lower() == "all":
            return "all"
        raise InputIntegrityError(
            "requested_cols must be 'all', None, or an iterable of column suffixes."
        )
    return {
        str(value).strip().lower() for value in requested_cols if str(value).strip()
    }


def _link_extra_columns(
    link_base: str, requested: str | set[str]
) -> list[tuple[str, str]]:
    """
    Build optional link data columns plus mandatory source, datestamp and scratch.

    All selects the predefined columns. A subset adds recognized columns in fixed order
    and unknown valid suffixes as nullable text in sorted order. The nullable marker is
    ignored. Source is added even for an empty subset; datestamp/scratch always follow.

    Example:
        >>> [suffix for suffix, _ in _link_extra_columns("agent_work_link", set())]
        ['source', 'datestamp', 'scratch']


    :param link_base: Validated singular link prefix used to form column names.
    :param requested: all or a normalized suffix set from _normalise_requested_link_columns.
    :return: Ordered suffix/DDL-fragment pairs.
    """
    all_columns: dict[str, str] = {
        "priority": "bigint default 0",
        "primary": "bigint null default 0",
        "type": "text null",
        "origin": "text null",
        "source": "text null",
        "policy": "text null",
        "data": "text null",
        "index": "text null",
        "sequence_number": "bigint null",
        "is_required": "bigint default 1",
    }
    if requested == "all":
        suffixes = list(all_columns)
    else:
        suffixes = [suffix for suffix in all_columns if suffix in requested]
        for extra in sorted(requested - set(all_columns) - {"nullable"}):
            _assert_safe_identifier(extra, kind="link extra column suffix")
            all_columns[extra] = "text null"
            suffixes.append(extra)
        if "source" not in suffixes:
            suffixes.append("source")

    out: list[tuple[str, str]] = []
    for suffix in suffixes:
        column = _assert_safe_identifier(f"{link_base}_{suffix}", kind="link column")
        out.append((suffix, f"{_q(column)} {all_columns[suffix]}"))
    out.append(
        (
            "datestamp",
            f"{_q(link_base + '_datestamp')} timestamp with time zone default current_timestamp",
        )
    )
    out.append(("scratch", f"{_q(link_base + '_scratch')} text null"))
    return out


def _link_constraints(
    *,
    link_type: str,
    link_base: str,
    left_base: str,
    right_base: str,
    left_fk_col: str,
    right_fk_col: str,
    has_type: bool,
    has_priority: bool,
) -> list[str]:
    """
    Build cardinality and optional priority/type uniqueness constraints.

    Use ordinary PostgreSQL UNIQUE constraints: nullable members retain PostgreSQL
    NULL semantics. Type-specific variants depend on actual type/priority columns.
    Unknown link types raise NotImplementedError.

    Example:
        A one_one link makes each foreign-key column independently unique;
        a rating link with type makes left-ID/type pairs unique.


    :param link_type: Relationship cardinality: many_many, many_many_non_exclusive, one_many, many_one, one_one, one_one_normalized or rating.
    :param link_base: Singular link prefix used to name constraints and optional columns.
    :param left_base: Singular left-table prefix used in one-to-one constraint names.
    :param right_base: Singular right-table prefix used in one-to-one constraint names.
    :param left_fk_col: Left-side foreign-key column name.
    :param right_fk_col: Right-side foreign-key column name.
    :param has_type: Whether the link contains a type column.
    :param has_priority: Whether the link contains a priority column.
    :return: Ordered table-constraint SQL fragments.
    """
    constraints: list[str] = []
    type_col = f"{link_base}_type"
    priority_col = f"{link_base}_priority"

    def constraint_name(suffix: str) -> str:
        """
        Validate and quote one constraint name using the enclosing link prefix.

        Example:
            With link_base agent_work_link, suffix unique_pair names
            ``"agent_work_link_unique_pair"``.


        :param suffix: Constraint-specific suffix appended to the enclosing link_base.
        :return: Quoted generated constraint identifier.
        """
        return _q(_assert_safe_identifier(f"{link_base}_{suffix}", kind="constraint"))

    if link_type == "many_many":
        constraints.append(
            f"constraint {constraint_name('unique_pair')} unique ({_q(right_fk_col)}, {_q(left_fk_col)})"
        )
        if has_priority:
            constraints.append(
                f"constraint {constraint_name('priority_per_left')} unique ({_q(left_fk_col)}, {_q(priority_col)})"
            )
    elif link_type == "many_many_non_exclusive":
        if has_type:
            constraints.append(
                f"constraint {constraint_name('unique_pair_type')} "
                f"unique ({_q(right_fk_col)}, {_q(left_fk_col)}, {_q(type_col)})"
            )
            if has_priority:
                constraints.append(
                    f"constraint {constraint_name('priority_per_left_type')} "
                    f"unique ({_q(left_fk_col)}, {_q(type_col)}, {_q(priority_col)})"
                )
        else:
            constraints.append(
                f"constraint {constraint_name('unique_pair_nonexclusive')} "
                f"unique ({_q(right_fk_col)}, {_q(left_fk_col)})"
            )
    elif link_type == "one_many":
        constraints.append(
            f"constraint {constraint_name('one_many')} unique ({_q(right_fk_col)})"
        )
        if has_priority:
            constraints.append(
                f"constraint {constraint_name('priority_per_left')} unique ({_q(left_fk_col)}, {_q(priority_col)})"
            )
    elif link_type == "many_one":
        constraints.append(
            f"constraint {constraint_name('many_one')} unique ({_q(left_fk_col)})"
        )
        if has_priority:
            constraints.append(
                f"constraint {constraint_name('priority_per_right')} unique ({_q(right_fk_col)}, {_q(priority_col)})"
            )
    elif link_type in {"one_one", "one_one_normalized"}:
        constraints.append(
            f"constraint {constraint_name(left_base + '_appears_once')} unique ({_q(left_fk_col)})"
        )
        constraints.append(
            f"constraint {constraint_name(right_base + '_appears_once')} unique ({_q(right_fk_col)})"
        )
    elif link_type == "rating":
        if has_type:
            constraints.append(
                f"constraint {constraint_name('one_type_per_left')} unique ({_q(left_fk_col)}, {_q(type_col)})"
            )
        else:
            constraints.append(
                f"constraint {constraint_name('one_rating_per_left')} unique ({_q(left_fk_col)})"
            )
    else:
        raise NotImplementedError(f"PostgreSQL link_type not recognized: {link_type!r}")
    return constraints


def _link_table_name_col_name(
    primary_table: str, secondary_table: str
) -> tuple[str, str]:
    """
    Sort singular table bases and derive the conventional link table/prefix pair.

    Example:
        >>> _link_table_name_col_name("works", "agents")
        ('agent_work_links', 'agent_work_link')


    :param primary_table: First main table in the requested relationship.
    :param secondary_table: Second main table in the requested relationship.
    :return: Plural link-table name and singular link-column prefix.
    """
    bases = [
        plural_singular_mapper(str(primary_table)),
        plural_singular_mapper(str(secondary_table)),
    ]
    bases.sort()
    column_name = _assert_safe_identifier(
        f"{bases[0]}_{bases[1]}_link", kind="link column base"
    )
    return f"{column_name}s", column_name
