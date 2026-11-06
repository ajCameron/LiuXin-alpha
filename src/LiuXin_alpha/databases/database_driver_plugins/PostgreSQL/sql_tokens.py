"""Adapt SQL parameters without rewriting quoted text, comments or native operators.

This is a lexical adapter, not a SQLite-to-PostgreSQL grammar translator. Qmark
bindings are recognized at expression positions; PostgreSQL's binary ?, ?|, ?&
and @? operators remain operators. Native %s/%(name)s bindings are also accepted,
but mixing the two parameter styles in one statement is rejected.

Temporal values and same-named JSON operands can have identical token shapes.
Execution resolves these with parameter arity where possible; otherwise callers
must use native parameters or CAST. Syntax-only translation has no such arity.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import islice

_DOLLAR = re.compile(r"\$(?:[^\W\d]\w*)?\$", re.UNICODE)
_WORD = re.compile(r"[^\W\d][\w$]*", re.UNICODE)
_NUMBER = re.compile(r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
_PARAMETER = re.compile(r"%(?:s|\([^)]+\)s)")
_TEMPORAL_TYPES = frozenset(
    {"TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", "DATE", "INTERVAL"}
)
_EXPRESSION_PREFIXES = frozenset(
    "SELECT WHERE AND OR NOT WHEN THEN ELSE CASE VALUES SET RETURNING LIMIT OFFSET "
    "HAVING BY DISTINCT ALL ON IN IS BETWEEN AS LIKE ILIKE SIMILAR TO ESCAPE "
    "FROM USING AT TIME ZONE INTERVAL FIRST NEXT FETCH".split()
)
# These unreserved words introduce an expression only inside a SQL phrase.
# Outside that context they can name an attribute, including JSON operands.
_CONTEXTUAL_PREFIXES: dict[str, set[tuple[str, ...]]] = {
    "BY": {("ORDER",), ("GROUP",)},
    "FIRST": {("FETCH",)},
    "NEXT": {("FETCH",)},
    "TIME": {("AT",)},
    "ZONE": {("AT", "TIME")},
    "INTERVAL": set(),
}


@dataclass(frozen=True)
class SQLToken:
    """Keep exact source spelling together with the lexical role."""

    kind: str
    text: str


def _quoted_end(sql: str, start: int, quote: str, *, escapes: bool = False) -> int:
    """Consume SQL doubled delimiters and explicit E-string backslash escapes."""
    position = start + 1
    while position < len(sql):
        if escapes and sql[position] == "\\":
            position += 2
        elif sql[position] == quote:
            if sql[position : position + 2] == quote * 2:
                position += 2
            else:
                return position + 1
        else:
            position += 1
    raise ValueError("Unterminated SQL quoted text")


def _comment_end(sql: str, start: int) -> int:
    """Consume PostgreSQL's nested block comments as one opaque token."""
    position, depth = start + 2, 1
    while position < len(sql):
        pair = sql[position : position + 2]
        if pair == "/*":
            depth += 1
            position += 2
        elif pair == "*/":
            depth -= 1
            position += 2
            if depth == 0:
                return position
        else:
            position += 1
    raise ValueError("Unterminated SQL block comment")


def sql_tokens(sql: str) -> Iterator[SQLToken]:
    """Scan standard-conforming PostgreSQL SQL plus SQLite backtick identifiers."""
    position = 0
    while position < len(sql):
        start, character = position, sql[position]
        if character.isspace():
            while position < len(sql) and sql[position].isspace():
                position += 1
            kind = "space"
        elif sql.startswith("--", position):
            ends = [
                end
                for separator in ("\n", "\r")
                if (end := sql.find(separator, position)) >= 0
            ]
            position = min(ends, default=len(sql))
            kind = "comment"
        elif sql.startswith("/*", position):
            position = _comment_end(sql, position)
            kind = "comment"
        elif character in "eE" and sql[position + 1 : position + 2] == "'":
            position = _quoted_end(sql, position + 1, "'", escapes=True)
            kind = "literal"
        elif character in "'\"`":
            position = _quoted_end(sql, position, character)
            kind = "backtick" if character == "`" else "literal"
        elif dollar := _DOLLAR.match(sql, position):
            delimiter = dollar[0]
            end = sql.find(delimiter, dollar.end())
            if end < 0:
                raise ValueError("Unterminated SQL dollar quote")
            position, kind = end + len(delimiter), "literal"
        elif parameter := _PARAMETER.match(sql, position):
            position, kind = parameter.end(), "parameter"
        elif word := _WORD.match(sql, position):
            position, kind = word.end(), "word"
        elif number := _NUMBER.match(sql, position):
            position, kind = number.end(), "literal"
        elif sql[position : position + 2] in {"?|", "?&", "@?", "??", "%%", "::"}:
            position, kind = position + 2, "operator"
        else:
            position += 1
            kind = "qmark" if character == "?" else "punctuation"
        yield SQLToken(kind, sql[start:position])


def _temporal_qmark(tokens: list[SQLToken], position: int) -> tuple[int, str] | None:
    """Locate a temporal value after optional precision and time-zone modifiers."""
    following = list(
        islice(
            (
                index
                for index in range(position + 1, len(tokens))
                if tokens[index].kind not in {"space", "comment"}
            ),
            8,
        )
    )
    words = [tokens[index].text.upper() for index in following]
    offset = (
        3
        if len(words) >= 3
        and words[0] == "("
        and words[1].isdigit()
        and words[2] == ")"
        else 0
    )
    if words[offset : offset + 3] in (
        ["WITH", "TIME", "ZONE"],
        ["WITHOUT", "TIME", "ZONE"],
    ):
        offset += 3
    if offset >= len(following) or tokens[following[offset]].kind != "qmark":
        return None
    right = tokens[following[offset + 1]] if offset + 1 < len(following) else None
    return following[offset], _temporal_suffix_role(right)


def _temporal_suffix_role(right: SQLToken | None) -> str:
    """Keep genuine operand/alias ambiguity distinct from definite SQL boundaries."""
    if right is None:
        return "binding"
    if right.kind in {"parameter", "qmark"} or right.text == "(":
        return "operator"
    if right.kind == "literal" and not right.text.startswith('"'):
        return "operator"
    if right.kind == "word" and right.text.upper() in {
        "FROM",
        "AS",
        "WHERE",
        "GROUP",
        "HAVING",
        "WINDOW",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "FOR",
        "AND",
        "OR",
        "IS",
        "IN",
        "BETWEEN",
        "LIKE",
        "ILIKE",
        "THEN",
        "ELSE",
        "END",
    }:
        return "binding"
    if right.kind in {"word", "literal", "backtick"}:
        return "ambiguous"
    return "binding"


def _resolve_temporal_bindings(
    tokens: list[SQLToken], positions: set[int], count: int | None
) -> None:
    """Resolve all-or-none assignments; refuse to guess between multiple meanings."""
    if (
        not positions
        or count is None
        or any(token.kind == "parameter" for token in tokens)
    ):
        return
    fixed = sum(
        token.kind == "binding"
        for index, token in enumerate(tokens)
        if index not in positions
    )
    needed = count - fixed
    if needed not in {0, len(positions)}:
        raise ValueError(
            "Ambiguous temporal qmark bindings: use native %s parameters "
            "or CAST(? AS type) to distinguish typed values from JSON operators"
        )
    for index in positions:
        tokens[index] = SQLToken(
            "binding" if needed else "operator", tokens[index].text
        )


def _classified_tokens(sql: str, parameter_count: int | None = None) -> list[SQLToken]:
    """Separate operators from bindings, resolving temporal ambiguity by arity.

    ``interval ? day`` can name two JSON operands or contain a typed binding.
    Suffix keywords alone cannot settle that distinction. Execution supplies the
    parameter count; an unresolved assignment requires native parameters or CAST.
    Syntax-only translation leaves ambiguous qmarks as operators.
    """
    result = []
    expression = True
    qualified = False
    previous: tuple[str, ...] = ()
    tokens = list(sql_tokens(sql))
    temporal: set[int] = set()
    roles: dict[int, str] = {}
    for position, token in enumerate(tokens):
        kind, text = token.kind, token.text
        if kind in {"space", "comment"}:
            result.append(token)
            continue
        if kind == "qmark":
            kind = roles.get(position, "binding" if expression else "operator")
            if kind == "ambiguous":
                temporal.add(position)
                kind = "operator"
            expression = kind == "operator"
        elif kind == "word":
            # A word after a qualification dot names an attribute, even when
            # its spelling is also a keyword (t.first, t.next, t.time).
            word = text.upper()
            if word in _TEMPORAL_TYPES and expression and not qualified:
                if temporal_qmark := _temporal_qmark(tokens, position):
                    roles[temporal_qmark[0]] = temporal_qmark[1]
            if qualified:
                expression = False
            elif word in _CONTEXTUAL_PREFIXES:
                expression = any(
                    previous[-len(phrase) :] == phrase
                    for phrase in _CONTEXTUAL_PREFIXES[word]
                )
            elif word == "AT":
                # AT TIME ZONE follows an operand. At an expression's start,
                # the same unreserved word is an attribute name.
                expression = not expression
            else:
                expression = word in _EXPRESSION_PREFIXES
        elif kind in {"literal", "backtick", "parameter"} or text in {")", "]"}:
            expression = False
        else:
            expression = True
        qualified = text == "."
        previous = (*previous[-1:], text.upper())
        result.append(SQLToken(kind, text))
    _resolve_temporal_bindings(result, temporal, parameter_count)
    return result


def adapt_sql(
    sql: str,
    *,
    identifiers: bool = True,
    placeholders: bool = True,
    bound: bool = False,
    parameter_count: int | None = None,
) -> str:
    """Translate syntax tokens and escape literal percent signs for bound execution.

    Unbound execution leaves question marks untouched. Callers requesting only
    syntax translation can set placeholders=True without bound execution. Native
    pyformat accepts existing %% escapes; qmark SQL treats every original percent
    as literal. Quoted text and comments never become parameter locations.
    Bound execution supplies parameter_count to disambiguate temporal values and
    JSON operands. Multiple possible assignments raise before SQL is executed.
    """
    tokens = _classified_tokens(str(sql), parameter_count if placeholders else None)
    native = any(token.kind == "parameter" for token in tokens)
    qmarks = any(token.kind == "binding" for token in tokens)
    if placeholders and native and qmarks:
        raise ValueError("Cannot mix qmark and PostgreSQL pyformat bindings")
    output = []
    for token in tokens:
        text = token.text
        if identifiers and token.kind == "backtick":
            text = '"' + text[1:-1].replace("``", "`").replace('"', '""') + '"'
        if placeholders and token.kind == "binding":
            output.append("%s")
            continue
        if bound and token.kind != "parameter":
            if native:
                text = text.replace("%%", "%")
            text = text.replace("%", "%%")
        output.append(text)
    return "".join(output)


def split_sql_statements(sql: str) -> Iterator[str]:
    """Split at top-level semicolons while retaining quoted procedural bodies.

    Used when maintenance hooks require a boundary before every statement.
    Unhooked native scripts are passed whole to PostgreSQL instead.
    """
    pending = []
    for token in sql_tokens(str(sql)):
        pending.append(token.text)
        if token.kind == "punctuation" and token.text == ";":
            yield "".join(pending)
            pending = []
    remaining = "".join(pending)
    if remaining.strip():
        yield remaining
