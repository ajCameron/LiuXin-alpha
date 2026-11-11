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
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from itertools import islice

# Match PostgreSQL scan.l's ASCII classes plus high-bit identifier bytes.
# In decoded Unicode SQL every non-ASCII code point belongs to that latter
# class, including combining marks, emoji and non-breaking spaces. Dollar
# tags share identifier rules except that '$' cannot occur inside a tag.
_IDENT_START = r"[A-Za-z_\x80-\U0010ffff]"
_DOLLAR = re.compile(r"\$(?:" + _IDENT_START + r"[A-Za-z_0-9\x80-\U0010ffff]*)?\$")
_WORD = re.compile(_IDENT_START + r"[A-Za-z_0-9$\x80-\U0010ffff]*")
_SPACE = " \t\n\r\f"
# PostgreSQL quote continuation permits whitespace containing a newline and
# SQL line comments, but not block comments or a new E prefix (scan.l).
# At most one comment precedes the first newline; a repeated greedy comment
# group would backtrack exponentially on an ordinary trailing comment at EOF.
_CONTINUED_QUOTE = re.compile(
    r"[ \t\f]*(?:--[^\r\n]*)?[\r\n](?:[ \t\r\n\f]|--[^\r\n]*[\r\n])*'"
)
_NUMBER = re.compile(r"(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_PARAMETER = re.compile(r"%(?:s|\([^)]+\)s)")
_TEMPORAL_TYPES = frozenset(
    {"TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", "DATE", "INTERVAL"}
)
_EXPRESSION_PREFIXES = frozenset(
    "SELECT WHERE AND OR NOT WHEN THEN ELSE CASE VALUES SET RETURNING LIMIT OFFSET "
    "HAVING BY DISTINCT ALL ON IN IS BETWEEN AS LIKE ILIKE SIMILAR TO ESCAPE "
    "FROM USING AT TIME ZONE INTERVAL FIRST NEXT FETCH "
    "FOR LEADING TRAILING BOTH PLACING VARIADIC".split()
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
    "SYMMETRIC": {("BETWEEN",)},
    "ASYMMETRIC": {("BETWEEN",)},
}


@dataclass(frozen=True)
class SQLToken:
    """Keep exact source spelling together with the lexical role."""

    kind: str
    text: str


def _keyword(text: str) -> str:
    """Fold ASCII syntax only; Unicode identifiers must never become keywords."""
    return text.upper() if text.isascii() else text


def _quoted_end(sql: str, start: int, quote: str, *, escapes: bool = False) -> int:
    """Consume doubled delimiters and backslash escapes when that mode is active."""
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


def _string_end(sql: str, start: int, *, escapes: bool = False) -> int:
    """Keep newline-separated continuations in the original literal's mode."""
    position = _quoted_end(sql, start, "'", escapes=escapes)
    while continuation := _CONTINUED_QUOTE.match(sql, position):
        position = _quoted_end(sql, continuation.end() - 1, "'", escapes=escapes)
    return position


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


def sql_tokens(
    sql: str, *, standard_conforming_strings: bool | Callable[[], bool] = True
) -> Iterator[SQLToken]:
    """Scan PostgreSQL SQL using the connection's ordinary-string escape mode.

    A callable lets a lazy script consumer refresh the mode after executing each
    yielded statement. Standalone syntax translation defaults to standard strings.
    Explicit E-strings always retain their own escape mode across continuations.
    """
    position = 0
    while position < len(sql):
        start, character = position, sql[position]
        if character in _SPACE:
            while position < len(sql) and sql[position] in _SPACE:
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
            position = _string_end(sql, position + 1, escapes=True)
            kind = "literal"
        elif character == "'":
            standard = (
                standard_conforming_strings()
                if callable(standard_conforming_strings)
                else standard_conforming_strings
            )
            position = _string_end(sql, position, escapes=not standard)
            kind = "literal"
        elif character in '"`':
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
    words = [_keyword(tokens[index].text) for index in following]
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
    if right.kind == "word" and _keyword(right.text) in {
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


def _operator_end(tokens: list[SQLToken], position: int) -> int | None:
    """Recognize OPERATOR([schema.]symbol) without treating its name as a value."""
    significant = (
        index
        for index in range(position + 1, len(tokens))
        if tokens[index].kind not in {"space", "comment"}
    )
    opening = next(significant, None)
    if opening is None or tokens[opening].text != "(":
        return None
    body: list[SQLToken] = []
    for index in significant:
        token = tokens[index]
        if token.text == ")":
            if (
                len(body) > 2
                and body[1].text == "."
                and (
                    body[0].kind in {"word", "backtick"} or body[0].text.startswith('"')
                )
            ):
                body = body[2:]
            symbols = "+-*/<>=~!@#%^&|`?"
            if body and all(
                part.kind in {"operator", "qmark", "punctuation"}
                and all(character in symbols for character in part.text)
                for part in body
            ):
                return index
            return None
        if token.text in {"(", ";"}:
            return None
        body.append(token)
    return None


def _expression_tokens(
    sql: str, *, standard_conforming_strings: bool
) -> list[SQLToken]:
    """Keep explicit operator names opaque while retaining their operand role."""
    tokens = list(
        sql_tokens(sql, standard_conforming_strings=standard_conforming_strings)
    )
    result: list[SQLToken] = []
    position, previous = 0, ""
    while position < len(tokens):
        token = tokens[position]
        if (
            token.kind == "word"
            and _keyword(token.text) == "OPERATOR"
            and previous != "."
        ):
            end = _operator_end(tokens, position)
            if end is not None:
                token = SQLToken(
                    "qualified_operator",
                    "".join(part.text for part in tokens[position : end + 1]),
                )
                position = end
        result.append(token)
        if token.kind not in {"space", "comment"}:
            previous = token.text
        position += 1
    return result


def _classified_tokens(
    sql: str,
    parameter_count: int | None = None,
    *,
    standard_conforming_strings: bool = True,
) -> list[SQLToken]:
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
    tokens = _expression_tokens(
        sql, standard_conforming_strings=standard_conforming_strings
    )
    groups: list[str] = []
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
            word = _keyword(text)
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
            elif word in {"AT", "BETWEEN", "ESCAPE"}:
                # These infix phrases follow an operand. At an expression's
                # start, the same spelling is an attribute name instead.
                expression = not expression
            elif word == "NOT":
                # Unary NOT still expects an operand; infix NOT BETWEEN/LIKE
                # retains the left operand until the following operator word.
                pass
            elif word == "SET":
                expression = not expression or not previous or previous[-1] == ";"
            elif word == "VALUES":
                expression = _values_prefix(tokens, position, expression, previous)
            elif word in {"ROWS", "RANGE", "GROUPS"}:
                # Frame clauses follow an ordering/partition operand, or start
                # an OVER/WINDOW-AS group. At an expression start these same
                # unreserved words still name JSON columns (ORDER BY rows ? ...).
                expression = not expression or (
                    previous[-1:] == ("(",)
                    and bool(groups)
                    and groups[-1] in {"OVER", "AS"}
                )
            else:
                expression = word in _EXPRESSION_PREFIXES
        elif text == "(":
            groups.append(
                "DISTINCT ON"
                if previous == ("DISTINCT", "ON")
                else (previous[-1] if previous else "")
            )
            expression = True
        elif text == ")":
            expression = (groups.pop() if groups else "") == "DISTINCT ON"
        elif kind in {"literal", "backtick", "parameter"} or text == "]":
            expression = False
        else:
            expression = True
        qualified = text == "."
        previous = (*previous[-1:], _keyword(text))
        result.append(SQLToken(kind, text))
    _resolve_temporal_bindings(result, temporal, parameter_count)
    return result


def _values_prefix(
    tokens: list[SQLToken], position: int, expression: bool, previous: tuple[str, ...]
) -> bool:
    """Distinguish a VALUES row clause from a JSON column named values.

    Parenthesized rows reset the expression state themselves. A qmark may
    instead bind an entire row tuple, including in a nested VALUES query.
    Such a row ends at a clause boundary; a JSON operator needs a right operand.
    """
    if not expression or not previous or previous[-1] == ";":
        return True
    following = list(
        islice(
            (
                tokens[index]
                for index in range(position + 1, len(tokens))
                if tokens[index].kind not in {"space", "comment"}
            ),
            2,
        )
    )
    if not following or following[0].kind != "qmark":
        return False
    right = following[1] if len(following) == 2 else None
    return _temporal_suffix_role(right) == "binding"


def adapt_sql(
    sql: str,
    *,
    identifiers: bool = True,
    placeholders: bool = True,
    bound: bool = False,
    parameter_count: int | None = None,
    standard_conforming_strings: bool = True,
) -> str:
    """Translate syntax tokens and escape literal percent signs for bound execution.

    Unbound execution leaves question marks untouched. Callers requesting only
    syntax translation can set placeholders=True without bound execution. Native
    pyformat accepts existing %% escapes; qmark SQL treats every original percent
    as literal. Quoted text and comments never become parameter locations.
    Bound execution supplies parameter_count to disambiguate temporal values and
    JSON operands. Multiple possible assignments raise before SQL is executed.
    Connections supply their current standard_conforming_strings setting; pure
    syntax translation without a connection keeps the standard-string default.
    """
    tokens = _classified_tokens(
        str(sql),
        parameter_count if placeholders else None,
        standard_conforming_strings=standard_conforming_strings,
    )
    native = any(token.kind == "parameter" for token in tokens)
    qmarks = any(token.kind == "binding" for token in tokens)
    if placeholders and native and qmarks:
        raise ValueError("Cannot mix qmark and PostgreSQL pyformat bindings")
    output = []
    for token in tokens:
        text = token.text
        if identifiers:
            text = _identifier_text(token)
        if placeholders and token.kind == "binding":
            output.append("%s")
            continue
        if bound and token.kind != "parameter":
            if native:
                text = text.replace("%%", "%")
            text = text.replace("%", "%%")
        output.append(text)
    return "".join(output)


def _identifier_text(token: SQLToken) -> str:
    """Translate identifiers inside an opaque operator name as well as outside."""
    if token.kind == "backtick":
        return '"' + token.text[1:-1].replace("``", "`").replace('"', '""') + '"'
    if token.kind == "qualified_operator":
        return "".join(_identifier_text(part) for part in sql_tokens(token.text))
    return token.text


def split_sql_statements(
    sql: str, *, standard_conforming_strings: bool | Callable[[], bool] = True
) -> Iterator[str]:
    """Split at top-level semicolons while retaining quoted procedural bodies.

    Used when maintenance hooks require a boundary before every statement.
    Unhooked native scripts are passed whole to PostgreSQL instead.
    A mode callback follows settings changed by earlier executed statements.
    """
    pending = []
    for token in sql_tokens(
        str(sql), standard_conforming_strings=standard_conforming_strings
    ):
        pending.append(token.text)
        if token.kind == "punctuation" and token.text == ";":
            yield "".join(pending)
            pending = []
    remaining = "".join(pending)
    if remaining.strip(_SPACE):
        yield remaining
