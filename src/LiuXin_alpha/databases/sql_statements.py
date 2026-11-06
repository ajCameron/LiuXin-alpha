"""Classify SQLite row statements for shared transaction and maintenance policy."""

import re


def sqlite_row_operation(sql: str) -> tuple[str, bool]:
    """Identify a SQLite row operation, including CTEs and replacement conflicts.

    Keep quoted values/identifiers opaque and ignore comments. CTE bodies are
    parenthesized; their SELECTs do not identify the outer operation. This only
    classifies row SQL for transaction ownership, rather than validating SQL.
    """
    tokens = re.finditer(
        r"--[^\n]*|/\*.*?\*/|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\""
        r"|`(?:``|[^`])*`|\[[^\]]*\]|[ \t\r\n\f\ufeff]+"
        r"|[A-Za-z_\x80-\U0010ffff][A-Za-z_0-9$\x80-\U0010ffff]*|.",
        str(sql),
        flags=re.S,
    )
    # SQLite accepts every non-ASCII character in an identifier, including
    # combining marks and emoji, and '$' after its first character. Python's
    # Unicode \w is narrower. Retain other tokens instead of silently dropping
    # a name or punctuation and shifting the CTE grammar. A leading BOM is space.
    words = [
        match[0]
        for match in tokens
        if not match[0].startswith(("--", "/*")) and match[0].strip(" \t\r\n\f\ufeff")
    ]
    # SQLite also accepts empty statements before the statement being prepared.
    start = 0
    while start < len(words) and words[start] == ";":
        start += 1
    words = words[start:]
    if not words:
        return "", False
    position = 0
    if words[0].upper() == "WITH":
        position = 2 if len(words) > 1 and words[1].upper() == "RECURSIVE" else 1
        while position < len(words):
            position += 1  # The CTE name may itself be a keyword, e.g. replace.
            if position < len(words) and words[position] == "(":
                position = _after_group(words, position)  # Optional column names.
            if position >= len(words) or words[position].upper() != "AS":
                return "", False
            position += 1
            while position < len(words) and words[position].upper() in {
                "NOT",
                "MATERIALIZED",
            }:
                position += 1
            if position >= len(words) or words[position] != "(":
                return "", False
            position = _after_group(words, position)
            if position >= len(words) or words[position] != ",":
                break
            position += 1
    words = [word.upper() for word in words[position:]]
    operation = words[0] if words else ""
    replacing = operation == "REPLACE" or (
        operation in {"INSERT", "UPDATE"} and words[1:3] == ["OR", "REPLACE"]
    )
    return operation, replacing


def _after_group(tokens: list[str], position: int) -> int:
    """Skip a CTE column list or body without inspecting its quoted content."""
    depth = 0
    for index in range(position, len(tokens)):
        if tokens[index] == "(":
            depth += 1
        elif tokens[index] == ")":
            depth -= 1
            if depth == 0:
                return index + 1
    return len(tokens)
