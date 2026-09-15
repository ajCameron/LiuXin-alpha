"""
Check that documentation exemptions preserve ownership size and statement limits.

Synthetic Python snippets are fixtures, including intentionally undocumented or
invalid code. Tests measure them without executing them or importing production.
"""

from __future__ import annotations

import ast

import pytest

from tests.support.docstring_ownership import SourceMetrics, body_without_docstring


@pytest.mark.parametrize(
    "header", ["", "class Owner:", "def owner():", "async def owner():"]
)
def test_leading_docstrings_on_each_owner(header: str) -> None:
    """
    Exempt a leading literal on modules, classes, and both function forms.

    Example:
        >>> test_leading_docstrings_on_each_owner('class Owner:')


    :param header: Declaration line, or an empty string for a module body.
    :return: None after checking the retained size and direct body statements.
    """
    source = header + '\n    "Docs"\n    pass\n' if header else '"Docs"\npass\n'
    metrics = SourceMetrics(source)
    owner = metrics.tree.body[0] if header else metrics.tree
    assert metrics.line_count() == (2 if header else 1)
    assert metrics.line_count(owner) == (2 if header else 1)
    assert [type(node) for node in body_without_docstring(owner)] == [ast.Pass]


@pytest.mark.parametrize(
    "literal",
    [
        '""',
        '"  "',
        'r"raw"',
        'u"unicode"',
        "'single'",
        '"""multi\n\nline"""',
        '("one"\n "two")',
    ],
)
def test_literal_spellings_and_empty_docstrings(literal: str) -> None:
    """
    Recognize empty, prefixed, multiline, and implicitly concatenated docs.

    Example:
        >>> test_literal_spellings_and_empty_docstrings('r"raw"')


    :param literal: Valid leading string expression to place before executable code.
    :return: None after verifying only the following assignment is counted.
    """
    metrics = SourceMetrics(literal + "\nvalue = 1\n")
    assert metrics.line_count() == 1
    assert [type(node) for node in body_without_docstring(metrics.tree)] == [ast.Assign]


@pytest.mark.parametrize(
    "source",
    [
        'value = "assigned"\n',
        'b"bytes"\n',
        'f"formatted"\n',
        'f"{value}"\n',
        '"one" + "two"\n',
        'pass\n"late string"\n',
        'if True:\n    "conditional string"\n',
        'lambda: "lambda value"\n',
    ],
)
def test_non_docstrings_keep_their_lines_and_statements(source: str) -> None:
    """
    Keep ordinary strings and expressions that Python does not treat as docs.

    Example:
        >>> test_non_docstrings_keep_their_lines_and_statements('f"formatted"')


    :param source: Complete fixture containing no recognized leading docstring.
    :return: None after requiring unchanged file size and top-level statement membership.
    """
    metrics = SourceMetrics(source)
    assert metrics.line_count() == len(source.splitlines())
    assert body_without_docstring(metrics.tree) == metrics.tree.body


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ('"Docs"; value = 1\n', 1),
        ('"Docs";\n', 1),
        ('def café(): "hé"; return run()\n', 1),
        ('class É: "hé"\n', 1),
        ('"""Docs\ncontinued"""; value = 1\n', 1),
        ('"Docs"  # retained comment\n', 1),
        ('"""Docs\ncontinued"""  # retained comment\n', 1),
        ('# before\n\n"Docs"\n\n# after\nvalue = 1\n', 5),
        ('("one"\n # retained\n\n "two"\n)\nvalue = 1\n', 3),
        ('"""# literal content\n\nmore"""\nvalue = 1\n', 1),
    ],
)
def test_mixed_lines_comments_and_blank_lines(source: str, expected: int) -> None:
    """
    Retain shared code/comment lines and whitespace outside literal content.

    Unicode names exercise the difference between AST byte and token character
    columns. Parentheses belonging solely to a docstring can be excluded, while
    comments and blank lines between concatenated literals remain counted.

    Example:
        >>> test_mixed_lines_comments_and_blank_lines('"Docs"; value = 1', 1)


    :param source: Complete fixture mixing documentation and retained physical lines.
    :param expected: Independently specified count after only documentation-only lines are excluded.
    :return: None after checking the exact retained file size.
    """
    assert SourceMetrics(source).line_count() == expected


def test_nested_owners_and_decorator_span_boundaries() -> None:
    """
    Exclude nested docs once and preserve existing AST decorator boundaries.

    Example:
        >>> test_nested_owners_and_decorator_span_boundaries()


    :return: None after verifying file, outer async, class, and inner method sizes.
    """
    source = "\n".join(
        [
            "@decorate",
            "async def outer():",
            '    """Outer docs',
            "",
            "    more.",
            '    """',
            "    # keep",
            "    class Nested:",
            '        "Class docs"',
            "        def inner(self):",
            '            "Inner docs"',
            "            return 1",
            "",
            "    return Nested",
        ]
    )
    metrics = SourceMetrics(source)
    outer = metrics.tree.body[0]
    nested = outer.body[1]
    inner = nested.body[1]
    assert metrics.line_count() == 8
    assert metrics.line_count(outer) == 7
    assert metrics.line_count(nested) == 3
    assert metrics.line_count(inner) == 2


def test_body_filter_preserves_delegation_evidence_and_input_tree() -> None:
    """
    Keep original call nodes and retain a second string as a separate statement.

    Example:
        >>> test_body_filter_preserves_delegation_evidence_and_input_tree()


    :return: None after checking list independence, nested bodies, and delegate call identity.
    """
    metrics = SourceMetrics('def f():\n    "Docs"\n    return target(1)\n')
    original = ast.dump(metrics.tree, include_attributes=True)
    owner = metrics.tree.body[0]
    body = body_without_docstring(owner)
    assert len(body) == 1
    assert body[0] is owner.body[1]
    assert isinstance(body[0], ast.Return) and isinstance(body[0].value, ast.Call)
    body.append(ast.Pass())
    assert ast.dump(metrics.tree, include_attributes=True) == original
    assert body_without_docstring(metrics.tree)[0].body is owner.body
    extra = ast.parse(
        'def f():\n    "Docs"\n    "Another string"\n    return target(1)\n'
    ).body[0]
    assert [type(node) for node in body_without_docstring(extra)] == [
        ast.Expr,
        ast.Return,
    ]


@pytest.mark.parametrize("ceiling", [120, 250, 450, 900])
def test_large_docs_do_not_hide_oversized_files(ceiling: int) -> None:
    """
    Allow large documentation while still detecting one line beyond each file cap.

    Example:
        >>> test_large_docs_do_not_hide_oversized_files(120)


    :param ceiling: Existing ownership file limit, used without altering its comparison.
    :return: None after checking exact-limit acceptance and oversized rejection.
    """
    docs = '"""\n' + "Documentation.\n" * 1000 + '"""\n'
    bounded = docs + "pass\n" * ceiling
    oversized = bounded + "pass\n"
    assert SourceMetrics(bounded).line_count() == ceiling
    assert SourceMetrics(bounded).line_count() <= ceiling
    assert SourceMetrics(oversized).line_count() == ceiling + 1
    assert not SourceMetrics(oversized).line_count() <= ceiling


@pytest.mark.parametrize(
    ("statements", "inclusive_ok", "exclusive_ok"),
    [(9, True, True), (10, True, False), (159, True, False), (160, False, False)],
)
def test_function_spans_preserve_limit_arithmetic(
    statements: int, inclusive_ok: bool, exclusive_ok: bool
) -> None:
    """
    Preserve inclusive 160-line and exclusive less-than-ten span comparisons.

    Example:
        >>> test_function_spans_preserve_limit_arithmetic(9, True, True)


    :param statements: Number of retained body statements after a long function docstring.
    :param inclusive_ok: Expected result for the existing inclusive 160-line ceiling.
    :param exclusive_ok: Expected result for the facade's exclusive span below ten.
    :return: None after validating both comparisons and the exact inclusive size.
    """
    source = (
        'def f():\n    """\n'
        + "    Docs.\n" * 200
        + '    """\n'
        + "    pass\n" * statements
    )
    metrics = SourceMetrics(source)
    size = metrics.line_count(metrics.tree.body[0])
    assert size == statements + 1
    assert (size <= 160) is inclusive_ok
    assert (size - 1 < 10) is exclusive_ok


@pytest.mark.parametrize("ending", ["\n", "\r\n", "\r"])
def test_newline_encodings_and_final_newline(ending: str) -> None:
    """
    Keep equivalent measurements across Python newline encodings and EOF endings.

    Example:
        >>> test_newline_encodings_and_final_newline(chr(10))


    :param ending: LF, CRLF, or CR separator used throughout the source fixture.
    :return: None after checking function spans with and without a final newline.
    """
    source = ending.join(["# header", "def f():", '    "Docs"', "    return 1"])
    for text in (source, source + ending):
        metrics = SourceMetrics(text)
        assert metrics.line_count() == 3
        assert metrics.line_count(metrics.tree.body[0]) == 2


@pytest.mark.parametrize(
    "source", ["", "\n", "# comment\n\n", "\fvalue = 1\n", 'value = "a\u2028b"\n']
)
def test_unchanged_file_counts_without_docs(source: str) -> None:
    """
    Preserve splitlines counts even for empty input and non-newline separators.

    Example:
        >>> test_unchanged_file_counts_without_docs('')


    :param source: Valid source with no docstring, including splitlines edge cases.
    :return: None after comparing against the original physical-file measurement.
    """
    assert SourceMetrics(source).line_count() == len(source.splitlines())


def test_invalid_source_and_missing_locations_fail_closed() -> None:
    """
    Reject invalid Python and unlocated AST nodes rather than returning a size.

    Example:
        >>> test_invalid_source_and_missing_locations_fail_closed()


    :return: None after checking explicit failures for malformed measurement inputs.
    """
    with pytest.raises(SyntaxError):
        SourceMetrics("def broken(")
    with pytest.raises(ValueError, match="source line locations"):
        SourceMetrics("pass").line_count(ast.Pass())
