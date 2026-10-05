"""
Measure ownership limits while excluding recognized Python docstrings.

Only leading string-literal expressions on modules, classes, and sync/async
functions qualify. Comments, blank lines outside literals, and other strings
still count. A line shared with non-documentation tokens remains counted in full.
Parsing and tokenization never import or execute the measured source.
"""

from __future__ import annotations

import ast
import io
import tokenize
from bisect import bisect_right

type DocstringOwner = ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef


def body_without_docstring(node: DocstringOwner) -> list[ast.stmt]:
    """
    Copy an owner's direct body, omitting only its leading literal docstring.

    Retained statements are the original AST objects, in order. The input body
    and all nested bodies remain unchanged; a second string expression still
    counts. Empty literal docstrings qualify even though their value is falsey.

    Example:
        >>> owner = ast.parse('def f(): "Docs"; return run()').body[0]
        >>> [type(statement).__name__ for statement in body_without_docstring(owner)]
        ['Return']


    :param node: Parsed module, class, function, or async function owning a body.
    :return: New list containing the direct statements other than the docstring.
    """
    return node.body[1:] if ast.get_docstring(node) is not None else node.body[:]


def _character_position(
    lines: list[str], line: int, byte_column: int
) -> tuple[int, int]:
    """
    Convert an AST UTF-8 byte column to the character column used by tokenize.

    Example:
        >>> _character_position(['é = 1'], 1, 3)
        (1, 2)


    :param lines: Parser-aligned source lines, with newline endings retained.
    :param line: One-based AST line number within lines.
    :param byte_column: Zero-based UTF-8 offset at a complete character boundary.
    :return: One-based line number and zero-based character column.
    """
    return line, len(lines[line - 1].encode("utf-8")[:byte_column].decode("utf-8"))


def _docstring_only_lines(tree: ast.Module, lines: list[str]) -> set[int]:
    """
    Locate lines occupied by docstring literals or their parentheses alone.

    Token spans distinguish comments and blank lines between concatenated
    literals from content inside a multiline literal. Semicolons and all tokens
    outside a docstring expression keep their lines counted. Sorted, disjoint
    expression spans allow token lookup without comparing every token to every
    owner. No AST nodes or source lines are modified.

    Example:
        >>> lines = ['"Docs"' + chr(10), 'value = 1' + chr(10)]
        >>> sorted(_docstring_only_lines(ast.parse(''.join(lines)), lines))
        [1]


    :param tree: Parsed module corresponding exactly to lines.
    :param lines: Source split at Python newlines, retaining each ending.
    :return: One-based line numbers eligible for exclusion from size limits.
    """
    spans = []
    for owner in ast.walk(tree):
        if isinstance(
            owner, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            if ast.get_docstring(owner) is None:
                continue
            expression = owner.body[0]
            assert expression.end_lineno is not None
            assert expression.end_col_offset is not None
            spans.append(
                (
                    _character_position(
                        lines, expression.lineno, expression.col_offset
                    ),
                    _character_position(
                        lines, expression.end_lineno, expression.end_col_offset
                    ),
                )
            )
    spans.sort()
    starts = [start for start, _end in spans]
    documentation, retained = set(), set()
    layout = {
        tokenize.INDENT,
        tokenize.DEDENT,
        tokenize.NL,
        tokenize.NEWLINE,
        tokenize.ENDMARKER,
    }
    for token in tokenize.generate_tokens(io.StringIO("".join(lines)).readline):
        if token.type in layout:
            continue
        index = bisect_right(starts, token.start) - 1
        in_docstring = index >= 0 and token.end <= spans[index][1]
        destination = (
            documentation
            if in_docstring and token.type in {tokenize.STRING, tokenize.OP}
            else retained
        )
        destination.update(range(token.start[0], token.end[0] + 1))
    return documentation - retained


class SourceMetrics:
    """
    Parse source once and reuse docstring-aware file and declaration sizes.

    File counts retain splitlines semantics. Declaration counts retain the
    inclusive AST lineno/end_lineno span, including nested code and excluding
    decorators outside that span. Only documentation-only lines are subtracted.
    Nodes passed to line_count must come from tree; do not mutate that tree.

    Example:
        >>> metrics = SourceMetrics('def f():' + chr(10) + '    "Docs"' + chr(10) + '    return 1')
        >>> metrics.line_count(), metrics.line_count(metrics.tree.body[0])
        (2, 2)
    """

    def __init__(self, source: str) -> None:
        """
        Parse source and cache the lines that contain documentation alone.

        Universal newline conversion keeps token and AST coordinates aligned
        for LF, CRLF, and CR source. Invalid Python raises SyntaxError; it never
        produces a permissive size result.

        Example:
            >>> SourceMetrics('value = 1').line_count()
            1


        :param source: Complete Python module text to measure without executing it.
        :return: None after creating tree and caching file/line measurements.
        """
        self._lines = io.StringIO(source, newline=None).readlines()
        self.tree = ast.parse("".join(self._lines))
        self._docstring_lines = _docstring_only_lines(self.tree, self._lines)
        self._file_line_count = sum(
            len(line.splitlines())
            for number, line in enumerate(self._lines, 1)
            if number not in self._docstring_lines
        )

    def line_count(self, node: ast.AST | None = None) -> int:
        """
        Count retained file lines or the inclusive span of a parsed declaration.

        Use line_count(node) - 1 for an existing end-minus-start comparison;
        this preserves its exclusive-span arithmetic and comparison operator.
        Nested docstrings are excluded from containing spans too, once per line.
        Missing AST locations raise ValueError instead of reporting a small size.

        Example:
            >>> metrics = SourceMetrics('"Docs"' + chr(10) + 'pass')
            >>> metrics.line_count(metrics.tree)
            1


        :param node: Node from tree to measure, or None/the module for the whole file.
        :return: Retained physical file count or inclusive AST line count.
        """
        if node is None or node is self.tree:
            return self._file_line_count
        start = getattr(node, "lineno", None)
        end = getattr(node, "end_lineno", None)
        if start is None or end is None:
            raise ValueError("A measured node must have source line locations")
        return (
            end
            - start
            + 1
            - sum(start <= line <= end for line in self._docstring_lines)
        )
