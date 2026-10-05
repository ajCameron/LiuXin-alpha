#!/usr/bin/env python3
"""
Normalize existing Python docstrings to LiuXin's project convention.
"""

from __future__ import annotations

import argparse
import ast
import difflib
import inspect
import json
import re
import sys
import tokenize
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

_DEFINITION_TYPES = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
_FIELD_PATTERN = re.compile(r"^:[A-Za-z][^:]*:")
_PARAM_PATTERN = re.compile(r"^:param\s+([^:]+):(.*)$")
_RETURN_PATTERN = re.compile(r"^:returns?:(.*)$")


@dataclass(frozen=True)
class Audit:
    """
    Summarize docstring coverage and normalization for one source set.

    Example:
        >>> Audit(functions=2).add(Audit(functions=1)).functions
        3

    :ivar modules: Number of parsed module definitions.
    :ivar classes: Number of explicit class definitions, including nested classes.
    :ivar functions: Number of named synchronous and asynchronous functions.
    :ivar missing: Definitions with absent or blank literal docstrings.
    :ivar missing_examples: Documented classes and functions without an example section.
    :ivar non_normalized: Source files whose safe docstrings need normalization.
    :ivar unsafe: Docstrings requiring manual editing to avoid losing source or prose.
    """

    modules: int = 0
    classes: int = 0
    functions: int = 0
    missing: int = 0
    missing_examples: int = 0
    non_normalized: int = 0
    unsafe: int = 0

    def add(self, other: Audit) -> Audit:
        """
        Return the field-wise sum of two audit results.

        Example:
            >>> Audit(classes=1).add(Audit(classes=2)).classes
            3


        :param other: Counts from another source file or partial source set.
        :return: A new audit containing both sets of counts without mutating either input.
        """

        return Audit(
            modules=self.modules + other.modules,
            classes=self.classes + other.classes,
            functions=self.functions + other.functions,
            missing=self.missing + other.missing,
            missing_examples=self.missing_examples + other.missing_examples,
            non_normalized=self.non_normalized + other.non_normalized,
            unsafe=self.unsafe + other.unsafe,
        )


class UnsafeDocstring(ValueError):
    """
    Report a literal that cannot be mechanically rewritten losslessly.

    Example:
        >>> str(UnsafeDocstring("contains escapes"))
        'contains escapes'
    """


def _python_files(paths: Sequence[Path]) -> list[Path]:
    """
    Resolve files and recursively expand directories in stable order.

    Example:
        >>> [str(path) for path in _python_files([Path("b.py"), Path("a.py")])]
        ['a.py', 'b.py']


    :param paths: Python files or directories to expand recursively.
    :return: Sorted unique Python paths; existence is checked when each file is read.
    """

    files: set[Path] = set()
    for path in paths:
        if path.is_dir():
            files.update(path.rglob("*.py"))
        elif path.suffix == ".py":
            files.add(path)
    return sorted(files)


def _nodes(
    tree: ast.Module,
) -> Iterable[ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef]:
    """
    Yield a module followed by all of its class and function definitions.

    Example:
        >>> [type(node).__name__ for node in _nodes(ast.parse("def f(): pass"))]
        ['Module', 'FunctionDef']


    :param tree: Parsed module whose documentation boundaries should be examined.
    :return: The module followed by every nested class and named function.
    """

    yield tree
    yield from (node for node in ast.walk(tree) if isinstance(node, _DEFINITION_TYPES))


def _parameters(
    node: ast.FunctionDef | ast.AsyncFunctionDef, *, implicit_receiver: bool = True
) -> list[str]:
    """
    Return explicit callable parameters in declaration order.

    Example:
        >>> function = ast.parse("def f(self, value, *, flag=False): pass").body[0]
        >>> _parameters(function)  # type: ignore[arg-type]
        ['value', 'flag']


    :param node: Function whose explicit signature fields are being normalized.
    :param implicit_receiver: Whether method binding supplies the first positional argument.
    :return: Explicit parameter names in signature order, without variadic stars.
    """

    parameters = [
        argument.arg
        for argument in (
            *node.args.posonlyargs,
            *node.args.args,
        )
    ]
    if implicit_receiver and parameters:
        parameters.pop(0)
    if node.args.vararg is not None:
        parameters.append(node.args.vararg.arg)
    parameters.extend(argument.arg for argument in node.args.kwonlyargs)
    if node.args.kwarg is not None:
        parameters.append(node.args.kwarg.arg)
    return parameters


def _split_fields(lines: list[str]) -> tuple[list[str], list[list[str]]]:
    """
    Split prose from its ordered Sphinx field groups.

    Example:
        >>> _split_fields(["Summary.", "", ":param value: input"])
        (['Summary.', ''], [[':param value: input']])


    :param lines: Cleaned docstring content split into individual lines.
    :return: Leading prose and field groups with their continuation lines preserved.
    """

    try:
        start = next(
            index for index, line in enumerate(lines) if _FIELD_PATTERN.match(line)
        )
    except StopIteration:
        return lines, []

    prose = lines[:start]
    fields: list[list[str]] = []
    for line in lines[start:]:
        if _FIELD_PATTERN.match(line):
            fields.append([line])
        elif fields:
            fields[-1].append(line)
        else:
            prose.append(line)
    return prose, fields


def _field_sections(
    fields: list[list[str]],
) -> tuple[dict[str, list[str]], list[str] | None, list[list[str]]]:
    """
    Partition parameter, return, and additional field groups.

    Example:
        >>> parameters, returns, other = _field_sections([[":param value:"], [":return:"]])
        >>> (sorted(parameters), returns, other)
        (['value'], [':return:'], [])


    :param fields: reST field blocks, including indented continuation text.
    :return: Parameter blocks by name, the return block, and other ordered fields.
    :raises UnsafeDocstring: If repeated parameter or return fields would overwrite prose.
    """

    parameters: dict[str, list[str]] = {}
    returns: list[str] | None = None
    other: list[list[str]] = []
    for field in fields:
        parameter_match = _PARAM_PATTERN.match(field[0])
        if parameter_match is not None:
            name, description = parameter_match.groups()
            declaration = name.strip().rsplit(maxsplit=1)
            parameter = declaration[-1].lstrip("*")
            if parameter in parameters:
                raise UnsafeDocstring(f"duplicate parameter field: {parameter}")
            parameters[parameter] = [
                f":param {parameter}:" + description,
                *field[1:],
            ]
            if len(declaration) == 2:
                other.append([f":type {parameter}: {declaration[0]}"])
            continue
        return_match = _RETURN_PATTERN.match(field[0])
        if return_match is not None:
            if returns is not None:
                raise UnsafeDocstring("duplicate return field")
            returns = [":return:" + return_match.group(1), *field[1:]]
            continue
        other.append(field)
    return parameters, returns, other


def _normalized_body(
    node: ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef,
    docstring: str,
    *,
    implicit_receiver: bool = False,
) -> list[str]:
    """
    Build a definition's normalized, unindented docstring body.

    Example:
        >>> function = ast.parse("def f(value): pass").body[0]
        >>> body = "Summary." + chr(10) * 2 + "Example:" + chr(10) + "    >>> f(1)"
        >>> _normalized_body(function, body)[-2:]
        [':param value:', ':return:']


    :param node: Declaration whose documentation fields are being normalized.
    :param docstring: Existing literal content, including its descriptive prose.
    :param implicit_receiver: Whether to omit a bound method's first positional argument.
    :return: Cleaned prose followed by canonical, ordered reST fields.
    """

    lines = inspect.cleandoc(docstring).splitlines()
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return lines

    prose, fields = _split_fields(lines)
    parameter_fields, return_field, other_fields = _field_sections(fields)
    explicit_parameters = _parameters(node, implicit_receiver=implicit_receiver)
    unknown_parameters = parameter_fields.keys() - set(explicit_parameters)
    if unknown_parameters:
        raise UnsafeDocstring(
            f"parameter fields absent from signature: {sorted(unknown_parameters)}"
        )
    normalized_fields = [
        parameter_fields.get(name, [f":param {name}:"]) for name in explicit_parameters
    ]
    normalized_fields.append(return_field or [":return:"])
    normalized_fields.extend(other_fields)

    body = prose
    while body and not body[-1]:
        body.pop()
    body.extend(["", ""])
    for field in normalized_fields:
        while field and not field[-1]:
            field.pop()
        body.extend(field)
    return body


def _replacement(
    node: ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef,
    source_lines: list[str],
    *,
    implicit_receiver: bool = False,
) -> tuple[int, int, list[str]] | None:
    """
    Build one source-line replacement for an existing safe docstring.

    Example:
        >>> quotes = chr(34) * 3
        >>> source = ['def f():', f'    {quotes}Summary.{quotes}', '    pass']
        >>> function = ast.parse(chr(10).join(source)).body[0]
        >>> replacement = _replacement(function, source)
        >>> replacement is not None and replacement[2][:2] == ['    ' + quotes, '    Summary.']
        True


    :param node: Declaration with an existing literal docstring.
    :param source_lines: Source split into lines without newline terminators.
    :param implicit_receiver: Whether method binding supplies the first argument.
    :return: A zero-based line replacement, or ``None`` when no docstring exists.
    :raises UnsafeDocstring: If line replacement could lose code, comments, or prose.
    """

    docstring = ast.get_docstring(node, clean=False)
    if docstring is None:
        return None
    expression = node.body[0]
    if not isinstance(expression, ast.Expr) or not isinstance(
        expression.value, ast.Constant
    ):
        raise UnsafeDocstring("docstring expression is not a constant string")

    start = expression.lineno - 1
    end = expression.end_lineno
    if end is None:
        raise UnsafeDocstring("docstring expression has no ending source line")
    first_line = source_lines[start].encode("utf-8")
    last_line = source_lines[end - 1].encode("utf-8")
    if first_line[: expression.col_offset].strip():
        raise UnsafeDocstring("docstring shares its opening line with executable code")
    if last_line[expression.end_col_offset :].strip():
        raise UnsafeDocstring(
            "docstring shares its closing line with code or a comment"
        )
    segment = "\n".join(source_lines[start:end])
    if "\\" in segment or "'''" in docstring or '"""' in docstring:
        raise UnsafeDocstring("literal contains escapes or a triple-quote sequence")

    indent = source_lines[start][
        : len(source_lines[start]) - len(source_lines[start].lstrip())
    ]
    body = _normalized_body(node, docstring, implicit_receiver=implicit_receiver)
    replacement = [indent + '"""']
    replacement.extend(indent + line if line else "" for line in body)
    replacement.append(indent + '"""')
    return start, end, replacement


def normalize_source(source: str, filename: str) -> tuple[str, Audit]:
    """
    Return normalized source and an audit without modifying the file.

    Example:
        >>> quotes = chr(34) * 3
        >>> source = chr(10).join(['def f():', f'    {quotes}Example:', '        >>> f()', f'        {quotes}', ''])
        >>> normalized, audit = normalize_source(source, "sample.py")
        >>> (normalized.startswith(chr(10).join(['def f():', f'    {quotes}', ''])), audit.functions)
        (True, 1)


    :param source: Python source text; no file is modified by this function.
    :param filename: Name included in syntax and safety diagnostics.
    :return: Normalized source and coverage counts, retaining unsafe literals unchanged.
    :raises SyntaxError: If the input cannot be parsed as Python.
    :raises UnsafeDocstring: If the final executable AST differs from the original.
    """

    tree = ast.parse(source, filename=filename)
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    source_lines = source.splitlines()
    replacements: list[tuple[int, int, list[str]]] = []
    audit = Audit()
    for node in _nodes(tree):
        if isinstance(node, ast.Module):
            audit = audit.add(Audit(modules=1))
        elif isinstance(node, ast.ClassDef):
            audit = audit.add(Audit(classes=1))
        else:
            audit = audit.add(Audit(functions=1))

        docstring = ast.get_docstring(node, clean=False)
        if not docstring or not docstring.strip():
            audit = audit.add(Audit(missing=1))
            continue
        if not isinstance(node, ast.Module) and "Example:" not in docstring:
            audit = audit.add(Audit(missing_examples=1))
        try:
            replacement = _replacement(
                node,
                source_lines,
                implicit_receiver=_has_implicit_receiver(node, parents),
            )
        except UnsafeDocstring:
            audit = audit.add(Audit(unsafe=1))
            continue
        if replacement is not None:
            replacements.append(replacement)

    normalized_lines = source_lines[:]
    for start, end, replacement_lines in sorted(replacements, reverse=True):
        normalized_lines[start:end] = replacement_lines
    trailing_newline = "\n" if source.endswith("\n") else ""
    normalized = "\n".join(normalized_lines) + trailing_newline
    if executable_structure(source, filename) != executable_structure(
        normalized, filename
    ):
        raise UnsafeDocstring("normalization changed the executable syntax tree")
    if normalized != source:
        audit = audit.add(Audit(non_normalized=1))
    return normalized, audit


def _has_implicit_receiver(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    """
    Distinguish methods from free, nested, and explicitly static functions.

    Conditional blocks inside a class preserve method binding. Decorated static
    methods do not lose their first real argument even if it is named ``self``.

    Example:
        >>> _has_implicit_receiver(ast.parse('value = 1'), {})
        False


    :param node: Declaration whose enclosing lexical scope is being examined.
    :param parents: Direct AST parent lookup for the parsed module.
    :return: Whether a class namespace binds the function's first positional argument.
    """

    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return False
    if any(
        ast.unparse(value).rsplit(".", 1)[-1] == "staticmethod"
        for value in node.decorator_list
    ):
        return False
    parent = parents.get(node)
    while parent is not None and not isinstance(
        parent, (ast.Module, *_DEFINITION_TYPES)
    ):
        parent = parents.get(parent)
    return isinstance(parent, ast.ClassDef)


def executable_structure(source: str, filename: str = "<source>") -> str:
    """
    Describe executable syntax while ignoring literal documentation and locations.

    Remove only the first string expression in module, class, and function bodies.
    Other string expressions, signatures, decorators, and statement order remain
    part of the comparison. This does not prove that callers observing ``__doc__``
    see identical text; documentation is deliberately allowed to change.

    Example:
        >>> before = '"before"' + chr(10) + 'value = 1'
        >>> after = '"after"' + chr(10) + 'value = 1'
        >>> executable_structure(before) == executable_structure(after)
        True


    :param source: Python source to parse without importing or executing it.
    :param filename: Filename included in syntax-error diagnostics.
    :return: A location-independent AST dump without literal docstring statements.
    :raises SyntaxError: If the source is not valid Python.
    """

    tree = ast.parse(source, filename=filename)
    for node in _nodes(tree):
        if ast.get_docstring(node, clean=False) is not None:
            node.body = node.body[1:]
    return ast.dump(tree, include_attributes=False)


def _read(path: Path) -> str:
    """
    Read Python source using its declared PEP 263 encoding.

    Example:
        >>> source = _read(Path("scripts/normalize_docstrings.py"))  # doctest: +SKIP


    :param path: Python file whose encoding cookie or UTF-8 default should be honored.
    :return: Decoded source text, without importing the module.
    :raises OSError: If the file cannot be opened or read.
    """

    with tokenize.open(path) as source_file:
        return source_file.read()


def _patch(path: Path, source: str, normalized: str) -> list[str]:
    """
    Emit one file section in the repository patch-tool dialect.

    Example:
        >>> _patch(Path("scripts/normalize_docstrings.py"), "same", "same")
        []


    :param path: Source path inside the current working directory.
    :param source: Original source text used as the patch context.
    :param normalized: Proposed replacement text that has passed structural checks.
    :return: An ``apply_patch`` file section, or an empty list for unchanged source.
    """

    if source == normalized:
        return []
    relative = path.resolve().relative_to(Path.cwd().resolve())
    diff = list(
        difflib.unified_diff(
            source.splitlines(),
            normalized.splitlines(),
            fromfile=str(relative),
            tofile=str(relative),
            lineterm="",
        )
    )
    hunks = ["@@" if line.startswith("@@ ") else line for line in diff[2:]]
    return [f"*** Update File: {relative}", *hunks]


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run normalization, patch emission, or a read-only convention check.

    Example:
        >>> exit_code = main(["--check", "src/LiuXin_alpha/storage/api"])  # doctest: +SKIP


    :param argv: Explicit command-line arguments, or ``None`` for process arguments.
    :return: Nonzero for read/parse failures, or an incomplete requested convention check.
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--patch", action="store_true", help="emit an apply_patch patch")
    mode.add_argument(
        "--check", action="store_true", help="fail if normalization is needed"
    )
    mode.add_argument(
        "--audit-json", action="store_true", help="print coverage as JSON"
    )
    arguments = parser.parse_args(argv)

    patch_lines = ["*** Begin Patch"]
    audit = Audit()
    failures: list[str] = []
    for path in _python_files(arguments.paths):
        try:
            source = _read(path)
            normalized, file_audit = normalize_source(source, str(path))
        except (SyntaxError, UnicodeError, OSError, UnsafeDocstring) as error:
            failures.append(f"{path}: {type(error).__name__}: {error}")
            continue
        audit = audit.add(file_audit)
        patch_lines.extend(_patch(path, source, normalized))
    patch_lines.append("*** End Patch")

    if arguments.patch:
        if len(patch_lines) > 2:
            print("\n".join(patch_lines))
    elif arguments.audit_json:
        print(json.dumps({**audit.__dict__, "failures": failures}, indent=2))
    if failures:
        print("\n".join(failures), file=sys.stderr)
    incomplete = audit.non_normalized or audit.missing or audit.unsafe
    return int(bool(failures or (arguments.check and incomplete)))


if __name__ == "__main__":
    raise SystemExit(main())
