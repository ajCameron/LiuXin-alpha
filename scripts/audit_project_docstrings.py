#!/usr/bin/env python3
"""
Inventory documentation gaps across repository-owned Python source without imports.

The default scope includes tracked Python files in initialized submodules and
new, non-ignored Python files in the main checkout. Virtual environments, ignored
build artifacts, and anonymous lambdas are not source definitions in this audit.
Private, nested, asynchronous, test, example, and inherited definitions count.

The report checks observable conventions; it cannot prove that prose accurately
explains an implementation. Review descriptions against source before declaring
a file complete. Explicit paths provide a batch view, never a project-wide pass.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import tokenize
from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DefinitionNode = ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
PARAMETER = re.compile(r"^:param\s+([^:]+):(.*)$")
RETURN = re.compile(r"^:returns?:(.*)$")


@dataclass(frozen=True)
class Definition:
    """
    Identify a source declaration and whether it receives an implicit instance.

    Example:
        >>> item = next(definitions(ast.parse('def f(): pass')))
        >>> item.name
        '<module>'

    :ivar node: Parsed module, class, or named function; never executed.
    :ivar name: Dotted lexical name, including enclosing classes and functions.
    :ivar bound: Whether the first positional parameter is supplied by binding.
    """

    node: DefinitionNode
    name: str
    bound: bool = False


def definitions(
    node: ast.AST, prefix: str = "", class_scope: bool = False
) -> Iterator[Definition]:
    """
    Walk named declarations while retaining their lexical ownership.

    Definitions inside conditionals keep the enclosing scope. Entering a function
    ends class scope, so nested helpers and static methods retain all parameters.

    Example:
        >>> source = chr(10).join(['class A:', '    def f(self): pass'])
        >>> [item.name for item in definitions(ast.parse(source))]
        ['<module>', 'A', 'A.f']


    :param node: AST subtree whose definitions should be visited.
    :param prefix: Dotted name of the enclosing declaration, if any.
    :param class_scope: Whether this subtree is directly in a class namespace.
    :return: Definitions in source traversal order, starting with the module.
    """

    if isinstance(node, ast.Module):
        yield Definition(node, "<module>")
    elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        name = f"{prefix}.{node.name}" if prefix else node.name
        decorators = {
            ast.unparse(value).rsplit(".", 1)[-1] for value in node.decorator_list
        }
        bound = (
            class_scope
            and not isinstance(node, ast.ClassDef)
            and "staticmethod" not in decorators
        )
        yield Definition(node, name, bound)
        prefix = name
        class_scope = isinstance(node, ast.ClassDef)
    for child in ast.iter_child_nodes(node):
        yield from definitions(child, prefix, class_scope)


def parameters(definition: Definition) -> list[str]:
    """
    List explicit arguments in signature order, excluding only implicit receivers.

    Example:
        >>> node = ast.parse('def f(first, *items, flag=False, **options): pass').body[0]
        >>> parameters(Definition(node, 'f'))
        ['first', 'items', 'flag', 'options']


    :param definition: Function declaration and its binding context.
    :return: Parameter names without variadic stars, or an empty list for other kinds.
    """

    node = definition.node
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return []
    positional = [item.arg for item in (*node.args.posonlyargs, *node.args.args)]
    names = positional[1:] if definition.bound else positional
    if node.args.vararg:
        names.append(node.args.vararg.arg)
    names.extend(item.arg for item in node.args.kwonlyargs)
    if node.args.kwarg:
        names.append(node.args.kwarg.arg)
    return names


def _has_description(lines: list[str], index: int, inline: str) -> bool:
    """
    Recognize inline or indented continuation text belonging to a reST field.

    Example:
        >>> _has_description([':return:', '    The selected row.'], 0, '')
        True


    :param lines: Cleaned docstring lines in their original order.
    :param index: Line containing the field declaration.
    :param inline: Text after the field's closing colon.
    :return: Whether the field has nonblank descriptive content.
    """

    if inline.strip():
        return True
    for continuation in lines[index + 1 :]:
        if continuation.strip():
            return continuation[0].isspace()
    return False


def documentation_issues(
    definition: Definition, source: str | Sequence[str]
) -> tuple[str, ...]:
    """
    Report missing prose, examples, canonical fields, and delimiter layout.

    Field presence is not semantic sign-off: descriptions still require a source
    review. Existing field text is only inspected, never rewritten or discarded.

    Example:
        >>> item = next(definitions(ast.parse('value = 1')))
        >>> documentation_issues(item, 'value = 1')
        ('missing_docstring',)


    :param definition: One named source declaration and its lexical context.
    :param source: Complete source or pre-split lines for inspecting literal delimiters.
    :return: Stable issue codes; an empty tuple means structural checks passed.
    """

    node = definition.node
    docstring = ast.get_docstring(node)
    if not docstring or not docstring.strip():
        return ("missing_docstring",)
    lines = docstring.splitlines()
    issues = []
    summary = lines[0].strip()
    if summary.startswith(":") or summary.lower().startswith(("todo", "fixme")):
        issues.append("missing_description")
    source_lines = source.splitlines() if isinstance(source, str) else source
    expression = node.body[0]
    first = (
        source_lines[expression.lineno - 1]
        .encode("utf-8")[expression.col_offset :]
        .decode("utf-8")
        .strip()
    )
    last = source_lines[(expression.end_lineno or expression.lineno) - 1].strip()
    if (
        (expression.end_lineno or expression.lineno) - expression.lineno < 2
        or first.lower() not in {'"""', 'r"""', 'u"""'}
        or last != '"""'
    ):
        issues.append("delimiter_layout")
    if not isinstance(node, ast.Module) and "Example:" not in docstring:
        issues.append("missing_example")
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return tuple(issues)
    actual = []
    returns = []
    for index, line in enumerate(lines):
        match = PARAMETER.match(line)
        if match:
            declaration, text = match.groups()
            actual.append(declaration.rsplit(maxsplit=1)[-1].lstrip("*"))
            if not _has_description(lines, index, text):
                issues.append("empty_parameter_description")
        match = RETURN.match(line)
        if match:
            returns.append(index)
            if not _has_description(lines, index, match.group(1)):
                issues.append("empty_return_description")
    if actual != parameters(definition):
        issues.append("parameter_fields")
    if len(returns) != 1:
        issues.append("return_field")
    return tuple(dict.fromkeys(issues))


def project_paths(root: Path = ROOT) -> tuple[Path, ...]:
    """
    Discover tracked source, submodule source, and new non-ignored Python files.

    Example:
        >>> paths = project_paths()  # doctest: +SKIP


    :param root: Git working-tree root whose source should be inventoried.
    :return: Deduplicated absolute paths in repository-relative lexical order.
    :raises subprocess.CalledProcessError: If Git cannot enumerate the checkout.
    """

    names = set()
    for flags in (("--recurse-submodules",), ("--others", "--exclude-standard")):
        output = subprocess.check_output(
            ["git", "-C", str(root), "ls-files", "-z", *flags, "--", "*.py", "*.pyi"]
        )
        names.update(name.decode("utf-8") for name in output.split(b"\0") if name)
    return tuple(root / name for name in sorted(names))


def audit_paths(paths: Sequence[Path], root: Path = ROOT) -> dict[str, object]:
    """
    Parse every requested file and collect definition-level findings and failures.

    No source module is imported. Parse and filesystem errors remain explicit
    failures; they cannot silently reduce coverage or produce a clean check.

    Example:
        >>> audit_paths([], Path.cwd())['totals']
        {}


    :param paths: Python source files to inspect in the supplied order.
    :param root: Root used to display portable source paths in reports.
    :return: Counts, issue totals, detailed findings, and file-read/parse errors.
    """

    totals: Counter[str] = Counter()
    issues: Counter[str] = Counter()
    findings = []
    failures = []
    for path in paths:
        name = path.relative_to(root).as_posix()
        try:
            with tokenize.open(path) as stream:
                source = stream.read()
            tree = ast.parse(source, filename=name)
        except (SyntaxError, UnicodeError, OSError) as error:
            failures.append({"path": name, "error": str(error)})
            continue
        source_lines = source.splitlines()
        for definition in definitions(tree):
            node = definition.node
            kind = (
                "modules"
                if isinstance(node, ast.Module)
                else "classes"
                if isinstance(node, ast.ClassDef)
                else "functions"
            )
            totals[kind] += 1
            problems = documentation_issues(definition, source_lines)
            if "missing_docstring" in problems:
                totals[f"missing_{kind}"] += 1
            issues.update(problems)
            if problems:
                findings.append(
                    {
                        "path": name,
                        "name": definition.name,
                        "kind": kind,
                        "line": getattr(node, "lineno", 1),
                        "issues": problems,
                    }
                )
    return {
        "totals": dict(totals),
        "issue_counts": dict(issues),
        "findings": findings,
        "failures": failures,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """
    Print a whole-project or explicitly bounded documentation audit.

    Example:
        >>> main(['--check', 'scripts/audit_project_docstrings.py'])  # doctest: +SKIP


    :param argv: Command-line arguments, or ``None`` to use the process arguments.
    :return: One for read/parse errors or an incomplete ``--check``; otherwise zero.
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Explicit batch files; omit for the whole project.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail while any documentation issues remain.",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Include per-definition findings in stdout JSON.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the full generated JSON audit to this report path.",
    )
    args = parser.parse_args(argv)
    paths = (
        tuple(path.resolve() for path in args.paths) if args.paths else project_paths()
    )
    report = audit_paths(paths)
    report["scope"] = "explicit-files" if args.paths else "whole-project"
    report["files_requested"] = len(paths)
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                key: value
                for key, value in report.items()
                if args.details or key != "findings"
            },
            indent=2,
        )
    )
    return int(bool(report["failures"] or (args.check and report["findings"])))


if __name__ == "__main__":
    raise SystemExit(main())
