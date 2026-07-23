#!/usr/bin/env python3
"""Add and verify baseline annotations in :mod:`LiuXin_alpha.file_formats`.

This is intentionally a conservative migration tool. It preserves existing
annotations, infers only syntax-level types which cannot depend on runtime
context, and marks unresolved legacy boundaries as ``typing.Any``. The latter
are explicit review points which can be narrowed package-by-package without
leaving callables silently untyped.
"""

from __future__ import annotations

import argparse
import ast
from collections.abc import Iterable, Sequence
from pathlib import Path
import sys
from typing import Final

import libcst as cst


REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_ROOT: Final = REPO_ROOT / "src" / "LiuXin_alpha" / "file_formats"


def _annotation(code: str) -> cst.Annotation:
    return cst.Annotation(cst.parse_expression(code))


def _literal_annotation(expression: cst.BaseExpression | None) -> str:
    if expression is None:
        return "_typing.Any"
    if isinstance(expression, cst.Name):
        return {
            "True": "bool",
            "False": "bool",
            "None": "_typing.Any",
        }.get(expression.value, "_typing.Any")
    if isinstance(expression, cst.SimpleString):
        try:
            value = expression.evaluated_value
        except Exception:
            return "_typing.Any"
        return "bytes" if isinstance(value, bytes) else "str"
    if isinstance(expression, cst.Integer):
        return "int"
    if isinstance(expression, cst.Float):
        return "float"
    if isinstance(expression, cst.Imaginary):
        return "complex"
    if isinstance(expression, cst.List):
        return "list[_typing.Any]"
    if isinstance(expression, cst.Dict):
        return "dict[_typing.Any, _typing.Any]"
    if isinstance(expression, cst.Set):
        return "set[_typing.Any]"
    if isinstance(expression, cst.Tuple):
        return "tuple[_typing.Any, ...]"
    if isinstance(expression, cst.Lambda):
        return "_typing.Callable[..., _typing.Any]"
    return "_typing.Any"


def _return_expression_annotation(expression: cst.BaseExpression | None) -> str:
    if expression is None:
        return "None"
    if isinstance(expression, cst.Name):
        return {
            "True": "bool",
            "False": "bool",
            "None": "None",
        }.get(expression.value, "_typing.Any")
    if isinstance(expression, cst.FormattedString):
        return "str"
    if isinstance(expression, cst.Comparison):
        return "bool"
    if isinstance(expression, cst.BooleanOperation):
        return "bool"
    if isinstance(expression, cst.UnaryOperation):
        return _return_expression_annotation(expression.expression)
    return _literal_annotation(expression)


class _FunctionFlow(cst.CSTVisitor):
    """Collect returns and yields without descending into nested callables."""

    def __init__(self) -> None:
        self.returns: list[cst.BaseExpression | None] = []
        self.has_yield = False

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
        return False

    def visit_Lambda(self, node: cst.Lambda) -> bool:
        return False

    def visit_Return(self, node: cst.Return) -> None:
        self.returns.append(node.value)

    def visit_Yield(self, node: cst.Yield) -> None:
        self.has_yield = True


def _return_annotation(node: cst.FunctionDef) -> str:
    if node.name.value == "__init__":
        return "None"

    flow = _FunctionFlow()
    node.body.visit(flow)
    if flow.has_yield:
        if node.asynchronous is not None:
            return "_typing.AsyncIterator[_typing.Any]"
        return "_typing.Iterator[_typing.Any]"
    if not flow.returns:
        return "None"

    inferred = {_return_expression_annotation(expression) for expression in flow.returns}
    if "_typing.Any" in inferred:
        return "_typing.Any"
    if inferred == {"None"}:
        return "None"
    if "None" in inferred:
        inferred.remove("None")
        return " | ".join((*sorted(inferred), "None"))
    return " | ".join(sorted(inferred))


class _AnnotationTransformer(cst.CSTTransformer):
    def __init__(self) -> None:
        self.changed = False
        self.used_typing = False
        self._containers: list[str] = []
        self._method_stack: list[bool] = []

    def visit_ClassDef(self, node: cst.ClassDef) -> bool:
        self._containers.append("class")
        return True

    def leave_ClassDef(
        self,
        original_node: cst.ClassDef,
        updated_node: cst.ClassDef,
    ) -> cst.ClassDef:
        self._containers.pop()
        return updated_node

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
        self._method_stack.append(
            bool(self._containers and self._containers[-1] == "class")
        )
        self._containers.append("function")
        return True

    def _annotate_parameter(
        self,
        parameter: cst.Param,
        *,
        receiver: bool = False,
    ) -> cst.Param:
        if parameter.annotation is not None:
            return parameter
        if receiver and parameter.name.value == "self":
            inferred = "_typing.Self"
        elif receiver and parameter.name.value == "cls":
            inferred = "type[_typing.Self]"
        else:
            inferred = _literal_annotation(parameter.default)
        self.changed = True
        self.used_typing |= inferred.startswith("_typing.")
        if parameter.default is not None:
            return parameter.with_changes(
                annotation=_annotation(inferred),
                equal=cst.AssignEqual(
                    whitespace_before=cst.SimpleWhitespace(" "),
                    whitespace_after=cst.SimpleWhitespace(" "),
                ),
            )
        return parameter.with_changes(annotation=_annotation(inferred))

    def leave_FunctionDef(
        self,
        original_node: cst.FunctionDef,
        updated_node: cst.FunctionDef,
    ) -> cst.FunctionDef:
        self._containers.pop()
        is_method = self._method_stack.pop()
        parameters = updated_node.params
        positional = (*parameters.posonly_params, *parameters.params)
        receiver = positional[0] if is_method and positional else None
        star_arg = parameters.star_arg
        if isinstance(star_arg, cst.Param):
            star_arg = self._annotate_parameter(star_arg)
        star_kwarg = parameters.star_kwarg
        if star_kwarg is not None:
            star_kwarg = self._annotate_parameter(star_kwarg)

        updated_parameters = parameters.with_changes(
            posonly_params=tuple(
                self._annotate_parameter(
                    parameter,
                    receiver=parameter is receiver,
                )
                for parameter in parameters.posonly_params
            ),
            params=tuple(
                self._annotate_parameter(
                    parameter,
                    receiver=parameter is receiver,
                )
                for parameter in parameters.params
            ),
            star_arg=star_arg,
            kwonly_params=tuple(
                self._annotate_parameter(parameter)
                for parameter in parameters.kwonly_params
            ),
            star_kwarg=star_kwarg,
        )

        returns = updated_node.returns
        if returns is None:
            inferred = _return_annotation(original_node)
            returns = _annotation(inferred)
            self.changed = True
            self.used_typing |= inferred.startswith("_typing.")

        return updated_node.with_changes(
            params=updated_parameters,
            returns=returns,
        )


def _is_docstring(statement: cst.BaseStatement) -> bool:
    return (
        isinstance(statement, cst.SimpleStatementLine)
        and len(statement.body) == 1
        and isinstance(statement.body[0], cst.Expr)
        and isinstance(statement.body[0].value, cst.SimpleString)
    )


def _is_future_import(statement: cst.BaseStatement) -> bool:
    return (
        isinstance(statement, cst.SimpleStatementLine)
        and len(statement.body) == 1
        and isinstance(statement.body[0], cst.ImportFrom)
        and isinstance(statement.body[0].module, cst.Name)
        and statement.body[0].module.value == "__future__"
    )


def _has_future_annotations(module: cst.Module) -> bool:
    for statement in module.body:
        if not _is_future_import(statement):
            continue
        import_from = statement.body[0]
        assert isinstance(import_from, cst.ImportFrom)
        if isinstance(import_from.names, cst.ImportStar):
            continue
        if any(alias.evaluated_name == "annotations" for alias in import_from.names):
            return True
    return False


def _with_required_imports(
    module: cst.Module,
    *,
    add_future: bool,
    add_typing: bool,
) -> cst.Module:
    body = list(module.body)
    future_index = 1 if body and _is_docstring(body[0]) else 0
    while future_index < len(body) and _is_future_import(body[future_index]):
        future_index += 1

    if add_future:
        body.insert(
            future_index,
            cst.parse_statement("from __future__ import annotations\n"),
        )
        future_index += 1
    if add_typing:
        typing_import = cst.parse_statement("import typing as _typing\n")
        assert isinstance(typing_import, cst.SimpleStatementLine)
        body.insert(
            future_index,
            typing_import.with_changes(leading_lines=(cst.EmptyLine(),)),
        )
    return module.with_changes(body=tuple(body))


def annotate_source(source: str) -> tuple[str, bool]:
    module = cst.parse_module(source)
    transformer = _AnnotationTransformer()
    updated = module.visit(transformer)

    add_future = not _has_future_annotations(updated)
    add_typing = transformer.used_typing and "import typing as _typing" not in source
    if add_future or add_typing:
        updated = _with_required_imports(
            updated,
            add_future=add_future,
            add_typing=add_typing,
        )
    code = updated.code
    if code and not code.endswith(("\n", "\r")):
        code += updated.default_newline
    changed = transformer.changed or add_future or add_typing or code != source
    return code, changed


def _python_files(paths: Sequence[Path]) -> tuple[Path, ...]:
    files: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved.is_dir():
            files.update(resolved.rglob("*.py"))
        elif resolved.suffix == ".py":
            files.add(resolved)
        else:
            raise ValueError(f"Not a Python file or directory: {path}")
    return tuple(sorted(files))


def _annotation_gaps(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    gaps: list[str] = []
    has_future = any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in tree.body
    )
    if not has_future:
        gaps.append("missing 'from __future__ import annotations'")

    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.returns is None:
            gaps.append(f"line {node.lineno}: {node.name} has no return annotation")
        parameters = (
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
        )
        for parameter in parameters:
            if parameter.annotation is None:
                gaps.append(
                    f"line {node.lineno}: {node.name}.{parameter.arg} is unannotated"
                )
        for parameter in (node.args.vararg, node.args.kwarg):
            if parameter is not None and parameter.annotation is None:
                gaps.append(
                    f"line {node.lineno}: {node.name}.{parameter.arg} is unannotated"
                )
    return gaps


def check_annotations(files: Iterable[Path]) -> int:
    gap_count = 0
    file_count = 0
    for path in files:
        file_count += 1
        gaps = _annotation_gaps(path)
        gap_count += len(gaps)
        for gap in gaps:
            print(f"{path.relative_to(REPO_ROOT)}: {gap}")
    if gap_count:
        print(f"{gap_count} annotation gaps across {file_count} Python files.")
        return 1
    print(f"Annotation coverage complete across {file_count} Python files.")
    return 0


def write_annotations(files: Iterable[Path]) -> int:
    changed_count = 0
    materialised = tuple(files)
    for path in materialised:
        source = path.read_text(encoding="utf-8")
        updated, changed = annotate_source(source)
        if changed:
            path.write_text(updated, encoding="utf-8")
            changed_count += 1
    print(f"Annotated {changed_count} of {len(materialised)} Python files.")
    return check_annotations(materialised)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check",
        action="store_true",
        help="verify complete callable annotation coverage (the default)",
    )
    mode.add_argument(
        "--write",
        action="store_true",
        help="add conservative annotations before checking coverage",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        default=[DEFAULT_ROOT],
        help="files or directories below file_formats (default: the full package)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    paths = args.paths or [DEFAULT_ROOT]
    try:
        files = _python_files(paths)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    if args.write:
        return write_annotations(files)
    return check_annotations(files)


if __name__ == "__main__":
    raise SystemExit(main())
