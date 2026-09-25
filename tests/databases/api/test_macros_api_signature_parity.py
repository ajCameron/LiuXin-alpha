"""
Compare declared API and concrete method surfaces by parsing source ASTs without importing the implementations.

Resolution covers top-level classes and supported import aliases. Cached module
records are mutable and do not refresh automatically when source files change;
inheritance traversal is a test approximation rather than Python MRO evaluation.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/api/test_macros_api_signature_parity.py
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"


@dataclass(frozen=True)
class MethodSpec:
    """
    Store an immutable method kind, rendered arguments, and return annotation for exact signature comparisons.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/api/test_macros_api_signature_parity.py
    """
    kind: str
    args: str
    returns: str | None


def path_from_module(module: str) -> Path | None:
    """
    Find a source module file before trying its package __init__.py.

    Check existence beneath SRC_ROOT; this helper does not validate module identifiers
    or import code.

    Example:
        >>> path_from_module('missing_docstring_test_module') is None
        True


    :param module: Dotted module name whose components are joined beneath SRC_ROOT.
    :return: First existing candidate Path, or None.
    """
    module_path = SRC_ROOT.joinpath(*module.split("."))
    file_path = module_path.with_suffix(".py")
    if file_path.exists():
        return file_path
    init_path = module_path / "__init__.py"
    if init_path.exists():
        return init_path
    return None


def resolve_relative_module(current_module: str, level: int, imported_module: str | None) -> str:
    """
    Resolve an import name by slicing the current module or package components.

    For level zero return the imported name or an empty string. A known non-package file
    loses its final component before relative slicing; unresolved current paths are
    treated as packages. Levels are not validated.

    Example:
        >>> resolve_relative_module('any.module', 0, 'collections')
        'collections'


    :param current_module: Dotted name containing the import statement.
    :param level: AST import level: zero is absolute, positive values select relative
        parents.
    :param imported_module: Imported suffix, or None for a bare relative import.
    :return: Resolved dotted name; may be empty.
    """
    if level == 0:
        return imported_module or ""

    current_path = path_from_module(current_module)
    parts = current_module.split(".")
    if current_path is not None and current_path.name != "__init__.py":
        parts = parts[:-1]

    parts = parts[: len(parts) - level + 1]
    if imported_module:
        parts += imported_module.split(".")
    return ".".join([p for p in parts if p])


@dataclass
class ModuleInfo:
    """
    Hold a mutable index of top-level class nodes and import aliases for one parsed module.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/api/test_macros_api_signature_parity.py
    """
    module: str
    classes: dict[str, ast.ClassDef]
    imports: dict[str, str]
    from_imports: dict[str, tuple[str, str]]


@lru_cache(maxsize=None)
def load_module(module: str) -> ModuleInfo | None:
    """
    Parse and cache top-level classes, imports, and non-star from-import aliases.

    Read UTF-8 source when a path exists. Filesystem and parse errors propagate. The
    unbounded cache retains missing-module results and returns the same mutable
    ModuleInfo on repeated calls.

    Example:
        >>> load_module('missing_docstring_test_module') is None
        True


    :param module: Dotted source module name to resolve.
    :return: Cached ModuleInfo, or None when no source path exists.
    """
    path = path_from_module(module)
    if path is None:
        return None

    tree = ast.parse(path.read_text(encoding="utf-8"))
    classes: dict[str, ast.ClassDef] = {}
    imports: dict[str, str] = {}
    from_imports: dict[str, tuple[str, str]] = {}

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            classes[node.name] = node
            continue
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports[alias.asname or alias.name] = alias.name
            continue
        if isinstance(node, ast.ImportFrom):
            imported_from = resolve_relative_module(module, node.level, node.module)
            for alias in node.names:
                if alias.name == "*":
                    continue
                from_imports[alias.asname or alias.name] = (imported_from, alias.name)

    return ModuleInfo(module=module, classes=classes, imports=imports, from_imports=from_imports)


def decorator_name(dec: ast.AST) -> str | None:
    """
    Extract a decorator name, terminal attribute, or recursively unwrapped call target.

    Example:
        >>> decorator_name(ast.parse('factory()', mode='eval').body)
        'factory'


    :param dec: Decorator expression AST node.
    :return: Name string, or None for unsupported AST forms; aliases are not resolved.
    """
    if isinstance(dec, ast.Name):
        return dec.id
    if isinstance(dec, ast.Attribute):
        return dec.attr
    if isinstance(dec, ast.Call):
        return decorator_name(dec.func)
    return None


def classify_method(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """
    Classify decorators with property precedence over classmethod and staticmethod.

    Recognize terminal decorator names and direct getter/setter/deleter attributes;
    unresolved or unrecognized decorators leave the ordinary-method classification.

    Example:
        >>> classify_method(ast.parse('def f(): pass').body[0])
        'method'


    :param node: Synchronous or asynchronous function AST node.
    :return: One of property, classmethod, staticmethod, or method.
    """
    names = {decorator_name(d) for d in node.decorator_list}
    if "property" in names:
        return "property"
    for dec in node.decorator_list:
        if isinstance(dec, ast.Attribute) and dec.attr in {"setter", "deleter", "getter"}:
            return "property"
    if "classmethod" in names:
        return "classmethod"
    if "staticmethod" in names:
        return "staticmethod"
    return "method"


def resolve_base(base: ast.expr, module: ModuleInfo) -> tuple[str, str] | None:
    """
    Resolve a simple base name or one-level attribute through the module class/import index.

    Example:
        >>> resolve_base(ast.parse('Unknown', mode='eval').body, ModuleInfo('x', {}, {}, {})) is None
        True


    :param base: Base-class expression from a ClassDef.
    :param module: ModuleInfo providing local classes and import alias mappings.
    :return: Tuple of (module_name, class_name), or None for an unsupported or
        unresolved expression.
    """
    if isinstance(base, ast.Name):
        if base.id in module.classes:
            return module.module, base.id
        if base.id in module.from_imports:
            return module.from_imports[base.id]
        return None

    if isinstance(base, ast.Attribute) and isinstance(base.value, ast.Name):
        alias = base.value.id
        if alias in module.imports:
            return module.imports[alias], base.attr
        if alias in module.from_imports:
            imported_module, imported_name = module.from_imports[alias]
            return f"{imported_module}.{imported_name}", base.attr

    return None


def collect_methods(
    module_name: str,
    class_name: str,
    seen: set[tuple[str, str]] | None = None,
    *,
    strict: bool = False,
) -> dict[str, MethodSpec]:
    """
    Collect inherited and directly declared method specifications from cached source ASTs.

    Ignore underscore-prefixed methods except __init__. Preserve rendered argument and
    return annotations exactly. Later direct definitions replace earlier ones, including
    property accessors.

    Visit bases in source order, allowing later bases to overwrite earlier entries; this
    is not Python MRO resolution. A shared visited set suppresses repeats. Missing
    recursive bases are tolerated even when the initial lookup is strict.

    Example:
        >>> collect_methods('missing_docstring_test_module', 'Missing')
        {}


    :param module_name: Dotted module name containing the requested class.
    :param class_name: Top-level class name to inspect.
    :param seen: Visited (module, class) pairs; a nonempty supplied set is mutated,
        while None or an empty set is replaced.
    :param strict: Raise AssertionError for an unresolved initial module or class;
        defaults to False and is not passed to recursive calls.
    :return: Mapping from method name to MethodSpec; empty for visited or unresolved
        non-strict lookups.
    """
    seen = seen or set()
    key = (module_name, class_name)
    if key in seen:
        return {}
    seen.add(key)

    module = load_module(module_name)
    if module is None:
        if strict:
            raise AssertionError(f"Cannot resolve module path for {module_name!r}")
        return {}

    cls = module.classes.get(class_name)
    if cls is None:
        if strict:
            raise AssertionError(f"Class {class_name!r} not found in module {module_name!r}")
        return {}

    methods: dict[str, MethodSpec] = {}

    for base in cls.bases:
        resolved = resolve_base(base, module)
        if resolved is None:
            continue
        methods.update(collect_methods(resolved[0], resolved[1], seen))

    for node in cls.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith("_") and node.name != "__init__":
            # Private implementation helpers are deliberately not API methods.
            continue
        methods[node.name] = MethodSpec(
            kind=classify_method(node),
            args=ast.unparse(node.args),
            returns=ast.unparse(node.returns) if node.returns is not None else None,
        )

    return methods


def test_macros_api_matches_sqlite_macros_full_signature_surface() -> None:
    """
    Check the macros API covers every SQLite macro method with identical kind, arguments, and return text.

    The selected surface includes __init__ and permits additional API methods.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/api/test_macros_api_signature_parity.py::test_macros_api_matches_sqlite_macros_full_signature_surface


    :return: None; failed expectations raise AssertionError.
    """
    api_methods = collect_methods("LiuXin_alpha.databases.api.macros_api", "MacrosAPI", strict=True)
    concrete_methods = collect_methods(
        "LiuXin_alpha.databases.database_driver_plugins.SQL.macros",
        "SQLiteDatabaseMacros",
        strict=True,
    )

    missing = sorted(set(concrete_methods) - set(api_methods))
    assert not missing, (
        "MacrosAPI is missing methods present on SQLiteDatabaseMacros: "
        + ", ".join(missing[:20])
        + (f" ... (+{len(missing) - 20} more)" if len(missing) > 20 else "")
    )

    kind_mismatches = sorted(
        name
        for name in concrete_methods
        if name in api_methods and concrete_methods[name].kind != api_methods[name].kind
    )
    assert not kind_mismatches, (
        "MacrosAPI has method-kind mismatches (method/property/classmethod/staticmethod): "
        + ", ".join(kind_mismatches[:20])
        + (f" ... (+{len(kind_mismatches) - 20} more)" if len(kind_mismatches) > 20 else "")
    )

    signature_mismatches = sorted(
        name
        for name in concrete_methods
        if name in api_methods
        and (
            concrete_methods[name].args != api_methods[name].args
            or concrete_methods[name].returns != api_methods[name].returns
        )
    )
    assert not signature_mismatches, (
        "MacrosAPI has signature mismatches versus SQLiteDatabaseMacros: "
        + ", ".join(signature_mismatches[:20])
        + (f" ... (+{len(signature_mismatches) - 20} more)" if len(signature_mismatches) > 20 else "")
    )


def test_portable_macros_api_is_concrete_on_every_sql_backend() -> None:
    """
    Check SQLite and PostgreSQL macro classes cover the portable API with identical method kinds and arguments.

    This AST check does not compare return annotations, instantiate backends, or
    evaluate abstractness.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/api/test_macros_api_signature_parity.py::test_portable_macros_api_is_concrete_on_every_sql_backend


    :return: None; failed expectations raise AssertionError.
    """
    portable_methods = collect_methods(
        "LiuXin_alpha.databases.api.portable_macros_api",
        "PortableMacrosAPI",
        strict=True,
    )
    sqlite_methods = collect_methods(
        "LiuXin_alpha.databases.database_driver_plugins.SQL.macros",
        "SQLiteDatabaseMacros",
        strict=True,
    )
    postgres_methods = collect_methods(
        "LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver",
        "PostgresDatabaseMacros",
        strict=True,
    )

    for backend, methods in (("SQLite", sqlite_methods), ("PostgreSQL", postgres_methods)):
        missing = sorted(set(portable_methods) - set(methods))
        assert not missing, f"{backend} is missing portable macros: {', '.join(missing)}"
        mismatched = sorted(
            name
            for name, spec in portable_methods.items()
            if methods[name].kind != spec.kind or methods[name].args != spec.args
        )
        assert not mismatched, (
            f"{backend} has portable macro signature mismatches: {', '.join(mismatched)}"
        )
