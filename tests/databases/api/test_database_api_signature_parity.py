"""
Compare declared API and concrete method surfaces by parsing source ASTs without importing the implementations.

Resolution covers top-level classes and supported import aliases. Cached module
records are mutable and do not refresh automatically when source files change;
inheritance traversal is a test approximation rather than Python MRO evaluation.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/api/test_database_api_signature_parity.py
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"


@dataclass(frozen=True)
class ClassRef:
    """
    Identify a source class by module and class name in an immutable pair.

    Example:
        >>> ClassRef('pkg.mod', 'API').class_name
        'API'
    """
    module: str
    class_name: str


@dataclass(frozen=True)
class MethodSpec:
    """
    Store an immutable method kind, rendered arguments, and return annotation plus argument weight for property selection.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/api/test_database_api_signature_parity.py
    """
    kind: str
    args: str
    returns: str | None
    arg_weight: int


@dataclass
class ModuleInfo:
    """
    Hold a mutable index of top-level class nodes and import aliases for one parsed module.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/api/test_database_api_signature_parity.py
    """
    module: str
    classes: dict[str, ast.ClassDef]
    imports: dict[str, str]
    from_imports: dict[str, tuple[str, str]]


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


def arg_weight(node: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
    """
    Count named parameters and each variadic slot, including the receiver.

    Example:
        >>> arg_weight(ast.parse('def f(self, x, *args, **kwargs): pass').body[0])
        4


    :param node: Function or async-function node whose arguments are counted.
    :return: Integer used to prefer a property accessor with fewer argument slots.
    """
    args = node.args
    return (
        len(args.posonlyargs)
        + len(args.args)
        + len(args.kwonlyargs)
        + (1 if args.vararg else 0)
        + (1 if args.kwarg else 0)
    )


def _strip_arg_annotations(args: ast.arguments) -> ast.arguments:
    """
    Copy argument containers and names while removing annotations and argument type comments.

    Default-expression AST nodes remain shared although their lists are copied. New
    argument nodes omit source locations. This preserves call shape for comparison
    without resolving type syntax.

    Example:
        >>> args = ast.parse('def f(x: int = 1): pass').body[0].args
        >>> ast.unparse(_strip_arg_annotations(args))
        'x=1'


    :param args: Original argument specification; not mutated.
    :return: A new ast.arguments instance.
    """

    def strip(a: ast.arg) -> ast.arg:
        """
        Copy one argument name into an unannotated AST argument node.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/api/test_database_api_signature_parity.py


        :param a: Original argument node; not mutated.
        :return: New ast.arg without annotation, type comment, or copied source locations.
        """
        return ast.arg(arg=a.arg, annotation=None, type_comment=None)

    return ast.arguments(
        posonlyargs=[strip(a) for a in args.posonlyargs],
        args=[strip(a) for a in args.args],
        vararg=strip(args.vararg) if args.vararg else None,
        kwonlyargs=[strip(a) for a in args.kwonlyargs],
        kw_defaults=list(args.kw_defaults),
        kwarg=strip(args.kwarg) if args.kwarg else None,
        defaults=list(args.defaults),
    )


def _normalize_returns(ret: ast.expr | None) -> str | None:
    """
    Normalize selected return-annotation spellings through textual substitutions.

    Remove surrounding forward-reference quotes, lowercase selected collection aliases,
    and collapse the two supported str/LiteralString Union spellings. Quoted dotted
    identifiers return immediately; no type resolution is attempted.

    Example:
        >>> _normalize_returns(ast.parse('List[int]', mode='eval').body)
        'list[int]'


    :param ret: Return annotation AST expression, or None.
    :return: Normalized annotation text, or None for an absent annotation.
    """
    if ret is None:
        return None
    s = ast.unparse(ret)
    # If it's a quoted forward-ref like 'SomeType', normalize to SomeType.
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        inner = s[1:-1]
        if inner and all(part.isidentifier() for part in inner.split(".")):
            return inner
        s = inner

    return (
        s.replace("Dict[", "dict[")
        .replace("List[", "list[")
        .replace("Tuple[", "tuple[")
        .replace("Set[", "set[")
        .replace("FrozenSet[", "frozenset[")
        .replace("Union[str, LiteralString]", "str")
        .replace("Union[LiteralString, str]", "str")
    )


def _returns_compatible(expected: str | None, actual: str | None) -> bool:
    """
    Apply the parity test rules for equal annotations, Self, API suffixes, and simple Union members.

    An actual Self is always accepted for a nonmissing expectation. Union membership
    uses comma-split text rather than recursive type parsing. A missing annotation
    matches only another missing annotation.

    Example:
        >>> _returns_compatible('ThingAPI', 'Thing')
        True
        >>> _returns_compatible('int', 'str')
        False


    :param expected: Normalized API annotation string, or None.
    :param actual: Normalized concrete annotation string, or None.
    :return: True when one of the textual compatibility rules succeeds.
    """
    if expected is None or actual is None:
        return expected == actual

    if expected == actual:
        return True

    if actual == "Self":
        return True

    if expected.endswith("API") and actual == expected.removesuffix("API"):
        return True

    if actual.startswith("Union[") and actual.endswith("]"):
        members = [member.strip() for member in actual[len("Union[") : -1].split(",")]
        if expected in members:
            return True

    return False


def resolve_base(base: ast.expr, module: ModuleInfo) -> ClassRef | None:
    """
    Resolve a simple base name or one-level attribute through the module class/import index.

    Example:
        >>> resolve_base(ast.parse('Unknown', mode='eval').body, ModuleInfo('x', {}, {}, {})) is None
        True


    :param base: Base-class expression from a ClassDef.
    :param module: ModuleInfo providing local classes and import alias mappings.
    :return: ClassRef identifying the base, or None for an unsupported or unresolved
        expression.
    """
    if isinstance(base, ast.Name):
        if base.id in module.classes:
            return ClassRef(module=module.module, class_name=base.id)
        if base.id in module.from_imports:
            imported_module, imported_name = module.from_imports[base.id]
            return ClassRef(module=imported_module, class_name=imported_name)
        return None

    if isinstance(base, ast.Attribute) and isinstance(base.value, ast.Name):
        alias = base.value.id
        if alias in module.imports:
            return ClassRef(module=module.imports[alias], class_name=base.attr)
        if alias in module.from_imports:
            imported_module, imported_name = module.from_imports[alias]
            return ClassRef(module=f"{imported_module}.{imported_name}", class_name=base.attr)

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

    Ignore class-private mangled helpers; retain other methods including underscore
    names. Strip argument annotations and normalize return text. For repeated property
    accessors keep the lowest argument weight.

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
        methods.update(collect_methods(resolved.module, resolved.class_name, seen))

    for node in cls.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith("__") and not node.name.endswith("__"):
            # Ignore class-private name-mangled helpers.
            continue

        spec = MethodSpec(
            kind=classify_method(node),
            args=ast.unparse(_strip_arg_annotations(node.args)),
            returns=_normalize_returns(node.returns),
            arg_weight=arg_weight(node),
        )

        existing = methods.get(node.name)
        if existing is not None and spec.kind == "property" and existing.kind == "property":
            # Prefer getter-like property signatures over setter-like signatures.
            if spec.arg_weight < existing.arg_weight:
                methods[node.name] = spec
            continue

        methods[node.name] = spec

    return methods


# Constructors are frequently DI-heavy and legitimately differ across implementations.
ALWAYS_IGNORED_NAMES: set[str] = {"__init__"}


@pytest.mark.parametrize(
    ("api_ref", "concrete_refs", "ignored_names"),
    [
        (
            ClassRef("LiuXin_alpha.databases.api.database_api.database_generator_api", "DatabaseGeneratorAPI"),
            [
                ClassRef(
                    "LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator",
                    "SQLiteDatabaseGenerator",
                )
            ],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.row_api", "RowAPI"),
            [ClassRef("LiuXin_alpha.databases.row", "Row")],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.driver_wrapper_api.driver_wrapper_api", "DatabaseDriverWrapperAPI"),
            [ClassRef("LiuXin_alpha.databases.driver_wrapper", "DriverWrapper")],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.driver_api.driver_api", "DatabaseDriverAPI"),
            [
                ClassRef("LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver", "DatabaseDriver"),
                ClassRef("LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw.databasedriver", "DatabaseDriver"),
            ],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.maintenance_api", "DatabaseMaintainerAPI"),
            [ClassRef("LiuXin_alpha.databases.maintenance.service", "Maintainer")],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.maintenance_api", "MaintenanceBotAPI"),
            [ClassRef("LiuXin_alpha.databases.maintenance.engine", "MaintenanceEngine")],
            set(),
        ),
    ],
)
def test_database_api_signature_parity(
    api_ref: ClassRef,
    concrete_refs: list[ClassRef],
    ignored_names: set[str],
) -> None:
    # The API is a minimum contract: each concrete implementation must implement
    # at least the API surface, but may add additional methods.
    """
    Check concrete classes cover the selected API method surface with matching call shapes.

    Ignore __init__ and case-specific exclusions. Compare return compatibility only when
    both sides annotate it; additional concrete methods are allowed. Where multiple
    concrete classes are supplied, compare their complete MethodSpec values on the API
    surface.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/api/test_database_api_signature_parity.py::test_database_api_signature_parity


    :param api_ref: API ClassRef for this parametrized case.
    :param concrete_refs: Sequence of concrete ClassRef implementations to compare.
    :param ignored_names: Additional method names excluded from parity checks.
    :return: None; failed expectations raise AssertionError.
    """
    api_methods = collect_methods(api_ref.module, api_ref.class_name, strict=True)

    ignored = set(ignored_names) | ALWAYS_IGNORED_NAMES
    for name in ignored:
        api_methods.pop(name, None)

    concrete_maps: list[tuple[ClassRef, dict[str, MethodSpec]]] = []
    for ref in concrete_refs:
        concrete_methods = collect_methods(ref.module, ref.class_name, strict=True)
        for name in ignored:
            concrete_methods.pop(name, None)
        concrete_maps.append((ref, concrete_methods))

    for ref, concrete_methods in concrete_maps:
        missing = sorted(set(api_methods) - set(concrete_methods))
        assert not missing, (
            f"{ref.class_name} is missing {len(missing)} API methods from {api_ref.class_name}: "
            + ", ".join(missing[:20])
            + (f" ... (+{len(missing) - 20} more)" if len(missing) > 20 else "")
        )

        kind_mismatches = sorted(
            name for name in api_methods if concrete_methods[name].kind != api_methods[name].kind
        )
        assert not kind_mismatches, (
            f"{ref.class_name} has {len(kind_mismatches)} method-kind mismatches vs {api_ref.class_name}: "
            + ", ".join(kind_mismatches[:20])
            + (f" ... (+{len(kind_mismatches) - 20} more)" if len(kind_mismatches) > 20 else "")
        )

        signature_mismatches = sorted(
            name
            for name in api_methods
            if (
                concrete_methods[name].args != api_methods[name].args
                or (
                    concrete_methods[name].returns is not None
                    and api_methods[name].returns is not None
                    and not _returns_compatible(api_methods[name].returns, concrete_methods[name].returns)
                )
            )
        )
        assert not signature_mismatches, (
            f"{ref.class_name} has {len(signature_mismatches)} signature mismatches vs {api_ref.class_name}: "
            + ", ".join(signature_mismatches[:20])
            + (f" ... (+{len(signature_mismatches) - 20} more)" if len(signature_mismatches) > 20 else "")
        )

    # Optional extra sanity: if there are multiple concretes (e.g. sqlite + apsw),
    # ensure they agree on the API method shapes.
    if len(concrete_maps) > 1:
        base_ref, base_methods = concrete_maps[0]
        disagreements: list[str] = []
        for name in api_methods:
            base = base_methods[name]
            for other_ref, other_methods in concrete_maps[1:]:
                other = other_methods[name]
                if base != other:
                    disagreements.append(
                        f"{name}: {base_ref.class_name}{base} != {other_ref.class_name}{other}"
                    )
        assert not disagreements, (
            f"Concrete implementations disagree on API method signatures for {api_ref.class_name}: "
            + "; ".join(disagreements[:10])
            + (f" ... (+{len(disagreements) - 10} more)" if len(disagreements) > 10 else "")
        )
