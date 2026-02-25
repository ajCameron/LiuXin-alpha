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
    module: str
    class_name: str


@dataclass(frozen=True)
class MethodSpec:
    kind: str
    args: str
    returns: str | None
    arg_weight: int


@dataclass
class ModuleInfo:
    module: str
    classes: dict[str, ast.ClassDef]
    imports: dict[str, str]
    from_imports: dict[str, tuple[str, str]]


def path_from_module(module: str) -> Path | None:
    module_path = SRC_ROOT.joinpath(*module.split("."))
    file_path = module_path.with_suffix(".py")
    if file_path.exists():
        return file_path
    init_path = module_path / "__init__.py"
    if init_path.exists():
        return init_path
    return None


def resolve_relative_module(current_module: str, level: int, imported_module: str | None) -> str:
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
    if isinstance(dec, ast.Name):
        return dec.id
    if isinstance(dec, ast.Attribute):
        return dec.attr
    if isinstance(dec, ast.Call):
        return decorator_name(dec.func)
    return None


def classify_method(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
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
    args = node.args
    return len(args.posonlyargs) + len(args.args) + len(args.kwonlyargs) + (1 if args.vararg else 0) + (1 if args.kwarg else 0)


def resolve_base(base: ast.expr, module: ModuleInfo) -> ClassRef | None:
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
            # Ignore class-private name-mangled helpers. These are implementation details and
            # cannot be reliably expressed as abstract methods across subclasses.
            continue

        spec = MethodSpec(
            kind=classify_method(node),
            args=ast.unparse(node.args),
            returns=ast.unparse(node.returns) if node.returns is not None else None,
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


def merge_concrete_signatures(concrete_refs: list[ClassRef]) -> dict[str, MethodSpec]:
    merged: dict[str, MethodSpec] = {}

    for ref in concrete_refs:
        methods = collect_methods(ref.module, ref.class_name, strict=True)
        for name, spec in methods.items():
            current = merged.get(name)
            if current is None:
                merged[name] = spec
                continue
            assert current.kind == spec.kind and current.args == spec.args and current.returns == spec.returns, (
                "Concrete implementations disagree on method signatures "
                f"for {name!r}: {current} vs {spec}"
            )

    return merged


@pytest.mark.parametrize(
    ("api_ref", "concrete_refs", "ignored_names"),
    [
        (
            ClassRef("LiuXin_alpha.databases.api.base", "DatabaseBuilderAPI"),
            [
                ClassRef(
                    "LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator",
                    "SQLiteDatabaseBuilder",
                )
            ],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.base", "RowAPI"),
            [ClassRef("LiuXin_alpha.databases.row", "Row")],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.driver_wrapper", "DatabaseDriverWrapperAPI"),
            [ClassRef("LiuXin_alpha.databases.database_driver_plugins.driver_wrapper", "DriverWrapper")],
            {"__init__"},
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.driver", "DatabaseDriverAPI"),
            [
                ClassRef("LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver", "DatabaseDriver"),
                ClassRef("LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw.databasedriver", "DatabaseDriver"),
            ],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.maintenance", "DatabaseCacheAPI"),
            [ClassRef("LiuXin_alpha.customize.cache.base_cache", "CacheAPI")],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.maintenance", "DatabaseMaintainerAPI"),
            [ClassRef("LiuXin_alpha.databases.maintenance_bot", "Maintainer")],
            set(),
        ),
        (
            ClassRef("LiuXin_alpha.databases.api.maintenance", "MaintenanceBotAPI"),
            [ClassRef("LiuXin_alpha.databases.maintenance_bot", "MaintenanceBot")],
            set(),
        ),
    ],
)
def test_database_api_signature_parity(
    api_ref: ClassRef,
    concrete_refs: list[ClassRef],
    ignored_names: set[str],
) -> None:
    api_methods = collect_methods(api_ref.module, api_ref.class_name, strict=True)
    concrete_methods = merge_concrete_signatures(concrete_refs)

    for name in ignored_names:
        api_methods.pop(name, None)
        concrete_methods.pop(name, None)

    missing = sorted(set(concrete_methods) - set(api_methods))
    assert not missing, (
        f"{api_ref.class_name} is missing {len(missing)} methods: "
        + ", ".join(missing[:20])
        + (f" ... (+{len(missing) - 20} more)" if len(missing) > 20 else "")
    )

    kind_mismatches = sorted(
        name
        for name in concrete_methods
        if name in api_methods and concrete_methods[name].kind != api_methods[name].kind
    )
    assert not kind_mismatches, (
        f"{api_ref.class_name} has {len(kind_mismatches)} method-kind mismatches: "
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
        f"{api_ref.class_name} has {len(signature_mismatches)} signature mismatches: "
        + ", ".join(signature_mismatches[:20])
        + (f" ... (+{len(signature_mismatches) - 20} more)" if len(signature_mismatches) > 20 else "")
    )
