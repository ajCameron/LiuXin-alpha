from __future__ import annotations

import ast

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SURFACES_ROOT = REPO_ROOT / "src" / "LiuXin_alpha" / "surfaces"

FORBIDDEN_PREFIXES = (
    "LiuXin_alpha.caches",
    "LiuXin_alpha.catalog",
    "LiuXin_alpha.databases",
    "LiuXin_alpha.library",
    "LiuXin_alpha.metadata.read_sources",
    "LiuXin_alpha.storage",
)

INTENTIONAL_INFRASTRUCTURE_EXCEPTIONS = {
    "cli/postgres.py",
    # Mixed ingest is currently a local composition/operations boundary.  It
    # owns database and Store lifecycle rather than acting as an application
    # presentation surface; metadata and other Core-backed CLI modules remain
    # covered by this test.
    "cli/storage.py",
    "renderers/calibre_metadata.py",
    # These two existing store-creation views enumerate backend choices before
    # a Core command is submitted. Keep the exception narrow to those modules.
    "terminal/commands/new_store.py",
    "thumbnail_cache.py",
    "web_readwrite/app.py",
}


def _surface_modules() -> list[Path]:
    return sorted(SURFACES_ROOT.rglob("*.py"))


def test_application_surfaces_do_not_import_owned_subsystems() -> None:
    violations: list[str] = []
    for path in _surface_modules():
        relative = path.relative_to(SURFACES_ROOT).as_posix()
        if relative in INTENTIONAL_INFRASTRUCTURE_EXCEPTIONS:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            module = ""
            if isinstance(node, ast.ImportFrom):
                module = str(node.module or "")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(FORBIDDEN_PREFIXES):
                        violations.append(
                            "{}:{} imports {}".format(
                                relative,
                                node.lineno,
                                alias.name,
                            )
                        )
                continue
            if module.startswith(FORBIDDEN_PREFIXES):
                violations.append(
                    "{}:{} imports {}".format(
                        relative,
                        node.lineno,
                        module,
                    )
                )
    assert violations == []


def test_application_surfaces_do_not_use_generic_core_invoke() -> None:
    violations: list[str] = []
    for path in _surface_modules():
        relative = path.relative_to(SURFACES_ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and node.value == "invoke":
                violations.append("{}:{}".format(relative, node.lineno))
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr.startswith("invoke_")
            ):
                violations.append(
                    "{}:{} calls {}".format(
                        relative,
                        node.lineno,
                        node.func.attr,
                    )
                )
    assert violations == []


def test_surface_runner_automation_accepts_local_or_rpc_core() -> None:
    runners = (
        "run_web_readonly.py",
        "run_web_calibre_readonly.py",
        "run_api_readonly.py",
        "run_opds_readonly.py",
        "run_web_readwrite.py",
    )
    for filename in runners:
        source = (REPO_ROOT / "scripts" / filename).read_text(
            encoding="utf-8"
        )
        assert "--database" in source
        assert "--core-endpoint" in source
        assert "LiuXin_alpha.databases" not in source
