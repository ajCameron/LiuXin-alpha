"""Dependency classification must explain, never weaken, the protected ratchet."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.check_modern_import_cycles import (
    ImportKind,
    build_graph,
    collect_imports,
    forbidden_dependency,
    main,
    strongly_connected_components,
)


def _write(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_cycle_checker_reports_only_real_multi_module_cycles(tmp_path: Path) -> None:
    _write(tmp_path / "demo" / "__init__.py", "")
    _write(tmp_path / "demo" / "one.py", "from demo import two\n")
    _write(tmp_path / "demo" / "two.py", "from . import one\n")
    _write(tmp_path / "demo" / "leaf.py", "VALUE = 1\n")

    graph = build_graph(tmp_path, protected_prefixes=("demo",))

    assert strongly_connected_components(graph) == (("demo.one", "demo.two"),)


def test_current_modern_seams_are_acyclic() -> None:
    assert main([]) == 0


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import demo.leaf", ImportKind.IMPORT_TIME),
        ("class Host:\n    import demo.leaf", ImportKind.IMPORT_TIME),
        ("def run():\n    import demo.leaf", ImportKind.DEFERRED),
        ("async def run():\n    import demo.leaf", ImportKind.DEFERRED),
        ("def run():\n    class Host:\n        import demo.leaf", ImportKind.DEFERRED),
        (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    import demo.leaf",
            ImportKind.TYPE_ONLY,
        ),
        (
            "from typing import TYPE_CHECKING as TC\nif TC:\n    import demo.leaf",
            ImportKind.TYPE_ONLY,
        ),
        (
            "import typing as t\nif t.TYPE_CHECKING:\n    import demo.leaf",
            ImportKind.TYPE_ONLY,
        ),
        (
            "import typing\nif typing.TYPE_CHECKING:\n    import demo.leaf",
            ImportKind.TYPE_ONLY,
        ),
        (
            "from typing import TYPE_CHECKING\nif not TYPE_CHECKING:\n    import demo.leaf",
            ImportKind.IMPORT_TIME,
        ),
        (
            "from typing import TYPE_CHECKING\nif not TYPE_CHECKING:\n    pass\nelse:\n    import demo.leaf",
            ImportKind.TYPE_ONLY,
        ),
        (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    pass\nelse:\n    import demo.leaf",
            ImportKind.IMPORT_TIME,
        ),
        (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    def run():\n        import demo.leaf",
            ImportKind.TYPE_ONLY,
        ),
        (
            "from typing import TYPE_CHECKING\ndef run():\n    if TYPE_CHECKING:\n        import demo.leaf",
            ImportKind.TYPE_ONLY,
        ),
        ("if unknown_condition:\n    import demo.leaf", ImportKind.IMPORT_TIME),
        (
            "if unknown_condition:\n    pass\nelse:\n    import demo.leaf",
            ImportKind.IMPORT_TIME,
        ),
    ],
)
def test_import_contexts_remain_in_combined_graph(
    tmp_path: Path, source: str, expected: ImportKind
) -> None:
    _write(tmp_path / "demo/owner.py", source + "\n")
    _write(tmp_path / "demo/leaf.py", "")
    inventory = collect_imports(tmp_path, protected_prefixes=("demo",))
    edges = [edge for edge in inventory.edges if edge.target == "demo.leaf"]
    assert len(edges) == 1
    assert edges[0].kind == expected
    assert edges[0].line == len(source.splitlines())
    assert inventory.graph()["demo.owner"] == {"demo.leaf"}
    assert inventory.graph((expected,))["demo.owner"] == {"demo.leaf"}
    assert inventory.graph(set(ImportKind) - {expected})["demo.owner"] == set()
    assert build_graph(tmp_path, protected_prefixes=("demo",)) == inventory.graph()


def test_relative_package_imports_resolve_and_prefixes_respect_boundaries(
    tmp_path: Path,
) -> None:
    _write(tmp_path / "demo/__init__.py", "from .nested import leaf\n")
    _write(tmp_path / "demo/nested/__init__.py", "from . import leaf\n")
    _write(tmp_path / "demo/nested/leaf.py", "from .. import sibling\n")
    _write(tmp_path / "demo/sibling.py", "from .nested.leaf import VALUE\n")
    _write(tmp_path / "demolition.py", "import demo\n")
    graph = build_graph(tmp_path, protected_prefixes=("demo",))
    assert "demolition" not in graph
    assert graph["demo"] == {"demo.nested", "demo.nested.leaf"}
    assert graph["demo.nested"] == {"demo.nested.leaf"}
    assert graph["demo.nested.leaf"] == {"demo", "demo.sibling"}
    assert graph["demo.sibling"] == {"demo.nested.leaf"}


def test_context_is_restored_after_branches_and_nested_bodies(tmp_path: Path) -> None:
    _write(
        tmp_path / "demo/owner.py",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    import demo.types\n"
        "else:\n"
        "    import demo.runtime\n"
        "def build():\n"
        "    if TYPE_CHECKING:\n"
        "        import demo.local_types\n"
        "    else:\n"
        "        import demo.local_runtime\n"
        "    class Local:\n"
        "        import demo.local_class\n"
        "class Outer:\n"
        "    import demo.outer_class\n"
        "from .tail import *\n",
    )
    inventory = collect_imports(tmp_path, protected_prefixes=("demo",))
    contexts = {
        edge.target: edge.kind
        for edge in inventory.edges
        if edge.target.startswith("demo.")
    }
    assert contexts == {
        "demo.types": ImportKind.TYPE_ONLY,
        "demo.runtime": ImportKind.IMPORT_TIME,
        "demo.local_types": ImportKind.TYPE_ONLY,
        "demo.local_runtime": ImportKind.DEFERRED,
        "demo.local_class": ImportKind.DEFERRED,
        "demo.outer_class": ImportKind.IMPORT_TIME,
        "demo.tail": ImportKind.IMPORT_TIME,
    }


@pytest.mark.parametrize(
    ("source", "kind"),
    [
        ("from . import one\n", ImportKind.IMPORT_TIME),
        ("def run():\n    from . import one\n", ImportKind.DEFERRED),
        (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    from . import one\n",
            ImportKind.TYPE_ONLY,
        ),
    ],
)
def test_gate_rejects_every_cycle_context(
    tmp_path: Path, capsys, source: str, kind: ImportKind
) -> None:
    root = tmp_path / "LiuXin_alpha/catalog/write"
    _write(root / "one.py", "from . import two\n")
    _write(root / "two.py", source)
    assert main(["--source-root", str(tmp_path)]) == 1
    output = capsys.readouterr().out
    assert "Dependency component:" in output
    assert f"[{kind}] -> LiuXin_alpha.catalog.write.one" in output
    assert f"{root / 'two.py'}:{len(source.splitlines())}" in output


@pytest.mark.parametrize(
    ("owner", "statement", "reason"),
    [
        ("caches/write/child.py", "from . import BaseWriter", "assembling package"),
        (
            "caches/write/generic/child.py",
            "from .. import BaseWriter",
            "assembling package",
        ),
        (
            "caches/write/child.py",
            "import LiuXin_alpha.caches.write as writers",
            "assembling package",
        ),
        (
            "surfaces/read_model/api.py",
            "from ..web_readonly.app import _escape",
            "web applications",
        ),
        (
            "surfaces/images/api.py",
            "import LiuXin_alpha.surfaces.web_calibre_readonly.app",
            "web applications",
        ),
        ("surfaces/catalog/api.py", "from .. import web_readonly", "web applications"),
        ("surfaces/presentation.py", "from .core import CoreRow", "independent leaves"),
        (
            "surfaces/acquisition_types.py",
            "from LiuXin_alpha.core import CoreClientAPI",
            "independent leaves",
        ),
    ],
)
@pytest.mark.parametrize("context", tuple(ImportKind))
def test_direction_rules_reject_acyclic_back_imports(
    tmp_path: Path,
    capsys,
    owner: str,
    statement: str,
    reason: str,
    context: ImportKind,
) -> None:
    source = statement + "\n"
    if context == ImportKind.DEFERRED:
        source = "def run():\n    " + source
    elif context == ImportKind.TYPE_ONLY:
        source = "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    " + source
    _write(tmp_path / "LiuXin_alpha" / owner, source)
    _write(tmp_path / "LiuXin_alpha/surfaces/web_readonly/__init__.py", "")
    assert strongly_connected_components(build_graph(tmp_path)) == ()
    assert main(["--source-root", str(tmp_path)]) == 1
    output = capsys.readouterr().out
    assert reason in output
    assert f"[{context}]" in output


def test_direction_rules_allow_owner_imports_and_application_composition(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path / "LiuXin_alpha/caches/write/child.py",
        "from .base_writer import BaseWriter\n",
    )
    _write(
        tmp_path / "LiuXin_alpha/surfaces/read_model/api.py",
        "from ..presentation import escape\n",
    )
    _write(
        tmp_path / "LiuXin_alpha/surfaces/web_readonly/app.py",
        "from ..read_model.api import ReadModelBackend\n",
    )
    inventory = collect_imports(tmp_path)
    assert all(forbidden_dependency(edge) is None for edge in inventory.edges)
    assert main(["--source-root", str(tmp_path)]) == 0


def test_missing_protected_sources_do_not_report_success(
    tmp_path: Path, capsys
) -> None:
    assert main(["--source-root", str(tmp_path)]) == 1
    assert "no protected modules" in capsys.readouterr().out
