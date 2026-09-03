from __future__ import annotations

from pathlib import Path

from scripts.check_modern_import_cycles import (
    build_graph,
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
