"""
Provide test build deployment package utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test build deployment package through a consuming regression::

        python -m pytest -q tests/scripts/test_build_deployment_package.py
"""
from __future__ import annotations

import importlib.util
import sys
import tarfile

from pathlib import Path
from types import SimpleNamespace


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "build_deployment_package.py"
SPEC = importlib.util.spec_from_file_location("build_deployment_package", SCRIPT_PATH)
assert SPEC is not None
build_deployment_package = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = build_deployment_package
SPEC.loader.exec_module(build_deployment_package)


def test_exclusion_rules_keep_deployment_bundle_source_focused() -> None:
    """
    Perform the test exclusion rules keep deployment bundle source focused operation under explicit file-format and conversion rules.

    Example:
        Exercise test exclusion rules keep deployment bundle source focused through a consuming regression::

            python -m pytest -q tests/scripts/test_build_deployment_package.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert build_deployment_package.should_exclude_path(
        Path("LiuXin_alpha_data/private.db"),
        include_tests=True,
    )
    assert build_deployment_package.should_exclude_path(
        Path("working-memory/test-results/full.json"),
        include_tests=True,
    )
    assert build_deployment_package.should_exclude_path(
        Path("src/liuxin_alpha.egg-info/PKG-INFO"),
        include_tests=True,
    )
    assert build_deployment_package.should_exclude_path(
        Path("tests/test_example.py"),
        include_tests=False,
    )
    assert not build_deployment_package.should_exclude_path(
        Path("tests/test_example.py"),
        include_tests=True,
    )
    assert not build_deployment_package.should_exclude_path(
        Path("src/LiuXin_alpha/__init__.py"),
        include_tests=False,
    )


def test_collect_package_files_prunes_excluded_directories(tmp_path: Path) -> None:
    """
    Perform the test collect package files prunes excluded directories operation under explicit file-format and conversion rules.

    Example:
        Exercise test collect package files prunes excluded directories through a consuming regression::

            python -m pytest -q tests/scripts/test_build_deployment_package.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    (tmp_path / "src" / "LiuXin_alpha").mkdir(parents=True)
    (tmp_path / "src" / "LiuXin_alpha" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_example.py").write_text("", encoding="utf-8")
    (tmp_path / "LiuXin_alpha_data").mkdir()
    (tmp_path / "LiuXin_alpha_data" / "private.db").write_text("secret", encoding="utf-8")
    (tmp_path / ".venv" / "bin").mkdir(parents=True)
    (tmp_path / ".venv" / "bin" / "python").write_text("", encoding="utf-8")

    without_tests = build_deployment_package.collect_package_files(tmp_path, include_tests=False)
    with_tests = build_deployment_package.collect_package_files(tmp_path, include_tests=True)

    assert Path("src/LiuXin_alpha/__init__.py") in without_tests
    assert Path("tests/test_example.py") not in without_tests
    assert Path("tests/test_example.py") in with_tests
    assert Path("LiuXin_alpha_data/private.db") not in with_tests
    assert Path(".venv/bin/python") not in with_tests


def test_generated_remote_helpers_cover_install_and_postgres_workflow() -> None:
    """
    Perform the test generated remote helpers cover install and postgres workflow operation under explicit file-format and conversion rules.

    Example:
        Exercise test generated remote helpers cover install and postgres workflow through a consuming regression::

            python -m pytest -q tests/scripts/test_build_deployment_package.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    install_script = build_deployment_package.render_remote_install_script()
    postgres_script = build_deployment_package.render_postgres_setup_script()
    bundle_readme = build_deployment_package.render_bundle_readme()

    assert (
        "LIUXIN_INSTALL_EXTRAS=\"${LIUXIN_INSTALL_EXTRAS:-postgres,search,archives,conversion}\""
        in install_script
    )
    assert "-m venv" in install_script
    assert "pip install -e" in install_script
    assert '"${VENV_DIR}/bin/liuxin" storage ingest --help' in install_script
    assert '"${VENV_DIR}/bin/liuxin" metadata --help' in install_script
    assert '"${VENV_DIR}/bin/liuxin" jobs --help' in install_script
    assert '"${VENV_DIR}/bin/liuxin" core --help' in install_script
    assert '"${VENV_DIR}/bin/liuxin" init --help' in install_script
    assert "postgres setup-sql" in postgres_script
    assert "--section server" in postgres_script
    assert "--section database" in postgres_script
    assert "--apply-server" in postgres_script
    assert "--init-schema" in postgres_script
    assert "scripts/run_postgres_live_smoke.py" in postgres_script
    assert ".venv/bin/liuxin storage ingest" in bundle_readme
    assert ".venv/bin/liuxin metadata dump-json" in bundle_readme
    assert ".venv/bin/liuxin plugins inspect" in bundle_readme
    assert ".venv/bin/liuxin connect /srv/liuxin" in bundle_readme
    assert ".venv/bin/liuxin storage repair plan" in bundle_readme
    assert ".venv/bin/liuxin storage recovery list" in bundle_readme
    assert ".venv/bin/liuxin init /srv/liuxin" in bundle_readme
    assert ".venv/bin/liuxin init --wizard" in bundle_readme
    assert "dev-docs/operational-cli.md" in bundle_readme
    assert "--preflight-only" in bundle_readme
    assert "143 is SIGTERM" in bundle_readme


def test_build_deployment_package_writes_tarball_with_generated_helpers(tmp_path: Path) -> None:
    """
    Perform the test build deployment package writes tarball with generated helpers operation under explicit file-format and conversion rules.

    Example:
        Exercise test build deployment package writes tarball with generated helpers through a consuming regression::

            python -m pytest -q tests/scripts/test_build_deployment_package.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    repo = tmp_path / "repo"
    _write_minimal_repo(repo)
    output_dir = tmp_path / "out"
    args = SimpleNamespace(
        output_dir=str(output_dir),
        name="liuxin-alpha-deployment",
        include_tests=False,
        exclude=[],
        force=False,
        dry_run=False,
    )

    result = build_deployment_package.build_deployment_package(args, repo_root=repo)

    archive = Path(result["output_path"])
    assert archive.is_file()
    assert Path(result["sha256_path"]).is_file()
    prefix = result["bundle_name"]
    with tarfile.open(archive, "r:gz") as tar:
        names = set(tar.getnames())

    assert f"{prefix}/deploy/remote_install.sh" in names
    assert f"{prefix}/deploy/postgres_remote_setup.sh" in names
    assert f"{prefix}/deployment_manifest.json" in names
    assert f"{prefix}/src/LiuXin_alpha/__init__.py" in names
    assert f"{prefix}/dev-docs/postgresql-backend.md" in names
    assert f"{prefix}/dev-docs/metadata-cli.md" in names
    assert f"{prefix}/dev-docs/operational-cli.md" in names
    assert f"{prefix}/dev-docs/storage/mixed_ingest_operations.md" in names
    assert f"{prefix}/LiuXin_alpha_data/private.db" not in names
    assert f"{prefix}/tests/test_example.py" not in names


def _write_minimal_repo(repo: Path) -> None:
    """
    Write minimal repo under the format's safety and compatibility rules.

    Example:
        Exercise  write minimal repo through a consuming regression::

            python -m pytest -q tests/scripts/test_build_deployment_package.py


    :param repo: Value supplied for repo under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    (repo / "src" / "LiuXin_alpha" / "surfaces" / "cli").mkdir(parents=True)
    (repo / "src" / "LiuXin_alpha" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "LiuXin_alpha" / "surfaces" / "cli" / "__main__.py").write_text("", encoding="utf-8")
    for filename in (
        "app.py",
        "capabilities.py",
        "catalogue.py",
        "common.py",
        "completion.py",
        "config_cli.py",
        "core_cli.py",
        "diagnostics.py",
        "ingest_runs.py",
        "jobs.py",
        "initialize.py",
        "metadata.py",
        "postgres.py",
        "serve.py",
        "storage.py",
        "workflows.py",
    ):
        (repo / "src" / "LiuXin_alpha" / "surfaces" / "cli" / filename).write_text("", encoding="utf-8")
    (
        repo / "src" / "LiuXin_alpha" / "surfaces" / "system_profile.py"
    ).write_text("", encoding="utf-8")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "create_venv.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    (repo / "scripts" / "run_postgres_live_smoke.py").write_text("", encoding="utf-8")
    (repo / "dev-docs").mkdir()
    (repo / "dev-docs" / "postgresql-backend.md").write_text(
        "# PostgreSQL\n",
        encoding="utf-8",
    )
    (repo / "dev-docs" / "metadata-cli.md").write_text(
        "# Metadata CLI\n",
        encoding="utf-8",
    )
    (repo / "dev-docs" / "operational-cli.md").write_text(
        "# Operational CLI\n",
        encoding="utf-8",
    )
    (repo / "dev-docs" / "storage").mkdir()
    (repo / "dev-docs" / "storage" / "mixed_ingest_operations.md").write_text(
        "# Mixed ingest\n",
        encoding="utf-8",
    )
    (repo / "tests").mkdir()
    (repo / "tests" / "test_example.py").write_text("", encoding="utf-8")
    (repo / "LiuXin_alpha_data").mkdir()
    (repo / "LiuXin_alpha_data" / "private.db").write_text("secret", encoding="utf-8")
    (repo / "pyproject.toml").write_text("[project]\nname = 'liuxin-alpha'\n", encoding="utf-8")
    (repo / "README.md").write_text("# LiuXin\n", encoding="utf-8")
