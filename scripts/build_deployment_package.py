#!/usr/bin/env python3
"""Build a deployable LiuXin source bundle.

The current project does not yet have a complete OS package definition. This
script creates the practical deployment artifact we can support now: a source
tarball with local data/cache output excluded, plus helper scripts for remote
Python installation and PostgreSQL setup.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = Path("dist/deployment")
DEFAULT_BUNDLE_NAME = "liuxin-alpha-deployment"

REQUIRED_DEPLOYMENT_PATHS = (
    Path("pyproject.toml"),
    Path("README.md"),
    Path("src/LiuXin_alpha"),
    Path("src/LiuXin_alpha/surfaces/cli/__main__.py"),
    Path("src/LiuXin_alpha/surfaces/cli/postgres.py"),
    Path("scripts/create_venv.sh"),
    Path("scripts/run_postgres_live_smoke.py"),
    Path("dev-docs/postgresql-backend.md"),
)

DEFAULT_EXCLUDED_TOP_LEVEL = {
    ".agents",
    ".codex",
    ".git",
    ".github",
    ".idea",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tmp",
    ".venv",
    "LiuXin_alpha_data",
    "LiuXin_data",
    "build",
    "dist",
    "htmlcov",
    "working-memory",
}
DEFAULT_EXCLUDED_PARTS = {
    "__pycache__",
    ".basedpyright",
    ".cache",
    ".hypothesis",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
}
DEFAULT_EXCLUDED_FILE_NAMES = {
    ".coverage",
    "codex-doctor.out",
    "coverage.xml",
    "exceptions.log",
}
DEFAULT_EXCLUDED_GLOBS = (
    "*.egg-info",
    "*.egg",
    "*.log",
    "*.pyc",
    "*.pyo",
    "*.tmp",
    "backup-MySQL-*.zip",
    "LiuXin-alpha-[0-9-]*.zip",
)


@dataclass(frozen=True)
class PackagePlan:
    bundle_name: str
    output_path: Path
    sha256_path: Path
    source_files: tuple[Path, ...]
    metadata: dict[str, object]


def _log(message: str) -> None:
    print(f"[deployment-package] {message}", file=sys.stderr, flush=True)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _resolve_from_repo(repo_root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return (repo_root / candidate).resolve()


def _run_git(repo_root: Path, args: Sequence[str]) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(repo_root),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return ""
    return result.stdout.strip()


def _relative_join(directory: Path, name: str) -> Path:
    if str(directory) == ".":
        return Path(name)
    return directory / name


def _matches_pattern(path_text: str, parts: tuple[str, ...], pattern: str) -> bool:
    if fnmatch.fnmatch(path_text, pattern):
        return True
    return any(fnmatch.fnmatch(part, pattern) for part in parts)


def should_exclude_path(
    relative_path: Path,
    *,
    include_tests: bool,
    is_dir: bool = False,
    extra_excludes: Sequence[str] = (),
) -> bool:
    parts = relative_path.parts
    if not parts:
        return False

    top_level = parts[0]
    if top_level == "tests" and not include_tests:
        return True
    if top_level in DEFAULT_EXCLUDED_TOP_LEVEL:
        return True
    if any(part in DEFAULT_EXCLUDED_PARTS for part in parts):
        return True

    name = parts[-1]
    if name in DEFAULT_EXCLUDED_FILE_NAMES:
        return True

    path_text = relative_path.as_posix()
    for pattern in (*DEFAULT_EXCLUDED_GLOBS, *extra_excludes):
        if _matches_pattern(path_text, parts, pattern):
            return True

    if is_dir and name in {"env", "venv", "ENV", "venv.bak", "env.bak"}:
        return True
    return False


def collect_package_files(
    repo_root: Path,
    *,
    include_tests: bool,
    extra_excludes: Sequence[str] = (),
) -> tuple[Path, ...]:
    files: list[Path] = []
    for root, dirs, filenames in os.walk(repo_root):
        root_path = Path(root)
        rel_dir = root_path.relative_to(repo_root)
        dirs[:] = [
            dirname
            for dirname in sorted(dirs)
            if not should_exclude_path(
                _relative_join(rel_dir, dirname),
                include_tests=include_tests,
                is_dir=True,
                extra_excludes=extra_excludes,
            )
        ]
        for filename in sorted(filenames):
            rel_path = _relative_join(rel_dir, filename)
            if should_exclude_path(rel_path, include_tests=include_tests, extra_excludes=extra_excludes):
                continue
            files.append(rel_path)
    return tuple(files)


def verify_required_paths(repo_root: Path) -> None:
    missing = [str(path) for path in REQUIRED_DEPLOYMENT_PATHS if not (repo_root / path).exists()]
    if missing:
        joined = "\n".join(f"  - {path}" for path in missing)
        raise SystemExit(f"Deployment package prerequisites are missing:\n{joined}")


def build_source_metadata(
    repo_root: Path,
    *,
    include_tests: bool,
    source_files: Sequence[Path],
    extra_excludes: Sequence[str],
) -> dict[str, object]:
    status = _run_git(repo_root, ["status", "--short"])
    dirty_paths = [line for line in status.splitlines() if line.strip()]
    return {
        "schema_version": 1,
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": {
            "git_commit": _run_git(repo_root, ["rev-parse", "--short", "HEAD"]) or None,
            "git_branch": _run_git(repo_root, ["branch", "--show-current"]) or None,
            "git_dirty": bool(dirty_paths),
            "git_status_short": dirty_paths,
            "repo_root_at_build": str(repo_root),
        },
        "contents": {
            "source_file_count": len(source_files),
            "include_tests": include_tests,
            "extra_excludes": list(extra_excludes),
            "excluded_top_level": sorted(DEFAULT_EXCLUDED_TOP_LEVEL),
        },
        "runtime": {
            "python": ">=3.12",
            "default_install_extras": "postgres,search",
            "cli_module": "LiuXin_alpha.surfaces.cli",
        },
        "postgres": {
            "runbook": "dev-docs/postgresql-backend.md",
            "live_smoke": "scripts/run_postgres_live_smoke.py",
            "generated_helpers": [
                "deploy/remote_install.sh",
                "deploy/postgres_remote_setup.sh",
            ],
        },
        "notes": [
            "This bundle is built from the working tree, including uncommitted source files.",
            "Local data, virtualenvs, coverage output, build output, and working-memory logs are excluded.",
            "PostgreSQL role passwords are intentionally not generated into SQL files.",
        ],
    }


def make_package_plan(args: argparse.Namespace, *, repo_root: Path = REPO_ROOT) -> PackagePlan:
    verify_required_paths(repo_root)
    output_dir = _resolve_from_repo(repo_root, args.output_dir)
    bundle_name = f"{args.name}-{_utc_timestamp()}"
    output_path = output_dir / f"{bundle_name}.tar.gz"
    sha256_path = output_path.with_suffix(output_path.suffix + ".sha256")
    source_files = collect_package_files(
        repo_root,
        include_tests=bool(args.include_tests),
        extra_excludes=tuple(args.exclude or ()),
    )
    metadata = build_source_metadata(
        repo_root,
        include_tests=bool(args.include_tests),
        source_files=source_files,
        extra_excludes=tuple(args.exclude or ()),
    )
    metadata["artifact"] = {
        "bundle_name": bundle_name,
        "archive_name": output_path.name,
        "archive_sha256_file": sha256_path.name,
    }
    return PackagePlan(
        bundle_name=bundle_name,
        output_path=output_path,
        sha256_path=sha256_path,
        source_files=source_files,
        metadata=metadata,
    )


def copy_source_files(repo_root: Path, bundle_root: Path, source_files: Sequence[Path]) -> None:
    for rel_path in source_files:
        source = repo_root / rel_path
        target = bundle_root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_symlink():
            os.symlink(os.readlink(source), target)
        else:
            shutil.copy2(source, target)


def render_remote_install_script() -> str:
    return r'''#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-${BUNDLE_ROOT}/.venv}"
LIUXIN_INSTALL_EXTRAS="${LIUXIN_INSTALL_EXTRAS:-postgres,search}"
RECREATE=0
SKIP_INSTALL=0

usage() {
    cat <<'EOF'
Usage: deploy/remote_install.sh [options]

Create or refresh the deployment virtual environment and install LiuXin from
this extracted bundle.

Options:
  --python <path>       Python interpreter to use (default: python3)
  --venv <path>         Virtual environment path (default: <bundle>/.venv)
  --extras <csv>        Extras to install (default: postgres,search; use none for plain install)
  --recreate            Remove and recreate the virtual environment
  --skip-install        Create/reuse the venv but do not run pip install
  -h, --help            Show this help

Environment:
  PYTHON_BIN
  VENV_DIR
  LIUXIN_INSTALL_EXTRAS
EOF
}

print_cmd() {
    printf '%q ' "$@"
    printf '\n'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --python)
            PYTHON_BIN="$2"
            shift 2
            ;;
        --venv)
            VENV_DIR="$2"
            shift 2
            ;;
        --extras)
            LIUXIN_INSTALL_EXTRAS="$2"
            shift 2
            ;;
        --recreate)
            RECREATE=1
            shift
            ;;
        --skip-install)
            SKIP_INSTALL=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    echo "Python interpreter not found: ${PYTHON_BIN}" >&2
    exit 1
fi

"${PYTHON_BIN}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else "LiuXin-alpha requires Python >= 3.12")'

if [[ ${RECREATE} -eq 1 && -d "${VENV_DIR}" ]]; then
    rm -rf "${VENV_DIR}"
fi

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
    CREATE_CMD=("${PYTHON_BIN}" -m venv "${VENV_DIR}")
    printf '[remote-install] Create venv: '
    print_cmd "${CREATE_CMD[@]}"
    "${CREATE_CMD[@]}"
else
    printf '[remote-install] Reusing venv: %s\n' "${VENV_DIR}"
fi

VENV_PYTHON="${VENV_DIR}/bin/python"

if [[ ${SKIP_INSTALL} -eq 0 ]]; then
    UPGRADE_CMD=("${VENV_PYTHON}" -m pip install --upgrade pip)
    printf '[remote-install] Upgrade pip: '
    print_cmd "${UPGRADE_CMD[@]}"
    "${UPGRADE_CMD[@]}"

    if [[ "${LIUXIN_INSTALL_EXTRAS}" == "none" ]]; then
        INSTALL_CMD=("${VENV_PYTHON}" -m pip install -e "${BUNDLE_ROOT}")
    else
        INSTALL_CMD=("${VENV_PYTHON}" -m pip install -e "${BUNDLE_ROOT}[${LIUXIN_INSTALL_EXTRAS}]")
    fi
    printf '[remote-install] Install LiuXin: '
    print_cmd "${INSTALL_CMD[@]}"
    "${INSTALL_CMD[@]}"
fi

cat <<EOF

LiuXin deployment environment ready.

CLI:
  "${VENV_PYTHON}" -m LiuXin_alpha.surfaces.cli --help

PostgreSQL setup helper:
  "${SCRIPT_DIR}/postgres_remote_setup.sh" --help
EOF
'''


def render_postgres_setup_script() -> str:
    return r'''#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_PYTHON="${VENV_PYTHON:-${BUNDLE_ROOT}/.venv/bin/python}"
DATABASE="${LIUXIN_POSTGRES_DATABASE:-liuxin}"
SCHEMA="${LIUXIN_POSTGRES_SCHEMA:-liuxin}"
OWNER_ROLE="${LIUXIN_POSTGRES_OWNER_ROLE:-liuxin_owner}"
RUNTIME_ROLE="${LIUXIN_POSTGRES_RUNTIME_ROLE:-liuxin_runtime}"
ADMIN_DATABASE="${LIUXIN_POSTGRES_ADMIN_DATABASE:-postgres}"
SQL_DIR="${SQL_DIR:-${SCRIPT_DIR}/generated}"
SERVER_SQL=""
DATABASE_SQL=""
PSQL_ADMIN="${PSQL_ADMIN:-sudo -u postgres psql}"
OWNER_URL="${LIUXIN_POSTGRES_OWNER_URL:-}"
OWNER_SERVICE="${LIUXIN_POSTGRES_OWNER_SERVICE:-}"
RUNTIME_URL="${LIUXIN_POSTGRES_RUNTIME_URL:-}"
RUNTIME_SERVICE="${LIUXIN_POSTGRES_RUNTIME_SERVICE:-}"
SMOKE_SCHEMA="${LIUXIN_POSTGRES_SMOKE_SCHEMA:-}"
APPLY_SERVER=0
APPLY_DATABASE=0
INIT_SCHEMA=0
CHECK_RUNTIME=0
RUN_SMOKE=0
NO_CREATE_DATABASE=0
NO_CREATE_ROLES=0
NO_PASSWORD_PROMPT=0

usage() {
    cat <<'EOF'
Usage: deploy/postgres_remote_setup.sh [options]

Generate LiuXin PostgreSQL setup SQL from the bundled CLI. By default this is
safe and only writes SQL files under deploy/generated.

Core options:
  --database <name>        Target database name (default: liuxin)
  --schema <name>          Target schema name (default: liuxin)
  --owner-role <role>      Role that owns the database/schema (default: liuxin_owner)
  --runtime-role <role>    Role used by LiuXin at runtime (default: liuxin_runtime)
  --sql-dir <path>         Directory for generated SQL files
  --server-sql <path>      Output path for server-level SQL
  --database-sql <path>    Output path for database-local SQL

Apply options:
  --apply-server           Run server SQL with PSQL_ADMIN against --admin-database
  --apply-database         Run database SQL with PSQL_ADMIN against --database
  --admin-database <name>  Maintenance DB for server SQL (default: postgres)
  --no-create-database     Do not emit CREATE DATABASE
  --no-create-roles        Do not emit role creation blocks

LiuXin validation options:
  --init-schema            Run `postgres init` as the owner target
  --check-runtime          Run `postgres check` as the runtime target
  --run-smoke              Run scripts/run_postgres_live_smoke.py as the owner target
  --owner-url <url>        Owner-role PostgreSQL URL for init/smoke
  --owner-service <name>   Owner-role PostgreSQL service profile for init/smoke
  --runtime-url <url>      Runtime-role PostgreSQL URL for check
  --runtime-service <name> Runtime-role PostgreSQL service profile for check
  --smoke-schema <name>    Schema for smoke test (default: <schema>_smoke)
  --no-password-prompt     Do not prompt interactively for missing passwords

Environment:
  PSQL_ADMIN='sudo -u postgres psql'
  VENV_PYTHON=<bundle>/.venv/bin/python
  LIUXIN_POSTGRES_DATABASE, LIUXIN_POSTGRES_SCHEMA
  LIUXIN_POSTGRES_OWNER_ROLE, LIUXIN_POSTGRES_RUNTIME_ROLE
  LIUXIN_POSTGRES_OWNER_URL, LIUXIN_POSTGRES_RUNTIME_URL
EOF
}

print_cmd() {
    printf '%q ' "$@"
    printf '\n'
}

run_psql_admin() {
    # PSQL_ADMIN is intentionally a command string so the default can include sudo arguments.
    # shellcheck disable=SC2086
    ${PSQL_ADMIN} "$@"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --database)
            DATABASE="$2"
            shift 2
            ;;
        --schema)
            SCHEMA="$2"
            shift 2
            ;;
        --owner-role)
            OWNER_ROLE="$2"
            shift 2
            ;;
        --runtime-role)
            RUNTIME_ROLE="$2"
            shift 2
            ;;
        --admin-database)
            ADMIN_DATABASE="$2"
            shift 2
            ;;
        --sql-dir)
            SQL_DIR="$2"
            shift 2
            ;;
        --server-sql)
            SERVER_SQL="$2"
            shift 2
            ;;
        --database-sql)
            DATABASE_SQL="$2"
            shift 2
            ;;
        --apply-server)
            APPLY_SERVER=1
            shift
            ;;
        --apply-database)
            APPLY_DATABASE=1
            shift
            ;;
        --init-schema)
            INIT_SCHEMA=1
            shift
            ;;
        --check-runtime)
            CHECK_RUNTIME=1
            shift
            ;;
        --run-smoke)
            RUN_SMOKE=1
            shift
            ;;
        --owner-url)
            OWNER_URL="$2"
            OWNER_SERVICE=""
            shift 2
            ;;
        --owner-service)
            OWNER_SERVICE="$2"
            OWNER_URL=""
            shift 2
            ;;
        --runtime-url)
            RUNTIME_URL="$2"
            RUNTIME_SERVICE=""
            shift 2
            ;;
        --runtime-service)
            RUNTIME_SERVICE="$2"
            RUNTIME_URL=""
            shift 2
            ;;
        --smoke-schema)
            SMOKE_SCHEMA="$2"
            shift 2
            ;;
        --no-create-database)
            NO_CREATE_DATABASE=1
            shift
            ;;
        --no-create-roles)
            NO_CREATE_ROLES=1
            shift
            ;;
        --no-password-prompt)
            NO_PASSWORD_PROMPT=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "Expected LiuXin venv Python at ${VENV_PYTHON}" >&2
    echo "Run deploy/remote_install.sh first, or set VENV_PYTHON." >&2
    exit 1
fi

mkdir -p "${SQL_DIR}"
if [[ -z "${SERVER_SQL}" ]]; then
    SERVER_SQL="${SQL_DIR}/liuxin-postgres-server.sql"
fi
if [[ -z "${DATABASE_SQL}" ]]; then
    DATABASE_SQL="${SQL_DIR}/liuxin-postgres-database.sql"
fi
if [[ -z "${SMOKE_SCHEMA}" ]]; then
    SMOKE_SCHEMA="${SCHEMA}_smoke"
fi

setup_base=(
    "${VENV_PYTHON}" -m LiuXin_alpha.surfaces.cli postgres setup-sql
    --database "${DATABASE}"
    --owner-role "${OWNER_ROLE}"
    --runtime-role "${RUNTIME_ROLE}"
    --schema "${SCHEMA}"
)
if [[ ${NO_CREATE_DATABASE} -eq 1 ]]; then
    setup_base+=(--no-create-database)
fi
if [[ ${NO_CREATE_ROLES} -eq 1 ]]; then
    setup_base+=(--no-create-roles)
fi

server_cmd=("${setup_base[@]}" --section server)
database_cmd=("${setup_base[@]}" --section database)

printf '[postgres-setup] Generate server SQL: '
print_cmd "${server_cmd[@]}"
"${server_cmd[@]}" > "${SERVER_SQL}"

printf '[postgres-setup] Generate database SQL: '
print_cmd "${database_cmd[@]}"
"${database_cmd[@]}" > "${DATABASE_SQL}"

printf '[postgres-setup] Server SQL: %s\n' "${SERVER_SQL}"
printf '[postgres-setup] Database SQL: %s\n' "${DATABASE_SQL}"

if [[ ${APPLY_SERVER} -eq 1 ]]; then
    printf '[postgres-setup] Apply server SQL with PSQL_ADMIN=%s\n' "${PSQL_ADMIN}"
    run_psql_admin -d "${ADMIN_DATABASE}" -f "${SERVER_SQL}"
else
    printf '[postgres-setup] Server SQL not applied. Re-run with --apply-server and the same database/schema/role options after reviewing it.\n'
fi

if [[ ${APPLY_DATABASE} -eq 1 ]]; then
    printf '[postgres-setup] Apply database SQL with PSQL_ADMIN=%s\n' "${PSQL_ADMIN}"
    run_psql_admin -d "${DATABASE}" -f "${DATABASE_SQL}"
else
    printf '[postgres-setup] Database SQL not applied. Re-run with --apply-database and the same database/schema/role options after reviewing it.\n'
fi

owner_target=()
if [[ -n "${OWNER_URL}" ]]; then
    owner_target=(--url "${OWNER_URL}")
elif [[ -n "${OWNER_SERVICE}" ]]; then
    owner_target=(--service "${OWNER_SERVICE}")
fi

runtime_target=()
if [[ -n "${RUNTIME_URL}" ]]; then
    runtime_target=(--url "${RUNTIME_URL}")
elif [[ -n "${RUNTIME_SERVICE}" ]]; then
    runtime_target=(--service "${RUNTIME_SERVICE}")
fi

password_prompt_args=()
if [[ ${NO_PASSWORD_PROMPT} -eq 1 ]]; then
    password_prompt_args=(--no-password-prompt)
fi

if [[ ${INIT_SCHEMA} -eq 1 ]]; then
    if [[ ${#owner_target[@]} -eq 0 ]]; then
        echo "--init-schema requires --owner-url, --owner-service, or matching LIUXIN_POSTGRES_OWNER_* env." >&2
        exit 2
    fi
    init_cmd=("${VENV_PYTHON}" -m LiuXin_alpha.surfaces.cli postgres init "${owner_target[@]}" --schema "${SCHEMA}" "${password_prompt_args[@]}")
    printf '[postgres-setup] Init LiuXin schema: '
    print_cmd "${init_cmd[@]}"
    "${init_cmd[@]}"
fi

if [[ ${CHECK_RUNTIME} -eq 1 ]]; then
    if [[ ${#runtime_target[@]} -eq 0 ]]; then
        echo "--check-runtime requires --runtime-url, --runtime-service, or matching LIUXIN_POSTGRES_RUNTIME_* env." >&2
        exit 2
    fi
    check_cmd=("${VENV_PYTHON}" -m LiuXin_alpha.surfaces.cli postgres check "${runtime_target[@]}" --schema "${SCHEMA}" "${password_prompt_args[@]}")
    printf '[postgres-setup] Check runtime role: '
    print_cmd "${check_cmd[@]}"
    "${check_cmd[@]}"
fi

if [[ ${RUN_SMOKE} -eq 1 ]]; then
    if [[ ${#owner_target[@]} -eq 0 ]]; then
        echo "--run-smoke requires --owner-url, --owner-service, or matching LIUXIN_POSTGRES_OWNER_* env." >&2
        exit 2
    fi
    smoke_cmd=("${VENV_PYTHON}" "${BUNDLE_ROOT}/scripts/run_postgres_live_smoke.py" "${owner_target[@]}" --schema "${SMOKE_SCHEMA}" --drop-schema "${password_prompt_args[@]}")
    printf '[postgres-setup] Run live smoke: '
    print_cmd "${smoke_cmd[@]}"
    "${smoke_cmd[@]}"
fi
'''


def render_bundle_readme() -> str:
    return """# LiuXin Deployment Bundle

This bundle was generated from the repository working tree. It is a practical
source deployment package, not a final OS package.

## Remote Install

```bash
tar -xzf liuxin-alpha-deployment-*.tar.gz
cd liuxin-alpha-deployment-*
deploy/remote_install.sh
```

The installer creates `.venv` in the extracted bundle and installs LiuXin with
the `postgres,search` extras by default.

## PostgreSQL Setup

Generate SQL first and review it:

```bash
deploy/postgres_remote_setup.sh \\
  --database liuxin \\
  --schema liuxin \\
  --owner-role liuxin_owner \\
  --runtime-role liuxin_runtime
```

Apply the server section from a maintenance database, repeating the same target
arguments, then set role passwords out of band:

```bash
deploy/postgres_remote_setup.sh \\
  --database liuxin \\
  --schema liuxin \\
  --owner-role liuxin_owner \\
  --runtime-role liuxin_runtime \\
  --apply-server
sudo -u postgres psql -d postgres
```

Apply the database-local section after the target database exists:

```bash
deploy/postgres_remote_setup.sh \\
  --database liuxin \\
  --schema liuxin \\
  --owner-role liuxin_owner \\
  --runtime-role liuxin_runtime \\
  --apply-database
```

Initialise and check LiuXin once owner/runtime credentials are configured:

```bash
deploy/postgres_remote_setup.sh \\
  --init-schema \\
  --check-runtime \\
  --owner-url postgresql://liuxin_owner@localhost/liuxin \\
  --runtime-url postgresql://liuxin_runtime@localhost/liuxin
```

See `dev-docs/postgresql-backend.md` for the longer runbook.
"""


def write_generated_files(bundle_root: Path, metadata: dict[str, object]) -> None:
    deploy_dir = bundle_root / "deploy"
    deploy_dir.mkdir(parents=True, exist_ok=True)

    generated_files = {
        deploy_dir / "remote_install.sh": render_remote_install_script(),
        deploy_dir / "postgres_remote_setup.sh": render_postgres_setup_script(),
        deploy_dir / "README.md": render_bundle_readme(),
    }
    for path, content in generated_files.items():
        path.write_text(content, encoding="utf-8")
        if path.suffix == ".sh":
            path.chmod(0o755)

    manifest_path = bundle_root / "deployment_manifest.json"
    manifest_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def create_tarball(bundle_root: Path, output_path: Path, *, bundle_name: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output_path, "w:gz", format=tarfile.PAX_FORMAT) as tar:
        tar.add(bundle_root, arcname=bundle_name)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_deployment_package(args: argparse.Namespace, *, repo_root: Path = REPO_ROOT) -> dict[str, object]:
    plan = make_package_plan(args, repo_root=repo_root)

    if args.dry_run:
        summary = {
            "bundle_name": plan.bundle_name,
            "output_path": str(plan.output_path),
            "sha256_path": str(plan.sha256_path),
            "source_file_count": len(plan.source_files),
            "metadata": plan.metadata,
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
        return summary

    if plan.output_path.exists() and not args.force:
        raise SystemExit(f"Output already exists: {plan.output_path}. Pass --force to overwrite.")
    if plan.sha256_path.exists() and not args.force:
        raise SystemExit(f"Checksum already exists: {plan.sha256_path}. Pass --force to overwrite.")

    _log(f"staging {len(plan.source_files)} source files")
    with tempfile.TemporaryDirectory(prefix="liuxin-deploy-build-") as temp_dir:
        bundle_root = Path(temp_dir) / plan.bundle_name
        bundle_root.mkdir(parents=True)
        copy_source_files(repo_root, bundle_root, plan.source_files)
        write_generated_files(bundle_root, plan.metadata)
        _log(f"writing archive: {plan.output_path}")
        create_tarball(bundle_root, plan.output_path, bundle_name=plan.bundle_name)

    digest = sha256_file(plan.output_path)
    plan.sha256_path.write_text(f"{digest}  {plan.output_path.name}\n", encoding="utf-8")
    result = {
        "bundle_name": plan.bundle_name,
        "output_path": str(plan.output_path),
        "sha256": digest,
        "sha256_path": str(plan.sha256_path),
        "source_file_count": len(plan.source_files),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a deployable LiuXin source bundle with remote PostgreSQL setup helpers.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for the deployment tarball (default: dist/deployment).",
    )
    parser.add_argument(
        "--name",
        default=DEFAULT_BUNDLE_NAME,
        help=f"Bundle name prefix (default: {DEFAULT_BUNDLE_NAME}).",
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="Include tests in the bundle. Tests are excluded by default to keep deployment artifacts smaller.",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="GLOB",
        help="Additional relative path glob to exclude. Can be repeated.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing output files.")
    parser.add_argument("--dry-run", action="store_true", help="Print the package plan without writing an archive.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    build_deployment_package(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
