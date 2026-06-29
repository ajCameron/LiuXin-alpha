#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-${REPO_ROOT}/.venv}"
INSTALL_EXTRAS="${ARTIFACTS_VENV_EXTRAS:-test,search}"
RECREATE=0
SKIP_INSTALL=0
SKIP_VENV=0
ARTIFACT_ARGS=()

usage() {
    cat <<'EOF'
Usage: scripts/build_artifacts.sh [wrapper-options] -- <build_artifacts.py args>
       scripts/build_artifacts.sh [wrapper-options] <build_artifacts.py args>

Create or refresh the repo-local virtual environment, then run
scripts/build_artifacts.py using that venv's Python.

Wrapper options:
  --python <path>       Python interpreter used to create the venv
  --venv <path>         Virtual environment directory (default: <repo>/.venv)
  --extras <csv>        Extras passed to create_venv.sh (default: test,search)
                        Use "none" for a plain editable install
  --new-venv            Recreate the venv before running
  --skip-install        Create/reuse the venv but skip pip install
  --skip-venv           Do not create/update the venv; just run the existing venv Python
  -h, --help            Show this wrapper help

Examples:
  scripts/build_artifacts.sh list --data-root LiuXin_alpha_data
  scripts/build_artifacts.sh verify --data-root LiuXin_alpha_data --artifact benchmark-smoke
  scripts/build_artifacts.sh build --data-root LiuXin_alpha_data --artifact benchmark-smoke --regenerate
  scripts/build_artifacts.sh --new-venv -- build --data-root LiuXin_alpha_data --artifact benchmark-smoke --regenerate

Environment:
  PYTHON_BIN             Default interpreter if --python is not supplied
  VENV_DIR               Default virtual environment path
  ARTIFACTS_VENV_EXTRAS  Default extras passed to create_venv.sh
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
            INSTALL_EXTRAS="$2"
            shift 2
            ;;
        --new-venv|--recreate)
            RECREATE=1
            shift
            ;;
        --skip-install)
            SKIP_INSTALL=1
            shift
            ;;
        --skip-venv)
            SKIP_VENV=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            ARTIFACT_ARGS=("$@")
            break
            ;;
        *)
            ARTIFACT_ARGS=("$@")
            break
            ;;
    esac
done

if [[ "${VENV_DIR}" != /* ]]; then
    VENV_DIR="${REPO_ROOT}/${VENV_DIR}"
fi

if [[ ${#ARTIFACT_ARGS[@]} -eq 0 ]]; then
    usage >&2
    exit 2
fi

CREATE_VENV_CMD=(
    "${SCRIPT_DIR}/create_venv.sh"
    --python "${PYTHON_BIN}"
    --venv "${VENV_DIR}"
    --extras "${INSTALL_EXTRAS}"
)
if [[ ${RECREATE} -eq 1 ]]; then
    CREATE_VENV_CMD+=(--recreate)
fi
if [[ ${SKIP_INSTALL} -eq 1 ]]; then
    CREATE_VENV_CMD+=(--skip-install)
fi

VENV_PYTHON="${VENV_DIR}/bin/python"
RUN_CMD=("${VENV_PYTHON}" "${SCRIPT_DIR}/build_artifacts.py" "${ARTIFACT_ARGS[@]}")

printf '[artifact-build] Repo root: %s\n' "${REPO_ROOT}" >&2
printf '[artifact-build] Venv: %s\n' "${VENV_DIR}" >&2

if [[ ${SKIP_VENV} -eq 0 ]]; then
    printf '[artifact-build] Venv setup: ' >&2
    print_cmd "${CREATE_VENV_CMD[@]}" >&2
    "${CREATE_VENV_CMD[@]}"
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "[artifact-build] Expected venv interpreter at ${VENV_PYTHON}" >&2
    exit 1
fi

printf '[artifact-build] Artifact command: ' >&2
print_cmd "${RUN_CMD[@]}" >&2
cd "${REPO_ROOT}"
exec "${RUN_CMD[@]}"
