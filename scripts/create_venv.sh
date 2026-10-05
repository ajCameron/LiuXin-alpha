#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${REPO_ROOT}/.venv"
INSTALL_EXTRAS="test,search,conversion"
RECREATE=0
SKIP_INSTALL=0

usage() {
    cat <<'EOF'
Usage: scripts/create_venv.sh [options]

Create or refresh the repo-local virtual environment in .venv.

Options:
  --python <path>          Python interpreter to use for venv creation
  --venv <path>            Virtual environment directory (default: <repo>/.venv)
  --extras <csv>           Extras to install with -e .[...]
                           Use "none" for a plain editable install
  --recreate               Remove and recreate the virtual environment
  --skip-install           Create the venv but skip pip install
  -h, --help               Show this help

Environment:
  PYTHON_BIN               Default interpreter if --python is not supplied

Examples:
  bash scripts/create_venv.sh
  bash scripts/create_venv.sh --python python3.12
  bash scripts/create_venv.sh --extras none
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

if [[ ${RECREATE} -eq 1 && -d "${VENV_DIR}" ]]; then
    rm -rf "${VENV_DIR}"
fi

VERSION_CHECK_CMD=(
    "${PYTHON_BIN}"
    -c
    'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else "LiuXin-alpha requires Python >= 3.12")'
)
CREATE_VENV_CMD=("${PYTHON_BIN}" -m venv "${VENV_DIR}")
VENV_PYTHON="${VENV_DIR}/bin/python"
PIP_UPGRADE_CMD=("${VENV_PYTHON}" -m pip install --upgrade pip)

if [[ "${INSTALL_EXTRAS}" == "none" ]]; then
    PIP_INSTALL_CMD=("${VENV_PYTHON}" -m pip install -e .)
else
    PIP_INSTALL_CMD=("${VENV_PYTHON}" -m pip install -e ".[${INSTALL_EXTRAS}]")
fi

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'Python: %s\n' "${PYTHON_BIN}"
printf 'Venv: %s\n' "${VENV_DIR}"

printf 'Version check: '
print_cmd "${VERSION_CHECK_CMD[@]}"
"${VERSION_CHECK_CMD[@]}"

if [[ ! -x "${VENV_PYTHON}" ]]; then
    printf 'Create step: '
    print_cmd "${CREATE_VENV_CMD[@]}"
    "${CREATE_VENV_CMD[@]}"
else
    printf 'Reusing existing venv: %s\n' "${VENV_DIR}"
fi

if [[ ${SKIP_INSTALL} -eq 0 ]]; then
    printf 'Install step: '
    print_cmd "${PIP_UPGRADE_CMD[@]}"
    "${PIP_UPGRADE_CMD[@]}"

    printf 'Install step: '
    print_cmd "${PIP_INSTALL_CMD[@]}"
    (
        cd "${REPO_ROOT}"
        "${PIP_INSTALL_CMD[@]}"
    )
fi

cat <<EOF

Virtual environment ready.

Activate it with:
  . "${VENV_DIR}/bin/activate"

Run tests with:
  "${VENV_PYTHON}" -m pytest
EOF
