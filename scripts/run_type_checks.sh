#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CREATE_VENV=0
RECREATE_VENV=0
SKIP_INSTALL=0
DRY_RUN=0
RUN_BASEDPYRIGHT=1
RUN_MYPY=1
TOOL_ARGS=()

usage() {
    cat <<'EOF'
Usage: scripts/run_type_checks.sh [options] [-- <tool args>]

Run the repo's static typing checks against the configured strict target set.

Options:
  --basedpyright          Run only basedpyright
  --mypy                  Run only mypy
  --all                   Run both basedpyright and mypy (default)
  --create-venv           Create/reuse .venv via scripts/create_venv.sh first
  --new-venv              Recreate .venv via scripts/create_venv.sh first
  --python <path>         Python interpreter for --create-venv/--new-venv
  --skip-install          Skip installing the typing extra
  --dry-run               Print commands without executing them
  -h, --help              Show this help

Extra tool args after "--" are allowed only when a single checker is selected.

Examples:
  scripts/run_type_checks.sh
  scripts/run_type_checks.sh --create-venv
  scripts/run_type_checks.sh --basedpyright -- --verbose
  scripts/run_type_checks.sh --mypy -- --show-traceback
EOF
}

print_cmd() {
    printf '%q ' "$@"
    printf '\n'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --basedpyright)
            RUN_BASEDPYRIGHT=1
            RUN_MYPY=0
            shift
            ;;
        --mypy)
            RUN_BASEDPYRIGHT=0
            RUN_MYPY=1
            shift
            ;;
        --all)
            RUN_BASEDPYRIGHT=1
            RUN_MYPY=1
            shift
            ;;
        --create-venv)
            CREATE_VENV=1
            shift
            ;;
        --new-venv)
            CREATE_VENV=1
            RECREATE_VENV=1
            shift
            ;;
        --python)
            if [[ $# -lt 2 ]]; then
                echo "Missing value for $1" >&2
                usage >&2
                exit 2
            fi
            PYTHON_BIN="$2"
            shift 2
            ;;
        --skip-install)
            SKIP_INSTALL=1
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            TOOL_ARGS=("$@")
            break
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ ${RUN_BASEDPYRIGHT} -eq 1 && ${RUN_MYPY} -eq 1 && ${#TOOL_ARGS[@]} -gt 0 ]]; then
    echo 'Extra tool args require selecting exactly one checker with --basedpyright or --mypy.' >&2
    exit 2
fi

CREATE_VENV_CMD=("bash" "${SCRIPT_DIR}/create_venv.sh" "--python" "${PYTHON_BIN}" "--extras" "typing")
if [[ ${RECREATE_VENV} -eq 1 ]]; then
    CREATE_VENV_CMD+=("--recreate")
fi

PIP_INSTALL_CMD=("${VENV_PYTHON}" -m pip install -e ".[typing]")
BASEDPYRIGHT_CMD=("${VENV_DIR}/bin/basedpyright")
MYPY_CMD=("${VENV_DIR}/bin/mypy")

if [[ ${RUN_BASEDPYRIGHT} -eq 1 && ${RUN_MYPY} -eq 0 && ${#TOOL_ARGS[@]} -gt 0 ]]; then
    BASEDPYRIGHT_CMD+=("${TOOL_ARGS[@]}")
elif [[ ${RUN_MYPY} -eq 1 && ${RUN_BASEDPYRIGHT} -eq 0 && ${#TOOL_ARGS[@]} -gt 0 ]]; then
    MYPY_CMD+=("${TOOL_ARGS[@]}")
fi

printf 'Repo root: %s\n' "${REPO_ROOT}"

if [[ ${CREATE_VENV} -eq 1 ]]; then
    printf 'Venv step: '
    print_cmd "${CREATE_VENV_CMD[@]}"
fi

if [[ ${CREATE_VENV} -eq 0 && ${SKIP_INSTALL} -eq 0 ]]; then
    printf 'Install step: '
    print_cmd "${PIP_INSTALL_CMD[@]}"
fi

if [[ ${RUN_BASEDPYRIGHT} -eq 1 ]]; then
    printf 'basedpyright step: '
    print_cmd "${BASEDPYRIGHT_CMD[@]}"
fi

if [[ ${RUN_MYPY} -eq 1 ]]; then
    printf 'mypy step: '
    print_cmd "${MYPY_CMD[@]}"
fi

if [[ ${DRY_RUN} -eq 1 ]]; then
    exit 0
fi

cd "${REPO_ROOT}"

if [[ ${CREATE_VENV} -eq 1 ]]; then
    "${CREATE_VENV_CMD[@]}"
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "Expected venv interpreter at ${VENV_PYTHON}. Create the repo-local .venv first." >&2
    exit 1
fi

if [[ ${CREATE_VENV} -eq 0 && ${SKIP_INSTALL} -eq 0 ]]; then
    "${PIP_INSTALL_CMD[@]}"
fi

if [[ ${RUN_BASEDPYRIGHT} -eq 1 && ! -x "${VENV_DIR}/bin/basedpyright" ]]; then
    echo "Expected basedpyright at ${VENV_DIR}/bin/basedpyright. Run without --skip-install or install the typing extra." >&2
    exit 1
fi

if [[ ${RUN_MYPY} -eq 1 && ! -x "${VENV_DIR}/bin/mypy" ]]; then
    echo "Expected mypy at ${VENV_DIR}/bin/mypy. Run without --skip-install or install the typing extra." >&2
    exit 1
fi

STATUS=0

if [[ ${RUN_BASEDPYRIGHT} -eq 1 ]]; then
    set +e
    "${BASEDPYRIGHT_CMD[@]}"
    CHECK_STATUS=$?
    set -e
    if [[ ${CHECK_STATUS} -ne 0 ]]; then
        STATUS=${CHECK_STATUS}
    fi
fi

if [[ ${RUN_MYPY} -eq 1 ]]; then
    set +e
    "${MYPY_CMD[@]}"
    CHECK_STATUS=$?
    set -e
    if [[ ${CHECK_STATUS} -ne 0 && ${STATUS} -eq 0 ]]; then
        STATUS=${CHECK_STATUS}
    fi
fi

exit "${STATUS}"
