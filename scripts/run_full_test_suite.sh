#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
RESULTS_DIR="${REPO_ROOT}/working-memory/test-results"
TIMESTAMP="$(date +%F-%H%M%S)"
REPORT_FILE="${RESULTS_DIR}/full-suite-${TIMESTAMP}.json"
WORKERS="auto"
DIST_MODE="worksteal"
SKIP_INSTALL=0
CREATE_VENV=0
RECREATE_VENV=0
PYTHON_BIN="${PYTHON_BIN:-python3}"
DRY_RUN=0
RUN_TK_SMOKE=0
ONLY_TK_SMOKE=0
PYTEST_ARGS=()

usage() {
    cat <<'EOF'
Usage: scripts/run_full_test_suite.sh [options] [-- <extra pytest args>]

Options:
  --workers <n|auto>       Pytest xdist worker count (default: auto)
  --dist <mode>            Pytest xdist distribution mode (default: worksteal)
  --results-dir <path>     Directory for JSON report output
  --report-file <path>     Exact JSON report file path to write
  --create-venv            Create/reuse .venv via scripts/create_venv.sh before testing
  --new-venv               Recreate .venv via scripts/create_venv.sh before testing
  --python <path>          Python interpreter for --create-venv/--new-venv
  --skip-install           Skip pip upgrade and dependency install
  --dry-run                Print commands without executing them
  --tk-smoke               Run the real Tkinter GUI smoke test after the main suite
  --only-tk-smoke          Run only the real Tkinter GUI smoke test
  -h, --help               Show this help

Examples:
  scripts/run_full_test_suite.sh
  scripts/run_full_test_suite.sh --new-venv --python python3.12
  scripts/run_full_test_suite.sh --new-venv --python python3.12 --only-tk-smoke
  scripts/run_full_test_suite.sh --workers 8
  scripts/run_full_test_suite.sh -- --maxfail=1 -k sync_store
EOF
}

print_cmd() {
    printf '%q ' "$@"
    printf '\n'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --dist)
            DIST_MODE="$2"
            shift 2
            ;;
        --results-dir)
            RESULTS_DIR="$2"
            REPORT_FILE="${RESULTS_DIR}/full-suite-${TIMESTAMP}.json"
            shift 2
            ;;
        --report-file)
            REPORT_FILE="$2"
            shift 2
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
        --tk-smoke)
            RUN_TK_SMOKE=1
            shift
            ;;
        --only-tk-smoke)
            RUN_TK_SMOKE=1
            ONLY_TK_SMOKE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            PYTEST_ARGS=("$@")
            break
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

mkdir -p "${RESULTS_DIR}"

CREATE_VENV_CMD=("bash" "${SCRIPT_DIR}/create_venv.sh" "--python" "${PYTHON_BIN}")
if [[ ${RECREATE_VENV} -eq 1 ]]; then
    CREATE_VENV_CMD+=("--recreate")
fi
PIP_UPGRADE_CMD=("${VENV_PYTHON}" -m pip install -U pip)
PIP_INSTALL_CMD=("${VENV_PYTHON}" -m pip install -e ".[test,search]")
PYTEST_CMD=(
    "${VENV_PYTHON}" -m pytest tests
    -n "${WORKERS}"
    --dist "${DIST_MODE}"
    --json-report
    --json-report-file "${REPORT_FILE}"
    -ra
)
TK_PREFLIGHT_CMD=(
    "${VENV_PYTHON}" -c
    $'import tkinter as tk\nroot = tk.Tk()\nroot.withdraw()\nroot.destroy()\nprint("tkinter smoke preflight ok")'
)
TK_PYTEST_CMD=(
    "${VENV_PYTHON}" -m pytest
    tests/surfaces/test_tkinter_gui.py::test_tkinter_gui_real_tk_smoke_renders_fake_backend
    -q
    -ra
)

if [[ ${#PYTEST_ARGS[@]} -gt 0 ]]; then
    PYTEST_CMD+=("${PYTEST_ARGS[@]}")
fi

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'Report file: %s\n' "${REPORT_FILE}"

if [[ ${CREATE_VENV} -eq 1 ]]; then
    printf 'Venv step: '
    print_cmd "${CREATE_VENV_CMD[@]}"
fi

if [[ ${SKIP_INSTALL} -eq 0 && ${CREATE_VENV} -eq 0 ]]; then
    printf 'Install step: '
    print_cmd "${PIP_UPGRADE_CMD[@]}"
    printf 'Install step: '
    print_cmd "${PIP_INSTALL_CMD[@]}"
fi

if [[ ${ONLY_TK_SMOKE} -eq 0 ]]; then
    printf 'Test step: '
    print_cmd "${PYTEST_CMD[@]}"
fi

if [[ ${RUN_TK_SMOKE} -eq 1 ]]; then
    printf 'Tk preflight step: '
    print_cmd "${TK_PREFLIGHT_CMD[@]}"
    printf 'Tk smoke step: '
    print_cmd "${TK_PYTEST_CMD[@]}"
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

if [[ ${SKIP_INSTALL} -eq 0 && ${CREATE_VENV} -eq 0 ]]; then
    "${PIP_UPGRADE_CMD[@]}"
    "${PIP_INSTALL_CMD[@]}"
fi

if [[ ${ONLY_TK_SMOKE} -eq 0 ]]; then
    "${PYTEST_CMD[@]}"
fi

if [[ ${RUN_TK_SMOKE} -eq 1 ]]; then
    if ! "${TK_PREFLIGHT_CMD[@]}"; then
        cat >&2 <<'EOF'
Tkinter smoke preflight failed. The venv inherits tkinter/display support from
its base Python; install the system tkinter package and ensure a display is
available.
EOF
        exit 1
    fi
    "${TK_PYTEST_CMD[@]}"
fi
