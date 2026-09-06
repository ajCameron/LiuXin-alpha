#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
RESULTS_DIR="${REPO_ROOT}/working-memory/test-results"
TIMESTAMP="$(date +%F-%H%M%S)"
RUN_ID="full-suite-${TIMESTAMP}"
REPORT_FILE=""
LOG_FILE=""
DONE_FILE=""
WORKERS="auto"
DIST_MODE="worksteal"
SKIP_INSTALL=0
CREATE_VENV=0
RECREATE_VENV=0
PYTHON_BIN="${PYTHON_BIN:-python3}"
DRY_RUN=0
RUN_TK_SMOKE=0
ONLY_TK_SMOKE=0
WRITE_LOG=1
WRITE_DONE_MARKER=1
ORIGINAL_ARGS=("$@")
PYTEST_ARGS=()

usage() {
    cat <<'EOF'
Usage: scripts/run_full_test_suite.sh [options] [-- <extra pytest args>]

Pytest reports each test result as it completes so long-running suites show
continuous progress in both the terminal and the artifact log.

Options:
  --run-id <name>          Artifact run id (default: full-suite-<timestamp>)
  --workers <n|auto>       Pytest xdist worker count (default: auto)
  --dist <mode>            Pytest xdist distribution mode (default: worksteal)
  --results-dir <path>     Directory for JSON/log/done output
  --report-file <path>     Exact JSON report file path to write
  --log-file <path>        Exact log file path to write
  --done-file <path>       Exact exit marker path to write
  --no-log                 Do not tee output to a log file
  --no-done-marker         Do not write an exit marker
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

run_with_heartbeat() {
    local label="$1"
    shift
    local started_seconds=${SECONDS}
    local heartbeat_pid
    local status=0

    (
        while sleep 30; do
            printf '[%s] %s still running (%ss elapsed)\n' \
                "$(date -Is)" \
                "${label}" \
                "$((SECONDS - started_seconds))"
        done
    ) &
    heartbeat_pid=$!

    if "$@"; then
        status=0
    else
        status=$?
    fi

    kill "${heartbeat_pid}" 2>/dev/null || true
    wait "${heartbeat_pid}" 2>/dev/null || true
    return "${status}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-id)
            RUN_ID="$2"
            shift 2
            ;;
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
            shift 2
            ;;
        --report-file)
            REPORT_FILE="$2"
            shift 2
            ;;
        --log-file)
            LOG_FILE="$2"
            shift 2
            ;;
        --done-file)
            DONE_FILE="$2"
            shift 2
            ;;
        --no-log)
            WRITE_LOG=0
            shift
            ;;
        --no-done-marker)
            WRITE_DONE_MARKER=0
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

REPORT_FILE="${REPORT_FILE:-${RESULTS_DIR}/${RUN_ID}.json}"
LOG_FILE="${LOG_FILE:-${RESULTS_DIR}/${RUN_ID}.log}"
DONE_FILE="${DONE_FILE:-${RESULTS_DIR}/${RUN_ID}.done}"

mkdir -p "${RESULTS_DIR}" "$(dirname -- "${REPORT_FILE}")"
if [[ ${WRITE_LOG} -eq 1 ]]; then
    mkdir -p "$(dirname -- "${LOG_FILE}")"
fi
if [[ ${WRITE_DONE_MARKER} -eq 1 ]]; then
    mkdir -p "$(dirname -- "${DONE_FILE}")"
fi

CREATE_VENV_CMD=("bash" "${SCRIPT_DIR}/create_venv.sh" "--python" "${PYTHON_BIN}")
if [[ ${RECREATE_VENV} -eq 1 ]]; then
    CREATE_VENV_CMD+=("--recreate")
fi
PIP_UPGRADE_CMD=("${VENV_PYTHON}" -m pip install -U pip)
PIP_INSTALL_CMD=("${VENV_PYTHON}" -m pip install -e ".[test,search,conversion]")
PYTEST_CMD=(
    "${VENV_PYTHON}" -u -m pytest tests
    -n "${WORKERS}"
    --dist "${DIST_MODE}"
    --json-report
    --json-report-file "${REPORT_FILE}"
    --color=yes
    -v
    -ra
)
TK_PREFLIGHT_CMD=(
    "${VENV_PYTHON}" -c
    $'import tkinter as tk\nroot = tk.Tk()\nroot.withdraw()\nroot.destroy()\nprint("tkinter smoke preflight ok")'
)
TK_PYTEST_CMD=(
    "${VENV_PYTHON}" -u -m pytest
    tests/surfaces/test_tkinter_gui.py::test_tkinter_gui_real_tk_smoke_renders_fake_backend
    -q
    --color=yes
    -ra
)

if [[ ${#PYTEST_ARGS[@]} -gt 0 ]]; then
    PYTEST_CMD+=("${PYTEST_ARGS[@]}")
fi

STARTED_AT="$(date -Is)"
INVOCATION=("$0" "${ORIGINAL_ARGS[@]}")

write_done_marker() {
    local status=$?
    trap - EXIT
    if [[ ${WRITE_DONE_MARKER} -eq 1 && ${DRY_RUN} -eq 0 ]]; then
        {
            printf 'run_id: %s\n' "${RUN_ID}"
            printf 'repo_root: %s\n' "${REPO_ROOT}"
            printf 'script: %s\n' "$0"
            printf 'started_at: %s\n' "${STARTED_AT}"
            printf 'finished_at: %s\n' "$(date -Is)"
            printf 'exit_code: %s\n' "${status}"
            printf 'report_file: %s\n' "${REPORT_FILE}"
            printf 'log_file: %s\n' "${LOG_FILE}"
            printf 'done_file: %s\n' "${DONE_FILE}"
            printf 'invocation: '
            print_cmd "${INVOCATION[@]}"
            if [[ ${CREATE_VENV} -eq 1 ]]; then
                printf 'venv_step: '
                print_cmd "${CREATE_VENV_CMD[@]}"
            fi
            if [[ ${SKIP_INSTALL} -eq 0 && ${CREATE_VENV} -eq 0 ]]; then
                printf 'install_step: '
                print_cmd "${PIP_UPGRADE_CMD[@]}"
                printf 'install_step: '
                print_cmd "${PIP_INSTALL_CMD[@]}"
            fi
            if [[ ${ONLY_TK_SMOKE} -eq 0 ]]; then
                printf 'test_step: '
                print_cmd "${PYTEST_CMD[@]}"
            fi
            if [[ ${RUN_TK_SMOKE} -eq 1 ]]; then
                printf 'tk_preflight_step: '
                print_cmd "${TK_PREFLIGHT_CMD[@]}"
                printf 'tk_smoke_step: '
                print_cmd "${TK_PYTEST_CMD[@]}"
            fi
        } > "${DONE_FILE}"
    fi
    exit "${status}"
}

if [[ ${DRY_RUN} -eq 0 ]]; then
    trap write_done_marker EXIT
    if [[ ${WRITE_LOG} -eq 1 ]]; then
        : > "${LOG_FILE}"
        exec > >(tee -a "${LOG_FILE}") 2>&1
    fi
fi

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'Run id: %s\n' "${RUN_ID}"
printf 'Report file: %s\n' "${REPORT_FILE}"
if [[ ${WRITE_LOG} -eq 1 ]]; then
    printf 'Log file: %s\n' "${LOG_FILE}"
fi
if [[ ${WRITE_DONE_MARKER} -eq 1 ]]; then
    printf 'Done marker: %s\n' "${DONE_FILE}"
fi

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
    run_with_heartbeat "pytest full suite" "${PYTEST_CMD[@]}"
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
    run_with_heartbeat "Tkinter smoke test" "${TK_PYTEST_CMD[@]}"
fi
