#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
RESULTS_DIR="${REPO_ROOT}/working-memory/test-results"
TIMESTAMP="$(date +%F-%H%M%S)"
RUN_ID="live-web-sources-${TIMESTAMP}"
LOG_FILE=""
DONE_FILE=""
PYTHON_BIN="${VENV_PYTHON}"
TRACEBACK_MODE="long"
CAPTURE_MODE="tee-sys"
DURATIONS="20"
SHOW_LOCALS=1
VERBOSITY="-vv"
DRY_RUN=0
ORIGINAL_ARGS=("$@")
PYTEST_ARGS=()

usage() {
    cat <<'EOF'
Usage: scripts/run_live_web_sources.sh [options] [-- <extra pytest args>]

Runs the gated live metadata web-source backend tests outside Codex sandboxing.
These tests make real outbound network requests and are expected to be
backend/rate-limit sensitive.

Options:
  --run-id <name>          Artifact run id (default: live-web-sources-<timestamp>)
  --python <path>          Python interpreter to use (default: .venv/bin/python)
  --results-dir <path>     Directory for log and done output
  --log-file <path>        Exact log file path to write
  --done-file <path>       Exact exit marker path to write
  --tb <mode>              Pytest traceback mode (default: long)
  --capture <mode>         Pytest capture mode (default: tee-sys)
  --durations <n>          Show n slowest tests (default: 20; use 0 for all)
  --no-showlocals          Do not include local variables in tracebacks
  --quiet                  Use concise pytest output (-q --tb=short)
  --dry-run                Print the command without executing it
  -h, --help               Show this help

Examples:
  scripts/run_live_web_sources.sh
  scripts/run_live_web_sources.sh --quiet
  scripts/run_live_web_sources.sh --tb=native --durations=0
  scripts/run_live_web_sources.sh -- -k 'google or openlibrary'
EOF
}

print_cmd() {
    printf '%q ' "$@"
    printf '\n'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-id)
            RUN_ID="$2"
            shift 2
            ;;
        --python)
            PYTHON_BIN="$2"
            shift 2
            ;;
        --results-dir)
            RESULTS_DIR="$2"
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
        --tb)
            TRACEBACK_MODE="$2"
            shift 2
            ;;
        --tb=*)
            TRACEBACK_MODE="${1#--tb=}"
            shift
            ;;
        --capture)
            CAPTURE_MODE="$2"
            shift 2
            ;;
        --capture=*)
            CAPTURE_MODE="${1#--capture=}"
            shift
            ;;
        --durations)
            DURATIONS="$2"
            shift 2
            ;;
        --durations=*)
            DURATIONS="${1#--durations=}"
            shift
            ;;
        --no-showlocals)
            SHOW_LOCALS=0
            shift
            ;;
        --quiet)
            VERBOSITY="-q"
            TRACEBACK_MODE="short"
            SHOW_LOCALS=0
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

LOG_FILE="${LOG_FILE:-${RESULTS_DIR}/${RUN_ID}.log}"
DONE_FILE="${DONE_FILE:-${RESULTS_DIR}/${RUN_ID}.done}"

PYTEST_CMD=(
    "${PYTHON_BIN}" -m pytest
    tests/metadata/web_sources/test_web_sources_live_backends.py
    "${VERBOSITY}"
    --color=yes
    -ra
    "--tb=${TRACEBACK_MODE}"
    "--capture=${CAPTURE_MODE}"
    "--durations=${DURATIONS}"
)

if [[ ${SHOW_LOCALS} -eq 1 ]]; then
    PYTEST_CMD+=(--showlocals)
fi

if [[ ${#PYTEST_ARGS[@]} -gt 0 ]]; then
    PYTEST_CMD+=("${PYTEST_ARGS[@]}")
fi

STARTED_AT="$(date -Is)"
INVOCATION=("$0" "${ORIGINAL_ARGS[@]}")

write_done_marker() {
    local status=$?
    trap - EXIT
    if [[ ${DRY_RUN} -eq 0 ]]; then
        {
            printf 'run_id: %s\n' "${RUN_ID}"
            printf 'repo_root: %s\n' "${REPO_ROOT}"
            printf 'script: %s\n' "$0"
            printf 'started_at: %s\n' "${STARTED_AT}"
            printf 'finished_at: %s\n' "$(date -Is)"
            printf 'exit_code: %s\n' "${status}"
            printf 'log_file: %s\n' "${LOG_FILE}"
            printf 'done_file: %s\n' "${DONE_FILE}"
            printf 'live_web_flag: LIUXIN_RUN_LIVE_WEB_TESTS=1\n'
            printf 'invocation: '
            print_cmd "${INVOCATION[@]}"
            printf 'test_step: '
            print_cmd "${PYTEST_CMD[@]}"
        } > "${DONE_FILE}"
    fi
    exit "${status}"
}

if [[ ${DRY_RUN} -eq 0 ]]; then
    cd "${REPO_ROOT}"
    mkdir -p "$(dirname -- "${LOG_FILE}")" "$(dirname -- "${DONE_FILE}")"
    trap write_done_marker EXIT
    : > "${LOG_FILE}"
    exec > >(tee -a "${LOG_FILE}") 2>&1
fi

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'Run id: %s\n' "${RUN_ID}"
printf 'Log file: %s\n' "${LOG_FILE}"
printf 'Done marker: %s\n' "${DONE_FILE}"
printf 'Live web flag: LIUXIN_RUN_LIVE_WEB_TESTS=1\n'
printf 'Traceback mode: %s\n' "${TRACEBACK_MODE}"
printf 'Capture mode: %s\n' "${CAPTURE_MODE}"
printf 'Durations: %s\n' "${DURATIONS}"
printf 'Show locals: %s\n' "${SHOW_LOCALS}"
printf 'Test step: '
print_cmd "${PYTEST_CMD[@]}"

if [[ ${DRY_RUN} -eq 1 ]]; then
    exit 0
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "Expected Python interpreter at ${PYTHON_BIN}. Create the repo-local .venv first or pass --python." >&2
    exit 1
fi

set +e
LIUXIN_RUN_LIVE_WEB_TESTS=1 "${PYTEST_CMD[@]}"
STATUS=$?
set -e

printf 'Pytest exit code: %s\n' "${STATUS}"
printf 'Log file: %s\n' "${LOG_FILE}"
exit "${STATUS}"
