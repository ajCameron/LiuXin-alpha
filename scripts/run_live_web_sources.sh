#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
RESULTS_DIR="${REPO_ROOT}/working-memory/test-results"
TIMESTAMP="$(date +%F-%H%M%S)"
LOG_FILE="${RESULTS_DIR}/live-web-sources-${TIMESTAMP}.log"
PYTHON_BIN="${VENV_PYTHON}"
TRACEBACK_MODE="long"
CAPTURE_MODE="tee-sys"
DURATIONS="20"
SHOW_LOCALS=1
VERBOSITY="-vv"
DRY_RUN=0
PYTEST_ARGS=()

usage() {
    cat <<'EOF'
Usage: scripts/run_live_web_sources.sh [options] [-- <extra pytest args>]

Runs the gated live metadata web-source backend tests outside Codex sandboxing.
These tests make real outbound network requests and are expected to be
backend/rate-limit sensitive.

Options:
  --python <path>          Python interpreter to use (default: .venv/bin/python)
  --results-dir <path>     Directory for the timestamped log file
  --log-file <path>        Exact log file path to write
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
        --python)
            PYTHON_BIN="$2"
            shift 2
            ;;
        --results-dir)
            RESULTS_DIR="$2"
            LOG_FILE="${RESULTS_DIR}/live-web-sources-${TIMESTAMP}.log"
            shift 2
            ;;
        --log-file)
            LOG_FILE="$2"
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

PYTEST_CMD=(
    "${PYTHON_BIN}" -m pytest
    tests/metadata/web_sources/test_web_sources_live_backends.py
    "${VERBOSITY}"
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

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'Log file: %s\n' "${LOG_FILE}"
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

cd "${REPO_ROOT}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "Expected Python interpreter at ${PYTHON_BIN}. Create the repo-local .venv first or pass --python." >&2
    exit 1
fi

mkdir -p "$(dirname -- "${LOG_FILE}")"

set +e
LIUXIN_RUN_LIVE_WEB_TESTS=1 "${PYTEST_CMD[@]}" 2>&1 | tee "${LOG_FILE}"
STATUS=${PIPESTATUS[0]}
set -e

printf 'Pytest exit code: %s\n' "${STATUS}"
printf 'Log file: %s\n' "${LOG_FILE}"
exit "${STATUS}"
