#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
RESULTS_DIR="${REPO_ROOT}/working-memory/test-results"
TIMESTAMP="$(date +%F-%H%M%S)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CREATE_VENV=0
RECREATE_VENV=0
SKIP_INSTALL=0
DRY_RUN=0
RUNNER_ARGS=()
PYTEST_ARGS=()

usage() {
    cat <<'EOF'
Usage: scripts/run_full_test_suite_cov.sh [options] [-- <extra pytest args>]

Run the full test suite with pytest-cov installed in the repo-local venv and
coverage flags baked in.

Options mirror scripts/run_full_test_suite.sh:
  --workers <n|auto>       Pytest xdist worker count (default: auto)
  --dist <mode>            Pytest xdist distribution mode (default: worksteal)
  --results-dir <path>     Directory for JSON and coverage report output
  --report-file <path>     Exact JSON report file path to write
  --create-venv            Create/reuse .venv via scripts/create_venv.sh before testing
  --new-venv               Recreate .venv via scripts/create_venv.sh before testing
  --python <path>          Python interpreter for --create-venv/--new-venv
  --skip-install           Skip the normal editable install step
  --dry-run                Print commands without executing them
  --tk-smoke               Run the real Tkinter GUI smoke test after the main suite
  --only-tk-smoke          Run only the real Tkinter GUI smoke test
  -h, --help               Show this help

Coverage output:
  terminal summary         term-missing:skip-covered
  HTML report              <results-dir>/coverage-html-<timestamp>
  XML report               <results-dir>/coverage-<timestamp>.xml

Examples:
  scripts/run_full_test_suite_cov.sh
  scripts/run_full_test_suite_cov.sh --new-venv --python python3.12
  scripts/run_full_test_suite_cov.sh --workers 8 -- --maxfail=1
EOF
}

print_cmd() {
    printf '%q ' "$@"
    printf '\n'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers|--dist|--results-dir|--report-file)
            if [[ $# -lt 2 ]]; then
                echo "Missing value for $1" >&2
                usage >&2
                exit 2
            fi
            if [[ "$1" == "--results-dir" ]]; then
                RESULTS_DIR="$2"
            elif [[ "$1" == "--report-file" ]]; then
                RESULTS_DIR="$(dirname -- "$2")"
            fi
            RUNNER_ARGS+=("$1" "$2")
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
        --tk-smoke|--only-tk-smoke)
            RUNNER_ARGS+=("$1")
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
COV_INSTALL_CMD=("${VENV_PYTHON}" -m pip install pytest-cov)
COVERAGE_HTML_DIR="${RESULTS_DIR}/coverage-html-${TIMESTAMP}"
COVERAGE_XML_FILE="${RESULTS_DIR}/coverage-${TIMESTAMP}.xml"
COVERAGE_DATA_FILE="${RESULTS_DIR}/.coverage-full-suite-${TIMESTAMP}"
COVERAGE_ARGS=(
    "--cov=src/LiuXin_alpha"
    "--cov-branch"
    "--cov-report=term-missing:skip-covered"
    "--cov-report=html:${COVERAGE_HTML_DIR}"
    "--cov-report=xml:${COVERAGE_XML_FILE}"
)
RUNNER_CMD=(
    "bash" "${SCRIPT_DIR}/run_full_test_suite.sh"
    "${RUNNER_ARGS[@]}"
    "--skip-install"
    "--"
    "${COVERAGE_ARGS[@]}"
    "${PYTEST_ARGS[@]}"
)

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'Coverage data: %s\n' "${COVERAGE_DATA_FILE}"
printf 'Coverage HTML: %s\n' "${COVERAGE_HTML_DIR}"
printf 'Coverage XML: %s\n' "${COVERAGE_XML_FILE}"

if [[ ${CREATE_VENV} -eq 1 ]]; then
    printf 'Venv step: '
    print_cmd "${CREATE_VENV_CMD[@]}"
fi

if [[ ${CREATE_VENV} -eq 0 && ${SKIP_INSTALL} -eq 0 ]]; then
    printf 'Install step: '
    print_cmd "${PIP_UPGRADE_CMD[@]}"
    printf 'Install step: '
    print_cmd "${PIP_INSTALL_CMD[@]}"
fi

printf 'Coverage install step: '
print_cmd "${COV_INSTALL_CMD[@]}"
printf 'Test step: '
print_cmd "${RUNNER_CMD[@]}"

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
    "${PIP_UPGRADE_CMD[@]}"
    "${PIP_INSTALL_CMD[@]}"
fi

"${COV_INSTALL_CMD[@]}"
COVERAGE_FILE="${COVERAGE_DATA_FILE}"
export COVERAGE_FILE
"${RUNNER_CMD[@]}"
