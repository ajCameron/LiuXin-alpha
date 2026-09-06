#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CREATE_VENV=0
RECREATE_VENV=0
INSTALL=0
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
  --install               Install/update the typing extra before checking
  --skip-install          Compatibility alias for the offline default
  --dry-run               Print commands without executing them
  -h, --help              Show this help

Extra tool args after "--" are allowed only when a single checker is selected.

Examples:
  scripts/run_type_checks.sh
  scripts/run_type_checks.sh --create-venv
  scripts/run_type_checks.sh --install
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
            INSTALL=0
            shift
            ;;
        --install)
            INSTALL=1
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
RUFF_CMD=(
    "${VENV_DIR}/bin/ruff"
    "check"
    "src/LiuXin_alpha/surfaces/presentation.py"
    "src/LiuXin_alpha/surfaces/acquisition_types.py"
    "tests/surfaces/test_shared_surface_dependencies.py"
    "tests/surfaces/test_read_model_failure_contracts.py"
    "tests/surfaces/test_surface_read_errors.py"
    "tests/surfaces/test_read_model_transport_errors.py"
    "tests/databases/caches/test_writer_dependencies.py"
    "src/LiuXin_alpha/core/commands.py"
    "src/LiuXin_alpha/core/queries.py"
    "src/LiuXin_alpha/core/program_endpoints"
    "src/LiuXin_alpha/core/program_api.py"
    "src/LiuXin_alpha/core/program_services"
    "src/LiuXin_alpha/surfaces/cli/storage.py"
    "src/LiuXin_alpha/surfaces/cli/storage_commands"
    "src/LiuXin_alpha/ingest/mixed_application.py"
    "src/LiuXin_alpha/catalog/write/host_api.py"
    "src/LiuXin_alpha/catalog/api/metadata_tools_api/facades.py"
    "src/LiuXin_alpha/storage/storage_manager/manager.py"
    "src/LiuXin_alpha/storage/storage_manager/mixins"
    "scripts/check_modern_import_cycles.py"
    "scripts/check_internal_type_contracts.py"
    "tests/scripts/test_internal_type_contracts.py"
    "tests/scripts/test_workflow_ownership.py"
    "tests/core/test_evacuation_workflow.py"
    "tests/core/test_program_workflow_facade.py"
    "tests/scripts/test_check_modern_import_cycles.py"
    "tests/scripts/test_public_documentation_boundaries.py"
    "tests/scripts/test_run_type_checks.py"
    "tests/storage/api/test_storage_manager_composition.py"
    "tests/storage/api/test_storage_manager_docstrings.py"
)
FILE_FORMAT_ANNOTATION_CMD=(
    "${VENV_PYTHON}"
    "${REPO_ROOT}/scripts/annotate_file_formats.py"
    "--check"
)
IMPORT_CYCLE_CMD=(
    "${VENV_PYTHON}"
    "${REPO_ROOT}/scripts/check_modern_import_cycles.py"
)
INTERNAL_CONTRACT_CMD=(
    "${VENV_PYTHON}"
    "${REPO_ROOT}/scripts/check_internal_type_contracts.py"
)
MODERN_COMPLEXITY_CMD=(
    "${VENV_DIR}/bin/ruff"
    "check"
    "--select"
    "C901"
    "--config"
    "lint.mccabe.max-complexity=10"
    "src/LiuXin_alpha/surfaces/presentation.py"
    "src/LiuXin_alpha/surfaces/acquisition_types.py"
    "src/LiuXin_alpha/core/program_endpoints"
    "src/LiuXin_alpha/core/program_api.py"
    "src/LiuXin_alpha/core/program_services"
    "src/LiuXin_alpha/ingest/mixed_application.py"
    "src/LiuXin_alpha/catalog/write/host_api.py"
    "src/LiuXin_alpha/catalog/api/metadata_tools_api/facades.py"
    "src/LiuXin_alpha/surfaces/cli/storage.py"
    "src/LiuXin_alpha/surfaces/cli/storage_commands"
    "scripts/check_modern_import_cycles.py"
)
STORAGE_MANAGER_COMPLEXITY_CMD=(
    "${VENV_DIR}/bin/ruff"
    "check"
    "--select"
    "C901"
    "--config"
    "lint.mccabe.max-complexity=15"
    "src/LiuXin_alpha/storage/storage_manager/manager.py"
    "src/LiuXin_alpha/storage/storage_manager/mixins"
)

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

if [[ ${CREATE_VENV} -eq 0 && ${INSTALL} -eq 1 ]]; then
    printf 'Install step: '
    print_cmd "${PIP_INSTALL_CMD[@]}"
fi

if [[ ${RUN_BASEDPYRIGHT} -eq 1 ]]; then
    printf 'basedpyright step: '
    print_cmd "${BASEDPYRIGHT_CMD[@]}"
    printf 'internal basedpyright contract step: '
    print_cmd "${INTERNAL_CONTRACT_CMD[@]}" --checker basedpyright
fi

if [[ ${RUN_MYPY} -eq 1 ]]; then
    printf 'mypy step: '
    print_cmd "${MYPY_CMD[@]}"
    printf 'internal mypy contract step: '
    print_cmd "${INTERNAL_CONTRACT_CMD[@]}" --checker mypy
fi
printf 'modern lint step: '
print_cmd "${RUFF_CMD[@]}"
printf 'file_formats annotation step: '
print_cmd "${FILE_FORMAT_ANNOTATION_CMD[@]}"
printf 'modern import-cycle step: '
print_cmd "${IMPORT_CYCLE_CMD[@]}"
printf 'modern complexity step: '
print_cmd "${MODERN_COMPLEXITY_CMD[@]}"
printf 'storage-manager complexity step: '
print_cmd "${STORAGE_MANAGER_COMPLEXITY_CMD[@]}"

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

if [[ ${CREATE_VENV} -eq 0 && ${INSTALL} -eq 1 ]]; then
    "${PIP_INSTALL_CMD[@]}"
fi

if [[ ${RUN_BASEDPYRIGHT} -eq 1 && ! -x "${VENV_DIR}/bin/basedpyright" ]]; then
    echo "Expected basedpyright at ${VENV_DIR}/bin/basedpyright. Re-run with --install or create the typing venv." >&2
    exit 1
fi

if [[ ${RUN_MYPY} -eq 1 && ! -x "${VENV_DIR}/bin/mypy" ]]; then
    echo "Expected mypy at ${VENV_DIR}/bin/mypy. Re-run with --install or create the typing venv." >&2
    exit 1
fi

if [[ ! -x "${VENV_DIR}/bin/ruff" ]]; then
    echo "Expected ruff at ${VENV_DIR}/bin/ruff. Re-run with --install or create the typing venv." >&2
    exit 1
fi

STATUS=0

"${FILE_FORMAT_ANNOTATION_CMD[@]}"
"${IMPORT_CYCLE_CMD[@]}"
"${RUFF_CMD[@]}"
"${MODERN_COMPLEXITY_CMD[@]}"
"${STORAGE_MANAGER_COMPLEXITY_CMD[@]}"

if [[ ${RUN_BASEDPYRIGHT} -eq 1 ]]; then
    set +e
    "${BASEDPYRIGHT_CMD[@]}"
    CHECK_STATUS=$?
    if [[ ${CHECK_STATUS} -eq 0 ]]; then
        "${INTERNAL_CONTRACT_CMD[@]}" --checker basedpyright
        CHECK_STATUS=$?
    fi
    set -e
    if [[ ${CHECK_STATUS} -ne 0 ]]; then
        STATUS=${CHECK_STATUS}
    fi
fi

if [[ ${RUN_MYPY} -eq 1 ]]; then
    set +e
    "${MYPY_CMD[@]}"
    CHECK_STATUS=$?
    if [[ ${CHECK_STATUS} -eq 0 ]]; then
        "${INTERNAL_CONTRACT_CMD[@]}" --checker mypy
        CHECK_STATUS=$?
    fi
    set -e
    if [[ ${CHECK_STATUS} -ne 0 && ${STATUS} -eq 0 ]]; then
        STATUS=${CHECK_STATUS}
    fi
fi

exit "${STATUS}"
