#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"

DATABASE_PATH=""
DB_TYPE="sqlite"
HOST="127.0.0.1"
PORT="8083"
TITLE="LiuXin API Read-Only"
NO_FILE_DOWNLOADS=0

usage() {
    cat <<'EOU'
Usage: scripts/run_api_readonly.sh --database <path> [options]

Options:
  --database <path>        Database path to open (required)
  --db-type <type>         Database driver type (default: sqlite)
  --host <host>            Bind host (default: 127.0.0.1)
  --port <port>            Bind port (default: 8083)
  --title <text>           Service title (default: LiuXin API Read-Only)
  --no-file-downloads      Disable file download / redirect links
  -h, --help               Show this help
EOU
}

print_cmd() {
    printf '%q ' "$@"
    printf '\n'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --database)
            DATABASE_PATH="$2"
            shift 2
            ;;
        --db-type)
            DB_TYPE="$2"
            shift 2
            ;;
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --title)
            TITLE="$2"
            shift 2
            ;;
        --no-file-downloads)
            NO_FILE_DOWNLOADS=1
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

if [[ -z "${DATABASE_PATH}" ]]; then
    echo "Missing required --database argument." >&2
    usage >&2
    exit 2
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "Expected venv interpreter at ${VENV_PYTHON}. Create the repo-local .venv first." >&2
    exit 1
fi

API_CMD=(
    "${VENV_PYTHON}" -m LiuXin_alpha.interfaces.api_readonly
    --database "${DATABASE_PATH}"
    --db-type "${DB_TYPE}"
    --host "${HOST}"
    --port "${PORT}"
    --title "${TITLE}"
)

if [[ ${NO_FILE_DOWNLOADS} -eq 1 ]]; then
    API_CMD+=(--no-file-downloads)
fi

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'API step: '
print_cmd "${API_CMD[@]}"

cd "${REPO_ROOT}"
PYTHONPATH=src "${API_CMD[@]}"
