#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"

DATABASE_PATH=""
DB_TYPE="sqlite"
METADATA_READ_SOURCE="database"
CACHE_TYPE="schema_backed"
NO_CACHE_DB_FALLBACK=0
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
  --metadata-read-source <database|cache>
                           Read metadata from the live database or cache (default: database)
  --cache-type <type>      Storage cache backend for --metadata-read-source cache (default: schema_backed)
  --no-cache-db-fallback   Do not fall back to live database reads for cache metadata reads
  --host <host>            Bind host (default: 127.0.0.1)
  --port <port>            Bind port (default: 8083)
  --title <text>           Service title (default: LiuXin API Read-Only)
  --no-file-downloads      Disable file download / redirect links
  -h, --help               Show this help

Examples:
  scripts/run_api_readonly.sh --database /home/blackjane/scratch_library.sqlite
  scripts/run_api_readonly.sh --database /home/blackjane/scratch_library.sqlite --metadata-read-source cache
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
        --metadata-read-source)
            METADATA_READ_SOURCE="$2"
            shift 2
            ;;
        --cache-type)
            CACHE_TYPE="$2"
            shift 2
            ;;
        --no-cache-db-fallback)
            NO_CACHE_DB_FALLBACK=1
            shift
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
    "${VENV_PYTHON}" -m LiuXin_alpha.surfaces.api_readonly
    --database "${DATABASE_PATH}"
    --db-type "${DB_TYPE}"
    --host "${HOST}"
    --port "${PORT}"
    --title "${TITLE}"
)

if [[ "${METADATA_READ_SOURCE}" != "database" ]]; then
    API_CMD+=(--metadata-read-source "${METADATA_READ_SOURCE}")
fi

if [[ "${CACHE_TYPE}" != "schema_backed" ]]; then
    API_CMD+=(--cache-type "${CACHE_TYPE}")
fi

if [[ ${NO_CACHE_DB_FALLBACK} -eq 1 ]]; then
    API_CMD+=(--no-cache-db-fallback)
fi

if [[ ${NO_FILE_DOWNLOADS} -eq 1 ]]; then
    API_CMD+=(--no-file-downloads)
fi

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'API step: '
print_cmd "${API_CMD[@]}"

cd "${REPO_ROOT}"
PYTHONPATH=src "${API_CMD[@]}"
