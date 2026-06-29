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
PORT="8080"
TITLE="LiuXin OPDS Read-Only"
PAGE_SIZE="25"
MAX_PAGE_SIZE="200"
OPDS_MAX_UNGROUPED_ITEMS="100"
NO_FILE_DOWNLOADS=0

usage() {
    cat <<'USAGE'
Usage: scripts/run_opds_readonly.sh --database <path> [options]

Options:
  --database <path>             Database path to open (required)
  --db-type <type>              Database driver type (default: sqlite)
  --metadata-read-source <database|cache>
                                Read metadata from the live database or cache (default: database)
  --cache-type <type>           Storage cache backend for --metadata-read-source cache (default: schema_backed)
  --no-cache-db-fallback        Do not fall back to live database reads for cache metadata reads
  --host <host>                 Bind host (default: 127.0.0.1)
  --port <port>                 Bind port (default: 8080)
  --title <text>                Service title (default: LiuXin OPDS Read-Only)
  --page-size <n>               Default page size (default: 25)
  --max-page-size <n>           Maximum page size (default: 200)
  --opds-max-ungrouped-items <n> Maximum OPDS category size before grouping (default: 100)
  --no-file-downloads           Disable file download / redirect links
  -h, --help                    Show this help

Examples:
  scripts/run_opds_readonly.sh --database /home/blackjane/scratch_library.sqlite --port 8082
  scripts/run_opds_readonly.sh --database /home/blackjane/scratch_library.sqlite --host 0.0.0.0 --port 8082
  scripts/run_opds_readonly.sh --database /home/blackjane/scratch_library.sqlite --metadata-read-source cache --port 8082
USAGE
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
        --page-size)
            PAGE_SIZE="$2"
            shift 2
            ;;
        --max-page-size)
            MAX_PAGE_SIZE="$2"
            shift 2
            ;;
        --opds-max-ungrouped-items)
            OPDS_MAX_UNGROUPED_ITEMS="$2"
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

WEB_CMD=(
    "${VENV_PYTHON}" -m LiuXin_alpha.surfaces.opds_readonly
    --database "${DATABASE_PATH}"
    --db-type "${DB_TYPE}"
    --host "${HOST}"
    --port "${PORT}"
    --title "${TITLE}"
    --page-size "${PAGE_SIZE}"
    --max-page-size "${MAX_PAGE_SIZE}"
    --opds-max-ungrouped-items "${OPDS_MAX_UNGROUPED_ITEMS}"
)

if [[ "${METADATA_READ_SOURCE}" != "database" ]]; then
    WEB_CMD+=(--metadata-read-source "${METADATA_READ_SOURCE}")
fi

if [[ "${CACHE_TYPE}" != "schema_backed" ]]; then
    WEB_CMD+=(--cache-type "${CACHE_TYPE}")
fi

if [[ ${NO_CACHE_DB_FALLBACK} -eq 1 ]]; then
    WEB_CMD+=(--no-cache-db-fallback)
fi

if [[ ${NO_FILE_DOWNLOADS} -eq 1 ]]; then
    WEB_CMD+=(--no-file-downloads)
fi

printf 'Repo root: %s\n' "${REPO_ROOT}"
printf 'OPDS step: '
print_cmd "${WEB_CMD[@]}"

cd "${REPO_ROOT}"
PYTHONPATH=src "${WEB_CMD[@]}"
