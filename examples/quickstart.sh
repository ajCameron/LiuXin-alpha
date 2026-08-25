#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-python3}"
TMP_ROOT="${TMPDIR:-/tmp}"
WORK_DIR="$(mktemp -d "${TMP_ROOT%/}/liuxin-alpha-examples-XXXXXX")"

cleanup() {
  if [[ "${KEEP_EXAMPLE_WORKDIR:-0}" == "1" ]]; then
    echo "[quickstart] Keeping work directory: ${WORK_DIR}"
    return
  fi
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

DB_PATH="${WORK_DIR}/demo.sqlite"
MANAGED_ROOT="${WORK_DIR}/managed_store"
MANUAL_ROOT="${WORK_DIR}/manual_store"
DRIVER_ROOT="${WORK_DIR}/filesystem_driver"
DRIVER_DB="${WORK_DIR}/driver_objects.sqlite"
UNMANAGED_ROOT="${WORK_DIR}/unmanaged_disk"
ASSIMILATED_ROOT="${WORK_DIR}/assimilated_store"

mkdir -p "${MANAGED_ROOT}" "${MANUAL_ROOT}" "${UNMANAGED_ROOT}/nested"
printf 'epub payload\n' > "${UNMANAGED_ROOT}/book_one.epub"
printf 'mobi payload\n' > "${UNMANAGED_ROOT}/nested/book_two.mobi"
printf 'plain text payload\n' > "${UNMANAGED_ROOT}/notes.txt"
printf 'not an ebook\n' > "${UNMANAGED_ROOT}/cover.jpg"

cd "${REPO_ROOT}"

echo "[quickstart] Repo root: ${REPO_ROOT}"
echo "[quickstart] Working dir: ${WORK_DIR}"
echo

echo "[1/11] Library facade round-trip"
"${PYTHON_BIN}" examples/library/library_facade_example.py \
  --database "${DB_PATH}" \
  --store-root "${MANAGED_ROOT}" \
  --create-db \
  --payload "hello from quickstart"
echo

echo "[2/11] Manual StorageManager round-trip"
"${PYTHON_BIN}" examples/storage/storage_manager_manual_roundtrip_example.py \
  --store-root "${MANUAL_ROOT}" \
  --payload "manual storage manager payload"
echo

echo "[3/11] Filesystem driver write-session round-trip"
"${PYTHON_BIN}" examples/storage/filesystem_driver_example.py \
  --store-root "${DRIVER_ROOT}" \
  --payload "filesystem driver payload"
echo

echo "[4/11] SQLite driver write-session round-trip"
"${PYTHON_BIN}" examples/storage/sqlite_driver_example.py \
  --database "${DRIVER_DB}" \
  --payload "SQLite driver payload"
echo

echo "[5/11] Assimilate selected files from an existing disk"
"${PYTHON_BIN}" examples/storage/assimilate_existing_disk_example.py \
  --source-root "${UNMANAGED_ROOT}" \
  --destination-root "${ASSIMILATED_ROOT}" \
  --extension epub \
  --extension mobi
echo

echo "[6/11] Register unmanaged disk via Library"
"${PYTHON_BIN}" examples/storage/library_register_unmanaged_disk_example.py \
  --database "${DB_PATH}" \
  --disk-root "${UNMANAGED_ROOT}" \
  --store-name "quickstart_unmanaged"
echo

echo "[7/11] Storage bootstrap report"
"${PYTHON_BIN}" examples/storage/storage_bootstrap_report_example.py \
  --database "${DB_PATH}"
echo

echo "[8/11] Reconcile helper with database path"
"${PYTHON_BIN}" examples/storage/reconcile_with_database_path_example.py \
  --database "${DB_PATH}" \
  --disk-root "${UNMANAGED_ROOT}" \
  --store-name "quickstart_unmanaged_via_path"
echo

echo "[9/11] comments_to_html utility"
"${PYTHON_BIN}" examples/utilities/comments_to_html_example.py \
  --text $'Line one\nLine two\n\nSecond paragraph'
echo

echo "[10/11] Conversion OEB -> EPUB"
"${PYTHON_BIN}" examples/conversion/conversion_oeb_to_epub_example.py \
  --output "${WORK_DIR}/example_from_oeb.epub"
echo

echo "[11/11] Conversion OEB -> MOBI"
"${PYTHON_BIN}" examples/conversion/conversion_oeb_to_mobi_example.py \
  --output "${WORK_DIR}/example_from_oeb.mobi"
echo

echo "[quickstart] Completed successfully."
echo "[quickstart] Database used: ${DB_PATH}"
