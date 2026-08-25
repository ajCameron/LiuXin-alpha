#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
TMP_ROOT="${TMPDIR:-/tmp}"
WORK_DIR="$(mktemp -d "${TMP_ROOT%/}/liuxin-catalog-examples-XXXXXX")"

cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

cd "${REPO_ROOT}"

examples=(
  catalog_metadata_bundle_example.py
  catalog_matching_example.py
  catalog_mutations_example.py
  catalog_writers_example.py
)

template_database="${WORK_DIR}/catalog-template.sqlite"
echo "[catalog] Running catalog_crud_example.py and creating the shared schema"
"${PYTHON_BIN}" examples/catalog/catalog_crud_example.py --database "${template_database}"
echo

export LIUXIN_CATALOG_EXAMPLE_TEMPLATE="${template_database}"
for example in "${examples[@]}"; do
  echo "[catalog] Running ${example}"
  "${PYTHON_BIN}" "examples/catalog/${example}"
  echo
done

echo "[catalog] All catalog examples completed successfully."
