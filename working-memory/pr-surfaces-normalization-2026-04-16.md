# PR Draft - Surfaces Normalization

## Title

Normalize `interfaces` to `surfaces` across runtime packages, tests, scripts, and docs

## Summary

## What changed

- moved the top-level runtime surface packages from the old `interfaces` tree
  into `src/LiuXin_alpha/surfaces`
- moved the corresponding tests into `tests/surfaces`
- updated runtime imports, script names, launcher descriptions, and benchmark
  wiring to use `surfaces`
- removed the temporary import-compat helper and switched remaining callers onto
  the current storage API (`store_bytes`, `locate_file`, `delete_location`,
  `iter_locations`)
- normalized working-memory/docs references and renamed the remaining
  `*-interface-*` note files

## Validation

- targeted moved-surface slice:
  - `PYTHONPATH=src PYTHONPYCACHEPREFIX=/tmp/liuxin_mainline_pycache /home/blackjane/LiuXin-alpha-wsl/.venv/bin/python -B -m pytest -p no:cacheprovider tests/surfaces/test_api_readonly.py tests/surfaces/test_catalog_api.py tests/surfaces/test_opds_readonly.py tests/surfaces/test_read_model_api.py tests/surfaces/test_web_readonly.py tests/surfaces/test_web_calibre_readonly.py tests/surfaces/test_images_api.py tests/surfaces/test_cli_squashfs.py -q`
  - result: `32 passed, 3 skipped`
- skipped provenance tests are still gated on legacy `file_derivations`; the
  current FRBR schema in this checkout exposes `digital_asset_derivations`

## Follow-Ups

- `file_formats` still expects `LiuXin_alpha.surfaces.gui2`, but there is no
  `src/LiuXin_alpha/surfaces/gui2/` package in this checkout
- SquashFS provenance still targets legacy `file_derivations` and should be
  ported to `digital_asset_derivations`
