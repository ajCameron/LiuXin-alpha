# Custom Column Field Matrix - 2026-03-16

Context:
- Batch A from the cache/emulation rewrite checklist targets the old custom-column field semantics.
- The legacy tree encoded that behavior as many tiny datatype-specific unittest files.
- Alpha already had the same behavior spread across schema/bootstrap/emulation tests, but the value round-trip coverage was still fragmented.

What changed:
- Rewrote [test_calibre_emulation_d2_custom_values.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py) into a compact parameterized matrix on the live Calibre-emulation seam.
- The new matrix covers single-value custom columns for:
  - `text`
  - `bool`
  - `int`
  - `float`
  - `rating`
  - `enumeration`
  - `comments`
  - `composite`
- It also keeps explicit semantic tests for:
  - multi-value text order-preserving dedupe
  - series custom columns across multiple accepted input shapes
  - datetime normalization from epoch and `Z`-suffixed ISO strings
- Assertions now check both:
  - `CalibreReader.read_custom_values(...)`
  - `iter_book_payloads(..., include_custom_values=True)`

Why this is the right seam:
- It stays on the active default suite.
- It avoids adding more opt-in legacy cache tests gated behind `LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS`.
- It matches the actual product behavior we still care about: Calibre-library custom-column round-trips through the emulation reader/payload path.

Validation:
- `PYTHONPATH=src:. .venv/bin/python -m py_compile tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py`
- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_d1_custom_columns_introspection.py tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py`
  - `17 passed`

Implication for Batch A:
- The first rewrite slice is now implemented.
- The next useful follow-on inside Batch A is not more scalar cases; it is the cache-side semantics that still matter for:
  - category visibility
  - update/failure behavior
  - any remaining custom-column field/table mapping not already pinned by the existing bootstrap tests.
