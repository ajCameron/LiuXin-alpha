# Metadata Malformed Input Fuzzing - 2026-05-16

Branch: `metadata-malformed-input-fuzzing`

## Goal

Add deterministic malformed-input and wrong-format coverage for metadata file
sources. Individual extractors should reject non-credible inputs deliberately;
a later central best-effort metadata facade can own sniffing, fallback routing,
and "try another extractor" behavior.

Durable docs:

- `docs/development/malformed-input-fuzzing.md`

## Registry Slice

Added a dedicated metadata reader registry facade at
`LiuXin_alpha.metadata.file_sources.registry`.

Shape:

- builtin readers remain declared in
  `LiuXin_alpha.customize.builtins.metadata_readers`
- runtime/plugin readers can call `register_metadata_reader_plugin`
- dispatcher compatibility globals still exist, but refresh when the registry
  revision changes
- registry entries expose normalized file types so corpus tests can enumerate
  readers without depending on legacy globals

Focused validation:

```bash
python3 -m py_compile \
  src/LiuXin_alpha/metadata/file_sources/registry.py \
  src/LiuXin_alpha/metadata/file_sources/__init__.py \
  tests/metadata/file_sources/test_metadata_reader_registry.py

python3 -m pytest \
  tests/metadata/file_sources/test_metadata_reader_registry.py \
  tests/metadata/file_sources/test_dispatcher_modernized.py \
  tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py::test_dispatcher_plugin_adapter_and_failure_edges \
  -q
```

Result: `18 passed`.
