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

## First Fuzz Lane

Added deterministic malformed/wrong-format payload tests for strict container
readers:

- EPUB
- DOCX
- ZIP

Corpus:

- empty bytes
- tiny binary bytes
- PNG header bytes
- HTML document bytes
- empty ZIP central-directory bytes

The first checked-in lane asserts that strict container readers raise the
dispatcher-level `RuntimeError` with an underlying format cause instead of
returning conservative fallback metadata for arbitrary bytes. The survey pass
also showed that several legacy/text-like readers still intentionally return
fallback metadata for junk inputs; keep those as a later policy pass rather than
flipping every reader at once.

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  tests/metadata/file_sources/test_metadata_reader_registry.py \
  tests/metadata/file_sources/test_dispatcher_modernized.py \
  tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py::test_dispatcher_plugin_adapter_and_failure_edges \
  -q
```

Result: `30 passed`.

## Structured XML Fuzz Lane

Tightened XML-ish metadata readers so parseable wrong-format XML no longer
turns into shell metadata by default:

- OPF now validates OPF package/metadata-shaped XML after parsing.
- FB2 now validates that the root document is FictionBook.
- both readers keep explicit fallback knobs for a later best-effort metadata
  facade, but dispatcher/plugin reads use the strict defaults.

Added deterministic dispatcher-level malformed/wrong-format cases for:

- OPF receiving empty bytes, tiny binary, PNG header, HTML, empty ZIP, and FB2
  XML
- FB2 receiving empty bytes, tiny binary, PNG header, HTML, empty ZIP, and OPF
  XML

Focused validation:

```bash
python3 -m pytest \
  tests/metadata/file_sources/test_malformed_input_fuzzing.py \
  tests/metadata/file_sources/test_opf_metadata_source.py \
  tests/metadata/file_sources/test_opf_edge_cases.py \
  tests/metadata/file_sources/test_fb2_metadata_source.py \
  tests/metadata/file_sources/test_fb2_edge_cases.py \
  -q
```

Result: `64 passed`.
