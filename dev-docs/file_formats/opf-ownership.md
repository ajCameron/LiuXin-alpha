# OPF utility ownership

Reviewed **2026-10-20**. Entry points, provider boundaries and the scope of the recorded hardening work were reviewed. See [next tasks](next_tasks.md). Sign-off sections describe their dated fixture scope; they do not certify every input, optional dependency or installed provider today.

Decision date: **2026-10-13**

OPF parsing, version parsing, language round-tripping, manifest-item creation,
unique package naming, and OPF pretty-printing are file-format operations. They
live in `LiuXin_alpha.file_formats.opf.utils` and are tested in
`tests/file_formats/opf/test_opf_utils.py`.

Generic `metadata.utils` must not import OEB/OPF implementations. It owns
format-neutral author, identifier, series-index, and resource helpers only. The
Calibre-compatible record factory `calibreMetaInformation` similarly belongs
beside `calibreMetadata` in `metadata.book.base`, rather than in the generic
utility module. Callers import the record factory from its owner.

These are ownership moves, not capability removals. The prior parsing,
normalization, manifest mutation, pretty-printing, construction, and copy
behaviors retain focused regression coverage.

The recorded ownership change reduced graph coupling. These counts describe that checkpoint, not a fresh graph measurement. Relative to the reviewed
2026-10-12 baseline:

- the largest runtime strongly connected component falls from 280 to 220;
- the largest combined component falls from 311 to 220; and
- the domain-only component falls from 17 to 9.

The import-footprint gate also keeps `metadata.utils` below 30 project modules
and prevents its cold import from reaching file formats or other higher-level
domains. The remaining metadata/file-format relationships include concrete
metadata readers and the legacy conversion stack; this decision does not claim
that the wider cycle programme is complete.
