# Malformed Input And Fuzz Testing

## Purpose

Malformed-input tests exist to prove that parsers, decoders, and metadata
extractors fail deliberately. They should catch expensive decode paths, unsafe
allocation, raw internal exceptions, partial mutation, and format confusion
before those behaviors reach production inputs.

## Contract

Low-level format readers and decoders should either:

- return valid parsed output for valid input
- raise a clear format-level or project-level exception for invalid input

They should not hang, consume resources wildly out of proportion to the input,
or rely on a higher-level router to recover from internal corruption. A central
"best effort metadata from this file" API can decide fallback routing later, but
individual format readers should reject inputs that are not credible instances
of their format.

## Determinism

Checked-in fuzz tests must be deterministic:

- use `random.Random(seed)` or explicit byte literals, not `os.urandom`
- keep the corpus small enough for normal CI
- promote every discovered slow, crashing, or surprising payload into a named
  regression case
- keep timeout wrappers around risky pure-Python decoders, but treat timeouts as
  bug reports, not as the desired failure mode

Property fuzzers are useful for local exploration, but CI should run a fixed
corpus whose failures can be reproduced exactly.

## Wrong-Format Extractor Tests

Metadata extractor tests should include wrong-file inputs as well as malformed
same-format inputs:

- empty and tiny byte streams
- truncated archives, XML, PDF, RTF, and binary containers
- valid files handed to the wrong extractor
- hostile or invalid text where decoding happens

The expected outcome is a conservative metadata result only when that is
intentional. Otherwise the extractor should raise a sane, predictable error
without leaking arbitrary `IndexError`, `KeyError`, `struct.error`, parser
internals, or runaway decode work.

Use `LiuXin_alpha.metadata.file_sources.registry` to enumerate metadata readers
for corpus-driven tests. Keep individual format readers strict; place
wrong-extension sniffing and fallback routing in a separate best-effort facade.

## Structured XML Readers

XML parse success is not enough to prove that a file belongs to a structured
metadata format. OPF and FB2 readers should validate their expected document
shape after parsing:

- OPF accepts OPF package/metadata-shaped XML by default.
- FB2 accepts FictionBook-rooted XML by default.
- Generic XML extraction and shell metadata fallback are explicit opt-in paths
  for internal callers or a future best-effort facade.

Wrong-format but parseable XML, such as HTML passed to OPF/FB2 or OPF passed to
FB2, should raise a format-level error through the individual reader.

## Archive Text Readers

Archive-backed text readers should distinguish malformed containers from valid
containers with sparse metadata:

- invalid ZIP bytes and empty/non-credible archives should raise
- HTMLZ archives with credible HTML/manifest content may return shell metadata
  when no OPF metadata is present
- TXTZ archives may fall back to embedded `.txt` content when no OPF metadata
  is present

This keeps legitimate legacy HTMLZ/TXTZ fixtures readable while still rejecting
arbitrary bytes and empty ZIP files.

## Binary Metadata Readers

Binary readers should reject arbitrary bytes before manufacturing filename-based
metadata:

- PDF requires a PDF header and parseable PDF objects by default.
- MOBI-family readers raise on unreadable MOBI headers by default.
- PDB raises when the wrapper header itself cannot be parsed by default.

PDB still returns header-only metadata for parseable but unsupported PDB
variants. That is a valid-container fallback, not a wrong-format fallback.
Explicit fallback flags are reserved for future best-effort routing APIs that
choose to keep shell metadata for broken files.

## Legacy And Specialty Readers

Older single-format readers should still reject non-credible wrappers even when
they preserve safe defaults for valid sparse files:

- RTF requires an RTF header; valid RTF without an `\info` block may return
  shell metadata.
- SNB requires a valid SNB archive; valid SNB archives without `book.snbf` may
  return shell metadata.
- LRX rejects arbitrary short or wrong headers; unsupported but identifiable
  Librie LRX remains a valid-container fallback.
- RB and IMP require their format magic headers; valid sparse wrappers may
  still return shell metadata.
- LIT raises on reader/container failures by default; explicit fallback is
  available for best-effort routing.
- PML remains text-like and can be sparse, but PMLZ requires a readable archive
  with at least one `.pml` member.
- Topaz requires a readable Topaz container by default.

Treat these as format-boundary checks. The future best-effort facade can decide
whether to retry another reader or keep filename-derived metadata.
