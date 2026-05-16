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
