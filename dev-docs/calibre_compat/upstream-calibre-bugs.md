# Bugs to report to upstream calibre

Status: **not yet reported.** Found **2026-11-17** and **2026-11-18** while porting
calibre's metadata readers (PRs #179 and #185) and hardening LiuXin's own readers
(PRs #180 and #186).

These are four ways a damaged or hostile file keeps a calibre metadata reader
busy for minutes, or for ever. Each section has the location, a reproducer to run
with `calibre-debug`, what was measured, and a suggested fix.

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
  All four are denial of service from a crafted file. That makes them candidates
  for a private advisory rather than a public bug, or for a public bug if calibre
  treats reader hangs as ordinary bugs. Choosing which is the reporter's call.
- **Order:** bugs 2 and 3 should go together. Fixing 2 lets never-closed info
  blocks reach the patterns in 3, which are then cubic rather than quadratic.

## Summary

| # | Where | Trigger | Effect |
|---|---|---|---|
| 1 | `ebooks/metadata/topaz.py`, `MetadataUpdater.get_headers` | A header or block count past the end of the file | 9-byte file: 7.8 minutes, 2.2 GB, then an error |
| 2 | `ebooks/metadata/rtf.py`, `get_document_info` | An `{\info` group that the end of the file cuts off | Never returns; memory grows about 100 MB a second |
| 3 | `ebooks/metadata/rtf.py`, the five field patterns | Many `{\info` groups and a field that isn't there | Quadratic: 56 kB takes 10 s. Cubic once 2 is fixed |
| 4 | `ebooks/metadata/html.py`, `handle_comment` | An HTML comment with a long run of characters and no spaces | Quadratic: 40,000 characters take 14–32 s; the 150,000 calibre reads, minutes |

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

## LiuXin's own fixes

- PR #180 fixes LiuXin's readers on `main`: `metadata/file_sources/topaz.py`
  (whose counts could reach 2**56, as it reads up to 8 bytes) and
  `metadata/file_sources/rtf.py`.
- PR #179 carries the same fixes in the ported calibre readers under
  `metadata/upstream_calibre/`, each line marked `# LiuXin:`.
- PR #186 fixes the same comment pattern in LiuXin's HTML reader on `main`
  (`metadata/file_sources/html.py`), which has no length cap, so a 1 MB comment
  would have taken hours. PR #185 carries the fix in the ported HTML reader.

Return to the [calibre compatibility index](README.md).
