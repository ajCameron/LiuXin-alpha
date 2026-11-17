# Bugs to report to upstream calibre

Status: **not yet reported.** Found **2026-11-17** while porting calibre's metadata
readers (PR #179) and hardening LiuXin's own readers (PR #180).

These are three ways a damaged or hostile file keeps a calibre metadata reader
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
  All three are denial of service from a crafted file. That makes them candidates
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

## LiuXin's own fixes

- PR #180 fixes LiuXin's readers on `main`: `metadata/file_sources/topaz.py`
  (whose counts could reach 2**56, as it reads up to 8 bytes) and
  `metadata/file_sources/rtf.py`.
- PR #179 carries the same fixes in the ported calibre readers under
  `metadata/upstream_calibre/`, each line marked `# LiuXin:`.

Return to the [calibre compatibility index](README.md).
