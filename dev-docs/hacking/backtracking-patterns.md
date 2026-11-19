# Backtracking patterns on hostile input

Written **2026-11-18**.

Python's `re` tries a pattern at every position of the text. A pattern that
can scan a long way from a position before failing costs the length of the text
at each try, so one `search` or `sub` over crafted text costs its square, or
worse. LiuXin reads files it did not write, and several patterns inherited from
calibre behave this way.

## Patterns fixed

| Pattern | Where | Crafted input | Before |
| --- | --- | --- | --- |
| `<\?[^<>]+encoding\s*=\s*['"](.*?)['"][^<>]*>` | `utils/libraries/calibre_chardet.py` (`ENCODING_PATS`) | `<?` and `encoding="` repeated | 8 KB: 6.7 s, growing with the cube; 50 KB, the most it reads: over half an hour |
| `<meta\s+?[^<>]*?content\s*=\s*['"]…charset=…` | the same | `<meta` and a run of spaces | quadratic: 16 KB took 2.0 s, 50 KB 19 s |
| `&(\S+?);` | `calibre_chardet.py` (`ENTITY_PATTERN`), `utils/text/xml_utils.py` | a run of `&` with no `;` | 80 KB: 33 s, and `xml_to_unicode(..., resolve_entities=True)` reads the whole file |

`xml_to_unicode` decodes OPF, NCX, HTML, FB2, CHM and LIT content, so each of
these reached most readers and converters.

Conversion resolved entities with its own copies of the entity pattern, and the
heuristic processor had a second slow pattern beside one of them:

| Pattern | Where | Crafted input | Before |
| --- | --- | --- | --- |
| `&(\S+?);` | `file_formats/conversion/preprocess.py` (`HTMLPreProcessor.PREPROCESS`), run on every HTML document converted | a run of `&` with no `;` | 80 KB: 33 s; 1 MB, extrapolated: about an hour and a half |
| `&(\S+?);` | `file_formats/conversion/utils.py` (`HeuristicProcessor.markup_pre`), `file_formats/lrf/html/convert_from.py` (`HTMLConverter.MARKUP_MASSAGE`), `file_formats/lrf/pylrs/pylrs.py` (`TocLabel`) | the same | the same |
| `.*?(?<=<pre>)(?P<text>.*?)</pre>` | `HeuristicProcessor.markup_pre`, for documents with few paragraph tags | any long text after the last `</pre>`, crafted or not | 40 KB: 8.9 s |

All of these now share `linear_time.ENTITY_PATTERN`, as do the decoder, the
XML helpers and the MOBI 6 reader. `file_formats/lrf/objects.py` reads LRF text
with `&amp;(\S+?);`, which is already linear: a try fails only on a run with no
`;` after its first character, and such a run holds no other `&amp;`, so failed
tries never scan the same text twice.

The MOBI 6 reader, `file_formats/mobi/reader/mobi6.py`, runs every MOBI 6 book's
text through more patterns and scans in conversion, and so does metadata reading
for books under 4 MB without an EXTH header. On crafted books of a few hundred
kilobytes these took minutes or more:

| Pass | Why it was slow | Now |
| --- | --- | --- |
| `PAGE_BREAK_PAT`, the `o:p` pattern | `\s*/{0,1}\s*` and `(?:\s+[^/>]*)` split a run of spaces every way | possessive `\s*+` and an atomic group: the matches cannot end another way; `sub_page_breaks` skips tries that must fail |
| `<\?xml`, `<o:p`, `name: ` tags | a try at each opening scans to a `>` that never comes | `sub_before_last`: no tries after the last `>` |
| the four swaps of style and block tags | every tag of a long run is tried, or each `<p` and `</p` scans to a missing `>` | `sub_from_run_starts`, `sub_skipping_failures` |
| `filepos=` links | an unclosed tag retries every `filepos=` against the rest of it | a lookahead checks first that the tag closes |
| entities around anchors, `&(\S+?);` | each `&` scans to the end of the text | `sub_skipping_failures`, `entities` |
| anchor placement | each link target searches afresh for `<` and `>` | `ForwardFinder` |
| CSS classes | each rule is compared with every rule so far | a dictionary |
| svg images, page-break anchors | placed by counting the parent's children | `addprevious` and `addnext` beside the element |

The reader also pasted the guide's toc and cover hrefs into XPath; they now pass
as XPath variables. Its PalmDB table already refuses offsets that go back, its
HUFF/CDIC reader caps each record's output and its EXTH reader refuses records
of size 0, which calibre's own reader does not.

## How they are fixed

`LiuXin_alpha.utils.linear_time` holds stand-ins. A `LinearPattern` wraps the
original compiled pattern and a function that finds the same matches in linear
time; it offers `search`, `finditer` and `sub` (with `count`), which is what
callers use. The original pattern stays written out as the definition of what
must match.

- The two declaration patterns are matched by hand, crossing each stretch of
  the text once, and each match is then made by a pattern with the found
  lengths fixed, so callers get a real `re.Match` with the original spans and
  groups.
- `&(\S+?);` is equivalent to `&(\S[^\s;]*);`, which reads a run once. When a
  try fails, a second branch takes the rest of the run, where every try fails
  the same way, and that match is not passed on.
- `sub_until_failure` takes the `<pre>` blocks one after another with `match`
  and stops at the first failure: a try after the last block lacks a `<pre>`
  with a `</pre>` after it, and so does every later try, which `re.sub` made
  anyway.

`calibre_chardet` builds `ENCODING_PATS` and `ENTITY_PATTERN` on first use, so
importing it does not load the helper module and the cold-import budgets are
unchanged.

## Rules for new fixes

- The stand-in must give the original pattern's results. Compare them on random
  text built from the pattern's own pieces, case variants included, for both
  `str` and `bytes`; `tests/utils/test_linear_time.py` shows the shape.
- Show the speed with crafted input of about a megabyte and a generous time
  limit, not a benchmark.
- Prefer possessive quantifiers (`*+`, `++`, Python 3.11 and later) or atomic
  groups where a run can only end one way; prefer a hand-written scan when the
  pattern retries the same stretch from many starts.

## Upstream

calibre 9.18 has the declaration patterns too (its `ebooks/chardet.py`), the
same entity and `<pre>` patterns in its conversion code, and the same MOBI 6
passes and XPath. They are recorded for reporting upstream in
[upstream-calibre-bugs.md](../calibre_compat/upstream-calibre-bugs.md): bugs 5,
9 and 10 (PR #181), and bug 11, the conversion patterns.

Return to [contributing and tools](README.md).
