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

## Still to fix

- `&(\S+?);` is also compiled in `file_formats/conversion/utils.py`,
  `file_formats/conversion/preprocess.py`, `file_formats/lrf/objects.py`,
  `file_formats/lrf/html/convert_from.py` and `file_formats/lrf/pylrs/pylrs.py`.
  These are on conversion paths; `LinearPattern(re.compile(r"&(\S+?);"), entities)`
  replaces each.
- LiuXin's MOBI 6 reader, `file_formats/mobi/reader/mobi6.py`, has its own
  backtracking clean-up patterns, XPath built from the book's text and repeated
  scans; they are being fixed separately.

calibre 9.18 has the declaration patterns too (its `ebooks/chardet.py`), and
the same entity pattern in its conversion code. The declaration hangs are
recorded for reporting upstream in
`dev-docs/calibre_compat/upstream-calibre-bugs.md` (PR #181).

Return to [contributing and tools](README.md).
