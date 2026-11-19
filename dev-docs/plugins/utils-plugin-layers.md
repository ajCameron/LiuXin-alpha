# Utils plugins: native modules and their fallbacks

Written **2026-11-18**, when the `dateutil_parser` plugin was added (PR #187);
the `aes` plugin was added on 2026-11-19 (PR #203).

`LiuXin_alpha.utils.plugins` stands in for calibre's `calibre_extensions`: the
compiled modules calibre ships (`speedup`, `lzx`, `msdes`, `chmlib`, `icu` and
so on), plus libraries LiuXin can carry its own copy of. Callers ask for a
plugin by name and get a module, or `None` and the reasons each layer failed:

```python
from LiuXin_alpha.utils.plugins import plugins

module, error = plugins["dateutil_parser"]
```

This is separate from the application plugin system in `LiuXin_alpha.plugins`
(see the [plugins index](README.md)).

## How a plugin is found

A plugin must be listed in `UNIVERSAL_PLUGINS` (or a platform list) in
`utils/plugins/__init__.py`. The first of these that loads wins, and the answer
is kept for the rest of the process:

1. **A compiled module for this platform**, imported as
   `utils.plugins.<linux|windows|osx>.<name>`.
2. **A compiled extension file** named `<name>` with any extension suffix, in
   the platform folder or a folder passed to `Plugins`.
3. **Fallbacks**, through `utils/plugins/resolver.py`, in this order:
   - the layer chosen last time on this machine, if its probe still passes;
   - `fallbacks.fallback_alpha.<name>`;
   - `fallbacks.fallback_beta.<name>`;
   - `fallbacks.<name>`.

A fallback module can define `__liuxin_plugin_probe__()`, returning
`(ok, reason)`. A layer whose import fails, or whose probe says no, is passed
over. The resolver remembers the layer it chose for each plugin in a JSON file
keyed by a machine fingerprint (`LIUXIN_PLUGIN_CACHE_PATH` overrides where it
lives), and tries that layer first next time.

## An installed package first, a vendored copy behind it

`fallback_alpha` can hold the better implementation, with a probe that checks it
works, and the pure-Python layer behind it. `magick` uses its probe to require
a working Wand install. `dateutil_parser` serves the installed python-dateutil
package from it, and LiuXin's vendored copy behind it:

| Layer | Module | Serves |
| --- | --- | --- |
| `fallback_alpha` | `fallbacks/fallback_alpha/dateutil_parser.py` | the installed python-dateutil package |
| `fallbacks` | `fallbacks/dateutil_parser.py` | LiuXin's vendored copy, `utils.libraries.python_dateutil` |

Both expose `parse`, `parser`, `parserinfo`, `isoparse`, `ParserError` and a
`SOURCE` label. Because the resolver remembers its choice, a vendored layer
chosen before the package was installed would otherwise keep being used. The
vendored layer's probe therefore declines whenever the package imports, which
sends the resolver back to `fallback_alpha`.

### The vendored python-dateutil

`utils/libraries/python_dateutil` is python-dateutil 2.9.0.post0, copied by
`scripts/vendor_python_dateutil.py` from an installed package. Two mechanical
changes are made, and each file's header records them:

- imports of `six` point at the `_six` module kept beside the copy, which holds
  the few names python-dateutil uses, for Python 3 only;
- absolute `dateutil` imports point at the vendored package.

The time-zone database archive is left out. dateutil falls back to the
system's database, and parsing dates does not use it. The copy keeps its
`LICENSE` (Apache 2.0 or BSD 3-Clause), which is shipped as package data.

Tests in `tests/utils/plugins/test_dateutil_parser_plugin.py` check that:

- the vendored parser gives the same answer as the installed package on 5,000
  random date-like strings, with and without `dayfirst`;
- re-vendoring the same version reproduces every file;
- the copy imports neither `six` nor `dateutil`;
- a child process that cannot import python-dateutil gets the vendored copy,
  passes calibre's own OPF 2 and OPF 3 tests, and reads dates;
- installing the package moves a remembered vendored choice back to it.

To update the copy, run the script on a newer installed python-dateutil, review
the diff, and run those tests.

### Why not `liuxin_dateutil`

LiuXin already carries python-dateutil 1.5 as `utils.libraries.liuxin_dateutil`,
which LiuXin's own date code uses (`utils/date.py`, `utils/iso8601.py`,
`utils/libraries/calibre_date.py`). Its parser is not a safe stand-in for the
current one. On 30,000 random date-like strings, parsed with a default date:

| What happened | Strings |
| --- | --- |
| 1.5 returned a date, often just the default, for text 2.9 rejects as holding none (`T`, `--`, blank) | 2,131 |
| 1.5 crashed with `TypeError` where 2.9 rejects the text | 1,085 |
| 2.9 read a date that 1.5 rejects | 124 |
| Both read a date, but different ones (`010116`: 2.9 gives 2016-01-01, 1.5 gives 2001-01-16) | 58 |

The first row matters most for an archive: calibre fills in a default date, so
1.5 would turn an empty or junk `<dc:date>` into an invented publication date.
LiuXin's own date code still uses 1.5. Moving it onto the `dateutil_parser`
plugin would remove these behaviours, and is a separate change.

## `aes`: cryptography, then poppler, then pure Python

Written **2026-11-19** (PR #203). The `aes` plugin gives LiuXin's PDF reader
AES-CBC, for files encrypted with AES-128 or AES-256. Every layer exposes
`cbc_encrypt(key, iv, data)`, `cbc_decrypt(key, iv, data)` (whole blocks, no
padding) and `SOURCE`:

| Layer | Module | Serves | Probe |
| --- | --- | --- | --- |
| `fallback_alpha` | `fallbacks/fallback_alpha/aes.py` | the `cryptography` package (the `encryption` extra) | it imports and gives FIPS 197's known answer |
| `fallback_beta` | `fallbacks/fallback_beta/aes.py` | poppler: no cipher (`DECRYPTS_IN_PROCESS = False`); the PDF reader hands AES files to `pdfinfo`, which decrypts them | `cryptography` does not work, and `pdfinfo` is on the PATH |
| `fallbacks` | `fallbacks/aes.py` | AES in pure Python (FIPS 197, with 32-bit lookup tables): about 12 µs a block | neither of the above can serve |

As with `dateutil_parser`, each lower layer's probe declines while a better one
can serve, so a choice remembered before `cryptography` or poppler was installed
moves back up.

Tests in `tests/utils/plugins/test_aes_plugin.py` check that:

- the pure-Python layer gives FIPS 197's and SP 800-38A's answers, and agrees
  with `cryptography` on 2,000 random keys and messages;
- child processes that refuse `cryptography`, with and without `pdfinfo` on
  the PATH, get the poppler and pure-Python layers, and the second reads an
  AES-256 PDF;
- `cryptography` replaces a remembered pure-Python choice once it imports.

`tests/file_formats/pdf/test_pdf_object_reader.py` reads every AES file of the
PDF corpus through each layer.

## Adding a plugin

1. Add the name to `UNIVERSAL_PLUGINS`, or to a platform list.
2. Put the pure-Python implementation in `fallbacks/<name>.py`, with the same
   public names as the native module.
3. If an installed package or a better implementation should come first, put
   it in `fallback_alpha/<name>.py`, with a probe that checks it actually
   works. If it has a lower layer behind it, give that layer a probe that
   steps aside once the preferred one is usable.
4. Moving a line in `utils/plugins/__init__.py` moves its dynamic import in
   `scripts/layer_direction_baseline.json`; update the line number there.

Return to the [plugins index](README.md).
