# Calibre compatibility: current boundaries

Checked: **2026-10-19**, against the current LiuXin-alpha source tree.

Calibre compatibility spans source-library readers, metadata adapters, generated
SQL projections and web responses. Each has its own contract and identity space.
There is no single adapter that makes LiuXin interchangeable with Calibre.

## What exists today

| Boundary | Current owner | Behaviour |
| --- | --- | --- |
| Inspect a source library | [Calibre database emulation](../src/LiuXin_alpha/utils/calibre_compat/calibre_database_emulation/__init__.py) | Reads `metadata.db`, inspects schema/custom columns, streams normalized payloads and reports filesystem drift. |
| Recover sidecar metadata | [OPF sidecar reader](../src/LiuXin_alpha/utils/calibre_compat/calibre_database_emulation/opf_sidecar.py) | Reads metadata from library folders; the scan/job helpers use this route when `metadata.db` is absent. |
| Adapt metadata values | [Calibre-like containers](../src/LiuXin_alpha/metadata/containers/calibre_like_book_metadata/__init__.py) and [compatibility Metadata](../src/LiuXin_alpha/utils/calibre_compat/ebooks/metadata/book/base.py) | Expose supported fields and conversion methods. These objects do not establish canonical database identity. |
| Project canonical records into SQL | [WEMI views](../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/aggregate_sql/wemi_views.sql) | SQLite `books`/`books_v` expose one row per W–E–M ray; `titles`/`titles_v` expose one row per Work. They are lossy read projections. |
| Serve Calibre-style web responses | [Web application](../src/LiuXin_alpha/surfaces/web_calibre_readonly/app.py) and [catalogue adapter](../src/LiuXin_alpha/surfaces/catalog/api.py) | Serve selected HTML, AJAX, OPDS and acquisition routes over Core reads. Book-token lookup resolves a Work ID. |
| Diagnose inherited imports | [Import diagnostics](../src/LiuXin_alpha/utils/calibre_compat/import_diagnostics.py) | Observes missing `calibre.utils.*` imports. It does not supply the missing module or load a plugin itself. |

The [mapping guide](<calibre_compat/mapping calibre to LiuXin.md>) explains the
identity differences with examples. The [import policy](<calibre_compat/00 - Calibre Compatibility Policy.md>)
is a **proposal** for preserving source records and assigning WEMI identities.
Its compatibility-record tables and Catalog adapter have not been implemented.

## Read a source library

`CalibreDB.from_root()` and `CalibreReader.from_root()` default to read-only SQLite
connections. Reading payloads does not create Works, Items or asset replicas in a
LiuXin catalogue. The `read_only=False` option is a low-level connection choice;
it does not provide a supported Calibre library writer.

For a small metadata preview, validate the schema/version policy explicitly and
close the streaming iterator when stopping early:

```python
from contextlib import closing
from itertools import islice

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import (
    CalibreReader,
    CalibreVersionPolicy,
)


def preview_calibre_titles(library_root):
    reader = CalibreReader.from_root(library_root, read_only=True)
    schema = reader.schema_info(
        best_effort=False,
        version_policy=CalibreVersionPolicy(
            allow_application_id_mismatch=False,
            allow_newer_user_version=False,
        ),
    )
    with closing(reader.iter_book_payloads(
        include_formats=False,
        include_cover_path=False,
        filesystem_reconcile=False,
        best_effort=False,
    )) as payloads:
        titles = [(payload.calibre_book_id, payload.title) for payload in islice(payloads, 5)]
    return schema, titles
```

Pass a source-library directory containing `metadata.db`. The returned IDs belong
to that source library. `schema_info()` owns and closes its inspection connection;
the payload generator owns a separate connection and closes it in `finally`.
A connection returned by `CalibreDB.connect()`, or a file handle returned by
`open_format()`/`open_cover()`, belongs to the caller and must also be closed.

The version plan compares observed pragmas with the packaged schema snapshot and
caller policy. It does not upgrade the source library or certify arbitrary
Calibre releases. `best_effort=True` can return a refusal plan and issues instead
of raising. Calling `iter_book_payloads(best_effort=False)` validates core tables,
but does not itself apply a caller-supplied version policy; perform the separate
schema check when that policy matters.

## Scan results and proposed import actions

`scan_calibre_library()` summarizes metadata and drift. `iter_import_jobs()` yields
`CalibreImportJob` values containing source mode, action, reasons and an optional
normalized payload. Neither function executes a canonical LiuXin import.

The current `CalibreImportPolicy` controls these action classifications, rather
than choosing a WEMI anchor:

| Action | Default classification | Payload |
| --- | --- | --- |
| `full` | At least one resolved format and no error-level drift. | Metadata and resolved file references. |
| `metadata_only` | No resolved formats, or error-level drift such as a missing book folder. | Metadata with formats and cover path removed by default. |
| `skip` | A caller-configured action. | `None`, with reasons retained. |

A missing format among several can produce a warning while another available
format still permits `full`. With the default policy, unsafe path drift prevents
`full`. These are reported classifications, not import receipts or proof that
all payload bytes have been read and verified. Inspect the reasons and warnings.

The scan helpers choose the sidecar reader when `metadata.db` is absent. A present
but corrupt database is not an automatic request to discard it and switch to OPF.

## Metadata, SQL and web projections

Metadata conversion supports the fields implemented by the relevant adapter;
it is not a lossless round trip of every Calibre field or custom column. In
particular, `CalibreLikeLiuXinBookMetaData.from_calibre()` enumerates supported
attributes. Retained conversion, rendering and formatting helpers serve specific
callers; their existence is not a promise that every third-party plugin will run.

The generated SQLite views read canonical WEMI/storage records. They do not
preserve an imported Calibre `books.id`, and they are not write targets for
bibliographic changes. Use [Catalog mutations](catalog/catalog-write-validation-contract.md)
for persistence and [storage workflows](storage_and_backup/storage_api.md) for
byte placement.

The Calibre-style web application uses the shared Core read model. Its book tokens
refer to Works, while a SQL `books_v.book_id` identifies a ray. Use the
[startup guide](read-only-surface-appliance-startup.md) for operational commands
and the [surface index](surfaces/README.md) for the serving owners. The application
implements a subset of the upstream interface.

## Diagnose missing imports

For a caller-supplied plugin-loading operation:

```python
from LiuXin_alpha.utils.calibre_compat import calibre_import_failure_logging


def load_with_calibre_diagnostics(load_plugin):
    with calibre_import_failure_logging():
        return load_plugin()
```

The context temporarily wraps the process-wide Python import function. Matching
`ModuleNotFoundError` failures are logged through
`LiuXin_alpha.calibre_compat.imports`, deduplicated by requested/missing module,
and re-raised. The application owns logging handlers. This diagnoses missing
imports; an actual compatibility implementation is still needed to satisfy them.
Use [plugin composition](plugins/plugin-composition.md) for loading and provider
ownership.

## Relevant checks

- [Source database and connection behaviour](../tests/databases/database_calibre_emultation/test_calibre_emulation_db.py).
- [Version-policy matrix](../tests/databases/database_calibre_emultation/test_calibre_emulation_b3_compat_matrix.py).
- [Streaming import-job classifications](../tests/databases/database_calibre_emultation/test_calibre_emulation_f2_iter_import_jobs.py).
- [Metadata conversion](../tests/metadata/containers/calibre_like_book_metadata/test_metadata_to_from_calibre.py).
- [Generated WEMI/compatibility views](../tests/databases/test_frbr_aggregate_views_wemi.py).
- [Web compatibility routes](../tests/surfaces/test_web_calibre_readonly.py).
- [Import diagnostics](../tests/utils/calibre_compat/test_import_diagnostics.py).

These suites cover their named adapters and fixtures. They are not a blanket
certification of Calibre releases, plugins or library round trips.
Return to the [compatibility index](calibre_compat/README.md).
