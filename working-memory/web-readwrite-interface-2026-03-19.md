# Web Read-Write Interface

Date: 2026-03-19

## Summary

Added a first local-first HTML mutation surface under `interfaces/web_readwrite`.
It reuses the existing read-only browse/search/detail/download stack and layers a
small generic CRUD UI on top for real tables.

## Scope Landed

- New interface package:
  - `src/LiuXin_alpha/interfaces/web_readwrite/`
- New launchers:
  - `scripts/run_web_readwrite.sh`
  - `scripts/run_web_readwrite.py`
- Focused interface tests:
  - `tests/interfaces/test_web_readwrite.py`

## Current Capabilities

- Browse/search/detail behavior inherited from `web_readonly`
- Generic table actions:
  - create row
  - edit row
  - delete row
- Row-level write actions on detail pages
- Delete-impact preview via `Library.describe_row_delete_impact(...)`
- Generic create/edit forms for non-view tables
- Basic value coercion:
  - empty string stays empty string
  - `NULL` maps to `None`
  - bool/int/float coercion by column type
- File rows keep the existing download / preview links

## Deliberate Constraints

- Local-first, not hardened for public internet exposure
- No auth
- No CSRF protection
- No specialized interlink editing yet
- No table-specific write widgets yet
- Compatibility views such as `titles` remain read-only from the generic UI

## Implementation Notes

- Generic creation uses `Row.from_idless_row_dict(...)`
- Generic updates use `Library.update_row_fields(...)`
- Generic deletion uses `Library.describe_row_delete_impact(...)` and
  `Library.delete_row(...)`
- The interface currently overrides row/table/home rendering minimally rather
  than extracting a larger shared read/write HTML layer

## Validation

Confirmed:

- `python3 -m py_compile scripts/run_web_readwrite.py`
- `bash -n scripts/run_web_readwrite.sh`
- `./scripts/run_web_readwrite.sh --help`
- `python3 scripts/run_web_readwrite.py --help`
- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `3 passed`

A broader interface regression slice was started against:

- `tests/interfaces/test_web_readwrite.py`
- `tests/interfaces/test_web_readonly.py`
- `tests/interfaces/test_web_calibre_readonly.py`

but the targeted read-write slice was the primary gate for this first landing.

## Next Likely Steps

1. Add interlink editing from row pages
2. Add better widgets for booleans, enums, JSON, and long-text fields
3. Add success/error flash notices instead of silent redirects
4. Add an explicit local-only guardrail if this interface remains unauthenticated

## Second Slice

Added the first inline interlink-management pass on row pages.

### New Capabilities

- Row pages now include `Manage linked entities` sections for interlinkable tables.
- Existing links can be:
  - updated in place by editing the underlying link-row metadata
  - removed directly from the primary row page
- New links can be added directly from the primary row page by selecting a target row id and optional link metadata.

### Current Interlink Scope

- Works for standard interlink tables discovered through the existing schema helpers.
- Uses the real link-table shape, not a parallel ad-hoc model.
- Current edit scope is link metadata columns only; the linked target row itself is not changed during link edit.

### Widget Improvements

Generic form controls are now more schema-aware:

- `*_type` fields backed by `{table}__types` or `allowed_types__{table}` render as `<select>` controls.
- foreign-key-like `*_id` fields now render with datalist suggestions when a referenced table can be inferred.
- repeated inline forms now use distinct DOM ids so the row pages remain structurally valid.

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `4 passed`
- Added coverage for:
  - inline interlink add/edit/delete from a work page
  - schema-aware widgets on the row page (`datalist` + type `<select>`)

## Third Slice

Added specialized work-link widgets plus redirect-based success notices.

### Specialized Work Link Sections

The row page no longer treats the highest-value work links as generic unnamed link-table editors.
Current work-specific sections are:

- `agents` -> `Manage credits`
- `labels` -> `Manage tags`
- `series` -> `Manage series`
- `languages` -> `Manage languages`

These sections now provide:

- custom section titles and intro text
- better target labels such as `Contributor row id`
- better field labels such as `Role` / `Language role` / `Relationship type`
- ordered metadata fields instead of raw column-order dumps
- inline metadata chips on existing link cards
- common work sections opened by default

### Notices

Added one shared notice mechanism for the write interface.

- redirect-based success/info notices now land on row/table pages
- same notice styling is reused for inline form errors
- current notice kinds:
  - `success`
  - `error`
  - `warning`
  - `info`

Current write flows using notices:

- create row
- edit row
- delete row
- add interlink
- edit interlink
- delete interlink
- no-op interlink edit

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `4 passed`
- broader interface slice:
  - `tests/interfaces/test_web_readwrite.py`
  - `tests/interfaces/test_web_readonly.py`
  - `tests/interfaces/test_web_calibre_readonly.py`
  - currently green in this landing pass

## Fourth Slice

Added inline linked-row creation helpers and table-specific write pages for the
highest-value editable tables.

### Linked Row Creation

Work detail pages now support creating and linking new target rows directly from
the linked-entity section for writable linked tables.

Current scope:

- `agents`
- `labels`
- `series`

Behavior:

- create the secondary row inline from the work page
- immediately create the interlink row
- preserve the existing link metadata fields in the same submit
- return to the work page with success notices anchored to the relevant section

`languages` is now explicitly treated as reference data rather than pretending it
is generically writable. The work page keeps the existing link-only UI there and
shows a create-disabled explanation.

### Specialized Create/Edit Pages

The write interface no longer uses the flat generic field dump for:

- `works`
- `files`
- `stores`

These forms are now grouped into domain-specific sections while still exposing a
fallback `Other fields` section so writable columns are not silently hidden.

Current grouping examples:

- `works`
  - `Identity`
  - `Classification`
  - `Origin`
  - `Notes`
- `files`
  - `Storage`
  - `Naming`
  - `Classification`
  - `Integrity`
  - `Source`
- `stores`
  - `Identity`
  - `Access`
  - `Capabilities`
  - `Consistency`

### Writability Guardrails

- Trigger-locked reference tables are now treated as read-only by the write UI,
  not just SQL views.
- This fixes the misleading generic-create affordance for `languages`.

### Widget Improvements

- `stores.store_kind` now renders from the known storage backend kinds.
- `stores.store_access_protocol` and `stores.store_auth_method` also render as
  constrained selects.

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `7 passed`
- broader interface slice:
  - `tests/interfaces/test_web_readwrite.py`
  - `tests/interfaces/test_web_readonly.py`
  - `tests/interfaces/test_web_calibre_readonly.py`
  - `28 passed`

## Next Likely Steps

1. Add “create linked row” helpers for more link-rich tables beyond `works`
2. Add file-upload / store-placement flows instead of metadata-only file row creation
3. Add specialized write pages for additional core tables if they become common edit surfaces
4. Add local-only guardrails and auth before any wider exposure

## Fifth Slice

Added the first real file-upload path to the write interface.

### New Capability

The interface can now:

- upload one local file through HTML
- place its bytes into a selected writable store via `Library.add_file(...)`
- create the matching `files` row automatically
- immediately reuse the existing read-only download path for retrieval

Current route:

- `GET /files/upload`
- `POST /files/upload`

### Current Behavior

- only writable stores are offered on the upload page
- storage manager bootstrap happens on submit before placement
- the selected store remains the placement preference for `add_file(...)`
- the resulting `files` row captures:
  - `file_store_id`
  - `file_storage_key`
  - `file_name`
  - `file_base_name`
  - `file_extension`
  - `file_mime_type`
  - `file_size_bytes`
  - `file_original_name`
  - `file_source`
  - optional `file_item_id`
  - optional `file_role`
  - optional `file_media_category`
  - optional `file_tag`

### Implementation Notes

- multipart parsing uses the stdlib `email` parser rather than deprecated `cgi`
- storage keys are derived conservatively:
  - relative to store root for local file stores when possible
  - otherwise the stored file URL is kept as the key
- this keeps existing download/preview resolution working for both:
  - local-path stores
  - non-path stores such as `single_file_sqlite`

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `8 passed`
- broader interface slice:
  - `tests/interfaces/test_web_readwrite.py`
  - `tests/interfaces/test_web_readonly.py`
  - `tests/interfaces/test_web_calibre_readonly.py`
  - `29 passed`

### Updated Next Likely Steps

1. Add upload affordances closer to entity pages once file-to-item/work flows are clearer
2. Add item/expression/manifestation-aware attachment helpers so uploaded files can be linked into the WEMI chain directly
3. Add “create linked row” helpers for more link-rich tables beyond `works`
4. Add local-only guardrails and auth before any wider exposure

## Sixth Slice

Extended the upload flow from a generic file page into work/item-aware attachment paths.

### New Capability

The write interface can now:

- upload directly from `works` row pages
- upload directly from `items` row pages
- attach bytes to an existing item
- create a minimal WEMI chain for a work upload:
  - `expression`
  - `manifestation`
  - `item`
  - `file`

Current context-aware routes:

- `GET /tables/works/<id>/upload`
- `POST /tables/works/<id>/upload`
- `GET /tables/items/<id>/upload`
- `POST /tables/items/<id>/upload`

### Current Behavior

- `works` and `items` row pages now expose an `Upload file` action
- item upload pages show an `Attachment target` section and attach the created file row to the existing item
- work upload pages show a `Generated chain` section and create:
  - one new linked `expression`
  - one new linked `manifestation`
  - one new `item` pointing at that manifestation
  - one new `files` row pointing at that item
- contextual upload pages now cancel back to the originating row instead of the generic files table
- generic `/files/upload` remains available for store placement without a row context

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `10 passed`
- broader interface slice:
  - `tests/interfaces/test_web_readwrite.py`
  - `tests/interfaces/test_web_readonly.py`
  - `tests/interfaces/test_web_calibre_readonly.py`
  - `31 passed`

### Updated Next Likely Steps

1. Add upload/attach flows from richer entity pages such as manifestations or expressions if those become direct edit surfaces
2. Add store-placement choices and post-upload attachment helpers closer to work detail sections rather than as separate forms
3. Add local-only guardrails and auth before any wider exposure

## Seventh Slice

Tightened generic agent editing so the write interface no longer asks users to guess `agent_type`.

### New Capability

The `agents.agent_type` field now renders as a constrained select with the live alpha vocabulary:

- `person`
- `organisation`
- `group`
- `pseudonym`

This applies both to:

- generic agent create/edit pages
- the inline `Create contributor and link` helper on work pages

### Implementation Notes

- there is no live `agents__types` reference table yet
- the write interface now exposes the current schema-level allowed values explicitly instead of falling back to free text
- small constrained selects now also render an `Allowed values:` help line beneath the control

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `11 passed`

## Ninth Slice

Upgraded the generic write-form layer with better widgets for date/time, JSON, and URI/path-style fields.

### New Capability

The write interface now renders:

- `*_date` fields as date widgets where possible
- `*_timestamp_ep_k` fields as `datetime-local` widgets where possible
- `*json*` fields as guarded JSON textareas
- URI/path/location fields with explicit usage hints and safer text input attributes

### Behavior

- date fields now guide toward `YYYY-MM-DD`
- epoch-millisecond timestamp fields accept:
  - local datetime input
  - raw epoch milliseconds
- valid `datetime-local` input is converted back to stored epoch milliseconds on submit
- JSON fields:
  - pretty-print existing valid JSON on render
  - reject invalid JSON on submit with a normal write-form error
- URI/path/location fields now:
  - disable spellcheck/autocapitalize
  - show explicit hints about URI/path expectations

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `12 passed`

## Tenth Slice

Locked in browse/search machine-value formatting parity with the read-only interface.

### Implementation Note

No separate runtime formatter was added to `web_readwrite`.

Instead, the write interface continues to inherit:

- the shared browse-cell formatter from `web_readonly`
- the shared exact-search result table renderer from `web_readonly`

and now has direct regression coverage proving that inherited parity.

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `13 passed`

## Eighth Slice

Extended the inline allowed-values help from `agents.agent_type` to all constrained select fields in the write interface.

### New Capability

Any field rendered as a constrained select now shows its allowed values directly under the control.

This now covers:

- explicit hard-coded select fields such as:
  - `agents.agent_type`
  - `stores.store_kind`
  - `stores.store_access_protocol`
  - `stores.store_auth_method`
- table-backed `*_type` fields resolved from `__types` tables
- boolean selects

### Behavior

- small constrained sets list all values directly
- large constrained sets show a preview and then:
  - `+ N more in dropdown`

This keeps huge relation-code vocabularies usable without dumping hundreds of values inline.

### Validation Update

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/interfaces/test_web_readwrite.py`
  - `11 passed`
