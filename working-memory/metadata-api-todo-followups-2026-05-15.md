# Metadata API TODO Follow-Ups - 2026-05-15

Scope: relevant TODOs left in the metadata API/container slice after the WEMI
relation-key, relation-link, primary-link, projection-view, and stale-TODO
cleanup work.

Current branch context:

- Branch: `metadata-link-terminology-cleanup`
- Follow-up PR: `#44` `Remove stale WEMI metadata TODOs`
- Stale WEMI TODOs already removed in `e0b74d5`.
- Items 1-3 are committed in `817f3a6`.
- Validation for items 1-3:
  `python3 -m pytest tests/metadata/api tests/metadata/containers tests/surfaces/test_renderers_metadata.py -q`
  -> `218 passed, 1 warning`.
  `git diff --check` -> clean.
- Renderer consolidation follow-up is committed in `4d87f44`:
  - moved Calibre-shaped metadata HTML rendering into
    `src/LiuXin_alpha/surfaces/renderers/calibre_metadata.py`
  - kept `metadata.book.render` and `calibreMetadata.to_html()` as local-import
    compatibility delegates to avoid import loops
  - fixed moved legacy imports and Py3 `hexlify` string handling in
    `search_href()`
- Renderer follow-up validation:
  `python3 -m pytest tests/metadata/api/test_calibre_metadata_api.py tests/surfaces/test_renderers_metadata.py tests/surfaces/test_renderers_calibre_metadata.py -q`
  -> `10 passed`.
  `git diff --check` -> clean.
- Item 4 is committed in `67a2acd`:
  - split the generic agent profile contract into shared `AgentProfileAPI`,
    `HumanAgentProfileAPI`, and `OrganisationAgentProfileAPI`
  - added concrete `HumanAgentProfile` and `OrganisationAgentProfile`
    containers backed by flat data from `agents` plus the relevant
    `human_agents` / `org_agents` sidecar columns
  - kept `AgentProfile.from_mapping()` as a small factory for combined
    agent/sidecar mappings
- Item 4 validation:
  `python3 -m pytest tests/metadata/api tests/metadata/containers -q`
  -> `218 passed, 1 warning`.
- Item 5 is complete locally:
  - added shared `WemiIdentityAPI` for WEMI identity-row contracts
  - made work/expression/manifestation/item identity APIs inherit it and
    expose `WEMI_LEVEL`, `SOURCE_TABLE`, and `ID_FIELD`
  - replaced the high-level WEMI identity union alias with the shared base
    class export
- Item 5 validation:
  `python3 -m pytest tests/metadata/api/test_wemi_surface_symmetry.py tests/metadata/api/test_metadata_package_surface.py -q`
  -> `25 passed`.
  `python3 -m pytest tests/metadata/api tests/metadata/containers -q`
  -> `218 passed, 1 warning`.
  `git diff --check` -> clean.
- This list deliberately excludes older broad metadata TODOs in standardizers,
  file-source parsers, constants, and Calibre-like metadata containers.

## Numbered TODOs

1. DONE - Check row API coverage for metadata main tables.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/main_table_containers_api/row_api.py`
   - Current TODO: `Check we have one of these for every table`
   - Relevant because the API package should either expose a row protocol for
     every metadata-owned main table or document which tables intentionally use
     generic row mappings.
   - Resolution: `tests/metadata/api/test_non_wemi_container_api.py` now asserts
     that every registered concrete row in `NON_WEMI_MAIN_TABLE_ROW_CONTAINERS`
     has a matching exported `*RowAPI` protocol and satisfies it at runtime.
     The stale TODO comment was removed.

2. DONE - Decide whether `LiuXinTitleMetadataAPI.to_html()` belongs in surfaces.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/liuxin_metadata_api/liuxin_title_metadata_api.py`
   - Current TODO: `These might want to live over in surfaces`
   - Relevant because HTML rendering is presentation behavior and may not
     belong on the core metadata contract.
   - Resolution: moved metadata HTML rendering to
     `src/LiuXin_alpha/surfaces/renderers/metadata.py` as
     `metadata_to_html()`, removed `to_html()` from `LiuXinMetadataAPI`, and
     removed the concrete Calibre-like LiuXin metadata implementation method.
     Added an import-boundary test so importing `LiuXin_alpha.metadata.api`
     does not import `LiuXin_alpha.surfaces.renderers`.

3. DONE - Decide whether `LiuXinTitleMetadataAPI.format_series_index()` belongs in the
   core metadata API.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/liuxin_metadata_api/liuxin_title_metadata_api.py`
   - Current TODO: `Likewise this is an interface thing`
   - Relevant because series-index rendering is display/interface behavior, not
     obviously metadata storage behavior.
   - Resolution: moved series-index rendering to
     `src/LiuXin_alpha/surfaces/renderers/metadata.py` as
     `series_index_to_text()`, removed `format_series_index()` from
     `LiuXinMetadataAPI`, and removed the concrete Calibre-like LiuXin metadata
     implementation method.

4. DONE - Split `AgentProfileAPI` into human and organisation profile shapes.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/wemi_containers_api/agent_containers/agent_profile_api.py`
   - Current TODO: `HumanAgentProfile and OrganisationAgentProfile`
   - Relevant because humans and organisations likely need different intrinsic
     profile fields while still sharing identity/link semantics.

5. DONE - Consider a generic identity base class for WEMI identity APIs.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/wemi_containers_api/work_containers/work_identity_api.py`
   - Current TODO: `This feels like it could be a generic thing`
   - Relevant because work/expression/manifestation/item identity contracts
     repeat mapping/string/update surface patterns.
   - Resolution: added shared `WemiIdentityAPI` and made the four concrete
     WEMI identity APIs inherit it while publishing per-level table/id metadata.

6. Tighten `expression_flags` typing.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/wemi_containers_api/expression_containers/expression_identity_api.py`
   - Current TODO: `Not sure what this should be, but it shouldn't be a string.`
   - Relevant because flags should probably be a structured enum, bitset, or
     typed collection instead of a free-form text field.

7. Add or reject a `get_all_related` helper.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/wemi_containers_api/manifestation_containers/manifestation_metadata_api.py`
   - Current TODO: `Add "get_all_related" method`
   - Relevant because callers may need a typed way to enumerate every relation
     bucket without knowing each relation key in advance.

8. Clarify `manifestation_format_detail`.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/wemi_containers_api/manifestation_containers/manifestation_identity_api.py`
   - Current TODO: `Not... sure what this is/is for?`
   - Relevant because the field exists on the identity contract and hydrators;
     it needs either a documented meaning or removal/rename.

9. Consider a shared WEMI metadata object/relation-method base class.
   - Source: `src/LiuXin_alpha/metadata/api/containers_api/wemi_containers_api/item_containers/item_metadata_api.py`
   - Current TODO: `Common "WEMIObject" base class for these methods?`
   - Relevant because relation-link CRUD, primary-link helpers, validation, and
     convenience relation properties are repeated across WEMI metadata APIs.

## Suggested Working Order

Work down the numbered list unless a dependency suggests otherwise. Items 1-5
and the renderer consolidation follow-up are complete; item 6 is next. Item 9
is related to the WEMI identity-base work but should be reviewed separately:
item 9 is about metadata-bundle relation behavior.
