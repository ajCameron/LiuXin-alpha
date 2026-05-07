-- =====================================================
-- FRBR/WEMI read-model views
--
-- NOTE: Order matters when views depend on each other.
-- These are UI/compatibility helpers that project common "book-ish" surfaces
-- out of the canonical WEMI graph. They are intentionally lossy.
--
-- NOTE: We use DROP/CREATE for idempotency because SQLite's support for
--       `CREATE VIEW IF NOT EXISTS` varies across older builds.
-- =====================================================

-- -----------------------------------------------------
-- View: wemi_rays_v
--
-- A "ray" is one unique Work -> Expression -> Manifestation path.
-- This is a convenient unit for UI browsing, default selection, and
-- compatibility layers (e.g. calibre-like book rows).
-- -----------------------------------------------------

DROP VIEW IF EXISTS `wemi_rays_v`;

CREATE VIEW `wemi_rays_v` AS
SELECT
  w.`work_id` AS `work_id`,
  e.`expression_id` AS `expression_id`,
  m.`manifestation_id` AS `manifestation_id`,

  (w.`work_id` || ':' || e.`expression_id` || ':' || m.`manifestation_id`) AS `ray_id`,

  ewl.`expression_work_link_priority` AS `work_expression_priority`,
  ewl.`expression_work_link_primary`  AS `work_expression_primary`,
  ewl.`expression_work_link_origin`   AS `work_expression_origin`,

  eml.`expression_manifestation_link_priority` AS `expression_manifestation_priority`,
  eml.`expression_manifestation_link_primary`  AS `expression_manifestation_primary`,
  eml.`expression_manifestation_link_origin`   AS `expression_manifestation_origin`,

  -- Component display bits
  COALESCE(e.`expression_title_override`, w.`work_canonical_title`, w.`work_title`) AS `display_work_title`,

  COALESCE(
    e.`expression_label`,
    CASE
      WHEN e.`expression_year` IS NOT NULL THEN CAST(e.`expression_year` AS TEXT)
      ELSE NULL
    END
  ) AS `display_expression_bit`,

  TRIM(
    COALESCE(m.`manifestation_edition_statement`, '') ||
    CASE WHEN m.`manifestation_pub_year` IS NOT NULL THEN (' ' || CAST(m.`manifestation_pub_year` AS TEXT)) ELSE '' END ||
    CASE WHEN m.`manifestation_format_detail` IS NOT NULL THEN (' ' || m.`manifestation_format_detail`) ELSE '' END
  ) AS `display_manifestation_bit`,

  -- A calibre-ish combined title (lossy by design, but convenient)
  TRIM(
    COALESCE(e.`expression_title_override`, w.`work_title`, '') ||
    CASE
      WHEN COALESCE(e.`expression_label`, e.`expression_year`) IS NOT NULL
        THEN (' (' || COALESCE(e.`expression_label`, CAST(e.`expression_year` AS TEXT)) || ')')
      ELSE ''
    END ||
    CASE
      WHEN COALESCE(m.`manifestation_edition_statement`, m.`manifestation_format_detail`, m.`manifestation_pub_year`) IS NOT NULL
        THEN (' ' || TRIM(
          COALESCE(m.`manifestation_edition_statement`, '') ||
          CASE WHEN m.`manifestation_pub_year` IS NOT NULL THEN (' ' || CAST(m.`manifestation_pub_year` AS TEXT)) ELSE '' END ||
          CASE WHEN m.`manifestation_format_detail` IS NOT NULL THEN (' ' || m.`manifestation_format_detail`) ELSE '' END
        ))
      ELSE ''
    END
  ) AS `calibre_like_title`

FROM `works` w
JOIN `expression_work_links` ewl
  ON ewl.`expression_work_link_work_id` = w.`work_id`
JOIN `expressions` e
  ON e.`expression_id` = ewl.`expression_work_link_expression_id`
JOIN `expression_manifestation_links` eml
  ON eml.`expression_manifestation_link_expression_id` = e.`expression_id`
JOIN `manifestations` m
  ON m.`manifestation_id` = eml.`expression_manifestation_link_manifestation_id`
;


-- -----------------------------------------------------
-- View: wemi_primary_rays_v
--
-- A single default ray per Work, using primary markers and priority ordering.
-- -----------------------------------------------------

DROP VIEW IF EXISTS `wemi_primary_rays_v`;

CREATE VIEW `wemi_primary_rays_v` AS
SELECT *
FROM (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY r.`work_id`
      ORDER BY
        COALESCE(r.`work_expression_primary`, 0) DESC,
        COALESCE(r.`work_expression_priority`, 0) ASC,
        COALESCE(r.`expression_manifestation_primary`, 0) DESC,
        COALESCE(r.`expression_manifestation_priority`, 0) ASC,
        r.`expression_id` ASC,
        r.`manifestation_id` ASC
    ) AS `rn`
  FROM `wemi_rays_v` r
)
WHERE `rn` = 1
;


-- -----------------------------------------------------
-- View: wemi_ray_items_v
--
-- Expand rays to the item layer (one row per ray per item; ray rows remain even
-- when a manifestation has no items yet).
-- -----------------------------------------------------

DROP VIEW IF EXISTS `wemi_ray_items_v`;

CREATE VIEW `wemi_ray_items_v` AS
SELECT
  r.`ray_id`,
  r.`work_id`,
  r.`expression_id`,
  r.`manifestation_id`,
  i.`item_id`
FROM `wemi_rays_v` r
LEFT JOIN `items` i
  ON i.`item_manifestation_id` = r.`manifestation_id`
;


-- -----------------------------------------------------
-- View: wemi_work_stats_v
--
-- Helpful for top-level Work browsing UIs.
-- -----------------------------------------------------

DROP VIEW IF EXISTS `wemi_work_stats_v`;

CREATE VIEW `wemi_work_stats_v` AS
SELECT
  w.`work_id`,
  COALESCE(w.`work_canonical_title`, w.`work_title`) AS `work_display_title`,
  COUNT(DISTINCT r.`expression_id`) AS `expression_count`,
  COUNT(DISTINCT r.`manifestation_id`) AS `manifestation_count`,
  COUNT(DISTINCT i.`item_id`) AS `item_count`
FROM `works` w
LEFT JOIN `wemi_rays_v` r
  ON r.`work_id` = w.`work_id`
LEFT JOIN `items` i
  ON i.`item_manifestation_id` = r.`manifestation_id`
GROUP BY w.`work_id`
;


-- -----------------------------------------------------
-- View: titles_v
--
-- Compatibility view for the legacy LiuXin `titles` table.
-- One row per Work, with a deterministic "best guess" for publication date
-- via the Work's primary ray (if present).
--
-- This is intentionally lossy and read-only.
-- -----------------------------------------------------

DROP VIEW IF EXISTS `titles`;
DROP VIEW IF EXISTS `titles_v`;

CREATE VIEW `titles_v` AS
SELECT
  w.`work_id` AS `title_id`,

  COALESCE(w.`work_canonical_title`, w.`work_title`) AS `title`,
  COALESCE(w.`work_sort_title`, w.`work_canonical_title`, w.`work_title`) AS `title_sort`,
  NULL AS `title_phash`,

  -- Legacy field: creator sort. Not modelled directly in WEMI yet.
  NULL AS `title_creator_sort`,

  -- Publication / copyright dates: best-effort (ISO-ish).
  CASE
    WHEN m.`manifestation_pub_year` IS NOT NULL THEN printf('%04d-01-01', m.`manifestation_pub_year`)
    WHEN w.`work_original_year` IS NOT NULL THEN printf('%04d-01-01', w.`work_original_year`)
    ELSE NULL
  END AS `title_pub_date`,

  CASE
    WHEN w.`work_original_year` IS NOT NULL THEN printf('%04d-01-01', w.`work_original_year`)
    ELSE NULL
  END AS `title_copyright_date`,

  NULL AS `title_wikipedia`,
  NULL AS `title_fiction_length_category`,
  NULL AS `title_type`,
  NULL AS `title_wordcount`,

  COALESCE(pr.`work_expression_origin`, w.`work_discovery_note`) AS `title_source`,
  NULL AS `title_source_path`,
  NULL AS `title_source_name`,

  -- Legacy datestamps were DATETIME-ish strings; we project epoch_ms timestamps.
  datetime(COALESCE(w.`work_created_timestamp_ep_k`, 0) / 1000, 'unixepoch')  AS `title_created_datestamp`,
  datetime(COALESCE(w.`work_modified_timestamp_ep_k`, 0) / 1000, 'unixepoch') AS `title_datestamp`,
  datetime(COALESCE(w.`work_modified_timestamp_ep_k`, 0) / 1000, 'unixepoch') AS `title_last_modified`,

  w.`work_scratch` AS `title_scratch`

FROM `works` w
LEFT JOIN `wemi_primary_rays_v` pr
  ON pr.`work_id` = w.`work_id`
LEFT JOIN `manifestations` m
  ON m.`manifestation_id` = pr.`manifestation_id`
;

-- Alias for older code that expects a `titles` relation.
CREATE VIEW `titles` AS
SELECT * FROM `titles_v`
;


-- -----------------------------------------------------
-- View: books_v
--
-- A "book" is projected as one row per WEMI ray.
-- This aligns with the intuitive (calibre-ish) unit of "a specific edition/format"
-- without forcing the rest of the schema to pretend the world is a pyramid.
--
-- This is intentionally lossy and read-only.
-- -----------------------------------------------------

DROP VIEW IF EXISTS `books`;
DROP VIEW IF EXISTS `books_v`;

CREATE VIEW `books_v` AS
WITH `item_atomic_assets` AS (
  SELECT
    r.`ray_id` AS `ray_id`,
    r.`work_id` AS `work_id`,
    r.`expression_id` AS `expression_id`,
    r.`manifestation_id` AS `manifestation_id`,
    i.`item_id` AS `item_id`,
    da.`digital_asset_id` AS `digital_asset_id`,
    dal.`digital_asset_item_link_type` AS `digital_asset_link_type`,
    da.`digital_asset_media_category` AS `digital_asset_media_category`,
    da.`digital_asset_size_bytes` AS `digital_asset_size_bytes`
  FROM `wemi_rays_v` r
  JOIN `items` i
    ON i.`item_manifestation_id` = r.`manifestation_id`
  JOIN `digital_asset_item_links` dal
    ON dal.`digital_asset_item_link_item_id` = i.`item_id`
  JOIN `digital_assets` da
    ON da.`digital_asset_id` = dal.`digital_asset_item_link_digital_asset_id`

  UNION ALL

  SELECT
    r.`ray_id` AS `ray_id`,
    r.`work_id` AS `work_id`,
    r.`expression_id` AS `expression_id`,
    r.`manifestation_id` AS `manifestation_id`,
    i.`item_id` AS `item_id`,
    da.`digital_asset_id` AS `digital_asset_id`,
    cdail.`composite_digital_asset_item_link_type` AS `digital_asset_link_type`,
    da.`digital_asset_media_category` AS `digital_asset_media_category`,
    da.`digital_asset_size_bytes` AS `digital_asset_size_bytes`
  FROM `wemi_rays_v` r
  JOIN `items` i
    ON i.`item_manifestation_id` = r.`manifestation_id`
  JOIN `composite_digital_asset_item_links` cdail
    ON cdail.`composite_digital_asset_item_link_item_id` = i.`item_id`
  JOIN `composite_digital_asset_digital_asset_links` cdadl
    ON cdadl.`composite_digital_asset_digital_asset_link_composite_digital_asset_id` = cdail.`composite_digital_asset_item_link_composite_digital_asset_id`
  JOIN `digital_assets` da
    ON da.`digital_asset_id` = cdadl.`composite_digital_asset_digital_asset_link_digital_asset_id`
),
`ray_atomic_assets` AS (
  SELECT DISTINCT
    `ray_id`,
    `digital_asset_id`,
    `digital_asset_link_type`,
    `digital_asset_media_category`,
    `digital_asset_size_bytes`
  FROM `item_atomic_assets`
)
SELECT
  r.`ray_id` AS `book_id`,

  -- Sort/display helpers (legacy-ish)
  r.`calibre_like_title` AS `book_sort`,
  NULL AS `book_flags`,

  -- Publication / copyright dates (ISO-ish)
  COALESCE(
    m.`manifestation_pub_date`,
    CASE
      WHEN m.`manifestation_pub_year` IS NOT NULL THEN printf('%04d-01-01', m.`manifestation_pub_year`)
      WHEN w.`work_original_year` IS NOT NULL THEN printf('%04d-01-01', w.`work_original_year`)
      ELSE NULL
    END
  ) AS `book_pubdate`,

  CASE
    WHEN w.`work_original_year` IS NOT NULL THEN printf('%04d-01-01', w.`work_original_year`)
    ELSE NULL
  END AS `book_copyright_date`,

  -- Stable identity token for the projected book.
  r.`ray_id` AS `book_uuid`,

  -- Covers: best-effort, derived from any atomic assets reachable from the item layer.
  CASE
    WHEN SUM(
      CASE
        WHEN raa.`digital_asset_link_type` = 'cover' OR raa.`digital_asset_media_category` = 'cover' THEN 1
        ELSE 0
      END
    ) > 0 THEN 1
    ELSE 0
  END AS `book_has_cover`,

  0 AS `book_has_local_cover`,

  -- Last-modified: max modified timestamp across the ray nodes.
  datetime(
    (MAX(
      COALESCE(w.`work_modified_timestamp_ep_k`, 0),
      COALESCE(e.`expression_modified_timestamp_ep_k`, 0),
      COALESCE(m.`manifestation_modified_timestamp_ep_k`, 0)
    )) / 1000,
    'unixepoch'
  ) AS `book_last_modified`,

  NULL AS `book_fingerprint`,
  NULL AS `book_paths`,

  -- Size: sum the distinct atomic assets reachable from the ray's items.
  COALESCE(SUM(COALESCE(raa.`digital_asset_size_bytes`, 0)), 0) AS `book_size`,

  NULL AS `book_rating`,

  -- Legacy datestamps were DATETIME-ish strings; we project epoch_ms timestamps.
  datetime(
    (MIN(
      COALESCE(w.`work_created_timestamp_ep_k`, 0),
      COALESCE(e.`expression_created_timestamp_ep_k`, 0),
      COALESCE(m.`manifestation_created_timestamp_ep_k`, 0)
    )) / 1000,
    'unixepoch'
  ) AS `book_created_datestamp`,

  datetime(
    (MAX(
      COALESCE(w.`work_modified_timestamp_ep_k`, 0),
      COALESCE(e.`expression_modified_timestamp_ep_k`, 0),
      COALESCE(m.`manifestation_modified_timestamp_ep_k`, 0)
    )) / 1000,
    'unixepoch'
  ) AS `book_datestamp`,

  NULL AS `book_scratch`,

  -- Extra traceability fields (non-legacy, but extremely useful).
  r.`work_id` AS `book_work_id`,
  r.`expression_id` AS `book_expression_id`,
  r.`manifestation_id` AS `book_manifestation_id`,
  r.`ray_id` AS `book_ray_id`

FROM `wemi_rays_v` r
JOIN `works` w
  ON w.`work_id` = r.`work_id`
JOIN `expressions` e
  ON e.`expression_id` = r.`expression_id`
JOIN `manifestations` m
  ON m.`manifestation_id` = r.`manifestation_id`
LEFT JOIN `ray_atomic_assets` raa
  ON raa.`ray_id` = r.`ray_id`
GROUP BY r.`ray_id`
;

-- Alias for older code that expects a `books` relation.
CREATE VIEW `books` AS
SELECT * FROM `books_v`
;


-- -----------------------------------------------------
-- View: digital_asset_inventory_v
--
-- A storage-centric view of all digital assets attached to Items that belong to a ray.
-- Rows are emitted per (ray, digital_asset, replica).
-- -----------------------------------------------------

DROP VIEW IF EXISTS `file_inventory_v`;
DROP VIEW IF EXISTS `digital_asset_inventory_v`;

CREATE VIEW `digital_asset_inventory_v` AS
WITH `item_atomic_assets` AS (
  SELECT
    r.`ray_id` AS `book_id`,
    r.`ray_id` AS `ray_id`,
    r.`work_id` AS `work_id`,
    r.`expression_id` AS `expression_id`,
    r.`manifestation_id` AS `manifestation_id`,
    i.`item_id` AS `item_id`,

    da.`digital_asset_id` AS `digital_asset_id`,
    dal.`digital_asset_item_link_type` AS `digital_asset_link_type`,
    dal.`digital_asset_item_link_priority` AS `digital_asset_link_priority`,
    dal.`digital_asset_item_link_primary` AS `digital_asset_link_primary`,
    dal.`digital_asset_item_link_origin` AS `digital_asset_link_origin`,

    NULL AS `composite_digital_asset_id`,
    NULL AS `composite_digital_asset_name`,
    NULL AS `composite_digital_asset_link_type`,
    NULL AS `composite_digital_asset_link_priority`,
    NULL AS `composite_digital_asset_link_primary`,
    NULL AS `composite_digital_asset_link_origin`,

    NULL AS `composite_member_link_type`,
    NULL AS `composite_member_sequence_number`,
    NULL AS `composite_member_is_required`,

    'direct' AS `digital_asset_attachment_scope`
  FROM `wemi_rays_v` r
  JOIN `items` i
    ON i.`item_manifestation_id` = r.`manifestation_id`
  JOIN `digital_asset_item_links` dal
    ON dal.`digital_asset_item_link_item_id` = i.`item_id`
  JOIN `digital_assets` da
    ON da.`digital_asset_id` = dal.`digital_asset_item_link_digital_asset_id`

  UNION ALL

  SELECT
    r.`ray_id` AS `book_id`,
    r.`ray_id` AS `ray_id`,
    r.`work_id` AS `work_id`,
    r.`expression_id` AS `expression_id`,
    r.`manifestation_id` AS `manifestation_id`,
    i.`item_id` AS `item_id`,

    da.`digital_asset_id` AS `digital_asset_id`,
    cdail.`composite_digital_asset_item_link_type` AS `digital_asset_link_type`,
    cdail.`composite_digital_asset_item_link_priority` AS `digital_asset_link_priority`,
    cdail.`composite_digital_asset_item_link_primary` AS `digital_asset_link_primary`,
    cdail.`composite_digital_asset_item_link_origin` AS `digital_asset_link_origin`,

    cda.`composite_digital_asset_id` AS `composite_digital_asset_id`,
    cda.`composite_digital_asset_name` AS `composite_digital_asset_name`,
    cdail.`composite_digital_asset_item_link_type` AS `composite_digital_asset_link_type`,
    cdail.`composite_digital_asset_item_link_priority` AS `composite_digital_asset_link_priority`,
    cdail.`composite_digital_asset_item_link_primary` AS `composite_digital_asset_link_primary`,
    cdail.`composite_digital_asset_item_link_origin` AS `composite_digital_asset_link_origin`,

    cdadl.`composite_digital_asset_digital_asset_link_type` AS `composite_member_link_type`,
    cdadl.`composite_digital_asset_digital_asset_link_sequence_number` AS `composite_member_sequence_number`,
    cdadl.`composite_digital_asset_digital_asset_link_is_required` AS `composite_member_is_required`,

    'composite_member' AS `digital_asset_attachment_scope`
  FROM `wemi_rays_v` r
  JOIN `items` i
    ON i.`item_manifestation_id` = r.`manifestation_id`
  JOIN `composite_digital_asset_item_links` cdail
    ON cdail.`composite_digital_asset_item_link_item_id` = i.`item_id`
  JOIN `composite_digital_assets` cda
    ON cda.`composite_digital_asset_id` = cdail.`composite_digital_asset_item_link_composite_digital_asset_id`
  JOIN `composite_digital_asset_digital_asset_links` cdadl
    ON cdadl.`composite_digital_asset_digital_asset_link_composite_digital_asset_id` = cda.`composite_digital_asset_id`
  JOIN `digital_assets` da
    ON da.`digital_asset_id` = cdadl.`composite_digital_asset_digital_asset_link_digital_asset_id`
)
SELECT
  iaa.`book_id` AS `book_id`,
  iaa.`ray_id` AS `ray_id`,

  iaa.`work_id` AS `work_id`,
  iaa.`expression_id` AS `expression_id`,
  iaa.`manifestation_id` AS `manifestation_id`,

  iaa.`item_id` AS `item_id`,

  iaa.`digital_asset_id` AS `digital_asset_id`,
  iaa.`digital_asset_link_type` AS `digital_asset_link_type`,
  iaa.`digital_asset_link_priority` AS `digital_asset_link_priority`,
  iaa.`digital_asset_link_primary` AS `digital_asset_link_primary`,
  iaa.`digital_asset_link_origin` AS `digital_asset_link_origin`,

  ar.`asset_replica_id` AS `asset_replica_id`,
  ar.`asset_replica_store_id` AS `asset_replica_store_id`,
  s.`store_name` AS `store_name`,
  s.`store_kind` AS `store_kind`,
  s.`store_access_protocol` AS `store_access_protocol`,
  s.`store_operational_role` AS `store_operational_role`,
  s.`store_root_uri` AS `store_root_uri`,

  ar.`asset_replica_folder_id` AS `asset_replica_folder_id`,
  fo.`folder_relpath` AS `folder_relpath`,

  ar.`asset_replica_storage_key` AS `asset_replica_storage_key`,

  CASE
    WHEN s.`store_root_uri` IS NULL THEN NULL
    WHEN substr(s.`store_root_uri`, -1) = '/' THEN s.`store_root_uri` || ar.`asset_replica_storage_key`
    ELSE s.`store_root_uri` || '/' || ar.`asset_replica_storage_key`
  END AS `asset_replica_uri`,

  da.`digital_asset_name` AS `digital_asset_name`,
  da.`digital_asset_base_name` AS `digital_asset_base_name`,
  da.`digital_asset_extension` AS `digital_asset_extension`,
  da.`digital_asset_tag` AS `digital_asset_tag`,
  da.`digital_asset_auto_name` AS `digital_asset_auto_name`,
  da.`digital_asset_use_auto_name` AS `digital_asset_use_auto_name`,

  da.`digital_asset_mime_type` AS `digital_asset_mime_type`,
  da.`digital_asset_media_category` AS `digital_asset_media_category`,
  da.`digital_asset_class_mask` AS `digital_asset_class_mask`,
  da.`digital_asset_visibility_mask` AS `digital_asset_visibility_mask`,
  da.`digital_asset_critical` AS `digital_asset_critical`,

  da.`digital_asset_size_bytes` AS `digital_asset_size_bytes`,
  da.`digital_asset_hash_sha256` AS `digital_asset_hash_sha256`,
  da.`digital_asset_hash_blake3` AS `digital_asset_hash_blake3`,
  da.`digital_asset_phash` AS `digital_asset_phash`,
  da.`digital_asset_corrupt` AS `digital_asset_corrupt`,
  da.`digital_asset_integrity_status` AS `digital_asset_integrity_status`,

  ar.`asset_replica_presence_status` AS `asset_replica_presence_status`,
  ar.`asset_replica_integrity_status` AS `asset_replica_integrity_status`,
  ar.`asset_replica_failure_reason` AS `asset_replica_failure_reason`,
  ar.`asset_replica_observed_size_bytes` AS `asset_replica_observed_size_bytes`,
  ar.`asset_replica_observed_hash_sha256` AS `asset_replica_observed_hash_sha256`,
  ar.`asset_replica_observed_hash_blake3` AS `asset_replica_observed_hash_blake3`,

  da.`digital_asset_source` AS `digital_asset_source`,
  da.`digital_asset_original_name` AS `digital_asset_original_name`,
  da.`digital_asset_original_path` AS `digital_asset_original_path`,
  da.`digital_asset_processed` AS `digital_asset_processed`,

  datetime(COALESCE(da.`digital_asset_created_timestamp_ep_k`, 0) / 1000, 'unixepoch') AS `digital_asset_created_datestamp`,
  datetime(COALESCE(da.`digital_asset_modified_timestamp_ep_k`, 0) / 1000, 'unixepoch') AS `digital_asset_datestamp`,

  CASE
    WHEN iaa.`digital_asset_link_type` = 'cover' OR da.`digital_asset_media_category` = 'cover' THEN 1
    ELSE 0
  END AS `digital_asset_is_cover`,

  iaa.`digital_asset_attachment_scope` AS `digital_asset_attachment_scope`,
  iaa.`composite_digital_asset_id` AS `composite_digital_asset_id`,
  iaa.`composite_digital_asset_name` AS `composite_digital_asset_name`,
  iaa.`composite_digital_asset_link_type` AS `composite_digital_asset_link_type`,
  iaa.`composite_digital_asset_link_priority` AS `composite_digital_asset_link_priority`,
  iaa.`composite_digital_asset_link_primary` AS `composite_digital_asset_link_primary`,
  iaa.`composite_digital_asset_link_origin` AS `composite_digital_asset_link_origin`,
  iaa.`composite_member_link_type` AS `composite_member_link_type`,
  iaa.`composite_member_sequence_number` AS `composite_member_sequence_number`,
  iaa.`composite_member_is_required` AS `composite_member_is_required`

FROM `item_atomic_assets` iaa
JOIN `digital_assets` da
  ON da.`digital_asset_id` = iaa.`digital_asset_id`
LEFT JOIN `asset_replicas` ar
  ON ar.`asset_replica_digital_asset_id` = da.`digital_asset_id`
LEFT JOIN `stores` s
  ON s.`store_id` = ar.`asset_replica_store_id`
LEFT JOIN `folders` fo
  ON fo.`folder_id` = ar.`asset_replica_folder_id`
;

-- Compatibility alias for older code that still expects a file-shaped inventory view.
CREATE VIEW `file_inventory_v` AS
SELECT
  `asset_replica_id` AS `file_id`,
  `book_id`,
  `ray_id`,
  `work_id`,
  `expression_id`,
  `manifestation_id`,
  `item_id`,
  `digital_asset_id`,
  `asset_replica_storage_key` AS `file_storage_key`,
  `asset_replica_uri` AS `file_uri`,
  CASE
    WHEN `digital_asset_link_type` = 'primary_payload' THEN 'content'
    ELSE `digital_asset_link_type`
  END AS `file_role`,
  `digital_asset_is_cover` AS `file_is_cover`,
  `digital_asset_size_bytes` AS `file_size_bytes`,
  `asset_replica_store_id` AS `file_store_id`,
  `asset_replica_folder_id` AS `file_folder_id`,
  `store_name`,
  `store_kind`,
  `store_access_protocol`,
  `store_operational_role`,
  `store_root_uri`,
  `folder_relpath`
FROM `digital_asset_inventory_v`
;



-- -----------------------------------------------------
-- View: agent_credits_v
--
-- Flatten agent credits onto "book-ish" rays.
-- One row per (ray, scope entity, agent, credit_type).
--
-- In this schema, "role/credit kind" is modelled as the link `*_type` value
-- (typically a MARC relator code such as 'aut', 'trl', etc.).
-- -----------------------------------------------------

DROP VIEW IF EXISTS `agent_credits_v`;

CREATE VIEW `agent_credits_v` AS
WITH
  rays AS (
    SELECT
      r.`ray_id`           AS `book_id`,
      r.`ray_id`           AS `ray_id`,
      r.`work_id`          AS `work_id`,
      r.`expression_id`    AS `expression_id`,
      r.`manifestation_id` AS `manifestation_id`
    FROM `wemi_rays_v` r
  ),
  ray_items AS (
    SELECT
      r.`ray_id`           AS `book_id`,
      r.`ray_id`           AS `ray_id`,
      r.`work_id`          AS `work_id`,
      r.`expression_id`    AS `expression_id`,
      r.`manifestation_id` AS `manifestation_id`,
      i.`item_id`          AS `item_id`
    FROM `wemi_rays_v` r
    JOIN `items` i
      ON i.`item_manifestation_id` = r.`manifestation_id`
  )
SELECT
  c.`book_id`,
  c.`ray_id`,
  c.`work_id`,
  c.`expression_id`,
  c.`manifestation_id`,
  c.`item_id`,

  c.`credit_entity_type`,
  c.`credit_entity_id`,
  c.`credit_scope_rank`,

  a.`agent_id`             AS `agent_id`,
  a.`agent_type`           AS `agent_type`,
  a.`agent_canonical_name` AS `agent_canonical_name`,
  a.`agent_sort_name`      AS `agent_sort_name`,

  c.`credit_type`,
  c.`credit_priority`,
  c.`credit_origin`,
  c.`credit_datestamp`

FROM (
  -- Work-scoped credits
  SELECT
    r.`book_id`,
    r.`ray_id`,
    r.`work_id`,
    r.`expression_id`,
    r.`manifestation_id`,
    NULL AS `item_id`,

    'work' AS `credit_entity_type`,
    r.`work_id` AS `credit_entity_id`,
    1 AS `credit_scope_rank`,

    awl.`agent_work_link_agent_id` AS `agent_id`,
    awl.`agent_work_link_type`     AS `credit_type`,
    COALESCE(awl.`agent_work_link_priority`, 0) AS `credit_priority`,
    NULL AS `credit_origin`,
    awl.`agent_work_link_datestamp` AS `credit_datestamp`
  FROM `rays` r
  JOIN `agent_work_links` awl
    ON awl.`agent_work_link_work_id` = r.`work_id`

  UNION ALL

  -- Expression-scoped credits
  SELECT
    r.`book_id`,
    r.`ray_id`,
    r.`work_id`,
    r.`expression_id`,
    r.`manifestation_id`,
    NULL AS `item_id`,

    'expression' AS `credit_entity_type`,
    r.`expression_id` AS `credit_entity_id`,
    2 AS `credit_scope_rank`,

    ael.`agent_expression_link_agent_id` AS `agent_id`,
    ael.`agent_expression_link_type`     AS `credit_type`,
    COALESCE(ael.`agent_expression_link_priority`, 0) AS `credit_priority`,
    ael.`agent_expression_link_origin`   AS `credit_origin`,
    ael.`agent_expression_link_datestamp` AS `credit_datestamp`
  FROM `rays` r
  JOIN `agent_expression_links` ael
    ON ael.`agent_expression_link_expression_id` = r.`expression_id`

  UNION ALL

  -- Manifestation-scoped credits
  SELECT
    r.`book_id`,
    r.`ray_id`,
    r.`work_id`,
    r.`expression_id`,
    r.`manifestation_id`,
    NULL AS `item_id`,

    'manifestation' AS `credit_entity_type`,
    r.`manifestation_id` AS `credit_entity_id`,
    3 AS `credit_scope_rank`,

    aml.`agent_manifestation_link_agent_id` AS `agent_id`,
    aml.`agent_manifestation_link_type`     AS `credit_type`,
    COALESCE(aml.`agent_manifestation_link_priority`, 0) AS `credit_priority`,
    NULL AS `credit_origin`,
    aml.`agent_manifestation_link_datestamp` AS `credit_datestamp`
  FROM `rays` r
  JOIN `agent_manifestation_links` aml
    ON aml.`agent_manifestation_link_manifestation_id` = r.`manifestation_id`

  UNION ALL

  -- Item-scoped credits (only where items exist)
  SELECT
    ri.`book_id`,
    ri.`ray_id`,
    ri.`work_id`,
    ri.`expression_id`,
    ri.`manifestation_id`,
    ri.`item_id`,

    'item' AS `credit_entity_type`,
    ri.`item_id` AS `credit_entity_id`,
    4 AS `credit_scope_rank`,

    ail.`agent_item_link_agent_id` AS `agent_id`,
    ail.`agent_item_link_type`     AS `credit_type`,
    COALESCE(ail.`agent_item_link_priority`, 0) AS `credit_priority`,
    ail.`agent_item_link_origin`   AS `credit_origin`,
    ail.`agent_item_link_datestamp` AS `credit_datestamp`
  FROM `ray_items` ri
  JOIN `agent_item_links` ail
    ON ail.`agent_item_link_item_id` = ri.`item_id`

) c
JOIN `agents` a
  ON a.`agent_id` = c.`agent_id`
;


-- -----------------------------------------------------
-- View: book_publishers_v
--
-- Project publisher-ish agent credits onto rays.
--
-- We treat MARC relator code 'pbl' as "publisher". (See constants/marc_relator_dicts.py)
--
-- NOTE:
--  - This is a *compatibility* surface. WEMI allows multiple publishers/imprints/etc.
--    Calibre (and many UIs) want a single display publisher.
--  - We therefore provide BOTH:
--      * book_publishers_v : all publisher credits per book (ray)
--      * publishers_v      : the deterministic "best" publisher per book (ray)
-- -----------------------------------------------------

DROP VIEW IF EXISTS `publishers_v`;
DROP VIEW IF EXISTS `book_publishers_v`;

CREATE VIEW `book_publishers_v` AS
WITH
  candidates AS (
    -- Prefer Manifestation-scoped publisher credits.
    SELECT
      r.`ray_id` AS `book_id`,
      r.`ray_id` AS `ray_id`,
      r.`work_id` AS `work_id`,
      r.`expression_id` AS `expression_id`,
      r.`manifestation_id` AS `manifestation_id`,

      'manifestation' AS `publisher_scope`,
      1 AS `publisher_scope_preference`,

      aml.`agent_manifestation_link_agent_id` AS `publisher_agent_id`,
      aml.`agent_manifestation_link_type` AS `publisher_credit_type`,
      COALESCE(aml.`agent_manifestation_link_priority`, 0) AS `publisher_priority`,
      NULL AS `publisher_origin`,
      aml.`agent_manifestation_link_datestamp` AS `publisher_datestamp`
    FROM `wemi_rays_v` r
    JOIN `agent_manifestation_links` aml
      ON aml.`agent_manifestation_link_manifestation_id` = r.`manifestation_id`
    WHERE aml.`agent_manifestation_link_type` = 'pbl'

    UNION ALL

    -- Fall back to Expression-scoped publisher credits.
    SELECT
      r.`ray_id` AS `book_id`,
      r.`ray_id` AS `ray_id`,
      r.`work_id` AS `work_id`,
      r.`expression_id` AS `expression_id`,
      r.`manifestation_id` AS `manifestation_id`,

      'expression' AS `publisher_scope`,
      2 AS `publisher_scope_preference`,

      ael.`agent_expression_link_agent_id` AS `publisher_agent_id`,
      ael.`agent_expression_link_type` AS `publisher_credit_type`,
      COALESCE(ael.`agent_expression_link_priority`, 0) AS `publisher_priority`,
      ael.`agent_expression_link_origin` AS `publisher_origin`,
      ael.`agent_expression_link_datestamp` AS `publisher_datestamp`
    FROM `wemi_rays_v` r
    JOIN `agent_expression_links` ael
      ON ael.`agent_expression_link_expression_id` = r.`expression_id`
    WHERE ael.`agent_expression_link_type` = 'pbl'

    UNION ALL

    -- Final fall back: Work-scoped publisher credits.
    SELECT
      r.`ray_id` AS `book_id`,
      r.`ray_id` AS `ray_id`,
      r.`work_id` AS `work_id`,
      r.`expression_id` AS `expression_id`,
      r.`manifestation_id` AS `manifestation_id`,

      'work' AS `publisher_scope`,
      3 AS `publisher_scope_preference`,

      awl.`agent_work_link_agent_id` AS `publisher_agent_id`,
      awl.`agent_work_link_type` AS `publisher_credit_type`,
      COALESCE(awl.`agent_work_link_priority`, 0) AS `publisher_priority`,
      NULL AS `publisher_origin`,
      awl.`agent_work_link_datestamp` AS `publisher_datestamp`
    FROM `wemi_rays_v` r
    JOIN `agent_work_links` awl
      ON awl.`agent_work_link_work_id` = r.`work_id`
    WHERE awl.`agent_work_link_type` = 'pbl'
  )
SELECT
  c.`book_id`,
  c.`ray_id`,
  c.`work_id`,
  c.`expression_id`,
  c.`manifestation_id`,

  c.`publisher_scope`,
  c.`publisher_scope_preference`,

  a.`agent_id` AS `publisher_agent_id`,
  a.`agent_type` AS `publisher_agent_type`,
  a.`agent_canonical_name` AS `publisher_name`,
  COALESCE(a.`agent_sort_name`, a.`agent_canonical_name`) AS `publisher_sort_name`,

  c.`publisher_credit_type`,
  c.`publisher_priority`,
  c.`publisher_origin`,
  c.`publisher_datestamp`

FROM candidates c
JOIN `agents` a
  ON a.`agent_id` = c.`publisher_agent_id`
;


-- -----------------------------------------------------
-- View: publishers_v
--
-- Deterministic "best" publisher per book (ray).
--
-- Ordering rules:
--  1) Prefer manifestation -> expression -> work scope
--  2) Prefer organisation/group agents over persons (but allow persons for self-pub)
--  3) Lower link priority first
--  4) Stable name/id tie-breaks
-- -----------------------------------------------------

CREATE VIEW `publishers_v` AS
SELECT *
FROM (
  SELECT
    bp.*,
    ROW_NUMBER() OVER (
      PARTITION BY bp.`book_id`
      ORDER BY
        bp.`publisher_scope_preference` ASC,
        CASE bp.`publisher_agent_type`
          WHEN 'organisation' THEN 1
          WHEN 'group' THEN 2
          WHEN 'pseudonym' THEN 3
          WHEN 'person' THEN 4
          ELSE 9
        END ASC,
        COALESCE(bp.`publisher_priority`, 0) ASC,
        bp.`publisher_sort_name` ASC,
        bp.`publisher_name` ASC,
        bp.`publisher_agent_id` ASC
    ) AS `rn`
  FROM `book_publishers_v` bp
)
WHERE `rn` = 1
;


-- -----------------------------------------------------
-- View: subjects_tags_v
--
-- Unify "tag-like" metadata for UI browsing and search seeding.
--
-- This projects three common facet families onto rays (books):
--   * subjects  (hierarchical, work-scoped)
--   * genres    (hierarchical, work-scoped)
--   * tags      (flat descriptive tags; may be attached at work/expression/item scope)
--
-- The view emits ONE ROW per (book_id, facet_kind, facet_scope, facet_id).
-- UIs can GROUP_CONCAT or de-duplicate as they see fit.
-- -----------------------------------------------------

DROP VIEW IF EXISTS `subjects_tags_v`;

CREATE VIEW `subjects_tags_v` AS

-- Subjects (work scoped)
SELECT
  r.`ray_id` AS `book_id`,
  r.`ray_id` AS `ray_id`,
  r.`work_id` AS `work_id`,
  r.`expression_id` AS `expression_id`,
  r.`manifestation_id` AS `manifestation_id`,

  'subject' AS `facet_kind`,
  'work' AS `facet_scope`,
  1 AS `facet_scope_rank`,

  s.`subject_id` AS `facet_id`,
  COALESCE(s.`subject_full`, s.`subject`) AS `facet_text`,
  COALESCE(s.`subject_sort`, s.`subject_full`, s.`subject`) AS `facet_sort`,
  COALESCE(swl.`subject_work_link_priority`, 0) AS `facet_priority`,
  NULL AS `facet_type`

FROM `wemi_rays_v` r
JOIN `subject_work_links` swl
  ON swl.`subject_work_link_work_id` = r.`work_id`
JOIN `subjects` s
  ON s.`subject_id` = swl.`subject_work_link_subject_id`

UNION ALL

-- Genres (work scoped)
SELECT
  r.`ray_id` AS `book_id`,
  r.`ray_id` AS `ray_id`,
  r.`work_id` AS `work_id`,
  r.`expression_id` AS `expression_id`,
  r.`manifestation_id` AS `manifestation_id`,

  'genre' AS `facet_kind`,
  'work' AS `facet_scope`,
  1 AS `facet_scope_rank`,

  g.`genre_id` AS `facet_id`,
  COALESCE(g.`genre_full`, g.`genre`) AS `facet_text`,
  COALESCE(g.`genre_sort`, g.`genre_full`, g.`genre`) AS `facet_sort`,
  COALESCE(gwl.`genre_work_link_priority`, 0) AS `facet_priority`,
  gwl.`genre_work_link_type` AS `facet_type`

FROM `wemi_rays_v` r
JOIN `genre_work_links` gwl
  ON gwl.`genre_work_link_work_id` = r.`work_id`
JOIN `genres` g
  ON g.`genre_id` = gwl.`genre_work_link_genre_id`

UNION ALL

-- Tags (work scoped)
SELECT
  r.`ray_id` AS `book_id`,
  r.`ray_id` AS `ray_id`,
  r.`work_id` AS `work_id`,
  r.`expression_id` AS `expression_id`,
  r.`manifestation_id` AS `manifestation_id`,

  'tag' AS `facet_kind`,
  'work' AS `facet_scope`,
  1 AS `facet_scope_rank`,

  t.`tag_id` AS `facet_id`,
  t.`tag` AS `facet_text`,
  t.`tag_phash` AS `facet_sort`,
  COALESCE(twl.`tag_work_link_priority`, 0) AS `facet_priority`,
  NULL AS `facet_type`

FROM `wemi_rays_v` r
JOIN `tag_work_links` twl
  ON twl.`tag_work_link_work_id` = r.`work_id`
JOIN `tags` t
  ON t.`tag_id` = twl.`tag_work_link_tag_id`

UNION ALL

-- Tags (expression scoped)
SELECT
  r.`ray_id` AS `book_id`,
  r.`ray_id` AS `ray_id`,
  r.`work_id` AS `work_id`,
  r.`expression_id` AS `expression_id`,
  r.`manifestation_id` AS `manifestation_id`,

  'tag' AS `facet_kind`,
  'expression' AS `facet_scope`,
  2 AS `facet_scope_rank`,

  t.`tag_id` AS `facet_id`,
  t.`tag` AS `facet_text`,
  t.`tag_phash` AS `facet_sort`,
  COALESCE(etl.`expression_tag_link_priority`, 0) AS `facet_priority`,
  NULL AS `facet_type`

FROM `wemi_rays_v` r
JOIN `expression_tag_links` etl
  ON etl.`expression_tag_link_expression_id` = r.`expression_id`
JOIN `tags` t
  ON t.`tag_id` = etl.`expression_tag_link_tag_id`

UNION ALL

-- Tags (item scoped)
SELECT
  r.`ray_id` AS `book_id`,
  r.`ray_id` AS `ray_id`,
  r.`work_id` AS `work_id`,
  r.`expression_id` AS `expression_id`,
  r.`manifestation_id` AS `manifestation_id`,

  'tag' AS `facet_kind`,
  'item' AS `facet_scope`,
  3 AS `facet_scope_rank`,

  t.`tag_id` AS `facet_id`,
  t.`tag` AS `facet_text`,
  t.`tag_phash` AS `facet_sort`,
  COALESCE(itl.`item_tag_link_priority`, 0) AS `facet_priority`,
  NULL AS `facet_type`

FROM `wemi_rays_v` r
JOIN `items` i
  ON i.`item_manifestation_id` = r.`manifestation_id`
JOIN `item_tag_links` itl
  ON itl.`item_tag_link_item_id` = i.`item_id`
JOIN `tags` t
  ON t.`tag_id` = itl.`item_tag_link_tag_id`
;


-- -----------------------------------------------------
-- View: identifiers_v
--
-- One row per identifier, across both:
--   * entity_identifiers (curated, per Work/Expression/Manifestation/Item/Agent)
--   * item_identifiers   (raw observations on Items)
--
-- This is intended for UI/debugging/interoperability.
-- It is read-only.
-- -----------------------------------------------------

DROP VIEW IF EXISTS `identifiers`;
DROP VIEW IF EXISTS `identifiers_v`;

CREATE VIEW `identifiers_v` AS
WITH
  base AS (
    SELECT
      'entity' AS `identifier_origin`,
      ei.`entity_identifier_id` AS `identifier_id`,
      ei.`entity_identifier_entity_type` AS `entity_type`,
      ei.`entity_identifier_entity_id` AS `entity_id`,
      ei.`entity_identifier_scheme` AS `identifier_scheme`,
      ei.`entity_identifier_value` AS `identifier_value`,
      COALESCE(ei.`entity_identifier_is_primary`, 0) AS `identifier_is_primary`,
      ei.`entity_identifier_provenance` AS `identifier_provenance`,
      NULL AS `identifier_source`,
      ei.`entity_identifier_created_timestamp_ep_k` AS `identifier_created_timestamp_ep_k`,
      ei.`entity_identifier_modified_timestamp_ep_k` AS `identifier_modified_timestamp_ep_k`
    FROM `entity_identifiers` ei

    UNION ALL

    SELECT
      'item' AS `identifier_origin`,
      ii.`item_identifier_id` AS `identifier_id`,
      'item' AS `entity_type`,
      ii.`item_identifier_item_id` AS `entity_id`,
      ii.`item_identifier_scheme` AS `identifier_scheme`,
      ii.`item_identifier_value` AS `identifier_value`,
      NULL AS `identifier_is_primary`,
      NULL AS `identifier_provenance`,
      ii.`item_identifier_source` AS `identifier_source`,
      ii.`item_identifier_created_timestamp_ep_k` AS `identifier_created_timestamp_ep_k`,
      ii.`item_identifier_modified_timestamp_ep_k` AS `identifier_modified_timestamp_ep_k`
    FROM `item_identifiers` ii
  )
SELECT
  b.`identifier_origin`,
  b.`identifier_id`,
  b.`entity_type`,
  b.`entity_id`,
  b.`identifier_scheme`,
  b.`identifier_value`,
  b.`identifier_is_primary`,
  b.`identifier_provenance`,
  b.`identifier_source`,
  b.`identifier_created_timestamp_ep_k`,
  b.`identifier_modified_timestamp_ep_k`,

  -- Optional display helpers (cheap joins, useful for UI/debugging)
  CASE
    WHEN b.`entity_type` = 'work' THEN COALESCE(w.`work_canonical_title`, w.`work_title`)
    WHEN b.`entity_type` = 'expression' THEN COALESCE(e.`expression_title_override`, e.`expression_label`, CAST(e.`expression_year` AS TEXT))
    WHEN b.`entity_type` = 'manifestation' THEN COALESCE(m.`manifestation_edition_statement`, m.`manifestation_format_detail`, CAST(m.`manifestation_pub_year` AS TEXT))
    WHEN b.`entity_type` = 'item' THEN COALESCE(i.`item_type`, CAST(i.`item_id` AS TEXT))
    WHEN b.`entity_type` = 'agent' THEN COALESCE(a.`agent_canonical_name`, a.`agent_sort_name`, CAST(a.`agent_id` AS TEXT))
    ELSE NULL
  END AS `entity_display_text`

FROM base b
LEFT JOIN `works` w
  ON b.`entity_type` = 'work' AND w.`work_id` = b.`entity_id`
LEFT JOIN `expressions` e
  ON b.`entity_type` = 'expression' AND e.`expression_id` = b.`entity_id`
LEFT JOIN `manifestations` m
  ON b.`entity_type` = 'manifestation' AND m.`manifestation_id` = b.`entity_id`
LEFT JOIN `items` i
  ON b.`entity_type` = 'item' AND i.`item_id` = b.`entity_id`
LEFT JOIN `agents` a
  ON b.`entity_type` = 'agent' AND a.`agent_id` = b.`entity_id`
;

-- Alias for older code that expects an `identifiers` relation.
CREATE VIEW `identifiers` AS
SELECT * FROM `identifiers_v`
;


-- -----------------------------------------------------
-- View: ingest_audit_v
--
-- Unified audit/provenance feed for workflow events.
--
-- Emits one row per (ray, event) where ray context exists.
-- If an event refers to an object that does not (yet) live on a ray, the
-- ray/book context columns will be NULL but the event still appears.
-- -----------------------------------------------------

DROP VIEW IF EXISTS `ingest_audit`;
DROP VIEW IF EXISTS `ingest_audit_v`;

CREATE VIEW `ingest_audit_v` AS
SELECT
  ('digital_asset:' || fwe.`digital_asset_workflow_event_id` || ':' || COALESCE(fi.`ray_id`, '')) AS `audit_id`,
  'digital_asset' AS `audit_scope`,
  fwe.`digital_asset_workflow_event_id` AS `audit_event_id`,

  fi.`book_id` AS `book_id`,
  fi.`ray_id` AS `ray_id`,

  fi.`work_id` AS `work_id`,
  fi.`expression_id` AS `expression_id`,
  fi.`manifestation_id` AS `manifestation_id`,

  fi.`item_id` AS `item_id`,
  fi.`digital_asset_id` AS `digital_asset_id`,
  fi.`asset_replica_id` AS `asset_replica_id`,
  fi.`digital_asset_id` AS `file_id`,

  fwe.`digital_asset_workflow_event_step_id` AS `step_id`,
  ws.`workflow_step_code` AS `step_code`,
  ws.`workflow_step_label` AS `step_label`,
  ws.`workflow_step_group` AS `step_group`,
  ws.`workflow_step_scope` AS `step_scope`,

  fwe.`digital_asset_workflow_event_from_status` AS `from_status`,
  fwe.`digital_asset_workflow_event_to_status` AS `to_status`,

  fwe.`digital_asset_workflow_event_actor` AS `actor`,
  fwe.`digital_asset_workflow_event_tool` AS `tool`,
  fwe.`digital_asset_workflow_event_run_id` AS `run_id`,
  fwe.`digital_asset_workflow_event_note` AS `note`,

  fwe.`digital_asset_workflow_event_created_timestamp_ep_k` AS `created_timestamp_ep_k`,
  datetime(fwe.`digital_asset_workflow_event_created_timestamp_ep_k` / 1000, 'unixepoch') AS `created_datestamp`,
  fwe.`digital_asset_workflow_event_modified_timestamp_ep_k` AS `modified_timestamp_ep_k`,
  datetime(fwe.`digital_asset_workflow_event_modified_timestamp_ep_k` / 1000, 'unixepoch') AS `modified_datestamp`,

  fwe.`digital_asset_workflow_event_scratch` AS `event_scratch`,

  fi.`asset_replica_uri` AS `asset_replica_uri`,
  fi.`store_name` AS `store_name`,
  fi.`store_kind` AS `store_kind`,
  fi.`asset_replica_uri` AS `file_uri`

FROM `digital_asset_workflow_events` fwe
JOIN `workflow_steps` ws
  ON ws.`workflow_step_id` = fwe.`digital_asset_workflow_event_step_id`
LEFT JOIN `digital_asset_inventory_v` fi
  ON fi.`digital_asset_id` = fwe.`digital_asset_workflow_event_digital_asset_id`

UNION ALL

SELECT
  ('item:' || iwe.`item_workflow_event_id` || ':' || COALESCE(r.`ray_id`, '')) AS `audit_id`,
  'item' AS `audit_scope`,
  iwe.`item_workflow_event_id` AS `audit_event_id`,

  r.`ray_id` AS `book_id`,
  r.`ray_id` AS `ray_id`,

  r.`work_id` AS `work_id`,
  r.`expression_id` AS `expression_id`,
  COALESCE(r.`manifestation_id`, it.`item_manifestation_id`) AS `manifestation_id`,

  it.`item_id` AS `item_id`,
  NULL AS `digital_asset_id`,
  NULL AS `asset_replica_id`,
  NULL AS `file_id`,

  iwe.`item_workflow_event_step_id` AS `step_id`,
  ws.`workflow_step_code` AS `step_code`,
  ws.`workflow_step_label` AS `step_label`,
  ws.`workflow_step_group` AS `step_group`,
  ws.`workflow_step_scope` AS `step_scope`,

  iwe.`item_workflow_event_from_status` AS `from_status`,
  iwe.`item_workflow_event_to_status` AS `to_status`,

  iwe.`item_workflow_event_actor` AS `actor`,
  iwe.`item_workflow_event_tool` AS `tool`,
  iwe.`item_workflow_event_run_id` AS `run_id`,
  iwe.`item_workflow_event_note` AS `note`,

  iwe.`item_workflow_event_created_timestamp_ep_k` AS `created_timestamp_ep_k`,
  datetime(iwe.`item_workflow_event_created_timestamp_ep_k` / 1000, 'unixepoch') AS `created_datestamp`,
  iwe.`item_workflow_event_modified_timestamp_ep_k` AS `modified_timestamp_ep_k`,
  datetime(iwe.`item_workflow_event_modified_timestamp_ep_k` / 1000, 'unixepoch') AS `modified_datestamp`,

  iwe.`item_workflow_event_scratch` AS `event_scratch`,

  NULL AS `asset_replica_uri`,
  NULL AS `store_name`,
  NULL AS `store_kind`,
  NULL AS `file_uri`

FROM `item_workflow_events` iwe
JOIN `workflow_steps` ws
  ON ws.`workflow_step_id` = iwe.`item_workflow_event_step_id`
JOIN `items` it
  ON it.`item_id` = iwe.`item_workflow_event_item_id`
LEFT JOIN `wemi_rays_v` r
  ON r.`manifestation_id` = it.`item_manifestation_id`
;

-- Alias for older code that expects an `ingest_audit` relation.
CREATE VIEW `ingest_audit` AS
SELECT * FROM `ingest_audit_v`
;
-- -----------------------------------------------------
-- View: duplicate_candidates_v
--
-- Heuristic groups for potential deduplication/merge review.
--
-- This is intentionally conservative: it only produces groups with >1 member.
-- Current heuristics:
--   * ISBN-based grouping (high confidence)
--   * Title+PrimaryAuthor+Year grouping (medium confidence)
-- -----------------------------------------------------

DROP VIEW IF EXISTS `duplicate_candidates`;
DROP VIEW IF EXISTS `duplicate_candidates_v`;

CREATE VIEW `duplicate_candidates_v` AS
WITH
  book_base AS (
    SELECT
      b.`book_id` AS `book_id`,
      b.`book_sort` AS `book_sort`,
      b.`book_pubdate` AS `book_pubdate`,
      CAST(SUBSTR(b.`book_pubdate`, 1, 4) AS INTEGER) AS `pub_year`,
      COALESCE(r.`display_work_title`, b.`book_sort`) AS `title_key_raw`
    FROM `books_v` b
    JOIN `wemi_rays_v` r
      ON r.`ray_id` = b.`book_id`
  ),
  author_pick AS (
    SELECT
      ac.`book_id` AS `book_id`,
      ac.`agent_canonical_name` AS `author_name`,
      ROW_NUMBER() OVER (
        PARTITION BY ac.`book_id`
        ORDER BY
          ac.`credit_scope_rank` ASC,
          COALESCE(ac.`credit_priority`, 0) ASC,
          ac.`agent_sort_name` ASC,
          ac.`agent_canonical_name` ASC,
          ac.`agent_id` ASC
      ) AS `rn`
    FROM `agent_credits_v` ac
    WHERE ac.`credit_type` IN ('aut', 'cre')
  ),
  primary_author AS (
    SELECT
      ap.`book_id` AS `book_id`,
      ap.`author_name` AS `author_name`,
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(TRIM(ap.`author_name`)), '''', ''), '"', ''), '.', ''), ',', ''), ':', ''), ';', ''), '(', ''), ')', ''), '[', ''), ']', ''), '{', ''), '}', ''), '-', ''), '_', ''), '/', ''), '\\', ''), '!', ''), '?', ''), '&', ''), ' ', '') AS `author_norm`
    FROM `author_pick` ap
    WHERE ap.`rn` = 1
  ),
  book_keys AS (
    SELECT
      bb.*,
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(TRIM(bb.`title_key_raw`)), '''', ''), '"', ''), '.', ''), ',', ''), ':', ''), ';', ''), '(', ''), ')', ''), '[', ''), ']', ''), '{', ''), '}', ''), '-', ''), '_', ''), '/', ''), '\\', ''), '!', ''), '?', ''), '&', ''), ' ', '') AS `title_norm`
    FROM `book_base` bb
  ),
  isbn_map AS (
    -- Identifiers attached to ray nodes
    SELECT
      r.`ray_id` AS `book_id`,
      REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(i.`identifier_value`)), 'ISBN', ''), '-', ''), ' ', ''), ':', '') AS `isbn_norm`
    FROM `wemi_rays_v` r
    JOIN `identifiers_v` i
      ON (
        (i.`entity_type` = 'work' AND i.`entity_id` = r.`work_id`) OR
        (i.`entity_type` = 'expression' AND i.`entity_id` = r.`expression_id`) OR
        (i.`entity_type` = 'manifestation' AND i.`entity_id` = r.`manifestation_id`)
      )
    WHERE LOWER(i.`identifier_scheme`) LIKE '%isbn%'

    UNION ALL

    -- Item identifiers observed on items under the manifestation
    SELECT
      ri.`ray_id` AS `book_id`,
      REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(i.`identifier_value`)), 'ISBN', ''), '-', ''), ' ', ''), ':', '') AS `isbn_norm`
    FROM `wemi_ray_items_v` ri
    JOIN `identifiers_v` i
      ON (i.`entity_type` = 'item' AND i.`entity_id` = ri.`item_id`)
    WHERE LOWER(i.`identifier_scheme`) LIKE '%isbn%'
  ),
  isbn_groups AS (
    SELECT
      m.`isbn_norm` AS `key_isbn`,
      COUNT(DISTINCT m.`book_id`) AS `member_count`,
      GROUP_CONCAT(DISTINCT m.`book_id`) AS `book_ids_csv`,
      MIN(bk.`book_sort`) AS `example_book_sort`,
      MIN(pa.`author_name`) AS `example_author_name`,
      MIN(bk.`pub_year`) AS `example_year`
    FROM `isbn_map` m
    JOIN `book_keys` bk
      ON bk.`book_id` = m.`book_id`
    LEFT JOIN `primary_author` pa
      ON pa.`book_id` = m.`book_id`
    WHERE COALESCE(m.`isbn_norm`, '') <> ''
    GROUP BY m.`isbn_norm`
    HAVING COUNT(DISTINCT m.`book_id`) > 1
  ),
  tya_groups AS (
    SELECT
      bk.`title_norm` AS `key_title_norm`,
      pa.`author_norm` AS `key_author_norm`,
      bk.`pub_year` AS `key_year`,
      COUNT(DISTINCT bk.`book_id`) AS `member_count`,
      GROUP_CONCAT(DISTINCT bk.`book_id`) AS `book_ids_csv`,
      MIN(bk.`book_sort`) AS `example_book_sort`,
      MIN(pa.`author_name`) AS `example_author_name`,
      MIN(bk.`pub_year`) AS `example_year`
    FROM `book_keys` bk
    JOIN `primary_author` pa
      ON pa.`book_id` = bk.`book_id`
    WHERE
      COALESCE(bk.`title_norm`, '') <> '' AND
      COALESCE(pa.`author_norm`, '') <> '' AND
      bk.`pub_year` IS NOT NULL
    GROUP BY bk.`title_norm`, pa.`author_norm`, bk.`pub_year`
    HAVING COUNT(DISTINCT bk.`book_id`) > 1
  )
SELECT
  'isbn' AS `candidate_kind`,
  ('ISBN:' || ig.`key_isbn`) AS `candidate_key`,
  0.95 AS `confidence`,
  ig.`member_count` AS `member_count`,
  ig.`book_ids_csv` AS `book_ids_csv`,
  ig.`example_book_sort` AS `example_book_sort`,
  ig.`example_author_name` AS `example_author_name`,
  ig.`example_year` AS `example_year`
FROM `isbn_groups` ig

UNION ALL

SELECT
  'title_author_year' AS `candidate_kind`,
  ('TYA:' || tg.`key_title_norm` || '|' || tg.`key_author_norm` || '|' || CAST(tg.`key_year` AS TEXT)) AS `candidate_key`,
  0.65 AS `confidence`,
  tg.`member_count` AS `member_count`,
  tg.`book_ids_csv` AS `book_ids_csv`,
  tg.`example_book_sort` AS `example_book_sort`,
  tg.`example_author_name` AS `example_author_name`,
  tg.`example_year` AS `example_year`
FROM `tya_groups` tg
;

CREATE VIEW `duplicate_candidates` AS
SELECT * FROM `duplicate_candidates_v`
;


-- -----------------------------------------------------
-- View: search_seed_v
--
-- One row per book (ray) with denormalised "seed" text fields.
-- Intended for feeding FTS / external indexers, and for UI quick-search.
-- -----------------------------------------------------

DROP VIEW IF EXISTS `search_seed`;
DROP VIEW IF EXISTS `search_seed_v`;

CREATE VIEW `search_seed_v` AS
SELECT
  b.`book_id` AS `book_id`,
  b.`book_ray_id` AS `ray_id`,
  b.`book_work_id` AS `work_id`,
  b.`book_expression_id` AS `expression_id`,
  b.`book_manifestation_id` AS `manifestation_id`,

  b.`book_sort` AS `seed_title`,

  -- Ordered, distinct-ish author list
  (
    SELECT REPLACE(GROUP_CONCAT(x.`name`), ',', '; ')
    FROM (
      SELECT DISTINCT
        ac.`agent_canonical_name` AS `name`,
        ac.`agent_sort_name` AS `sort_name`,
        ac.`credit_scope_rank` AS `scope_rank`,
        COALESCE(ac.`credit_priority`, 0) AS `prio`
      FROM `agent_credits_v` ac
      WHERE ac.`book_id` = b.`book_id`
        AND ac.`credit_type` IN ('aut', 'cre')
      ORDER BY `scope_rank` ASC, `prio` ASC, `sort_name` ASC, `name` ASC
    ) x
  ) AS `seed_authors`,

  -- Deterministic best publisher (if any)
  (
    SELECT p.`publisher_name`
    FROM `publishers_v` p
    WHERE p.`book_id` = b.`book_id`
    LIMIT 1
  ) AS `seed_publisher`,

  -- Identifiers across ray nodes (work/expression/manifestation) + item nodes
  (
    SELECT REPLACE(GROUP_CONCAT(y.`idtxt`), ',', ' ')
    FROM (
      SELECT DISTINCT
        (LOWER(i.`identifier_scheme`) || ':' || i.`identifier_value`) AS `idtxt`
      FROM `identifiers_v` i
      WHERE
        (i.`entity_type` = 'work' AND i.`entity_id` = b.`book_work_id`) OR
        (i.`entity_type` = 'expression' AND i.`entity_id` = b.`book_expression_id`) OR
        (i.`entity_type` = 'manifestation' AND i.`entity_id` = b.`book_manifestation_id`) OR
        (i.`entity_type` = 'item' AND i.`entity_id` IN (
          SELECT ri.`item_id` FROM `wemi_ray_items_v` ri WHERE ri.`ray_id` = b.`book_id`
        ))
      ORDER BY `idtxt` ASC
    ) y
  ) AS `seed_identifiers`,

  -- Tag-like facets (subject/genre/tag)
  (
    SELECT REPLACE(GROUP_CONCAT(z.`facet`), ',', '; ')
    FROM (
      SELECT DISTINCT
        st.`facet_text` AS `facet`,
        st.`facet_sort` AS `facet_sort`
      FROM `subjects_tags_v` st
      WHERE st.`book_id` = b.`book_id`
      ORDER BY `facet_sort` ASC, `facet` ASC
    ) z
  ) AS `seed_facets`,

  -- Final seed text: title + authors + publisher + identifiers + facets
  TRIM(
    COALESCE(b.`book_sort`, '') || ' ' ||
    COALESCE((
      SELECT REPLACE(GROUP_CONCAT(x.`name`), ',', ' ')
      FROM (
        SELECT DISTINCT ac.`agent_canonical_name` AS `name`, ac.`agent_sort_name` AS `sort_name`, ac.`credit_scope_rank` AS `scope_rank`, COALESCE(ac.`credit_priority`, 0) AS `prio`
        FROM `agent_credits_v` ac
        WHERE ac.`book_id` = b.`book_id` AND ac.`credit_type` IN ('aut', 'cre')
        ORDER BY `scope_rank` ASC, `prio` ASC, `sort_name` ASC, `name` ASC
      ) x
    ), '') || ' ' ||
    COALESCE((
      SELECT p.`publisher_name` FROM `publishers_v` p WHERE p.`book_id` = b.`book_id` LIMIT 1
    ), '') || ' ' ||
    COALESCE((
      SELECT REPLACE(GROUP_CONCAT(y.`idtxt`), ',', ' ')
      FROM (
        SELECT DISTINCT (LOWER(i.`identifier_scheme`) || ':' || i.`identifier_value`) AS `idtxt`
        FROM `identifiers_v` i
        WHERE
          (i.`entity_type` = 'work' AND i.`entity_id` = b.`book_work_id`) OR
          (i.`entity_type` = 'expression' AND i.`entity_id` = b.`book_expression_id`) OR
          (i.`entity_type` = 'manifestation' AND i.`entity_id` = b.`book_manifestation_id`) OR
          (i.`entity_type` = 'item' AND i.`entity_id` IN (
            SELECT ri.`item_id` FROM `wemi_ray_items_v` ri WHERE ri.`ray_id` = b.`book_id`
          ))
        ORDER BY `idtxt` ASC
      ) y
    ), '') || ' ' ||
    COALESCE((
      SELECT REPLACE(GROUP_CONCAT(z.`facet`), ',', ' ')
      FROM (
        SELECT DISTINCT st.`facet_text` AS `facet`, st.`facet_sort` AS `facet_sort`
        FROM `subjects_tags_v` st
        WHERE st.`book_id` = b.`book_id`
        ORDER BY `facet_sort` ASC, `facet` ASC
      ) z
    ), '')
  ) AS `seed_text`

FROM `books_v` b
;

CREATE VIEW `search_seed` AS
SELECT * FROM `search_seed_v`
;
