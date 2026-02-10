-- BREAK

-- Allows plugins to store arbitrary data attributes for each of the WEMI tables

-- -----------------------------------------------------
-- Table `works_plugin_data`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `works_plugin_data` (
  `works_plugin_data_id` INTEGER PRIMARY KEY ,

  `works_plugin_data_work` INTEGER NULL,

  `works_plugin_data_name` TEXT NULL,
  `works_plugin_data_val` TEXT NULL,

  -- timestamps
  `works_plugin_data_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `works_plugin_data_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `works_plugin_data_source_created_datestamp_ep_k` INTEGER NULL,
  `works_plugin_data_source_modified_datestamp_ep_k` INTEGER NULL,

  `works_plugin_scratch` TEXT NULL)
;

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `expressions_plugin_data`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `expressions_plugin_data` (
  `expressions_plugin_data_id` INTEGER PRIMARY KEY ,

  `expressions_plugin_data_expressions` INTEGER NULL,

  `expressions_plugin_data_name` TEXT NULL,
  `expressions_plugin_data_val` TEXT NULL,

  -- timestamps
  `expressions_plugin_data_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `expressions_plugin_data_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `expressions_plugin_scratch` TEXT NULL)
;

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `manifestations_plugin_data`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `manifestations_plugin_data` (
  `manifestations_plugin_data_id` INTEGER PRIMARY KEY ,

  `manifestations_plugin_data_manifestations` INTEGER NULL,

  `manifestations_plugin_data_name` TEXT NULL,
  `manifestations_plugin_data_val` TEXT NULL,

  -- timestamps
  `manifestations_plugin_data_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `manifestations_plugin_data_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `manifestations_plugin_scratch` TEXT NULL)
;

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `items_plugin_data`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `items_plugin_data` (
  `items_plugin_data_id` INTEGER PRIMARY KEY ,

  `items_plugin_data_items` INTEGER NULL,

  `items_plugin_data_name` TEXT NULL,
  `items_plugin_data_val` TEXT NULL,

  -- timestamps
  `items_plugin_data_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `items_plugin_data_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `items_plugin_scratch` TEXT NULL)
;

-- BREAK
