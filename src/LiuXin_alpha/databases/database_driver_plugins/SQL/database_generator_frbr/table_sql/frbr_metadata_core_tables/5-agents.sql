
-- BREAK

PRAGMA `foreign_keys` = ON;

-- BREAK
-- BREAK

-- agents: supertype for any actor involved in creation/publication/ownership/etc.
-- Subtype tables can hang off this, e.g. human_agents (persons), org_agents (organisations), etc.

CREATE TABLE IF NOT EXISTS `agents` (

  `agent_id` INTEGER PRIMARY KEY,

  -- What kind of agent is this?
  -- Suggested values: 'person', 'organisation', 'group', 'pseudonym'
  `agent_type` TEXT NOT NULL,

  -- Display/sort identity (canonical lives here; name-parts live in subtype tables)
  `agent_canonical_name` TEXT NOT NULL,
  `agent_sort_name` TEXT NULL,

  -- Optional extras for UI/search
  `agent_aliases` TEXT NULL,          -- e.g. a delimited list; or move to agent_aliases table later
  `agent_note` TEXT NULL,

    -- timestamps (epoch_ms)
  `agent_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `agent_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `agent_source_created_datestamp_ep_k` INTEGER NULL,
  `agent_source_modified_datestamp_ep_k` INTEGER NULL,

  `agent_scratch` TEXT NULL,

  -- Keep agent_type sane without introducing a lookup table yet
  CONSTRAINT `agents_agent_type_check`
    CHECK (`agent_type` IN ('person','organisation','group','pseudonym'))
);

-- BREAK
-- BREAK


-- Common lookup patterns
CREATE INDEX IF NOT EXISTS `idx_agents_type`
ON `agents`(`agent_type`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_agents_canonical_name`
ON `agents`(`agent_canonical_name`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_agents_sort_name`
ON `agents`(`agent_sort_name`);

-- BREAK
-- BREAK


-- human_agents: 1:1 subtype table for person-specific fields
-- Requires: agents(agent_id) with agents.agent_type = 'person' for these rows.

CREATE TABLE IF NOT EXISTS `human_agents` (
  `human_agent_id` INTEGER PRIMARY KEY,

  -- 1:1 link to the supertype
  `human_agent_agent_id` INTEGER NOT NULL UNIQUE,

  -- Name parts (optional; can be used to derive display/sort names in code)
  `human_agent_given_name`  TEXT NULL,
  `human_agent_middle_name` TEXT NULL,
  `human_agent_family_name` TEXT NULL,
  `human_agent_prefix`      TEXT NULL,   -- Dr, Sir, etc.
  `human_agent_suffix`      TEXT NULL,   -- Jr, III, etc.
  `human_agent_preferred_name` TEXT NULL, -- pen-name-ish but still a person

  -- Life dates (store as ISO8601 text: YYYY-MM-DD when known)
  `human_agent_birth_date` TEXT NULL,
  `human_agent_death_date` TEXT NULL,

  -- Lightweight bio / notes (identifiers belong in entity_identifiers)
  `human_agent_nationality` TEXT NULL,
  `human_agent_biography`   TEXT NULL,

  -- timestamps (epoch_ms)
  `human_agent_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `human_agent_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `human_agent_scratch` TEXT NULL,

  CONSTRAINT `human_agents_agent_fk`
    FOREIGN KEY (`human_agent_agent_id`)
    REFERENCES `agents`(`agent_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  -- Minimal sanity: if both are present, birth <= death (lexicographic works for ISO8601)
  CONSTRAINT `human_agents_birth_before_death`
    CHECK (
      `human_agent_birth_date` IS NULL
      OR `human_agent_death_date` IS NULL
      OR `human_agent_birth_date` <= `human_agent_death_date`
    )
);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_human_agents_agent_id`
ON `human_agents`(`human_agent_agent_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_human_agents_family_given`
ON `human_agents`(`human_agent_family_name`, `human_agent_given_name`);

-- BREAK
-- BREAK


-- org_agents: 1:1 subtype table for organisation-specific fields
-- Requires: agents(agent_id) with agents.agent_type = 'organisation' (or 'group').

CREATE TABLE IF NOT EXISTS `org_agents` (
  `org_agent_id` INTEGER PRIMARY KEY,

  -- 1:1 link to the supertype
  `org_agent_agent_id` INTEGER NOT NULL UNIQUE,

  -- Optional structured organisation identity
  `org_agent_legal_name`       TEXT NULL,  -- if different from agents.agent_canonical_name
  `org_agent_trading_name`     TEXT NULL,  -- DBA / imprint / brand
  `org_agent_registration_id`  TEXT NULL,  -- company number / registry id (scheme stored in entity_identifiers ideally)
  `org_agent_jurisdiction`     TEXT NULL,  -- 'GB', 'US-DE', etc.
  `org_agent_founded_date`     TEXT NULL,  -- ISO8601 'YYYY-MM-DD' when known
  `org_agent_dissolved_date`   TEXT NULL,  -- ISO8601

  -- Contact-ish / location-ish (keep light; full addresses can be another table later)
  `org_agent_website`          TEXT NULL,
  `org_agent_contact_email`    TEXT NULL,

  -- Light notes
  `org_agent_description`      TEXT NULL,

    -- timestamps (epoch_ms)
  `org_agent_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `org_agent_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `org_agent_scratch` TEXT NULL,

  CONSTRAINT `org_agents_agent_fk`
    FOREIGN KEY (`org_agent_agent_id`)
    REFERENCES `agents`(`agent_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  -- Minimal sanity: founded <= dissolved (lexicographic works for ISO8601)
  CONSTRAINT `org_agents_founded_before_dissolved`
    CHECK (
      `org_agent_founded_date` IS NULL
      OR `org_agent_dissolved_date` IS NULL
      OR `org_agent_founded_date` <= `org_agent_dissolved_date`
    )


);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_org_agents_agent_id`
ON `org_agents`(`org_agent_agent_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_org_agents_legal_trading`
ON `org_agents`(`org_agent_legal_name`, `org_agent_trading_name`);

-- BREAK
