CREATE TABLE IF NOT EXISTS org_agent_relations (
  org_agent_relation_id INTEGER PRIMARY KEY,

  org_agent_relation_child_agent_id  INTEGER NOT NULL,
  org_agent_relation_parent_agent_id INTEGER NOT NULL,

  org_agent_relation_type TEXT NOT NULL,   -- 'imprint_of','subsidiary_of','owned_by','division_of', ...
  org_agent_relation_start_date TEXT NULL, -- ISO8601
  org_agent_relation_end_date   TEXT NULL, -- ISO8601
  org_agent_relation_note TEXT NULL,

  org_agent_relation_datestamp DATETIME DEFAULT (STRFTIME('%s', 'now')),
  org_agent_relation_created_datestamp DATETIME DEFAULT (STRFTIME('%s', 'now')),

  org_agent_relation_scratch TEXT NULL,

  CONSTRAINT org_agent_relation_child_fk
    FOREIGN KEY (org_agent_relation_child_agent_id)
    REFERENCES agents(agent_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT org_agent_relation_parent_fk
    FOREIGN KEY (org_agent_relation_parent_agent_id)
    REFERENCES agents(agent_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT org_agent_relation_no_self
    CHECK (org_agent_relation_child_agent_id != org_agent_relation_parent_agent_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_org_agent_relations_unique
ON org_agent_relations(
  org_agent_relation_child_agent_id,
  org_agent_relation_parent_agent_id,
  org_agent_relation_type
);

CREATE INDEX IF NOT EXISTS idx_org_agent_relations_parent
ON org_agent_relations(org_agent_relation_parent_agent_id);

CREATE INDEX IF NOT EXISTS idx_org_agent_relations_child
ON org_agent_relations(org_agent_relation_child_agent_id);
