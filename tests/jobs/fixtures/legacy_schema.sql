-- Durable ledger schema before claim-token/payload migration (1e98ecf5).

CREATE TABLE IF NOT EXISTS job_definitions (
    job_definition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_kind TEXT NOT NULL,
    job_name TEXT NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    schedule_json TEXT,
    priority INTEGER NOT NULL DEFAULT 100,
    concurrency_policy TEXT NOT NULL,
    result_policy TEXT NOT NULL,
    timeout_s REAL,
    max_retries INTEGER NOT NULL DEFAULT 0,
    retry_backoff_s REAL NOT NULL DEFAULT 0,
    heartbeat_timeout_s REAL,
    created_timestamp_ep_k INTEGER NOT NULL,
    modified_timestamp_ep_k INTEGER NOT NULL,
    last_queued_timestamp_ep_k INTEGER,
    last_started_timestamp_ep_k INTEGER,
    last_finished_timestamp_ep_k INTEGER
);
CREATE TABLE IF NOT EXISTS job_runs (
    job_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_definition_id INTEGER NOT NULL,
    job_kind TEXT NOT NULL,
    trigger_kind TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    worker_id TEXT,
    lease_expires_timestamp_ep_k INTEGER,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    not_before_timestamp_ep_k INTEGER,
    progress_current INTEGER,
    progress_total INTEGER,
    progress_unit TEXT,
    progress_message TEXT,
    result_json TEXT,
    error_text TEXT,
    log_path TEXT,
    queued_timestamp_ep_k INTEGER,
    started_timestamp_ep_k INTEGER,
    heartbeat_timestamp_ep_k INTEGER,
    finished_timestamp_ep_k INTEGER,
    FOREIGN KEY(job_definition_id) REFERENCES job_definitions(job_definition_id)
);
CREATE INDEX IF NOT EXISTS idx_job_runs_state_not_before ON job_runs(state, not_before_timestamp_ep_k);
CREATE INDEX IF NOT EXISTS idx_job_runs_definition ON job_runs(job_definition_id);
CREATE TABLE IF NOT EXISTS job_run_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_run_id INTEGER NOT NULL,
    event_kind TEXT NOT NULL,
    event_message TEXT,
    event_json TEXT,
    created_timestamp_ep_k INTEGER NOT NULL,
    FOREIGN KEY(job_run_id) REFERENCES job_runs(job_run_id)
);
CREATE INDEX IF NOT EXISTS idx_job_run_events_run ON job_run_events(job_run_id, created_timestamp_ep_k);
