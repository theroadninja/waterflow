-- CREATE SCHEMA `waterflow` DEFAULT CHARACTER SET utf8 ;

-- select UTC_TIMESTAMP()
-- insert into jobs (job_id, job_input) VALUES ("abc123", NULL);


CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(32) NOT NULL,   -- uuid without dashes
    PRIMARY KEY (job_id),
    job_name VARCHAR(64),
    created_utc DATETIME NOT NULL,
    job_input BLOB,   -- 16 MB
    job_input_v TINYINT UNSIGNED, -- version of serialization used for job_input BLOB
    service_pointer VARCHAR(128),  -- any UTF8 string (json or base64)
    work_queue TINYINT UNSIGNED DEFAULT 0,  -- TODO remove default value
    INDEX (work_queue)
);


-- TODO:  small int for dag BLOB version
CREATE TABLE IF NOT EXISTS job_executions (
    job_id VARCHAR(32),
    created_utc DATETIME NOT NULL,
    updated_utc DATETIME NOT NULL,
    state TINYINT UNSIGNED,   -- 0 to 255
    worker VARCHAR(255),  -- TODO reference to some worker table???
    -- TODO last update utc and created utc
    dag BLOB,
    dag_v TINYINT UNSIGNED,
    work_queue TINYINT UNSIGNED DEFAULT 0,  -- TODO remove default value
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
    INDEX (work_queue)
);

CREATE TABLE IF NOT EXISTS job_tags (
    job_id VARCHAR(32),
    tag VARCHAR(255),

    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
    INDEX (tag)
);

CREATE TABLE IF NOT EXISTS tasks (
    job_id VARCHAR(32),
    task_id VARCHAR(32) NOT NULL,
    task_name VARCHAR(64),
    created_utc DATETIME NOT NULL,
    updated_utc DATETIME NOT NULL,
    state TINYINT UNSIGNED, -- 0 to 255
    task_input BLOB,
    task_input_v TINYINT UNSIGNED,
    service_pointer VARCHAR(128),  -- any UTF8 string (json or base64)
    work_queue TINYINT UNSIGNED DEFAULT 0,  -- tasks inherit the work queue of their job.  TODO remove default value

    PRIMARY KEY (task_id),
    CONSTRAINT uc_id UNIQUE (job_id, task_id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
    INDEX (state),
    INDEX (work_queue)
);

CREATE TABLE IF NOT EXISTS task_deps (
    job_id VARCHAR(32),
    task_id VARCHAR(32),
    neighboor_id VARCHAR(32),

    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (neighboor_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS error_events (
    job_id VARCHAR(32),
    task_id VARCHAR(32),  -- in this table, ok to be null
    event_code TINYINT UNSIGNED,  -- whether got failure from service or canceled by user, etc
    failure_message VARCHAR(255), -- like exception message.  do we need this?
    failure_obj BLOB, -- for things like stack traces

    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);