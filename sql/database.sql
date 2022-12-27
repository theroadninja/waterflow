-- CREATE SCHEMA `waterflow` DEFAULT CHARACTER SET utf8 ;

-- select UTC_TIMESTAMP()
-- insert into jobs (job_id, job_input) VALUES ("abc123", NULL);


CREATE TABLE jobs (
    job_id VARCHAR(32) NOT NULL,   -- uuid without dashes
    PRIMARY KEY (job_id),

    job_input BLOB   -- 16 MB
);

CREATE TABLE job_executions (
    job_id VARCHAR(32),
    state TINYINT UNSIGNED,   -- 0 to 255
    worker VARCHAR(255),  -- TODO reference to some worker table???
    -- TODO last update utc and created utc

    dag BLOB,

    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE tasks (
    job_id VARCHAR(32),
    task_id VARCHAR(32) NOT NULL,
    eligibility_state TINYINT UNSIGNED, -- 0 to 255
    task_input BLOB,

    PRIMARY KEY (task_id),
    CONSTRAINT uc_id UNIQUE (job_id, task_id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE task_deps (
    job_id VARCHAR(32),
    task_id VARCHAR(32),
    neighboor_id VARCHAR(32),

    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (neighboor_id) REFERENCES tasks(task_id) ON DELETE CASCADE

);

CREATE TABLE task_executions (
    job_id VARCHAR(32),
    task_id VARCHAR(32),
    exec_state TINYINT UNSIGNED,
    worker VARCHAR(255),
    -- TODO last update utc, created utc, etc
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);