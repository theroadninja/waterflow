# Waterflow

Waterflow is a simple DAG Execution engine backed my MySQL.

Waterflow is a good fit for you if:
- Airflow, Dagster, Flyte, etc are not flexible enough for you
- All of your work can be done with RPC calls
- The shape of your DAGs need to be calculated at runtime
- You have a high volume of jobs

Waterflow is a bad fit for you if:
- You need a powerful, expressive domain-specific-language to describe your DAGs
- You want to write your business logic in the scheduler system
- You need a Scheduler

# Architecture

Currently, Waterflow uses a stateless Flask app backed by a MySQL database.  A single, multithreaded python
worker is enough to run tens of thousands of jobs.

The database schema is in [sql/database.sql](sql/database.sql)

The flask server is in the package `waterflow.core` -- see [waterflow/flask/\_\_main\_\_.py](waterflow/flask/__main__.py)

The worker is in the package `waterflow.worker` -- see [waterflow/worker/\_\_main\_\_.py](waterflow/worker/__main__.py) 

Most of the logic for the database queries and state transitions of jobs and tasks lives in `dao.py`

## Waterflow UI

The UI is in a separate repo.  See https://github.com/kakun45/waterflow_ui

# Setup

The database connection is controlled by a json file in a `local/` folder that is not checked into git.

This is the format:
```
{
  "username": "username",
  "password": ".....",
  "hostname": "localhost",
  "dbname": "waterflow"
}
```

# Troubleshooting

# Increasing Mysql Max Connections

```
show variables like "max_connections";
set global max_connections = 1000;
```