## Optimization Ideas

* remember to perf test using task_deps.job_id in a where clause - does that improve perf?  (useful for delete cascae either way)
* assigning the max number of workers at once (calling get_and_start_tasks with the maximum number of allowed workers)
* probably need an index on created_utc column for jobs to efficiently delete old records w/o full table scan

* how many tasks can a single worker run?
* test effect of putting all VARCHAR values at max length.
* test whether having lots of tags creating a performance issue.
* test not having an index on job_tags.tag
* test not having an index on job.created_utc
