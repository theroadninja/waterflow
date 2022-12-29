## Optimization Ideas

* remember to perf test using task_deps.job_id in a where clause - does that improve perf?  (useful for delete cascae either way)
* assigning the max number of workers at once (calling get_and_start_tasks with the maximum number of allowed workers)
* probably need an index on created_utc column for jobs to efficiently delete old records w/o full table scan

## Single Threaded Tests

Steps:
1. schedule all N jobs
2. fetch dags for all jobs
3. in batches of M=100 tasks
3a. fetch M tasks
3b. mark M tasks complete

*Before adding index on state field*

Single threaded test running N jobs with 10 tasks per job in the shape of a fibonacci pyramid

```
1,000:    116 sec

20000 tasks run in 298.28511142730713 seconds
TEST COMPLETE: total time elapsed: 332.72497367858887 seconds
```


*After adding index on state field*

```
10000 tasks run in 71.45952224731445 seconds
TEST COMPLETE: total time elapsed: 87.48973441123962 seconds

20000 tasks run in 169.14807033538818 seconds
TEST COMPLETE: total time elapsed: 203.47183656692505 seconds

160000 tasks run in 2396.6012206077576 seconds
TEST COMPLETE: total time elapsed: 3006.3673684597015 seconds

3006 sec = 50 min
```

