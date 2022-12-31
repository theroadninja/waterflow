"""
This is the main code for the server.
"""
import datetime

from waterflow.dao import DagDao
from waterflow.dao_models import PendingJob, WorkItem, Dag

from waterflow.job import JobView1

##
## Inspection Methods
##

def get_job_count_summary():
    pass  # TODO

# TODO rename to get_job_info()
def get_job_details(dao: DagDao, job_id: str) -> JobView1:  # TODO move JobView1 to dao_models.JobView
    """
    Returns everything about a job, except for its tasks.
    """
    return dao.get_job_info(job_id)

def get_job_tasks(dao: DagDao, job_id: str):  # TODO maybe its just a flag to also receive tasks?
    pass # TODO dao.get_tasks_by_job()


##
## Execution Methods
##

def submit_job(dao: DagDao, job: PendingJob, now_utc=None) -> str:
    """
    Submits a new job for execution.

    :returns: the job id
    """

    # TODO what if we did the idempotence check in memory?
    now_utc = now_utc or datetime.datetime.utcnow()

    # TODO is this where we prune old jobs?  if we are adding one, we could simply delete 1 job
    #    - add one, delete one
    #    - delete limit 1
    #    - delete only if older than X days and more than Y jobs in table (or a configurable time frame)
    #         - maybe have more than one threshold?  we dont want jobs to live longer than 2 weeks anyway
    #    - obviously make sure we are only deleting failed or succeeded jobs
    #    - this is inefficient though -- locking the table over and over again for no reason...
    #         -  maybe only delete when rowcount % 10 == 0 or something
    #         -  or, always delete 100 instead of 1...
    #         -  and deletes only happen in a certain time window (like 5 min before the hour)
    #    - or do this on a background thread?
    #    - OR:  store the time of the last pruning in an operations table, and update using a where clause to check time,
    #           and only the requestor that was able to update the row does the pruning.

    return dao.add_job(job)


def get_work_item(dao: DagDao, work_queue: str, worker: str, now_utc=None) -> WorkItem:
    """
    Method to call to fetch a work item, which can be one of:
    1. fetching a DAG for a newly submitted job
    2. executing a task

    DAGs are prioritized because those calls are expected to take millis or seconds, while running tasks can take up to
    minutes or hours.
    """
    now_utc = now_utc or datetime.datetime.utcnow()

    fetch_tasks = dao.get_and_start_jobs([worker], work_queue, now_utc)
    if len(fetch_tasks) == 1:
        return WorkItem(dag_fetch=fetch_tasks[0])
    elif len(fetch_tasks) > 1:
        raise Exception("too many fetch tasks returned by DAO")

    tasks = dao.get_and_start_tasks([worker], work_queue, now_utc)
    if len(tasks) == 1:
        return WorkItem(run_task=tasks[0])
    elif len(tasks) > 1:
        raise Exception("too many tasks returned by DAO")

    return WorkItem()  #  TODO unit test this function


def set_dag_for_job(dao: DagDao, job_id: str, dag: Dag, work_queue: str, now_utc=None):

    now_utc = now_utc or datetime.datetime.utcnow()
    dao.set_dag(job_id, dag, work_queue, now_utc)
    dao.update_task_deps(job_id)
    # TODO set a limit of 1024-4096 tasks per job

def complete_task(dao: DagDao, job_id, task_id):

    dao.complete_task(job_id, task_id)
    dao.update_task_deps(job_id)
    dao.update_job_state(job_id)

## ######## BELOW THIS LINE NOT NEEDED FOR NEXT PERF TEST ########

def fail_job():
    pass  # TODO

def cancel_task():
    pass  # TODO

def fail_task():
    pass  # TODO

def retry_task():
    pass  # TODO

def keep_task_alive():
    pass  # TODO



