"""
This is the main code for the server.
"""
import datetime
import logging

from waterflow.core import make_id
from waterflow.dao import DagDao
from waterflow.dao_models import PendingJob, WorkItem, Dag, Task
from waterflow.rest import Dag as RestDag, Task as RestTask

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
    logger = logging.getLogger("server_methods")

    now_utc = now_utc or datetime.datetime.utcnow()

    # 1. dag fetch tasks
    logger.info("calling get_and_start_jobs()")
    fetch_tasks = dao.get_and_start_jobs([worker], work_queue, now_utc)
    if len(fetch_tasks) == 1:
        return WorkItem(dag_fetch=fetch_tasks[0])
    elif len(fetch_tasks) > 1:
        raise Exception("too many fetch tasks returned by DAO")

    # 2. tasks
    logger.info("calling get_and_start_tasks()")
    tasks = dao.get_and_start_tasks([worker], work_queue, now_utc)
    if len(tasks) == 1:
        return WorkItem(run_task=tasks[0])
    elif len(tasks) > 1:
        raise Exception("too many tasks returned by DAO")

    # 3. stale dag fetch tasks?
    # TODO maybe automatically return dag fetch tasks that are more than 10 minutes old

    return WorkItem()  #  TODO unit test this function


# TODO this is useless -- it takes the internal models as input, which already have the ids set
def set_dag_for_job(dao: DagDao, job_id: str, dag: Dag, work_queue: str, now_utc=None):

    now_utc = now_utc or datetime.datetime.utcnow()
    dao.set_dag(job_id, dag, work_queue, now_utc)
    dao.update_task_deps(job_id)
    # TODO set a limit of 1024-4096 tasks per job



def _rest_task_to_internal_task(rest_task: RestTask, task_id: str):
    return Task(
        task_id=task_id,
        task_name=rest_task.task_name,
        input64=rest_task.input64,
        input64_v=rest_task.input64_v,
        service_pointer=rest_task.service_pointer,
    )

def _transform_adj_list(adj_list_by_index, id_map):
    """
    The request identifies tasks by "index" -- an integer that has no meaning outside the scope of the request.  This
    function translate the adjacency list expressed using indexes to one using the newly-assigned string task ids.
    """
    id_map = {int(k): v for k,v in id_map.items()}

    return {
        id_map[int(task_index)]:  [id_map[int(i)] for i in neighboors]
        for task_index, neighboors in adj_list_by_index.items()
    }


def set_dag_for_job_REST(dao: DagDao, job_id: str, dag: RestDag, work_queue: str, now_utc=None):
    now_utc = now_utc or datetime.datetime.utcnow()

    id_map = {}
    internal_tasks = []
    for task_index,  in dag.tasks:
        task_id = make_id()
        id_map[task_index] = task_id
        internal_tasks.append(_rest_task_to_internal_task(dag.tasks[task_index], task_id))

    adj_list = _transform_adj_list(dag.adj_list, id_map)  # TODO why were ints transformed into strings?
    internal_dag = Dag(
        raw_dag64=None,  # TODO
        raw_dagv=0,  # TODO
        tasks=internal_tasks,
        adj_list=adj_list,
    )
    dao.set_dag(job_id, internal_dag, work_queue, now_utc)
    dao.update_task_deps(job_id)


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



