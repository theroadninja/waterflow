"""
This is the main code for the server.
"""
import datetime
import logging

from waterflow.core import make_id
from waterflow.server.dao import DagDao
from waterflow.core.dao_models import PendingJob, WorkItem, Dag, Task
from waterflow.rest import Dag as RestDag, Task as RestTask

from waterflow.core.dao_models import JobView1


def get_job_details(dao: DagDao, job_id: str) -> JobView1:
    """
    Returns everything about a job, except for its tasks.
    For testing.
    """
    return dao.get_job_info(job_id)


def submit_job(dao: DagDao, job: PendingJob, now_utc=None) -> str:
    """
    Submits a new job for execution.

    :returns: the job id
    """
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

    return WorkItem()


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
    id_map = {int(k): v for k, v in id_map.items()}

    return {
        id_map[int(task_index)]: [id_map[int(i)] for i in neighboors]
        for task_index, neighboors in adj_list_by_index.items()
    }


def set_dag_for_job_REST(
    dao: DagDao, job_id: str, dag: RestDag, work_queue: str, now_utc=None
):
    now_utc = now_utc or datetime.datetime.utcnow()

    id_map = {}
    internal_tasks = []
    for (task_index,) in dag.tasks:
        task_id = make_id()
        id_map[task_index] = task_id
        internal_tasks.append(
            _rest_task_to_internal_task(dag.tasks[task_index], task_id)
        )

    adj_list = _transform_adj_list(
        dag.adj_list, id_map
    )  # TODO why were ints transformed into strings?
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
