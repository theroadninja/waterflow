"""
Private data types used by the DAO.  Not meant to be part of the public API.

For the public API models, see `waterflow.rest`
"""
from dataclasses import dataclass
import datetime
from typing import List, Dict, Optional


@dataclass
class PendingJob:
    """
    A new job submitted for execution.
    """
    job_name: str
    job_input64: str  # base64 string
    job_input64_v: int = 0
    service_pointer: str = None  # any UTF8 string
    tags: List[str] = None  # TODO implement
    work_queue: int = 0


@dataclass
class FetchDagTask:  # TODO rename to FetchDagAssignment?
    """
    Info about a job needed to fetch the dag.
    """
    job_id: str
    job_input64: str
    # TODO job_input64_v also
    service_pointer: str
    work_queue: int
    worker: str


@dataclass
class TaskAssignment:
    """
    Data type returned to worker to tell it to run a task
    """
    job_id: str
    task_id: str
    task_name: str
    task_input64: str
    task_input64_v: int
    service_pointer: str


@dataclass
class WorkItem:
    dag_fetch: FetchDagTask = None  # TODO rename to 'fetch_task'
    run_task: TaskAssignment = None

    def empty(self):
        return self.dag_fetch is None and self.run_task is None


@dataclass
class Task:
    """
    This is the internal class that is used on the Dag object.
    Internal class that is only used to pass info to the DAO.
    """
    task_id: str  # this class is internal because external caller won't know the task ids ahead of time
    task_name: str
    input64: str
    input64_v: int = 0
    service_pointer: str = None


@dataclass
class Dag:
    """
    Internal class, only used to pass to the DAO.
    """
    raw_dag64: str  # the raw bytes of the dag, serialized as a base64 string
    raw_dagv: int  # version for the serialization of the dag
    tasks: Task
    adj_list: Dict[str, List[str]]


@dataclass
class JobStats:
    pending_count: int
    fetching_count: int
    running_count: int
    succeeded: int
    failed: int


@dataclass
class TaskStats:
    blocked_count: int
    pending_count: int
    paused_count: int
    running_count: int
    succeeded_count: int
    failed_count: int


@dataclass
class JobView1:
    """
    Only used for pulling info out of the DB about a job
    """
    job_id: str
    job_name: str
    job_input64: str
    job_input64_v: int
    service_pointer: str
    created_utc: datetime.datetime
    state: Optional[int]  # TODO why is this optional?
    worker: Optional[str]
    dag64: Optional[str]
    work_queue: str
    tags: List[str]


@dataclass
class TaskView1:
    """
    A readonly "view" of a task that includes its job_id
    """
    job_id: str
    task_id: str
    state: int
    task_input64: str
    updated_utc: datetime.datetime


def is_valid(
    tasks: List[Task], adj_list: Dict[str, List[str]]
):
    """
    :return True: if the given graph is valid
    """
    # TODO enforce a limit of 1024-4096 total tasks, to avoid the need for paging.
    # TODO enforce limits on task and job input sizes
    # TODO what about dags with only one tasks, or multiple unconnected tasks????
    # TODO make sure task list does not have duplicates
    # TODO detect cycles

    task_ids = set([task.task_id for task in tasks])
    if len(task_ids) < 1:
        raise ValueError("DAG must have at least one task")

    # every task id that appears somewhere in the adj list
    task_ids_in_adj = set()
    for node, links in adj_list.items():
        if not isinstance(links, List):
            raise ValueError("adj_list has non-list value")
        task_ids_in_adj.add(node)
        task_ids_in_adj.update(links)

    unknown_ids = task_ids_in_adj - task_ids
    if unknown_ids:
        raise ValueError(f"adj list contains unknown ids: {unknown_ids}")

    return True