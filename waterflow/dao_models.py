"""
Private data types used by the DAO.  Not meant to be part of the public API.
"""
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class PendingJob:
    """
    A new job submitted for execution.
    """
    job_name: str
    job_input64: str  # base64 string
    job_input64_v: int = 0  # TODO remove default value
    service_pointer: str = None  # any UTF8 string  # TODO remove default value
    tags: List[str] = None  # TODO implement, TODO remove default value
    work_queue: int = 0

@dataclass
class FetchDagTask:  # TODO rename to FetchDagAssignment?
    """
    Info about a job needed to fetch the dag.
    """
    job_id: str
    job_input64: str   # TODO need job_input64_v also!
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
    dag_fetch: FetchDagTask = None
    run_task: TaskAssignment = None

    def empty(self):
        return self.dag_fetch is None and self.run_task is None

@dataclass
class Task:
    """
    This is the internal class that is used on the Dag object.
    Internal class that is only used to pass info to the DAO.  See also job.py:Dag
    """
    # job_id: str
    task_id: str  # TODO this class is internal because external caller won't know the task ids ahead of time
    task_name: str
    # state: int
    input64: str
    input64_v: int = 0  # TODO remove default value
    service_pointer: str = None  # TODO remove default value


@dataclass
class Dag:
    """
    Internal class, only used to pass to the DAO.
    """
    raw_dag64: str  # the raw bytes of the dag, serialized as a base64 string
    raw_dagv: int  # version for the serialization of the dag
    tasks: Task
    adj_list: Dict[str, List[str]]