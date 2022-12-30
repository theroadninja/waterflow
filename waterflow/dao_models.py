"""
Private data types used by the DAO.  Not meant to be part of the public API.
"""
from dataclasses import dataclass
from typing import List

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
    job_input64: str
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