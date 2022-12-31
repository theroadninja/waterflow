from dataclasses import dataclass
import datetime
from enum import IntEnum
from typing import Dict, List, Optional

from .task import Task


# TODO move this stuff to dao_models.py

class JobExecutionState(IntEnum):  # enum b/c its pythonic (though stupid)
    PENDING = 0  # note:  PENDING means there is no execution row
    DAG_FETCH = 1  # TODO rename to "FETCHING" ?
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4

from waterflow import dao_models
Dag = dao_models.Dag  # TODO finish moving Dag to dao_models.py

@dataclass
class JobView1:  # TODO not sure what the final form will be
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