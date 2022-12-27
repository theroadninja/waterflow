from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, List

from .task import Task

class JobExecutionState(IntEnum):  # enum b/c its pythonic (though stupid)
    PENDING = 0  # note:  PENDING means there is no execution row
    DAG_FETCH = 1  # TODO rename to "FETCHING" ?
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4

@dataclass
class FetchDagTask:
    job_id: str
    job_input64: str
    worker: str

@dataclass
class Dag:
    """
    Internal class, only used to pass to the DAO.
    """
    raw_dag64: str  # the raw bytes of the dag, serialized as a base64 string
    raw_dagv: int  # version for the serialization of the dag
    tasks: Task
    adj_list: Dict[str, List[str]]