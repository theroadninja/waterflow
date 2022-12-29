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
    job_input64: str  # base64 string
    job_input64_v: int = 0  # TODO remove default value
    service_pointer: str = None  # any UTF8 string  # TODO remove default value
    tags: List[str] = None  # TODO implement, TODO remove default value

@dataclass
class FetchDagTask:
    """
    Info about a job needed to fetch the dag.
    """
    job_id: str
    job_input64: str
    service_pointer: str
    worker: str