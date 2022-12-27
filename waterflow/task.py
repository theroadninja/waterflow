"""
Internal Representation of a Task Object.

TODO should this just be the public one?
"""
from enum import IntEnum
from dataclasses import dataclass
from typing import List, Dict

class TaskEligibilityState(IntEnum):
    BLOCKED = 0
    READY = 1  # or "AVAILABLE" ?  ALLOWED?
    PAUSED = 2

class TaskExecState(IntEnum):
    PENDING = 0   # needed to allow retries.  row doesn't exist == pending, also set to pending on retry...
    RUNNING = 1
    SUCCEEDED = 2
    FAILED = 3

@dataclass
class Task:
    """
    Internal class that is only used to pass info to the DAO.  See also job.py:Dag
    """
    # job_id: str
    task_id: str  # TODO this class is internal because external caller won't know the task ids ahead of time
    # state: int
    input64: str

def is_valid(tasks: List[Task], adj_list: Dict[str, List[str]]):  # TODO rename to check_valid and throw exceptions instead
    """
    :return True if the given graph is valid
    """
    # TODO write unit test


    # TODO what about dags with only one tasks, or multiple unconnected tasks????

    task_ids = set([task.task_id for task in tasks])

    if len(task_ids) < 1:
        raise ValueError("DAG must have at least one task")

    # TODO make sure task list does not have duplicates

    # every task id apppears somewhere in the adj list
    task_ids_in_adj = set()
    for node, links in adj_list.items():
        if not isinstance(links, List):
            raise ValueError("adj_list has non-list value")
        task_ids_in_adj.add(node)
        task_ids_in_adj.update(links)
    if task_ids != task_ids_in_adj:
        return False

    # TODO detect cycles and other invalid situations!

    return True
