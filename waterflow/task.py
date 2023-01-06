"""
Internal Representation of a Task Object.

TODO should this just be the public one?
"""
from enum import IntEnum
from dataclasses import dataclass
import datetime
from typing import List, Dict, Optional

# class TaskEligibilityState(IntEnum):
#     BLOCKED = 0
#     READY = 1  # or "AVAILABLE" ?  ALLOWED?
#     PAUSED = 2
#
# class TaskExecState(IntEnum):
#     PENDING = 0   # needed to allow retries.  row doesn't exist == pending, also set to pending on retry...
#     RUNNING = 1
#     SUCCEEDED = 2
#     FAILED = 3


class TaskState(IntEnum):
    """
    New global state meant to replace TaskEligibilityState and TaskExecState
    """

    BLOCKED = 0
    PENDING = 1
    PAUSED = 2
    RUNNING = 3
    SUCCEEDED = 4
    FAILED = 5


# @dataclass
# class TaskExecution:
#     pass


from waterflow import dao_models

Task = dao_models.Task  # TODO finish moving to dao_models.py


@dataclass
class TaskView1:  # TODO not sure what the final form will be
    job_id: str
    task_id: str
    state: int
    task_input64: str
    updated_utc: datetime.datetime


def is_valid(
    tasks: List[Task], adj_list: Dict[str, List[str]]
):  # TODO rename to check_valid and throw exceptions instead
    """
    :return True if the given graph is valid
    """
    # TODO write unit test

    # TODO enforce a limit of 1024-4096 total tasks, to avoid the need for paging.
    # TODO also enforce a limit of task input size, and consider changing from BLOB to TEXT
    # TODO also enforce a limit of job input size, and consider changing from BLOB to TEXT

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

    unknown_ids = task_ids_in_adj - task_ids
    if unknown_ids:
        raise ValueError(f"adj list contains unknown ids: {unknown_ids}")

    # TODO detect cycles and other invalid situations!

    return True
