from enum import IntEnum


class TaskState(IntEnum):
    """
    Represents the execution state of the task.
    """
    BLOCKED = 0
    PENDING = 1
    PAUSED = 2
    RUNNING = 3
    SUCCEEDED = 4
    FAILED = 5