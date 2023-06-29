from enum import IntEnum


class JobExecutionState(IntEnum):
    PENDING = 0  # note:  PENDING means there is no execution row
    DAG_FETCH = 1  # TODO rename to "FETCHING" ?
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4