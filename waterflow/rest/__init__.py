"""
Objects related to Waterflow's REST interface.
"""
import dataclasses
from dataclasses import dataclass
import json
from typing import Dict, List


@dataclass
class Task:
    """
    Describes a task to be run the the DAG Execution System.
    """
    task_name: str
    input64: str
    input64_v: int = 0  # TODO remove default value
    service_pointer: str = None  # TODO remove default value

    @classmethod
    def from_json_dict(cls, d):
        return Task(
            d.get("task_name"),
            d.get("input64"),
            d.get("input64_v"),
            d.get("service_pointer"),
        )


@dataclass
class Dag:
    """
    Defines the graph of tasks for a job.
    """
    raw_dag64: str  # the raw bytes of the dag, serialized as a base64 string
    raw_dagv: int  # version for the serialization of the dag
    tasks: Dict[int, Task]
    adj_list: Dict[int, List[int]]

    @classmethod
    def from_json_dict(cls, d):
        # TODO:  cant do this until we define a custom JSON decoder:
        if d is None:
            raise ValueError("input is None")
        return Dag(
            d.get("raw_dag64"),
            d.get("raw_dagv"),
            {task_index : Task.from_json_dict(task) for task_index, task in d.get("tasks").items()},
            d.get("adj_list"),
        )

    def to_json_dict(self):
        """
        Returns the dict that you would pass to json.dumps().  Necessary because of the weird interface of the requests
        lib:
        requests.post(URL, json=dag.to_json())
        """
        return dataclasses.asdict(self)

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))


@dataclass
class FetchDagWorkItem:  # TODO rename to FetchDagAssignment?
    """
    Data type returned to worker to tell it to fetch the DAG for a job.
    """
    job_id: str
    job_input64: str   # TODO need job_input64_v also!
    service_pointer: str
    work_queue: int

    @classmethod
    def from_json_dict(cls, d):
        if d is None:
            return None
        return FetchDagWorkItem(
            job_id=d.get("job_id"),
            job_input64=d.get("job_input64"),
            service_pointer=d.get("service_pointer"),
            work_queue=d.get("work_queue"),
        )

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))


@dataclass
class RunTaskWorkItem:
    """
    Data type returned to worker to tell it to run a task
    """
    job_id: str
    task_id: str
    task_name: str
    task_input64: str
    task_input64_v: int
    service_pointer: str

    @classmethod
    def from_json_dict(cls, d):
        if d is None:
            return None
        return RunTaskWorkItem(
            job_id=d.get("job_id"),
            task_id=d.get("task_id"),
            task_name=d.get("task_name"),
            task_input64=d.get("task_input64"),
            task_input64_v=d.get("task_input64_v"),
            service_pointer=d.get("service_pointer"),
        )

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))


@dataclass
class WorkItem:
    fetch_task: FetchDagWorkItem = None
    run_task: RunTaskWorkItem = None

    def empty(self):
        return self.fetch_task is None and self.run_task is None

    @classmethod
    def from_json_dict(cls, d):
        if d is None:
            return None
        return WorkItem(
            fetch_task=FetchDagWorkItem.from_json_dict(d.get("fetch_dag")),  # TODO change to fetch_task
            run_task=RunTaskWorkItem.from_json_dict(d.get("run_task")),
        )

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))
