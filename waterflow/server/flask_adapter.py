"""
Code to convert between Flask's representation of a request and the internal data types.
"""
import json
from typing import Dict
from waterflow.core.dao_models import PendingJob, WorkItem, FetchDagTask, TaskAssignment
from waterflow.rest import Dag


def assert_string(value):
    if not isinstance(value, str):
        raise ValueError(f"value is not a string:  {value}")


def read_pending_job(work_queue, request_json: Dict):
    """
    Incoming submit_job request -> PendingJob object
    """
    return PendingJob(
        job_name=request_json["job_name"],
        job_input64=request_json["job_input64"],
        job_input64_v=request_json.get("job_input64_v"),
        service_pointer=request_json[
            "service_pointer"
        ],  # TODO maybe this should be separate fields?
        tags=request_json.get("tags"),
        work_queue=work_queue,
    )


def work_item_to_response(work_item: WorkItem) -> Dict:
    def fetch_task_to_dict(fetch_task: FetchDagTask):
        if fetch_task is None:
            return None
        assert_string(fetch_task.job_id)
        assert_string(fetch_task.job_input64)
        return {
            "job_id": fetch_task.job_id,
            "job_input64": fetch_task.job_input64,
            "service_pointer": fetch_task.service_pointer,  # TODO consider making this separate fields
            "work_queue": fetch_task.work_queue,
        }

    def run_task_to_dict(task_assign: TaskAssignment):
        if task_assign is None:
            return None
        return {
            "job_id": task_assign.job_id,
            "task_id": task_assign.task_id,
            "task_name": task_assign.task_name,
            "task_input64": task_assign.task_input64,
            "task_input64_v": task_assign.task_input64_v,
            "service_pointer": task_assign.service_pointer,
        }

    return {
        "fetch_dag": fetch_task_to_dict(work_item.dag_fetch),
        "run_task": run_task_to_dict(work_item.run_task),
    }


def read_dag_from_request(request_json: Dict):

    return Dag.from_json_dict(json.loads(request_json))
