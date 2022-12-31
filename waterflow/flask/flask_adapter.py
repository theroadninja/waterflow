"""
Code to convert between Flask's representation of a request and the internal data types.
"""
from typing import Dict
from waterflow.dao_models import PendingJob


def read_pending_job(work_queue, request_json: Dict):
    """
    Incoming submit_job request -> PendingJob object
    """
    return PendingJob(
        job_name=request_json["job_name"],
        job_input64=request_json["job_input64"],
        job_input64_v=request_json.get("job_input64_v"),
        service_pointer=request_json["service_pointer"],  # TODO maybe this should be separate fields?
        tags=request_json.get("tags"),
        work_queue=work_queue,
    )