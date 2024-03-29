"""
Submits a bunch of jobs for execution to a server interface for Waterflow.
"""
import json
import requests

from waterflow.core import load_local_config
from waterflow import to_base64_str, StopWatch


def submit_job(url_base, work_queue, job_input):

    job_name = "TestJob"
    job_input64 = to_base64_str(job_input)
    request = {
        "job_name": job_name,
        "job_input64": job_input64,
        "job_input64_v": 0,
        "service_pointer": "123",
        "tags": ["type=spark"],
    }

    # create the job
    resp = requests.post(f"{url_base}/api/submit_job/{work_queue}", json=request)
    job_id = json.loads(resp.content)["job_id"]
    print(f"submitted job {job_id}")


if __name__ == "__main__":
    config = load_local_config("worker_config.json")
    url_base = config["server_url_base"]
    stw = StopWatch()

    # JOB_COUNT = 14000
    JOB_COUNT = 64

    for i in range(JOB_COUNT):
        submit_job(url_base, 0, str(i))
    print(f"{JOB_COUNT} jobs added in {stw}")