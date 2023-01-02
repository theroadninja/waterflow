"""
Submits a bunch of jobs for execution to a flask interface for Waterflow.
"""
import json
import requests

from waterflow import to_base64_str



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
    url_base = "http://127.0.0.1:80"  # warning:  "localhost" can cause a use perf hit due to IPV6
    for i in range(8000):
        submit_job(url_base, 0, str(i))
    pass