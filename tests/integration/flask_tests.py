import json
import requests

from waterflow import to_base64_str


WORK_QUEUE = 0


def test_submit_job(url_base):
    job_name = "job0"
    job_input64 = to_base64_str("some input")
    request = {
        "job_name": job_name,
        "job_input64": job_input64,
        "job_input64_v": 0,
        "service_pointer": "123",  # TODO separate fields?
        "tags": ["type=spark"],
    }

    resp = requests.post(f"{url_base}/api/submit_job/{WORK_QUEUE}", json=request)
    print(resp.content)

def test_run_job(url_base):
    job_name = "job0"
    job_input64 = to_base64_str("some input")
    request = {
        "job_name": job_name,
        "job_input64": job_input64,
        "job_input64_v": 0,
        "service_pointer": "123",
        "tags": ["type=spark"],
    }


    # create the job
    resp = requests.post(f"{url_base}/api/submit_job/{WORK_QUEUE}", json=request)
    job_id = json.loads(resp.content)["job_id"]

    # start fetching the dag
    WORKER = "w0"
    resp = requests.get(f"{url_base}/api/get_work/{WORK_QUEUE}/{WORKER}", json=request)
    # "/api/get_work/<int:work_queue>/<string:worker>"
    print(resp.content)

    # TODO do I need to manually check HTTP codes?

    # finish fetching the dag



if __name__ == "__main__":
    URL_BASE = "http://localhost:80"

    test_submit_job(URL_BASE)
    test_run_job(URL_BASE)
