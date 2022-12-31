import json
import requests

from waterflow import to_base64_str

# return PendingJob(
#     job_name=request_json["job_name"],
#     job_input64=request_json["job_input64"],
#     job_input64_v=request_json.get("job_input64_v"),
#     service_pointer=request_json["service_pointer"],  # TODO maybe this should be separate fields?
#     tags=request_json.get("tags"),
#     work_queue=work_queue,
# )

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
    work_queue = 0

    resp = requests.post(f"{url_base}/api/submit_job/{work_queue}", json=request)
    print(resp.content)
    pass

if __name__ == "__main__":
    URL_BASE = "http://localhost:80"

    test_submit_job(URL_BASE)
