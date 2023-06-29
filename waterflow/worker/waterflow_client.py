"""
Client for the Waterflow service.
"""
import logging
import random
import requests
import time
import threading

# Performance Testing
error_500_lock = threading.Lock()


class Error500Counter:
    count = 0


def exp_backoff(failed_attempts, fixed_interval, exp_interval, max_wait, jitter):
    """
    The minimum, or starting interval is (fixed_interval + exp_interval^1)
    """
    if failed_attempts < 1:
        raise ValueError()
    if exp_interval <= 0:
        raise ValueError()
    return min(
        max_wait, fixed_interval + exp_interval**failed_attempts
    ) + random.uniform(0, jitter)


class WaterflowRestClient:
    def __init__(self, url_base, thread_index):
        self.url_base = url_base
        self.thread_index = thread_index  # for logging

    def make_http_call(self, http_fn):
        logger = logging.getLogger("worker")
        max_attempts = 100
        attempt_count = 0
        while attempt_count < max_attempts:
            try:
                attempt_count += 1
                resp = http_fn()
                resp.raise_for_status()
                return resp
            except requests.exceptions.HTTPError as ex:
                if ex.response.status_code == 429:

                    wait_sec = exp_backoff(
                        attempt_count,
                        fixed_interval=3,
                        exp_interval=2,
                        max_wait=60,
                        jitter=1.5,
                    )
                    logger.info(
                        f"thr={self.thread_index} Got 429, waiting {wait_sec} sec and retrying"
                    )  # TODO log lines should include the thread!  and API name
                    time.sleep(wait_sec)

                elif ex.response.status_code in range(500, 600):
                    with error_500_lock:
                        Error500Counter.count += 1

                    logger.info(
                        "Got 500 error, retrying"
                    )  # TODO log lines should include the thread!
                    time.sleep(random.uniform(5, 6))  # TODO better backoff?
                else:
                    raise ex

        raise Exception("max retries exceeded")

    def get_work(self, work_queue, full_worker_name, retries=100):
        return self.make_http_call(
            lambda: requests.get(
                f"{self.url_base}/api/get_work/{work_queue}/{full_worker_name}"
            )
        )

    def complete_task(self, job_id, task_id, retries=60):  # TODO reduce retries?
        return self.make_http_call(
            lambda: requests.post(
                f"{self.url_base}/api/complete_task/{job_id}/{task_id}"
            )
        )

    def set_dag(
        self, work_queue, job_id, dag, retries=32
    ):
        logger = logging.getLogger("worker")
        return self.make_http_call(
            lambda: requests.post(
                f"{self.url_base}/api/set_dag/{work_queue}/{job_id}", json=dag.to_json()
            )
        )