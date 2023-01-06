import logging
from dataclasses import dataclass
import json
import random
import requests
import time
import threading
from waterflow import StopWatch
from waterflow.rest import WorkItem, Dag, Task
from waterflow.mocks.sample_dags_http import make_diamond10


def fetch_dag(fetch_task):
    logger = logging.getLogger("worker")
    logger.info("fetch_dag()")
    time.sleep(5)
    logger.info("done fetching dag()")


def run_task(task):
    logger = logging.getLogger("worker")
    logger.info("Starting Worker")


@dataclass
class WorkerConfig:
    worker_name: str
    url_base: str
    work_queue: int
    sleep_sec_min: int = 2
    sleep_sec_max: int = 16
    halt_on_no_work: bool = False
    startup_jitter: bool = True


# TODO get rid of this
set_lock = threading.Lock()
error_500_lock = threading.Lock()
job_id_cache = set()
job_id_duplicates = set()


class Error500Counter:
    count = 0


class StopSignal:  # TOOD trap sigint
    def __init__(self):
        self.stop_requested = False


def exp_backoff(failed_attempts, fixed_interval, exp_interval, max_wait, jitter):
    """
    The minimum, or starting interval is (fixed_interval + exp_interval^1)
    """
    # return random.uniform(5,6)  # TODO remove
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
    ):  # TODO lower retries again?  and measure max needed
        logger = logging.getLogger("worker")
        return self.make_http_call(
            lambda: requests.post(
                f"{self.url_base}/api/set_dag/{work_queue}/{job_id}", json=dag.to_json()
            )
        )


def main_loop(stop_signal: StopSignal, thread_index: int, config: WorkerConfig):

    client = WaterflowRestClient(config.url_base, thread_index)

    # startup jitter
    if config.startup_jitter:
        time.sleep(random.uniform(0.1, 1.0))

    logger = logging.getLogger("worker")
    logger.info(f"Starting Thread {thread_index} -- " + str(threading.get_ident()))
    full_worker_name = f"{config.worker_name}_{thread_index}_" + str(
        threading.get_ident()
    )
    while not stop_signal.stop_requested:
        try:
            # look for work
            resp = client.get_work(config.work_queue, full_worker_name)
            try:
                work_item = WorkItem.from_json_dict(json.loads(resp.content))
            except json.decoder.JSONDecodeError as ex:
                logger.error(f"could not parse: {resp.content}")
                raise ex

            if work_item.fetch_task:
                job_id = work_item.fetch_task.job_id
                logger.info(
                    f"thread={thread_index} got a dag fetch task for job {job_id}"
                )

                with set_lock:
                    if job_id in job_id_cache:
                        job_id_duplicates.add(job_id)
                        raise Exception(f"DUPLICATE JOB ID DETECTED {job_id}")
                    else:
                        job_id_cache.add(job_id)

                time.sleep(1)  # simulate fetching a dag  # TODO RPC call goes here
                dag = make_diamond10()

                client.set_dag(config.work_queue, job_id, dag)

                # TODO should the server automatically re-assign dag fetch tasks if they've been idle for 10 minutes?

            elif work_item.run_task:
                job_id = work_item.run_task.job_id
                task_id = work_item.run_task.task_id
                logger.info(f"thread={thread_index} got task {task_id}")

                time.sleep(10)  # simulate running the task

                logger.debug("task {work_item.run_task.task_id} complete")
                client.complete_task(job_id, task_id)

            else:
                print(f"thread={thread_index} got nothing")
                if config.halt_on_no_work:
                    return
                time.sleep(random.randint(config.sleep_sec_min, config.sleep_sec_max))

            pass
        except Exception as ex:
            logger.exception(str(ex))

            # TODO only sleep on 429s
            time.sleep(random.uniform(1, 3))


class RestWorker:
    def __init__(self, logger, url_base, thread_count, work_queue):
        self.logger = logger
        self.url_base = url_base
        self.thread_count = thread_count
        self.work_queue = work_queue
        self.name = "test_worker"

    def main_loop(self):

        stw = StopWatch()
        stop_signal = StopSignal()
        worker_conf = WorkerConfig("Worker1", self.url_base, 0, halt_on_no_work=True)

        # TODO add a special thread to send keep-alive signals?

        threads = []
        for i in range(self.thread_count):
            t = threading.Thread(target=main_loop, args=(stop_signal, i, worker_conf))
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()

        self.logger.info(f"All worker threads have stopped.  Total exec time was {stw}")
        print(f"Duplicate job ids: {job_id_duplicates}")
        print(f"Count of 500 error: {Error500Counter.count}")
