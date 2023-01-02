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




class WaterflowRestClient:
    def __init__(self, url_base):
        self.url_base = url_base


    def get_work(self, work_queue, full_worker_name, retries=100):
        for _ in range(retries):
            try:
                resp = requests.get(f"{self.url_base}/api/get_work/{work_queue}/{full_worker_name}")
                resp.raise_for_status()
                return resp
            except requests.exceptions.HTTPError as ex:
                if ex.response.status_code == 429:
                    time.sleep(random.uniform(2, 4))
                else:
                    raise ex

        raise Exception("max retries exceeded")


    def complete_task(self, job_id, task_id, retries=60):  # TODO reduce retries?
        # TODO add retries
        for _ in range(retries):
            try:
                resp = requests.post(f"{self.url_base}/api/complete_task/{job_id}/{task_id}")
                resp.raise_for_status()
                return
            except requests.exceptions.HTTPError as ex:
                if ex.response.status_code == 429:
                    time.sleep(random.uniform(5, 6))
                else:
                    time.sleep(random.uniform(5, 6))  # TODO better behavior
        raise Exception("max retries exceeded")

    def set_dag(self, work_queue, job_id, dag, retries=16):  # TODO lower retries again?  and measure max needed
        logger = logging.getLogger("worker")

        # TODO implement retries
        #print(dag.to_json())
        for _ in range(retries):
            try:
                resp = requests.post(f"{self.url_base}/api/set_dag/{work_queue}/{job_id}", json=dag.to_json())
                resp.raise_for_status()
                return
            except requests.exceptions.HTTPError as ex:
                if ex.response.status_code == 429:
                    logger.info("Got 429 error, backing off and retrying")  # TODO log lines should include the thread!
                    time.sleep(random.uniform(5,6))  # TODO better backoff
                elif ex.response.status_code in range(500, 600):
                    with error_500_lock:
                        Error500Counter.count += 1

                    logger.info("Got 500 error, retrying")  # TODO log lines should include the thread!
                    time.sleep(random.uniform(5,6))  # TODO better backoff
                    pass  # retry
                else:
                    raise ex
        raise Exception("max retries exceeded")




def main_loop(stop_signal: StopSignal, thread_index: int, config: WorkerConfig):

    client = WaterflowRestClient(config.url_base)

    # startup jitter
    if config.startup_jitter:  # TODO disable and check execution for correctlness  (this is a perf op)
        time.sleep(random.uniform(0.1, 1.0))

    logger = logging.getLogger("worker")
    logger.info(f"Starting Thread {thread_index} -- " + str(threading.get_ident()))
    full_worker_name = f"{config.worker_name}_{thread_index}_" + str(threading.get_ident())
    while not stop_signal.stop_requested:
        try:
            # look for work
            resp = client.get_work(config.work_queue, full_worker_name)
            # resp = requests.get(f"{config.url_base}/api/get_work/{config.work_queue}/{full_worker_name}")
            # resp.raise_for_status()
            # TODO intelligently back off on 429s

            try:
                work_item = WorkItem.from_json_dict(json.loads(resp.content))
            except json.decoder.JSONDecodeError as ex:
                logger.error(f"could not parse: {resp.content}")
                raise ex


            if work_item.fetch_task:
                job_id = work_item.fetch_task.job_id
                logger.debug(f"thread={thread_index} got a dag fetch task for job {job_id}")

                with set_lock:
                    if job_id in job_id_cache:
                        job_id_duplicates.add(job_id)
                        raise Exception(f"DUPLICATE JOB ID DETECTED {job_id}")
                    else:
                        job_id_cache.add(job_id)

                time.sleep(1)  # simulate fetching a dag  # TODO RPC call goes here
                dag = make_diamond10()

                client.set_dag(config.work_queue, job_id, dag)
                # print(dag.to_json())
                # resp = requests.post(f"{config.url_base}/api/set_dag/{config.work_queue}/{job_id}", json=dag.to_json())
                # resp.raise_for_status()

                # TODO need to set the dag on the job
                # TODO should the server automatically re-assign dag fetch tasks if they've been idle for 10 minutes?

            elif work_item.run_task:
                job_id = work_item.run_task.job_id
                task_id = work_item.run_task.task_id
                logger.debug(f"thread={thread_index} got task {task_id}")

                # TODO bring back up to 10
                time.sleep(6)  # simulate running the task

                logger.debug("task {work_item.run_task.task_id} complete")

                # TODO need to mark the task complete
                client.complete_task(job_id, task_id)
                # resp = requests.post(f"{config.url_base}/api/complete_task/{job_id}/{task_id}")
                # resp.raise_for_status()

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

    # MODES:
    # 1. execute N[=1] takss, task at a time, and stop
    # 2. execute one task at a time, forever
    # 3. execute up to M tasks at a time, forever
    # NOTE:  we want to go multithreaded anyway, so that we can provide keep_alive updates to the server

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