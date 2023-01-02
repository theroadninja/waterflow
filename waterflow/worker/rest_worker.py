import logging
from dataclasses import dataclass
import json
import random
import requests
import time
import threading
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



class StopSignal:  # TOOD trap sigint
    def __init__(self):
        self.stop_requested = False


def main_loop(stop_signal: StopSignal, thread_index: int, config: WorkerConfig):
    logger = logging.getLogger("worker")
    logger.info(f"Starting Thread {thread_index} -- " + str(threading.get_ident()))
    full_worker_name = f"{config.worker_name}_{thread_index}_" + str(threading.get_ident())
    while not stop_signal.stop_requested:
        try:
            # look for work
            resp = requests.get(f"{config.url_base}/api/get_work/{config.work_queue}/{full_worker_name}")
            resp.raise_for_status()
            # TODO intelligently back off on 429s

            try:
                work_item = WorkItem.from_json_dict(json.loads(resp.content))
            except json.decoder.JSONDecodeError as ex:
                logger.error(f"could not parse: {resp.content}")
                raise ex


            if work_item.fetch_task:
                job_id = work_item.fetch_task.job_id
                logger.info(f"thread={thread_index} got a dag fetch task for job {job_id}")


                time.sleep(1)  # simulate fetching a dag  # TODO RPC call goes here
                dag = make_diamond10()

                print(dag.to_json())
                resp = requests.post(f"{config.url_base}/api/set_dag/{config.work_queue}/{job_id}", json=dag.to_json())
                resp.raise_for_status()

                # TODO need to set the dag on the job
                # TODO should the server automatically re-assign dag fetch tasks if they've been idle for 10 minutes?

            elif work_item.run_task:
                job_id = work_item.run_task.job_id
                task_id = work_item.run_task.task_id
                logger.info(f"thread={thread_index} got task {task_id}")

                # TODO bring back up to 10
                time.sleep(5)  # simulate running the task

                logger.info("task {work_item.run_task.task_id} complete")

                # TODO need to mark the task complete
                resp = requests.post(f"{config.url_base}/api/complete_task/{job_id}/{task_id}")
                resp.raise_for_status()

            else:
                print(f"thread={thread_index} got nothing")
                time.sleep(random.randint(config.sleep_sec_min, config.sleep_sec_max))

            pass
        except Exception as ex:
            logger.exception(str(ex))

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

        stop_signal = StopSignal()
        worker_conf = WorkerConfig("Worker1", self.url_base, 0)

        # TODO add a special thread to send keep-alive signals?

        threads = []
        for i in range(self.thread_count):
            t = threading.Thread(target=main_loop, args=(stop_signal, i, worker_conf))
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()

        self.logger.info("All worker threads have stopped")