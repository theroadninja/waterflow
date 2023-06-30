import logging
from dataclasses import dataclass
import json
import random
import requests
import signal
import time
import threading
from waterflow import StopWatch
from waterflow.rest import WorkItem, Dag, Task
from waterflow.core import StopSignal
from . import client_factory
from .waterflow_client import Error500Counter, WaterflowRestClient


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

job_id_cache = set()
job_id_duplicates = set()


def main_loop(stop_signal: StopSignal, thread_index: int, config: WorkerConfig):
    waterflow_client = WaterflowRestClient(config.url_base, thread_index)

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
            resp = waterflow_client.get_work(config.work_queue, full_worker_name)
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

                bclient = client_factory.get_client_for(work_item.fetch_task.service_pointer)
                dag = bclient.get_dag()
                waterflow_client.set_dag(config.work_queue, job_id, dag)


            elif work_item.run_task:
                job_id = work_item.run_task.job_id
                task_id = work_item.run_task.task_id
                logger.info(f"thread={thread_index} got task {task_id}")

                bclient = client_factory.get_client_for(work_item.run_task.service_pointer)
                bclient.run_task()


                logger.debug("task {work_item.run_task.task_id} complete")
                waterflow_client.complete_task(job_id, task_id)

            else:
                logger.info(f"thread={thread_index} got nothing")
                if config.halt_on_no_work:
                    logger.info(f"halting thread={thread_index} halt_on_no_work={config.halt_on_no_work}")
                    return
                time.sleep(random.randint(config.sleep_sec_min, config.sleep_sec_max))

        except Exception as ex:
            logger.exception(str(ex))

            # TODO only sleep on 429s
            time.sleep(random.uniform(1, 3))


class RestWorker:
    def __init__(self, logger, url_base, thread_count, work_queue, halt_on_no_work=False):
        self.logger = logger
        self.url_base = url_base
        self.thread_count = thread_count
        self.work_queue = work_queue
        self.name = "test_worker"
        self.halt_on_no_work = halt_on_no_work

    def run(self):

        stw = StopWatch()
        stop_signal = StopSignal()
        stop_signal.register(signal.SIGINT, signal.SIGTERM)
        worker_conf = WorkerConfig("Worker1", self.url_base, 0, halt_on_no_work=self.halt_on_no_work)

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
