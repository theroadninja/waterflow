import logging
import inspect
import json
import os

from waterflow.core import load_local_config
from waterflow.worker.rest_worker import RestWorker

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("worker")
    logger.info("Starting Worker")

    config = load_local_config("worker_config.json")

    # perf tests use thread count of 512
    thread_count = 32
    worker = RestWorker(logger, config["server_url_base"], thread_count, config["work_queue"])
    worker.run()
