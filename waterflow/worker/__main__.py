import logging

from waterflow.worker.rest_worker import RestWorker

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("worker")
    logger.info("Starting Worker")

    url_base = "http://127.0.0.1:80"

    thread_count = 512
    work_queue = 0
    worker = RestWorker(logger, url_base, thread_count, work_queue)

    worker.main_loop()