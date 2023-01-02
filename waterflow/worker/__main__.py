import logging

from waterflow.worker.rest_worker import RestWorker

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("worker")
    logger.info("Starting Worker")

    url_base = "http://127.0.0.1:80"
    thread_count = 16  # lots of conn pool issues at 20
    work_queue = 0
    worker = RestWorker(logger, url_base, thread_count, work_queue)

    worker.main_loop()

    # TODO why am I getting this error when the conn pool is set to a max of 32?
    # mysql.connector.errors.DatabaseError: 1040 (HY000): Too many connections
    # NOTE:  to fix I ran:  set global max_connections = 500;