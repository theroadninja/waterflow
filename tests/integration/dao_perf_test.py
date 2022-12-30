"""
Performance test against the dao alone.
"""
import time

import waterflow
from waterflow import to_base64_str
import waterflow.dao
from waterflow.dao_models import PendingJob
from waterflow.task import Task, TaskState
from waterflow.job import Dag


class StopWatch:
    # wall clock time
    def __init__(self):
        self.started = time.time()
        self.stopped = None

    def stop(self):
        self.stopped = time.time()

    def elapsed_sec(self) -> float:
        stopped = self.stopped or time.time()
        return stopped - self.started

    def __str__(self):
        sec = self.elapsed_sec()
        return f"{sec} seconds"

    def __repr__(self):
        return self.__str__()


def make_test_dag10():
    """
    Creates a test dag
             A
           /  \
          B    C
         / \  / \
        D   E    F
       / \ / \  / \
     G    H    I    J
    """

    a = waterflow.make_id()
    b = waterflow.make_id()
    c = waterflow.make_id()
    d = waterflow.make_id()
    e = waterflow.make_id()
    f = waterflow.make_id()
    g = waterflow.make_id()
    h = waterflow.make_id()
    i = waterflow.make_id()
    j = waterflow.make_id()
    tasks = [
        Task(a, task_name="A", input64=to_base64_str("A")),
        Task(b, task_name="B", input64=to_base64_str("B")),
        Task(c, task_name="C", input64=to_base64_str("C")),
        Task(d, task_name="D", input64=to_base64_str("D")),
        Task(e, task_name="E", input64=to_base64_str("E")),
        Task(f, task_name="F", input64=to_base64_str("F")),
        Task(g, task_name="G", input64=to_base64_str("G")),
        Task(h, task_name="H", input64=to_base64_str("H")),
        Task(i, task_name="I", input64=to_base64_str("I")),
        Task(j, task_name="J", input64=to_base64_str("J")),
    ]
    task_adj = {
        a: [b, c],
        b: [d, e],
        c: [e, f],
        d: [g, h],
        e: [h, i],
        f: [i, j],
    }
    return Dag(to_base64_str("TEST"), 0, tasks, task_adj)


def clear_tables(conn):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM jobs;")
        conn.commit()


def fetch_tasks_1by1(dao, n, worker="w"):
    """
    Fetches up to n tasks 1 by 1, stopping as soon as the server doesnt have one to give.
    """
    tasks = []
    while True:
        results = dao.get_and_start_tasks([worker])
        if len(results) == 0:
            return tasks
        elif len(results) == 1:
           tasks += results
        else:
            raise Exception()
        if len(tasks) >= n:
            return tasks




def single_threaded_test(conn_pool, job_count, task_batch_size=1000):
    dao = waterflow.dao.DagDao(conn_pool, "waterflow")
    stw_total = StopWatch()

    job_ids = []
    stw1 = StopWatch()
    for job_index in range(job_count):
        job_id = dao.add_job(PendingJob(waterflow.to_base64_str(f"job index={job_index}")))
        job_ids.append(job_id)

    print(f"{len(job_ids)} jobs created in {stw1}")

    stw2 = StopWatch()
    fetch_dag_tasks = []
    while True:
        tasks = dao.get_and_start_jobs(["w0"])
        if tasks:
            fetch_dag_tasks += tasks
        else:
            break

    print(f"{len(fetch_dag_tasks)} dag-fetch tasks pulled in {stw2}")


    stw3 = StopWatch()
    for fetch_task in fetch_dag_tasks:
        fetch_task.job_id
        dao.set_dag(fetch_task.job_id, make_test_dag10())  # NOTE: make_test_dag10() may slow it down
        dao.update_task_deps(fetch_task.job_id)  # TODO really need to start using server methods

    print(f"{len(fetch_dag_tasks)} dags set in {stw3}")


    # WORKERS = [f"w{i}" for i in range(task_batch_size)]
    stw_all_tasks = StopWatch()
    task_count = 0
    while True:
        stw_batch = StopWatch()
        # fetching all at once:
        # tasks = dao.get_and_start_tasks(WORKERS)
        tasks = fetch_tasks_1by1(dao, task_batch_size)
        task_count += len(tasks)
        if not tasks:
            print("no more tasks")
            break

        stw_batch.stop()

        stw_batch2 = StopWatch()
        batch_stop_sec = 0
        batch_update_sec = 0
        for task_assignment in tasks:
            stw_stop = StopWatch()
            dao.complete_task(task_assignment.job_id, task_assignment.task_id)
            batch_stop_sec += stw_stop.elapsed_sec()
            stw_update = StopWatch()
            dao.update_task_deps(task_assignment.job_id)  # TODO start using server methods so we dont have to remember this
            batch_update_sec += stw_update.elapsed_sec()
        print(f"batch of {len(tasks)} tasks pulled in {stw_batch}, marked complete in {stw_batch2} running total: {task_count} complete_task()={batch_stop_sec} update_task_deps()={batch_update_sec}")

    print(f"{task_count} tasks run in {stw_all_tasks}")
    print(f"TEST COMPLETE: total time elapsed: {stw_total}")
    # TODO verify all tasks and jobs show as completed in the DB


if __name__ == "__main__":
    conn_pool = waterflow.get_connection_pool_from_file("../../local/mysqlconfig.json", "waterflow_dao")

    # NOTE:  how to figure out which engine you are using:
    # select table_name, engine from information_schema.tables where table_schema = "waterflow";

    with conn_pool.get_connection() as conn:
        clear_tables(conn)

    # according to: https://dev.mysql.com/doc/connector-python/en/connector-python-connection-pooling.html
    # the mysql connector pool is thread safe
    single_threaded_test(conn_pool, 500)  # 10K takes 22 sec, 100K takes 338 sec

    # 1,000:    116 sec

    # 20000 tasks run in 298.28511142730713 seconds
    # TEST COMPLETE: total time elapsed: 332.72497367858887 seconds