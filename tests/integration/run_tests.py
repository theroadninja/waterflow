"""
Integration tests that use a live mysql database
"""

import waterflow
from waterflow import to_base64_str
import waterflow.dao
from waterflow.dao_models import PendingJob
from waterflow.job import Dag
from waterflow.task import Task, TaskState
from waterflow.mysql_config import MysqlConfig
import uuid

if __name__ == "__main__":
    # note: from getpass import getpass   # pycharm: run -> edit configurations -> emulate terminal in output console
    conn_pool = waterflow.get_connection_pool_from_file("../../local/mysqlconfig.json", "waterflow_dao")
    dao = waterflow.dao.DagDao(conn_pool, "waterflow")

    job_input = waterflow.to_base64_str("abc")

    job_id = dao.add_job(PendingJob(job_input))
    print(f"added job {job_id}")

    dao.get_and_start_jobs(workers=["worker1"])


    task1 = waterflow.make_id()
    task2 = waterflow.make_id()
    task3 = waterflow.make_id()
    task4 = waterflow.make_id()
    tasks = [
        Task(task_id=task1, task_name="A", input64=to_base64_str("A")),
        Task(task_id=task2, task_name="B", input64=to_base64_str("B")),
        Task(task_id=task3, task_name="C", input64=to_base64_str("C")),
        Task(task_id=task4, task_name="D", input64=to_base64_str("D")),
    ]
    task_adj = {
        task1: [task2, task3],
        task4: [task1],
    }

    dao.set_dag(job_id, Dag(waterflow.to_base64_str("def"), 0, tasks=tasks, adj_list=task_adj), work_queue=0)
    dao.update_task_deps(job_id)

    dao.start_task(job_id, task3, "worker1")
    dao.start_task(job_id, task2, "worker1")
    dao.complete_task(job_id, task3)
    dao.complete_task(job_id, task2)
    dao.update_task_deps(job_id)