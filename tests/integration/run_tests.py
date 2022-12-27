"""
Integration tests that use a live mysql database
"""

import waterflow
from waterflow import to_base64_str
import waterflow.dao
from waterflow.job import Dag
from waterflow.task import Task, TaskEligibilityState, TaskExecState
from waterflow.mysql_config import MysqlConfig
import uuid



def get_conn_pool():
    dbconf = MysqlConfig.from_file("../../local/mysqlconfig.json")
    return waterflow.get_connection_pool(dbconf, "waterflow_dao")






if __name__ == "__main__":
    from getpass import getpass   # pycharm: run -> edit configurations -> emulate terminal in output console
    import json
    import os

    conn_pool = get_conn_pool()
    conn_pool.get_connection()
    # conn = connect(host="localhost", user=username, password=pwd, database="waterflow")
    dao = waterflow.dao.DagDao(conn_pool, "waterflow")

    job_input = waterflow.to_base64_str("abc")

    job_id = dao.add_job(job_input)
    print(f"added job {job_id}")

    dao.get_and_start_jobs(workers=["worker1"])


    task1 = waterflow.make_id()
    task2 = waterflow.make_id()
    task3 = waterflow.make_id()
    task4 = waterflow.make_id()
    tasks = [
        Task(task_id=task1, input64=to_base64_str("A")),
        Task(task_id=task2, input64=to_base64_str("B")),
        Task(task_id=task3, input64=to_base64_str("C")),
        Task(task_id=task4, input64=to_base64_str("D")),
    ]
    task_adj = {
        task1: [task2, task3],
        task4: [task1],

    }

    dao.set_dag(job_id, Dag(waterflow.to_base64_str("def"), 0, tasks=tasks, adj_list=task_adj))

    dao.start_task(job_id, task3, "worker1")
    dao.start_task(job_id, task2, "worker1")
    dao.stop_task(job_id, task3, int(TaskExecState.SUCCEEDED))
    dao.stop_task(job_id, task2, int(TaskExecState.SUCCEEDED))
    dao.update_task_deps(job_id)