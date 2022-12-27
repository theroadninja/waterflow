"""
Integration tests that use a live mysql database
"""
import base64
from mysql.connector import connect, pooling
import waterflow.dao
from waterflow.task import Task, TaskEligibilityState, TaskExecState
import uuid


def get_conn_pool():
    if os.path.isfile("../../local/mysqlcreds.json"):
        with open("../../local/mysqlcreds.json") as f:
            d = json.loads(f.read())
        username, pwd = d["username"], d["password"]
    else:
        pwd = getpass("Password>")
        username = "root"
    dbname = "waterflow"
    host = "localhost"

    return pooling.MySQLConnectionPool(
        pool_name="waterflow_dao",
        pool_size=5,  # max for mysql?  default is 5
        pool_reset_session=True,
        host=host,
        database=dbname,
        user=username,
        password=pwd,
    )


def to_base64(s: str):
    if not isinstance(s, str):
        raise ValueError("s must be a string")
    return base64.b64encode(s.encode("UTF-8")).decode("UTF-8")

def get_id():
    return str(uuid.uuid4()).replace("-", "")

if __name__ == "__main__":
    from getpass import getpass   # pycharm: run -> edit configurations -> emulate terminal in output console
    import json
    import os

    conn_pool = get_conn_pool()
    conn_pool.get_connection()
    # conn = connect(host="localhost", user=username, password=pwd, database="waterflow")
    dao = waterflow.dao.DagDao(conn_pool, "waterflow")

    job_input = to_base64("abc")

    job_id = dao.add_job(job_input)
    print(f"added job {job_id}")

    dao.start_job(job_id, worker="worker1")


    task1 = get_id()
    task2 = get_id()
    task3 = get_id()
    task4 = get_id()
    tasks = [
        Task(task_id=task1, state=(int(TaskEligibilityState.BLOCKED)), input64=to_base64("A")),
        Task(task_id=task2, state=(int(TaskEligibilityState.BLOCKED)), input64=to_base64("B")),
        Task(task_id=task3, state=(int(TaskEligibilityState.BLOCKED)), input64=to_base64("C")),
        Task(task_id=task4, state=(int(TaskEligibilityState.BLOCKED)), input64=to_base64("D")),
    ]
    task_adj = {
        task1: [task2, task3],
        task4: [task1],

    }

    dao.set_dag(job_id, dag64=to_base64("def"), tasks=tasks, task_deps=task_adj)

    dao.start_task(job_id, task3, "worker1")
    dao.start_task(job_id, task2, "worker1")
    dao.stop_task(job_id, task3, int(TaskExecState.SUCCEEDED))
    dao.stop_task(job_id, task2, int(TaskExecState.SUCCEEDED))
    dao.update_task_deps(job_id)