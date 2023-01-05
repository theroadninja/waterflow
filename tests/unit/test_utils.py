import base64
import inspect
import os

import waterflow
from waterflow.dao import DagDao

MYPATH = os.path.dirname(os.path.abspath(inspect.stack()[0][1]))

def get_conn_pool():
    FILENAME = os.path.abspath("../local/unit_test_config.json")
    print(os.path.abspath(FILENAME))
    if not os.path.isfile(FILENAME):
        raise Exception(f"Can't find {FILENAME}")

    return waterflow.get_connection_pool_from_file(FILENAME, "unit_test_pool")

def path_to_sql():
    filename = f"{MYPATH}/../../sql/database.sql"
    if not os.path.isfile(filename):
        raise FileNotFoundError(filename)
    return filename

def drop_and_recreate_database(conn):
    with conn.cursor() as cursor:
        table_list = ",".join(DagDao.ALL_TABLES)
        drop_sql = f"DROP TABLE IF EXISTS {table_list};"
        cursor.execute(drop_sql)

        with open(path_to_sql()) as f:
            create_sql = f.read()
        results = cursor.execute(create_sql, multi=True)
        # we must do this or we get a "connection not available" error closing the connection
        for _ in results:
            pass  # print("{} {}".format(result.statement, result.fetchall()))


def task_view1_list_to_dict(results):
    """
    Takes the result of DagDao.get_tasks_by_job() and turns them into a dict where the
    keys of the dict are the task inputs (assumed to be a string)
    """
    return {base64.b64decode(task.task_input64).decode("UTF-8"): task for task in results}

def get_task_state(dao: DagDao, job_id, task_id):
    if not isinstance(task_id, str):
        raise ValueError("task_id is wrong type")
    tasks = dao.get_tasks_by_job(job_id)
    return [task for task in tasks if task.task_id == task_id][0].state