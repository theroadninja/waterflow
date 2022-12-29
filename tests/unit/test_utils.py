import base64
import inspect
import os

import waterflow
from waterflow.dao import DagDao

MYPATH = os.path.dirname(os.path.abspath(inspect.stack()[0][1]))

def get_conn_pool():
    # TODO switch to test database!

    UNIT_TEST_DATABASE = "waterflow_unit_tests"  # TODO

    FILENAME = os.path.abspath("../local/mysqlconfig.json")
    print(os.path.abspath(FILENAME))
    if not os.path.isfile(FILENAME):
        raise Exception(f"Can't fine {FILENAME}")

    return waterflow.get_connection_pool_from_file(FILENAME, "unit_test_pool")

def path_to_sql():
    filename = f"{MYPATH}/../../sql/database.sql"
    if not os.path.isfile(filename):
        raise FileNotFoundError(filename)
    return filename


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