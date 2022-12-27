import unittest
from dataclasses import dataclass
from typing import List, Dict

import waterflow
from waterflow import to_base64_str
from waterflow.job import JobExecutionState, Dag
from waterflow.dao import DagDao
from waterflow.task import Task

UNIT_TEST_DATABASE = "waterflow_unit_tests"


def get_conn_pool():
    # TODO switch to test database!

    #return waterflow.get_connection_pool_from_file("../../local/mysqlconfig.json", "unit_test_pool")
    return waterflow.get_connection_pool_from_file("../../local/mysqlconfig.json", "unit_test_pool")



def make_test_dag():
    """
          A
        /  \
       B    C
      / \  / \
     D   E    F
    """
    a = waterflow.make_id()
    b = waterflow.make_id()
    c = waterflow.make_id()
    d = waterflow.make_id()
    e = waterflow.make_id()
    f = waterflow.make_id()
    tasks = [
        Task(a, input64=to_base64_str("A")),
        Task(b, input64=to_base64_str("B")),
        Task(c, input64=to_base64_str("C")),
        Task(d, input64=to_base64_str("D")),
        Task(e, input64=to_base64_str("E")),
        Task(f, input64=to_base64_str("F")),
    ]
    task_adj = {
        a: [b, c],
        b: [d, e],
        c: [e, f],
    }
    return Dag(to_base64_str("TEST"), 0, tasks, task_adj)

class DaoTests(unittest.TestCase):

    def setUp(self):
        conn_pool = get_conn_pool()
        with conn_pool.get_connection() as conn:

            with conn.cursor() as cursor:
                table_list = ",".join(DagDao.ALL_TABLES)
                drop_sql = f"DROP TABLE IF EXISTS {table_list};"
                cursor.execute(drop_sql)

                with open("../../sql/database.sql") as f:
                    create_sql = f.read()
                results = cursor.execute(create_sql, multi=True)
                # we must do this or we get a "connection not available" error closing the connection
                for _ in results:
                    pass  # print("{} {}".format(result.statement, result.fetchall()))

    def test_jobs(self):
        dao = DagDao(get_conn_pool(), "waterflow")
        PENDING = int(JobExecutionState.PENDING)
        FETCHING = int(JobExecutionState.DAG_FETCH)

        self.assertEqual(0, dao.count_jobs(PENDING))
        dao.add_job(job_input64=waterflow.to_base64_str("A"))
        self.assertEqual(1, dao.count_jobs(PENDING))
        dao.add_job(job_input64=waterflow.to_base64_str("B"))
        self.assertEqual(2, dao.count_jobs(PENDING))
        dao.add_job(job_input64=waterflow.to_base64_str("C"))
        self.assertEqual(3, dao.count_jobs(PENDING))
        dao.add_job(job_input64=waterflow.to_base64_str("D"))
        self.assertEqual(4, dao.count_jobs(PENDING))
        dao.add_job(job_input64=waterflow.to_base64_str("E"))
        self.assertEqual(5, dao.count_jobs(PENDING))
        self.assertEqual(0, dao.count_jobs(FETCHING))

        dag_fetch_tasks = []
        tasks = dao.get_and_start_jobs([])
        dag_fetch_tasks += tasks
        self.assertEqual(0, len(tasks))
        self.assertEqual(5, dao.count_jobs(PENDING))
        self.assertEqual(0, dao.count_jobs(FETCHING))

        tasks = dao.get_and_start_jobs(["worker1"])
        dag_fetch_tasks += tasks
        self.assertEqual(1, len(tasks))
        self.assertEqual(4, dao.count_jobs(PENDING))
        self.assertEqual(1, dao.count_jobs(FETCHING))

        tasks = dao.get_and_start_jobs(["worker", "worker"])
        dag_fetch_tasks += tasks
        self.assertEqual(2, len(tasks))
        self.assertEqual(2, dao.count_jobs(PENDING))

        tasks = dao.get_and_start_jobs(["worker", "worker", "worker"])
        dag_fetch_tasks += tasks
        self.assertEqual(2, len(tasks))
        self.assertEqual(0, dao.count_jobs(PENDING))
        self.assertEqual(5, dao.count_jobs(FETCHING))

        DAG64 = to_base64_str("TEST")
        dag = make_test_dag()
        self.assertEqual(5, len(dag_fetch_tasks))
        dao.set_dag(dag_fetch_tasks[0].job_id, dag)
        self.assertEqual(0, dao.count_jobs(PENDING))
        self.assertEqual(4, dao.count_jobs(FETCHING))
