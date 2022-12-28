import base64
import re
import unittest
from dataclasses import dataclass
from typing import List, Dict

import waterflow
from waterflow import to_base64_str
from waterflow.job import JobExecutionState, Dag
from waterflow.dao import DagDao
from waterflow.task import Task, TaskEligibilityState, TaskExecState

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

def task_view1_list_to_dict(results):
    """
    Takes the result of DagDao.get_tasks_by_job() and turns them into a dict where the
    keys of the dict are the task inputs (assumed to be a string)
    """
    return {base64.b64decode(task.task_input64).decode("UTF-8"): task for task in results}


def count_table(conn, table_name):
    with conn.cursor() as cursor:
        # table names cannot be parameterized?  wtf?
        if not re.match("^\w+$", table_name) or len(table_name) > 16:
            raise Exception(f"invalid table name: {table_name}")

        cursor.execute(f"select count(*) from `{table_name}`")
        results = cursor.fetchall()
        for result in results:
            return result[0]

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
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")
        PENDING = int(JobExecutionState.PENDING)
        FETCHING = int(JobExecutionState.DAG_FETCH)

        self.assertEqual(0, dao.count_jobs(PENDING))
        tmp_job_id = dao.add_job(job_input64=waterflow.to_base64_str("A"))
        self.assertEqual(int(JobExecutionState.PENDING), dao.get_job_info(tmp_job_id).state)
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

        job0 = dag_fetch_tasks[0].job_id
        self.assertEqual(int(JobExecutionState.DAG_FETCH), dao.get_job_info(job0).state)


        self.assertEqual(5, len(dag_fetch_tasks))
        dao.set_dag(job0, dag)
        self.assertEqual(0, dao.count_jobs(PENDING))
        self.assertEqual(4, dao.count_jobs(FETCHING))

        # test job delete cascade
        with conn_pool.get_connection() as conn:
            self.assertTrue(count_table(conn, "jobs") > 0)
            self.assertTrue(count_table(conn, "job_executions") > 0)
            self.assertTrue(count_table(conn, "tasks") > 0)
            self.assertTrue(count_table(conn, "task_deps") > 0)
            # self.assertTrue(count_table(conn, "task_executions") > 0)

            with conn.cursor() as cursor:
                cursor.execute("delete from jobs")
                conn.commit()

            self.assertTrue(count_table(conn, "jobs") == 0)
            self.assertTrue(count_table(conn, "job_executions") == 0)
            self.assertTrue(count_table(conn, "tasks") == 0)
            self.assertTrue(count_table(conn, "task_deps") == 0)



    def test_job_execution(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")

        job0_id = dao.add_job(job_input64=waterflow.to_base64_str("JOB0"))
        self.assertEqual(int(JobExecutionState.PENDING), dao.get_job_info(job0_id).state)

        dag_fetch_tasks = dao.get_and_start_jobs(["worker1"])
        self.assertEqual(int(JobExecutionState.DAG_FETCH), dao.get_job_info(job0_id).state)
        job0 = dag_fetch_tasks[0].job_id


        dao.set_dag(job0_id, make_test_dag())
        self.assertEqual(int(JobExecutionState.RUNNING), dao.get_job_info(job0_id).state)

        dao.update_task_deps(job0)

        tasks = dao.get_tasks_by_job(job0)
        print(job0)  # TODO
        # job0_tasks = { base64.b64decode(task.task_input64).decode("UTF-8"): task for task in tasks}
        job0_tasks = task_view1_list_to_dict(tasks)
        self.assertEqual({"A", "B", "C", "D", "E", "F"}, job0_tasks.keys())
        self.assertTrue(6, len(job0_tasks))
        self.assertTrue(job0, job0_tasks["A"].job_id)
        self.assertTrue(32, len(job0_tasks["A"].task_id))
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["A"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["B"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["C"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["D"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["E"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["F"].eligibility_state)

        #     def stop_task(self, job_id, task_id, end_state: int):

        # should fail; task was never started
        with self.assertRaises(Exception):
            dao.stop_task(job0, task_id=job0_tasks["D"].task_id, end_state=int(TaskExecState.SUCCEEDED))

        dao.start_task(job0, task_id=job0_tasks["D"].task_id, worker="w0")
        dao.update_task_deps(job0)
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertTrue(6, len(job0_tasks))
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["A"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["B"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["C"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["D"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["E"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["F"].eligibility_state)
        self.assertIsNotNone(job0_tasks["D"].exec_state)
        self.assertEqual(int(TaskExecState.RUNNING), job0_tasks["D"].exec_state)

        dao.stop_task(job0, task_id=job0_tasks["D"].task_id, end_state=int(TaskExecState.SUCCEEDED))
        dao.update_task_deps(job0)

        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertTrue(6, len(job0_tasks))
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["A"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["B"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["C"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["D"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["E"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["F"].eligibility_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["D"].exec_state)

        dao.start_task(job0, task_id=job0_tasks["E"].task_id, worker="w0")
        dao.stop_task(job0, task_id=job0_tasks["E"].task_id, end_state=int(TaskExecState.SUCCEEDED))
        dao.update_task_deps(job0)
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["A"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["B"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["C"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["D"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["E"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["F"].eligibility_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["D"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["E"].exec_state)

        dao.start_task(job0, task_id=job0_tasks["F"].task_id, worker="w0")
        dao.stop_task(job0, task_id=job0_tasks["F"].task_id, end_state=int(TaskExecState.SUCCEEDED))
        dao.update_task_deps(job0)
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["A"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["B"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["C"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["D"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["E"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["F"].eligibility_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["D"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["E"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["F"].exec_state)

        dao.start_task(job0, task_id=job0_tasks["B"].task_id, worker="w0")
        dao.stop_task(job0, task_id=job0_tasks["B"].task_id, end_state=int(TaskExecState.SUCCEEDED))
        dao.update_task_deps(job0)
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskEligibilityState.BLOCKED), job0_tasks["A"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["B"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["C"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["D"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["E"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["F"].eligibility_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["D"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["E"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["F"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["B"].exec_state)

        dao.start_task(job0, task_id=job0_tasks["C"].task_id, worker="w0")
        dao.stop_task(job0, task_id=job0_tasks["C"].task_id, end_state=int(TaskExecState.SUCCEEDED))
        dao.update_task_deps(job0)
        self.assertFalse(dao.update_job_state(job0))
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["A"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["B"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["C"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["D"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["E"].eligibility_state)
        self.assertEqual(int(TaskEligibilityState.READY), job0_tasks["F"].eligibility_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["D"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["E"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["F"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["B"].exec_state)
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["C"].exec_state)

        dao.start_task(job0, task_id=job0_tasks["A"].task_id, worker="w0")
        dao.stop_task(job0, task_id=job0_tasks["A"].task_id, end_state=int(TaskExecState.SUCCEEDED))
        dao.update_task_deps(job0)
        self.assertTrue(dao.update_job_state(job0))
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskExecState.SUCCEEDED), job0_tasks["A"].exec_state)

        job = dao.get_job_info(job0)
        self.assertIsNotNone(job.created_utc)
        self.assertEqual(int(JobExecutionState.SUCCEEDED), job.state)

        # test job delete cascade
        with conn_pool.get_connection() as conn:
            self.assertTrue(count_table(conn, "jobs") > 0)
            self.assertTrue(count_table(conn, "job_executions") > 0)
            self.assertTrue(count_table(conn, "tasks") > 0)
            self.assertTrue(count_table(conn, "task_deps") > 0)
            self.assertTrue(count_table(conn, "task_executions") > 0)

            with conn.cursor() as cursor:
                cursor.execute("delete from jobs")
                conn.commit()

            self.assertTrue(count_table(conn, "jobs") == 0)
            self.assertTrue(count_table(conn, "job_executions") == 0)
            self.assertTrue(count_table(conn, "tasks") == 0)
            self.assertTrue(count_table(conn, "task_deps") == 0)
            self.assertTrue(count_table(conn, "task_executions") == 0)




    def test_linear_task_fetch(self):
        pass
        # TODO make a linear graph with A->B->C->D->E etc...
