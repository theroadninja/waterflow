import base64
import re
import unittest
from dataclasses import dataclass
from typing import List, Dict

from waterflow.exceptions import InvalidJobError, InvalidTaskState, InvalidJobState, NotImplementedYet
import waterflow
from waterflow import to_base64_str
from waterflow import event_codes
from waterflow.job import JobExecutionState, Dag
from waterflow.dao import DagDao
from waterflow.task import Task, TaskState
from .test_utils import get_conn_pool, path_to_sql, task_view1_list_to_dict

from waterflow.mocks.sample_dags import make_single_task_dag, make_linear_test_dag, make_test_dag


def count_table(conn, table_name):
    with conn.cursor() as cursor:
        # table names cannot be parameterized?  wtf?
        if not re.match("^\w+$", table_name) or len(table_name) > 16:
            raise Exception(f"invalid table name: {table_name}")

        cursor.execute(f"select count(*) from `{table_name}`")
        results = cursor.fetchall()
        for result in results:
            return result[0]


class DaoJobTests(unittest.TestCase):

    def setUp(self):
        conn_pool = get_conn_pool()
        with conn_pool.get_connection() as conn:

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

    def _assert_tasks_in_state(self, expected_state, tasks):
        for task in tasks:
            self.assertEqual(expected_state, task.state)

    def _assert_blocked(self, tasks):
        self._assert_tasks_in_state(int(TaskState.BLOCKED), tasks)

    def test_update_task_deps(self):
        # TODO - just manually insert the correct entries in the DB!
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")
        PENDING = int(JobExecutionState.PENDING)
        FETCHING = int(JobExecutionState.DAG_FETCH)

        self.assertEqual(0, dao.count_jobs(PENDING))
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("dep_test"))
        print(f"test_update_task_deps job_id={job_id}")

        dao.get_and_start_jobs(["w0"])
        dao.set_dag(job_id, make_test_dag())
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_tasks_in_state(int(TaskState.BLOCKED), job_tasks.values())
        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_tasks_in_state(int(TaskState.BLOCKED), [job_tasks["A"], job_tasks["B"], job_tasks["C"]])
        self._assert_tasks_in_state(int(TaskState.PENDING), [job_tasks["D"]]) #, job_tasks["E"], job_tasks["F"]])






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
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["A"].state)
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["B"].state)
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["C"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["D"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["E"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["F"].state)

        # should fail; task was never started
        with self.assertRaises(InvalidTaskState):
            dao.complete_task(job0, task_id=job0_tasks["D"].task_id)

        dao.start_task(job0, task_id=job0_tasks["D"].task_id, worker="w0")
        dao.update_task_deps(job0)
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertTrue(6, len(job0_tasks))
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["A"].state)
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["B"].state)
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["C"].state)
        self.assertEqual(int(TaskState.RUNNING), job0_tasks["D"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["E"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["F"].state)

        dao.complete_task(job0, task_id=job0_tasks["D"].task_id)
        dao.update_task_deps(job0)

        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertTrue(6, len(job0_tasks))
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["A"].state)
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["B"].state)
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["C"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["D"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["E"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["F"].state)

        dao.start_task(job0, task_id=job0_tasks["E"].task_id, worker="w0")
        dao.complete_task(job0, task_id=job0_tasks["E"].task_id)
        dao.update_task_deps(job0)
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["A"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["B"].state)
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["C"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["D"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["E"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["F"].state)

        dao.start_task(job0, task_id=job0_tasks["F"].task_id, worker="w0")
        dao.complete_task(job0, task_id=job0_tasks["F"].task_id)
        dao.update_task_deps(job0)
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["A"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["B"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["C"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["D"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["E"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["F"].state)

        dao.start_task(job0, task_id=job0_tasks["B"].task_id, worker="w0")
        dao.complete_task(job0, task_id=job0_tasks["B"].task_id)
        dao.update_task_deps(job0)
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskState.BLOCKED), job0_tasks["A"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["B"].state)
        self.assertEqual(int(TaskState.PENDING), job0_tasks["C"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["D"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["E"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["F"].state)

        dao.start_task(job0, task_id=job0_tasks["C"].task_id, worker="w0")
        dao.complete_task(job0, task_id=job0_tasks["C"].task_id)
        dao.update_task_deps(job0)
        self.assertFalse(dao.update_job_state(job0))
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskState.PENDING), job0_tasks["A"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["B"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["C"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["D"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["E"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["F"].state)

        dao.start_task(job0, task_id=job0_tasks["A"].task_id, worker="w0")
        dao.complete_task(job0, task_id=job0_tasks["A"].task_id)
        dao.update_task_deps(job0)
        self.assertTrue(dao.update_job_state(job0))
        job0_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job0))
        self.assertEqual(int(TaskState.SUCCEEDED), job0_tasks["A"].state)

        job = dao.get_job_info(job0)
        self.assertIsNotNone(job.created_utc)
        self.assertEqual(int(JobExecutionState.SUCCEEDED), job.state)

        # test job delete cascade
        with conn_pool.get_connection() as conn:
            self.assertTrue(count_table(conn, "jobs") > 0)
            self.assertTrue(count_table(conn, "job_executions") > 0)
            self.assertTrue(count_table(conn, "tasks") > 0)
            self.assertTrue(count_table(conn, "task_deps") > 0)

            with conn.cursor() as cursor:
                cursor.execute("delete from jobs")
                conn.commit()

            self.assertTrue(count_table(conn, "jobs") == 0)
            self.assertTrue(count_table(conn, "job_executions") == 0)
            self.assertTrue(count_table(conn, "tasks") == 0)
            self.assertTrue(count_table(conn, "task_deps") == 0)

    def test_linear_task_fetch(self):
        """
        Tests get_and_start_tasks()
        """
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")

        job_id = dao.add_job(job_input64=waterflow.to_base64_str("JOB0"))
        dag_fetch_tasks = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_linear_test_dag())
        dao.update_task_deps(job_id)

        print(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_tasks_in_state(int(TaskState.BLOCKED), [job_tasks["A"], job_tasks["B"], job_tasks["C"], job_tasks["D"]])
        self._assert_tasks_in_state(int(TaskState.PENDING), [job_tasks["E"]])

        # start task E
        task_assignments = dao.get_and_start_tasks(["w0"])
        self.assertEqual(1, len(task_assignments))
        self.assertEqual("E", base64.b64decode(task_assignments[0].task_input64).decode("UTF-8"))

        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_blocked([job_tasks["A"], job_tasks["B"], job_tasks["C"], job_tasks["D"]])
        self._assert_tasks_in_state(int(TaskState.RUNNING), [job_tasks["E"]])

        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_blocked([job_tasks["A"], job_tasks["B"], job_tasks["C"], job_tasks["D"]])
        self._assert_tasks_in_state(int(TaskState.RUNNING), [job_tasks["E"]])

        # finish task E
        dao.complete_task(job_id, job_tasks["E"].task_id)
        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_blocked([job_tasks["A"], job_tasks["B"], job_tasks["C"]])
        self._assert_tasks_in_state(int(TaskState.PENDING), [job_tasks["D"]])
        self._assert_tasks_in_state(int(TaskState.SUCCEEDED), [job_tasks["E"]])

        # start task D
        task_assignments = dao.get_and_start_tasks(["w0"])
        self.assertEqual(1, len(task_assignments))
        self.assertEqual("D", base64.b64decode(task_assignments[0].task_input64).decode("UTF-8"))
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_blocked([job_tasks["A"], job_tasks["B"], job_tasks["C"]])
        self._assert_tasks_in_state(int(TaskState.RUNNING), [job_tasks["D"]])
        self._assert_tasks_in_state(int(TaskState.SUCCEEDED), [job_tasks["E"]])

        # finish task D
        dao.complete_task(job_id, job_tasks["D"].task_id)
        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_blocked([job_tasks["A"], job_tasks["B"]])
        self._assert_tasks_in_state(int(TaskState.PENDING), [job_tasks["C"]])
        self._assert_tasks_in_state(int(TaskState.SUCCEEDED), [job_tasks["D"], job_tasks["E"]])

        # start task C
        task_assignments = dao.get_and_start_tasks(["w0"])
        self.assertEqual(1, len(task_assignments))
        self.assertEqual("C", base64.b64decode(task_assignments[0].task_input64).decode("UTF-8"))
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_blocked([job_tasks["A"], job_tasks["B"]])
        self._assert_tasks_in_state(int(TaskState.RUNNING), [job_tasks["C"]])
        self._assert_tasks_in_state(int(TaskState.SUCCEEDED), [job_tasks["D"], job_tasks["E"]])

        # finish task C
        dao.complete_task(job_id, job_tasks["C"].task_id)
        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_blocked([job_tasks["A"]])
        self._assert_tasks_in_state(int(TaskState.PENDING), [job_tasks["B"]])
        self._assert_tasks_in_state(int(TaskState.SUCCEEDED), [job_tasks["C"], job_tasks["D"], job_tasks["E"]])

        # start and finish B
        task_assignments = dao.get_and_start_tasks(["w0"])
        dao.complete_task(job_id, job_tasks["B"].task_id)
        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_tasks_in_state(int(TaskState.PENDING), [job_tasks["A"]])
        self._assert_tasks_in_state(int(TaskState.SUCCEEDED), [job_tasks["B"], job_tasks["C"], job_tasks["D"], job_tasks["E"]])

        #start and finish A
        task_assignments = dao.get_and_start_tasks(["w0"])
        dao.complete_task(job_id, job_tasks["A"].task_id)
        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_tasks_in_state(int(TaskState.SUCCEEDED), [job_tasks["A"], job_tasks["B"], job_tasks["C"], job_tasks["D"], job_tasks["E"]])

        # no tasks
        task_assignments = dao.get_and_start_tasks(["w0"])
        self.assertEqual(0, len(task_assignments))

    def test_fail_job(self):
        """
        Tests fail_job()
        """
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")

        # job doesnt exist
        with self.assertRaises(InvalidJobError):
            dao.fail_job("i_dont_exist_123", event_codes.JOB_CANCELED, "killed by unit test", None)

        # job failed while in PENDING state
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("JOB0"))
        print(f"test_fail_job() job_id={job_id}")
        self.assertEqual(int(JobExecutionState.PENDING), dao.get_job_info(job_id).state)
        failed = dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)
        self.assertTrue(failed)
        job_info = dao.get_job_info(job_id)
        self.assertEqual(int(JobExecutionState.FAILED), job_info.state)

        # job already in FAILED state
        with self.assertRaises(InvalidJobState):
            dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)

        # job failed while in DAG_FETCH state (and dag fetch returns after job failed)
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("JOB1"))
        print(f"test_fail_job() job_id={job_id}")
        dag_fetch_tasks = dao.get_and_start_jobs(["worker1"])
        self.assertEqual(job_id, dag_fetch_tasks[0].job_id)
        self.assertEqual(int(JobExecutionState.DAG_FETCH), dao.get_job_info(job_id).state)
        failed = dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)
        self.assertTrue(failed)
        job_info = dao.get_job_info(job_id)
        self.assertEqual(int(JobExecutionState.FAILED), job_info.state)
        # dag fetch return after job failed
        with self.assertRaises(InvalidJobState):
            dao.set_dag(job_id, make_single_task_dag())
        task_assignments = dao.get_and_start_tasks(["w0", "w1"])
        self.assertEqual(0, len(task_assignments))

        # job failed while in RUNNING state (and make sure tasks got canceled)
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("JOB2"))
        print(f"test_fail_job() job_id={job_id}")
        _ = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_single_task_dag())
        dao.update_task_deps(job_id)
        self.assertEqual(int(JobExecutionState.RUNNING), dao.get_job_info(job_id).state)
        with self.assertRaises(NotImplementedYet):
            failed = dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)
        # since we can't cancel it yet, pull the task off the queue so it doesnt mess up other tests
        task_assignments = dao.get_and_start_tasks(["w0", "w1"])
        self.assertEqual(1, len(task_assignments))

        # TODO when we can cancel jobs in the running state:  task fetch after failure
        # self.assertTrue(failed)
        # self.assertEqual(int(JobExecutionState.FAILED), dao.get_job_info(job_id).state)
        # # try to fetch task for failed job
        # task_assignments = dao.get_and_start_tasks(["w0", "w1"])
        # self.assertEqual(0, len(task_assignments))

        # TODO when we can cancel jobs in the running state:  task completion after failure

        # job failed while in SUCCEEDED state
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("JOB2"))
        print(f"test_fail_job() job_id={job_id}")
        _ = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_single_task_dag())
        dao.update_task_deps(job_id)
        task_assignments = dao.get_and_start_tasks(["w0", "w1"])
        self.assertEqual(1, len(task_assignments))
        dao.complete_task(job_id, task_assignments[0].task_id)
        dao.update_job_state(job_id)
        self.assertEqual(int(JobExecutionState.SUCCEEDED), dao.get_job_info(job_id).state)
        with self.assertRaises(InvalidJobState):
            dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)



        # BLOCKED TO CANCELED

    # TODO - test large numbers of workers!  (maybe use multiple linear jobs)
    # TODO - test trying to mark non-running/non-pending tasks as SUCCEEDED or FAILED, and other invalid transitions
    #  ( maybe just code up some kind of state model function to check the transitions? )

    # TODO also need tests for task failure, cancelation, etc

