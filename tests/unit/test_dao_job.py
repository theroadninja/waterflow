import base64
import datetime
import re
import unittest

from waterflow.core.exceptions import InvalidJobError, InvalidTaskState, InvalidJobState, NotImplementedYet
import waterflow
from waterflow import to_base64_str
from waterflow.core import event_codes
from waterflow.core.job_state import JobExecutionState
from waterflow.server.dao import DagDao
from waterflow.core.dao_models import PendingJob, JobStats
from waterflow.core.task_state import TaskState
from .test_utils import get_conn_pool, task_view1_list_to_dict, drop_and_recreate_database

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
            drop_and_recreate_database(conn)

    def test_job_name(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")
        job_id = dao.add_job(PendingJob(job_name="myfirstjob", job_input64=waterflow.to_base64_str("A")))
        self.assertEqual("myfirstjob", dao.get_job_info(job_id).job_name)

    def test_service_pointer(self):
        SERVICE_POINTER = "service;method;"
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")

        # normal
        job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("A"), service_pointer=SERVICE_POINTER))
        self.assertEqual(SERVICE_POINTER, dao.get_job_info(job_id).service_pointer)
        fetch_tasks = dao.get_and_start_jobs(["w0"])
        self.assertEqual(1, len(fetch_tasks))
        self.assertEqual(SERVICE_POINTER, fetch_tasks[0].service_pointer)

        # max length
        MAX_SERVICE_POINTER = "A" * 128
        job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("A"), service_pointer=MAX_SERVICE_POINTER))
        self.assertEqual(MAX_SERVICE_POINTER, dao.get_job_info(job_id).service_pointer)
        fetch_tasks = dao.get_and_start_jobs(["w0"])
        self.assertEqual(1, len(fetch_tasks))
        self.assertEqual(MAX_SERVICE_POINTER, fetch_tasks[0].service_pointer)

        # too long
        TOO_LONG = "A" * 129
        with self.assertRaises(ValueError):
            dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("A"), service_pointer=TOO_LONG))

    def test_job_tags(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")

        job = PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("A"), tags=["a=b", "c=d", "e=f"])
        job_id = dao.add_job(job)
        job_info = dao.get_job_info(job_id)
        self.assertEqual(sorted(["a=b", "c=d", "e=f"]), sorted(job_info.tags))

        tags = ["0", "1", "2", "3", "4", "5", "6", "7", "8"]
        job = PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("A"), tags=tags)
        with self.assertRaises(ValueError):
            dao.add_job(job)

    def test_bytes_vs_strings(self):
        dao = DagDao(get_conn_pool(), "waterflow")
        job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("A"), tags=["tag"]))
        tasks = dao.get_and_start_jobs(["worker1"])
        self.assertTrue(isinstance(tasks[0].job_input64, str))  # FetchDagTask

        dao.set_dag(job_id, make_single_task_dag(), 0)
        dao.update_task_deps(job_id)

        tasks2 = dao.get_and_start_tasks(["w0"], 0)
        self.assertEqual(job_id, tasks2[0].job_id)
        self.assertTrue(isinstance(tasks2[0].task_input64, str))  # TaskAssignment
        self.assertEqual("A", waterflow.from_base64_str(tasks2[0].task_input64))

    def test_jobs(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")
        PENDING = int(JobExecutionState.PENDING)
        FETCHING = int(JobExecutionState.DAG_FETCH)


        self.assertEqual(0, dao.count_jobs(PENDING))
        tmp_job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("A"), tags=["tag"]))
        self.assertEqual(int(JobExecutionState.PENDING), dao.get_job_info(tmp_job_id).state)
        self.assertEqual(1, dao.count_jobs(PENDING))
        dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("B")))
        self.assertEqual(2, dao.count_jobs(PENDING))
        dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("C")))
        self.assertEqual(3, dao.count_jobs(PENDING))
        dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("D")))
        self.assertEqual(4, dao.count_jobs(PENDING))
        dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("E")))
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
        self.assertTrue(isinstance(tasks[0].job_input64, str))

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
        dao.set_dag(job0, dag, work_queue=0)
        self.assertEqual(0, dao.count_jobs(PENDING))
        self.assertEqual(4, dao.count_jobs(FETCHING))

        # test job delete cascade
        with conn_pool.get_connection() as conn:
            self.assertTrue(count_table(conn, "jobs") > 0)
            self.assertTrue(count_table(conn, "job_executions") > 0)
            self.assertTrue(count_table(conn, "job_tags") > 0)
            self.assertTrue(count_table(conn, "tasks") > 0)
            self.assertTrue(count_table(conn, "task_deps") > 0)

            with conn.cursor() as cursor:
                cursor.execute("delete from jobs")
                conn.commit()

            self.assertTrue(count_table(conn, "jobs") == 0)
            self.assertTrue(count_table(conn, "job_executions") == 0)
            self.assertTrue(count_table(conn, "job_tags") == 0)
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
        job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("dep_test")))
        print(f"test_update_task_deps job_id={job_id}")

        dao.get_and_start_jobs(["w0"])
        dao.set_dag(job_id, make_test_dag(), work_queue=0)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_tasks_in_state(int(TaskState.BLOCKED), job_tasks.values())
        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self._assert_tasks_in_state(int(TaskState.BLOCKED), [job_tasks["A"], job_tasks["B"], job_tasks["C"]])
        self._assert_tasks_in_state(int(TaskState.PENDING), [job_tasks["D"]]) #, job_tasks["E"], job_tasks["F"]])






    def test_job_execution(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")

        job0_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("JOB0")))
        self.assertEqual(int(JobExecutionState.PENDING), dao.get_job_info(job0_id).state)

        dag_fetch_tasks = dao.get_and_start_jobs(["worker1"])
        self.assertEqual(int(JobExecutionState.DAG_FETCH), dao.get_job_info(job0_id).state)
        job0 = dag_fetch_tasks[0].job_id


        dao.set_dag(job0_id, make_test_dag(), work_queue=0)
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

        job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("JOB0")))
        dag_fetch_tasks = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_linear_test_dag(), work_queue=0)
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
        job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("JOB0")))
        print(f"test_fail_job() job_id={job_id}")
        self.assertEqual(int(JobExecutionState.PENDING), dao.get_job_info(job_id).state)
        with self.assertRaises(InvalidJobState):
            dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)

        # job failed while in DAG_FETCH state (and dag fetch returns after job failed)
        # dag_fetch_tasks = dao.get_and_start_jobs(["worker1"])
        # self.assertEqual(0, len(dag_fetch_tasks))
        # job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("JOB1")))
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
            dao.set_dag(job_id, make_single_task_dag(), work_queue=0)
        task_assignments = dao.get_and_start_tasks(["w0", "w1"])
        self.assertEqual(0, len(task_assignments))

        # job already in FAILED state
        with self.assertRaises(InvalidJobState):
            dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)

        # job failed while in RUNNING state (and make sure tasks got canceled) - TODO not implemented
        job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("JOB2")))
        print(f"test_fail_job() job_id={job_id}")
        fetch_tasks = dao.get_and_start_jobs(["worker1"])
        self.assertEqual(1, len(fetch_tasks))
        dao.set_dag(job_id, make_single_task_dag(), work_queue=0)
        dao.update_task_deps(job_id)
        self.assertEqual(int(JobExecutionState.RUNNING), dao.get_job_info(job_id).state)
        with self.assertRaises(NotImplementedYet):
            failed = dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)
        # since we can't cancel it yet, pull the task off the queue so it doesnt mess up other tests
        task_assignments = dao.get_and_start_tasks(["w0", "w1"])
        self.assertEqual(1, len(task_assignments))

        # TODO when we can cancel jobs in the running state:  task fetch after failure

        # TODO when we can cancel jobs in the running state:  task completion after failure

        # job failed while in SUCCEEDED state
        job_id = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("JOB2")))
        print(f"test_fail_job() job_id={job_id}")
        _ = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_single_task_dag(), work_queue=0)
        dao.update_task_deps(job_id)
        task_assignments = dao.get_and_start_tasks(["w0", "w1"])
        self.assertEqual(1, len(task_assignments))
        dao.complete_task(job_id, task_assignments[0].task_id)
        dao.update_job_state(job_id)
        self.assertEqual(int(JobExecutionState.SUCCEEDED), dao.get_job_info(job_id).state)
        with self.assertRaises(InvalidJobState):
            dao.fail_job(job_id, event_codes.JOB_CANCELED, "killed by unit test", None)

    def test_work_queue(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")

        job_id0 = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("JOB0"), work_queue=0))
        job_id1 = dao.add_job(PendingJob(job_name="j0", job_input64=waterflow.to_base64_str("JOB0"), work_queue=1))

        fetch_tasks = dao.get_and_start_jobs(["w0", "w1", "w2"], work_queue=1)
        self.assertEqual(1, len(fetch_tasks))
        self.assertEqual(job_id1, fetch_tasks[0].job_id)

        fetch_tasks = dao.get_and_start_jobs(["w0", "w1", "w2"], work_queue=0)
        self.assertEqual(1, len(fetch_tasks))
        self.assertEqual(job_id0, fetch_tasks[0].job_id)

    def test_prune_jobs(self):
        dao = DagDao(get_conn_pool(), "waterflow")

        ji = waterflow.to_base64_str("JB")
        job_id0 = dao.add_job(PendingJob(job_name="j0", job_input64=ji, work_queue=0), now_utc=datetime.datetime(2020, 1, 1))
        job_id1 = dao.add_job(PendingJob(job_name="j0", job_input64=ji, work_queue=0), now_utc=datetime.datetime(2020, 1, 1))
        job_id2 = dao.add_job(PendingJob(job_name="j0", job_input64=ji, work_queue=0),
                              now_utc=datetime.datetime(2020, 1, 1))
        job_id3 = dao.add_job(PendingJob(job_name="j0", job_input64=ji, work_queue=0), now_utc = datetime.datetime(2020, 1, 3))
        self.assertEqual(4, dao.count_jobs(int(JobExecutionState.PENDING)))

        count = dao.prune_jobs(datetime.datetime(2020, 1, 2), limit=1)
        self.assertEqual(1, count)
        self.assertEqual(3, dao.count_jobs(int(JobExecutionState.PENDING)))

        count = dao.prune_jobs(datetime.datetime(2020, 1, 2), limit=100)
        self.assertEqual(2, count)
        self.assertEqual(1, dao.count_jobs(int(JobExecutionState.PENDING)))

        self.assertEqual(job_id3, dao.get_job_info(job_id3).job_id)

        with self.assertRaises(Exception):
            dao.get_job_info(job_id0).job_id

        with self.assertRaises(Exception):
            dao.get_job_info(job_id1).job_id

        with self.assertRaises(Exception):
            dao.get_job_info(job_id2).job_id

    def test_job_stats(self):
        dao = DagDao(get_conn_pool(), "waterflow")  # TODO db name should not be hardcoded here

        self.assertEqual(JobStats(0, 0, 0, 0, 0), dao.get_job_stats())

        ji = waterflow.to_base64_str("JB")
        job_ids = [dao.add_job(PendingJob(job_name=f"j{j}", job_input64=ji, work_queue=0)) for j in range(15)]
        self.assertEqual(JobStats(15, 0, 0, 0, 0), dao.get_job_stats())

        fetch_tasks = dao.get_and_start_jobs(["w0", "w1", "w2", "w3", "w4", "w5", "w6","w7", "w8", "w9"])
        self.assertEqual(JobStats(5, 10, 0, 0, 0), dao.get_job_stats())

        failed_jobs = fetch_tasks[:1]
        for task in failed_jobs:
            dao.fail_job(task.job_id, event_codes.JOB_FAILED, "dag fetch failed", None)
        self.assertEqual(JobStats(5, 9, 0, 0, 1), dao.get_job_stats())

        running_jobs = fetch_tasks[2:7]
        for task in running_jobs:
            dao.set_dag(task.job_id, make_single_task_dag(), 0)
            dao.update_task_deps(task.job_id)
        self.assertEqual(JobStats(5, 4, 5, 0, 1), dao.get_job_stats())

        tasks = dao.get_and_start_tasks(["w0", "w1"], 0)
        self.assertEqual(2, len(tasks))
        for task in tasks:
            dao.complete_task(task.job_id, task.task_id)
            dao.update_task_deps(task.job_id)
            dao.update_job_state(task.job_id)
        self.assertEqual(JobStats(5, 4, 3, 2, 1), dao.get_job_stats())




    # TODO - test large numbers of workers!  (maybe use multiple linear jobs)
    # TODO - test trying to mark non-running/non-pending tasks as SUCCEEDED or FAILED, and other invalid transitions
    #  ( maybe just code up some kind of state model function to check the transitions? )

    # TODO also need tests for task failure, cancelation, etc

