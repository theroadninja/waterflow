import unittest

import waterflow
from waterflow.dao import DagDao
from waterflow.task import TaskState
from waterflow.job import JobExecutionState
from waterflow.exceptions import InvalidTaskState
from waterflow.mocks.sample_dags import make_linear_test_dag, make_linear_test_dag2
from .test_utils import get_conn_pool, path_to_sql, task_view1_list_to_dict

class DaoTaskTests(unittest.TestCase):

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


    def test_cancel_task(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")

        # cancel BLOCKED task
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("job"))
        _ = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_linear_test_dag2())
        dao.update_task_deps(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))

        self.assertEqual(int(TaskState.PENDING), job_tasks["B"].state)
        self.assertEqual(int(TaskState.BLOCKED), job_tasks["A"].state)
        dao.cancel_task(job_id, job_tasks["A"].task_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.PENDING), job_tasks["B"].state)
        self.assertEqual(int(TaskState.FAILED), job_tasks["A"].state)

        # cancel PENDING task
        dao.cancel_task(job_id, job_tasks["B"].task_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.FAILED), job_tasks["B"].state)
        self.assertEqual(int(TaskState.FAILED), job_tasks["A"].state)

    def test_cannot_cancel_task(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("job"))
        _ = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_linear_test_dag2())
        dao.update_task_deps(job_id)

        # cancel SUCCEEDED task
        task_assignments = dao.get_and_start_tasks(["w"])
        dao.complete_task(job_id, task_assignments[0].task_id)
        dao.update_task_deps(job_id)
        dao.update_job_state(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.SUCCEEDED), job_tasks["B"].state)
        with self.assertRaises(InvalidTaskState):
            dao.cancel_task(job_id, task_assignments[0].task_id)

        # cancel RUNNING task
        task_assignments = dao.get_and_start_tasks(["w"])
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.RUNNING), job_tasks["A"].state)
        with self.assertRaises(InvalidTaskState):
            dao.cancel_task(job_id, task_assignments[0].task_id)

    def test_job_incomplete_canceled_task(self):
        """
        Make sure job doesnt complete if there is a failed task
        """
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("job"))
        _ = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_linear_test_dag())
        dao.update_task_deps(job_id)

        # cancel one task
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        dao.cancel_task(job_id, job_tasks["A"].task_id)

        while True:
            task_assignments = dao.get_and_start_tasks(["w"])
            if len(task_assignments) < 1:
                break

            dao.complete_task(job_id, task_assignments[0].task_id)
            dao.update_task_deps(job_id)
            dao.update_job_state(job_id)

        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.FAILED), job_tasks["A"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job_tasks["B"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job_tasks["C"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job_tasks["D"].state)
        self.assertEqual(int(TaskState.SUCCEEDED), job_tasks["E"].state)

        # TODO can't cancel in FAILED state

    def test_fail_task(self):
        conn_pool = get_conn_pool()
        dao = DagDao(conn_pool, "waterflow")
        job_id = dao.add_job(job_input64=waterflow.to_base64_str("job"))
        _ = dao.get_and_start_jobs(["worker1"])
        dao.set_dag(job_id, make_linear_test_dag2())
        dao.update_task_deps(job_id)

        # can't fail BLOCKED or PENDING
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.BLOCKED), job_tasks["A"].state)
        with self.assertRaises(InvalidTaskState):
            dao.fail_task(job_id, job_tasks["A"].task_id)
        self.assertEqual(int(TaskState.PENDING), job_tasks["B"].state)
        with self.assertRaises(InvalidTaskState):
            dao.fail_task(job_id, job_tasks["B"].task_id)

        # can't fail DONE
        task_assignments = dao.get_and_start_tasks(["w"])
        self.assertTrue(len(task_assignments) == 1 and task_assignments[0].task_id == job_tasks["B"].task_id)
        dao.complete_task(job_id, job_tasks["B"].task_id)
        dao.update_task_deps(job_id)
        dao.update_job_state(job_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.SUCCEEDED), job_tasks["B"].state)
        with self.assertRaises(InvalidTaskState):
            dao.fail_task(job_id, job_tasks["B"].task_id)

        # can fail RUNNING (and make sure job can't complete with a failed task)
        task_assignments = dao.get_and_start_tasks(["w"])
        self.assertTrue(len(task_assignments) == 1 and task_assignments[0].task_id == job_tasks["A"].task_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.RUNNING), job_tasks["A"].state)
        dao.fail_task(job_id, job_tasks["A"].task_id)
        job_tasks = task_view1_list_to_dict(dao.get_tasks_by_job(job_id))
        self.assertEqual(int(TaskState.FAILED), job_tasks["A"].state)
        dao.update_job_state(job_id)
        job_info = dao.get_job_info(job_id)
        self.assertEqual(int(JobExecutionState.RUNNING), job_info.state)

        # can't fail if already FAILED - TODO can consider allowing this, for idempotence
        with self.assertRaises(InvalidTaskState):
            dao.fail_task(job_id, job_tasks["A"].task_id)

        # make sure task completion still overrides FAILURE (maybe it generates an event?)
        dao.complete_task(job_id, job_tasks["A"].task_id)


