import unittest

from waterflow.job import JobExecutionState
from waterflow import service_methods, to_base64_str, from_base64_str
from waterflow.dao_models import PendingJob
from waterflow.dao import DagDao
from waterflow.mocks.sample_dags import make_single_task_dag, make_linear_test_dag2
from .test_utils import get_conn_pool, drop_and_recreate_database


WORK_QUEUE = 0

class ServiceMethodTests(unittest.TestCase):

    def setUp(self):
        conn_pool = get_conn_pool()
        with conn_pool.get_connection() as conn:
            drop_and_recreate_database(conn)

    @classmethod
    def _get_dao(cls):
        conn_pool = get_conn_pool()
        return DagDao(conn_pool, "waterflow")

    @classmethod
    def _make_job(cls):
        return PendingJob("j0", to_base64_str("j0"), 0, None, None, WORK_QUEUE)

    def test_submit_job(self):
        dao = self._get_dao()

        JOB_NAME = "test_job"
        SERVICE_POINTER = "service;method;"
        job = PendingJob(JOB_NAME, to_base64_str("input"), 1, SERVICE_POINTER, ["job_type=spark"], work_queue=5)
        job_id = service_methods.submit_job(dao, job)

        job_view = service_methods.get_job_details(dao, job_id)
        self.assertEqual(job_id, job_view.job_id)
        self.assertEqual(JOB_NAME, job_view.job_name)
        self.assertEqual("input", from_base64_str(job_view.job_input64))
        self.assertEqual(1, job_view.job_input64_v)
        self.assertEqual(SERVICE_POINTER, job_view.service_pointer)
        self.assertEqual(["job_type=spark"], job_view.tags)
        self.assertEqual(5, job_view.work_queue)

    def test_execute_job(self):
        dao = self._get_dao()
        job_id = service_methods.submit_job(dao, self._make_job())
        work_item = service_methods.get_work_item(dao, WORK_QUEUE, "w0")
        self.assertIsNotNone(work_item.dag_fetch)
        self.assertIsNone(work_item.run_task)
        self.assertEqual(job_id, work_item.dag_fetch.job_id)

        service_methods.set_dag_for_job(dao, job_id, make_single_task_dag(), WORK_QUEUE)

        work_item = service_methods.get_work_item(dao, WORK_QUEUE, "w0")
        self.assertIsNotNone(work_item.run_task)
        self.assertIsNone(work_item.dag_fetch)

        service_methods.complete_task(dao, job_id, work_item.run_task.task_id)

        job_info = service_methods.get_job_details(dao, job_id)
        self.assertEqual(JobExecutionState.SUCCEEDED, job_info.state)

        work_item = service_methods.get_work_item(dao, WORK_QUEUE, "w0")
        self.assertTrue(work_item.empty())

    def test_execute_two_tasks(self):
        dao = self._get_dao()
        job_id = service_methods.submit_job(dao, self._make_job())
        _ = service_methods.get_work_item(dao, WORK_QUEUE, "w0")
        service_methods.set_dag_for_job(dao, job_id, make_linear_test_dag2(), WORK_QUEUE)

        work_item = service_methods.get_work_item(dao, WORK_QUEUE, "w0")
        service_methods.complete_task(dao, job_id, work_item.run_task.task_id)

        work_item = service_methods.get_work_item(dao, WORK_QUEUE, "w0")
        service_methods.complete_task(dao, job_id, work_item.run_task.task_id)

        job_info = service_methods.get_job_details(dao, job_id)
        self.assertEqual(JobExecutionState.SUCCEEDED, job_info.state)

        work_item = service_methods.get_work_item(dao, WORK_QUEUE, "w0")
        self.assertTrue(work_item.empty())
