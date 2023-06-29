import unittest

from waterflow.core.job_state import JobExecutionState
from waterflow import to_base64_str, from_base64_str
from waterflow.server import service_methods
from waterflow.core.dao_models import PendingJob
from waterflow.server.dao import DagDao
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

    def test_transform_adj_list(self):
        id_map = {
            1: "A",
            2: "B",
            3: "C",
            4: "D",
        }
        adj_list = {
            1: [2, 3],
            2: [4],
            3: [4],
        }
        result = service_methods._transform_adj_list(adj_list, id_map)
        self.assertEqual(3, len(result))
        self.assertEqual(["B", "C"], sorted(result["A"]))
        self.assertEqual(["D"], sorted(result["B"]))
        self.assertEqual(["D"], sorted(result["C"]))

        adj_list = {'0': [1, 2, 3, 4, 5, 6, 7, 8], '1': [9], '2': [9], '3': [9], '4': [9], '5': [9], '6': [9], '7': [9], '8': [9]}
        id_map = {
            '0': '42c77f367a554b748a80fd361f88225b', '1': 'c23328d71d844ca88e3b0a5e9126cb7a',
            '2': 'c7f1ac32fdc645659dbb8733e8142feb', '3': '68df49ff2fc44fbd9d16bd25c209f1ee',
            '4': '55db12a9ccde45edbe2b81f753433022', '5': '03fd1064f29d493ebeba4bae5188361e',
            '6': 'cb353db4dfe2447388279cb9154812ed', '7': '38188186233441ef84b28cc3eb9f2c56',
            '8': '8691a91eb776413896541afb380b3556', '9': 'a82e1524b0c7415d93298405c200c56b',
        }
        result = service_methods._transform_adj_list(adj_list, id_map)


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
