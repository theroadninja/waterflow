"""
Mock business logic client
"""
import time
from waterflow.mocks.sample_dags_http import make_diamond10

class BusinessLogicClient:
    def __init__(self):
        pass

    def run_task(self):
        time.sleep(5)  # simulate running the task

    def get_dag(self):
        time.sleep(1)  # simulate fetching a dag  # TODO RPC call goes here
        return make_diamond10()