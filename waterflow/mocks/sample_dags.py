import waterflow
from waterflow.task import Task
from waterflow.job import Dag

def make_single_task_dag():
    """
    Graph looks like this:
    A
    """
    a = waterflow.make_id()
    tasks = [
        Task(a, input64=waterflow.to_base64_str("A")),
    ]
    task_adj = {}
    return Dag(waterflow.to_base64_str("TEST"), 0, tasks, task_adj)