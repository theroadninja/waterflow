"""
TODO these samples are built using internal types
"""
import waterflow
from waterflow.core.dao_models import Dag, Task


def make_single_task_dag()-> Dag:
    """
    Graph looks like this:
    A
    """
    a = waterflow.make_id()
    tasks = [
        Task(a, task_name="A", input64=waterflow.to_base64_str("A")),
    ]
    task_adj = {}
    return Dag(waterflow.to_base64_str("TEST"), 0, tasks, task_adj)


def make_linear_test_dag():
    """
    A
     \
      B
       \
        C
         \
          D
           \
            E
    """
    a = waterflow.make_id()
    b = waterflow.make_id()
    c = waterflow.make_id()
    d = waterflow.make_id()
    e = waterflow.make_id()
    tasks = [
        Task(a, task_name="A", input64=waterflow.to_base64_str("A")),
        Task(b, task_name="B", input64=waterflow.to_base64_str("B")),
        Task(c, task_name="C", input64=waterflow.to_base64_str("C")),
        Task(d, task_name="D", input64=waterflow.to_base64_str("D")),
        Task(e, task_name="E", input64=waterflow.to_base64_str("E")),
    ]
    task_adj = {
        a: [b],
        b: [c],
        c: [d],
        d: [e],
    }
    return Dag(waterflow.to_base64_str("TEST"), 0, tasks, task_adj)

def make_linear_test_dag2():
    """
    A
     \
      B
    """
    a = waterflow.make_id()
    b = waterflow.make_id()
    tasks = [
        Task(a, task_name="A", input64=waterflow.to_base64_str("A")),
        Task(b, task_name="B", input64=waterflow.to_base64_str("B")),
    ]
    task_adj = {
        a: [b],
    }
    return Dag(waterflow.to_base64_str("TEST"), 0, tasks, task_adj)

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
        Task(a, task_name="A", input64=waterflow.to_base64_str("A")),
        Task(b, task_name="B", input64=waterflow.to_base64_str("B")),
        Task(c, task_name="C", input64=waterflow.to_base64_str("C")),
        Task(d, task_name="D", input64=waterflow.to_base64_str("D")),
        Task(e, task_name="E", input64=waterflow.to_base64_str("E")),
        Task(f, task_name="F", input64=waterflow.to_base64_str("F")),
    ]
    task_adj = {
        a: [b, c],
        b: [d, e],
        c: [e, f],
    }
    return Dag(waterflow.to_base64_str("TEST"), 0, tasks, task_adj)