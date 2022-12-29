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
        Task(a, input64=waterflow.to_base64_str("A")),
        Task(b, input64=waterflow.to_base64_str("B")),
        Task(c, input64=waterflow.to_base64_str("C")),
        Task(d, input64=waterflow.to_base64_str("D")),
        Task(e, input64=waterflow.to_base64_str("E")),
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
        Task(a, input64=waterflow.to_base64_str("A")),
        Task(b, input64=waterflow.to_base64_str("B")),
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
        Task(a, input64=waterflow.to_base64_str("A")),
        Task(b, input64=waterflow.to_base64_str("B")),
        Task(c, input64=waterflow.to_base64_str("C")),
        Task(d, input64=waterflow.to_base64_str("D")),
        Task(e, input64=waterflow.to_base64_str("E")),
        Task(f, input64=waterflow.to_base64_str("F")),
    ]
    task_adj = {
        a: [b, c],
        b: [d, e],
        c: [e, f],
    }
    return Dag(waterflow.to_base64_str("TEST"), 0, tasks, task_adj)