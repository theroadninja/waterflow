from waterflow import to_base64_str
from waterflow.rest import Dag, Task



def make_diamond10():
    """
    Creates a diamond-shaped dag with 10 tasks:

               A
        / /| | | | \ \
       B C D E F G H I
        \ \| | | |/ /
              J
    """

    tasks = {
        0: Task(task_name="A", input64=to_base64_str("a"), input64_v=0, service_pointer="TODO"),
        1: Task(task_name="B", input64=to_base64_str("b"), input64_v=0, service_pointer="TODO"),
        2: Task(task_name="C", input64=to_base64_str("c"), input64_v=0, service_pointer="TODO"),
        3: Task(task_name="D", input64=to_base64_str("d"), input64_v=0, service_pointer="TODO"),
        4: Task(task_name="E", input64=to_base64_str("e"), input64_v=0, service_pointer="TODO"),
        5: Task(task_name="F", input64=to_base64_str("f"), input64_v=0, service_pointer="TODO"),
        6: Task(task_name="G", input64=to_base64_str("g"), input64_v=0, service_pointer="TODO"),
        7: Task(task_name="H", input64=to_base64_str("h"), input64_v=0, service_pointer="TODO"),
        8: Task(task_name="I", input64=to_base64_str("i"), input64_v=0, service_pointer="TODO"),
        9: Task(task_name="J", input64=to_base64_str("j"), input64_v=0, service_pointer="TODO"),
    }
    adj_list = {
        0: [1, 2, 3, 4, 5, 6, 7, 8],
        1: [9],
        2: [9],
        3: [9],
        4: [9],
        5: [9],
        6: [9],
        7: [9],
        8: [9],
    }
    return Dag(raw_dag64=None, raw_dagv=0, tasks=tasks, adj_list=adj_list)