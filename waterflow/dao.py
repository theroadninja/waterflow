from enum import IntEnum
from mysql.connector import connect, pooling
from typing import List, Dict
import uuid

from waterflow.task import Task, TaskEligibilityState, TaskExecState
import waterflow.task

WORKER_LENGTH = 255  # must match the varchar length in the db schema

class JobExecutionState(IntEnum):  # enum b/c its pythonic (though stupid)
    # PENDING = 0
    DAG_FETCH = 1
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4

class DagDao:
    def __init__(self, mysql_conn_pool, dbname):
        self.conn_pool = mysql_conn_pool
        self.dbname = dbname  # to check table existence


    def query(self):  # TODO remove
        with self.conn_pool.get_connection() as conn:
            query = "SELECT * FROM jobs"
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    def db_check(self):
        """
        Connects to the Database and ensures that tables with the right names exist.
        """
        EXPECTED_TABLES = ['job_executions', 'jobs', 'task_deps', 'task_executions', 'tasks']

        sql = f"""
        select table_name from information_schema.tables
        where table_schema = "{self.dbname}"
        and table_name in ({",".join(["%s" for _ in EXPECTED_TABLES])})
        ;
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(EXPECTED_TABLES))
                results = [row[0] for row in cursor.fetchall()]

        return sorted(results) == sorted(EXPECTED_TABLES)

    def add_job(self, job_input64: str):
        """
        Called by scheduer/trigger to start a job (to enqueue it for starting).
        """
        if not isinstance(job_input64, str):
            raise ValueError("job_input must be a str; if you have bytes call .decode(UTF-8)")
        job_id = str(uuid.uuid4()).replace("-", "")

        sql = """
        insert into `jobs` (job_id, job_input)
        VALUES (%s, FROM_BASE64(%s));
        """
        # print(sql)  # TODO feature to auto log sql?
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=(job_id, job_input64))
                # works: print(f"last row id: {cursor.lastrowid}")
                conn.commit()  # required

        return job_id

    def start_job(self, job_id: str, worker: str):
        """
        Called when a worker started fetching the dag for a job.

        Inserts the job_execution row and marks which worker is working on it

        TODO:  need another method for if the worker times out and we try again (or is that just a re-run????)
        """
        sql = """
        insert into job_executions (job_id, state, worker, dag)
        values (%s, %s, %s, NULL);
        """

        # TODO add created_utc and last_updated_utc columns!

        if len(worker) > WORKER_LENGTH:
            raise ValueError(f"worker cannot be longer than {WORKER_LENGTH}")

        params = (job_id, int(JobExecutionState.DAG_FETCH), worker)
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=params)
                conn.commit()

    def set_dag(self, job_id: str, dag64: str, tasks: List[Task], task_deps: Dict[str, List[str]]):
        """
        Called when worker has finished fetching the dag.
        """
        if not isinstance(dag64, str):
            raise ValueError("dag64 must be a str; if you have bytes call .decode(UTF-8)")

        if not waterflow.task.is_valid(tasks, task_deps):
            raise ValueError("invalid task graph")

        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                update job_executions
                set dag=FROM_BASE64(%s), state=(%s)
                where job_id = %s AND state = %s AND dag IS NULL;
                """
                params = (dag64, int(JobExecutionState.RUNNING), job_id, int(JobExecutionState.DAG_FETCH))
                cursor.execute(sql, params=params)


                sql = """
                insert into `tasks` (job_id, task_id, eligibility_state, task_input)
                values (%s, %s, %s, %s);
                """
                for task in tasks:
                    params = (job_id, task.task_id, int(TaskEligibilityState.BLOCKED), task.input64)
                    cursor.execute(sql, params=params)

                sql = """
                INSERT INTO `task_deps` (job_id, task_id, neighboor_id)
                VALUES (%s, %s, %s);
                """
                for task_id, deps in task_deps.items():
                    for neighboor_id in deps:
                        params = (job_id, task_id, neighboor_id)
                        cursor.execute(sql, params=params)

                conn.commit()
                # prints last call, not the whole transaction: print(f"rows updated: {cursor.rowcount}")

    def update_task_deps(self, job_id):
        """
        Find any tasks that have no unfinished dependencies and mark them safe for execution.
        """

        SAVE_ME_SOMEWHERE = """
        select tasks.job_id, tasks.task_id, tasks.eligibility_state, 
        cast(from_base64(tasks.task_input) as char(255)) as input, 
        COUNT(task_deps.neighboor_id) as depcount,
        COUNT(IF(task_executions.exec_state = 2,1,NULL)) as donecount
        from tasks
        left join task_deps on tasks.task_id = task_deps.task_id
        left join task_executions on task_deps.neighboor_id = task_executions.task_id
        where tasks.job_id = "fe95688d1ada4eca9dc27731d390b1e5" and tasks.eligibility_state = 0 -- dont put task_executions.exec_state in the WHERE clause!
        
        group by tasks.job_id, tasks.task_id, input
        having depcount = donecount and depcount > 0
        ;
        """

        # find blocked tasks that can be switched to ready
        # this is useful for troubleshooting:  cast(from_base64(tasks.task_input) as char(255)) as input,
        # WARNING:  don't filter task_executions.exec_state in the where clause!
        ready_sql = """
        select tasks.task_id,  
        COUNT(task_deps.neighboor_id) as depcount,
        COUNT(task_executions.task_id) as donecount
        from tasks left join task_deps on tasks.task_id = task_deps.task_id
        left join task_executions on task_deps.neighboor_id = task_executions.task_id
        where tasks.job_id = %s and tasks.eligibility_state = %s and task_executions.exec_state = %s
        group by tasks.task_id
        having depcount = donecount and depcount > 0
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                params = (job_id, int(TaskEligibilityState.BLOCKED), int(TaskExecState.SUCCEEDED))
                cursor.execute(ready_sql, params)
                rows = cursor.fetchall()

                task_ids = [row[0] for row in rows]
                markers = ",".join(["%s"] * len(task_ids))
                update_sql = f"""
                UPDATE `tasks` SET tasks.eligibility_state = %s
                WHERE tasks.job_id = %s and tasks.eligibility_state = %s and tasks.task_id in ({markers})
                """
                params = (int(TaskEligibilityState.READY), job_id,  int(TaskEligibilityState.BLOCKED)) + tuple(task_ids)
                cursor.execute(update_sql, params)
                print(f"update_task_deps() {cursor.rowcount} rows updated")
                conn.commit()


    def update_job_state(self):  #a.k.a. "all tasks succeeded"
        pass  # mark job as done if all tasks finished

    def fail_job(self):  # manually canceled or dag failed
        pass  # TODO save a reason somewhere

    def pause_task(self):
        pass

    def unpause_task(self):
        pass

    def retry_task(self):  # actually just makes task go from FAILED to....
        pass

    def get_work_1(self):
        pass # TODO wont actually be implemented completely in the dao...will it?

    def get_work_2(self):
        pass # TODO wont actually be implemented completely in the dao...will it?


    def start_task(self, job_id, task_id, worker):
        """
        Called when a task is assigned to a worker
        """
        if len(worker) > WORKER_LENGTH:
            raise ValueError(f"worker cannot be longer than {WORKER_LENGTH}")

        sql = """
        INSERT INTO `task_executions` (job_id, task_id, exec_state, worker)
        VALUES (%s, %s, %s, %s)
        """
        params = (job_id, task_id, int(TaskExecState.RUNNING), worker)
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=params)
                if cursor.rowcount != 1:
                    raise Exception(f"rowcount was {cursor.rowcount}")
            conn.commit()

    def stop_task(self, job_id, task_id, end_state: int):
        if end_state not in [int(TaskExecState.FAILED), int(TaskExecState.SUCCEEDED)]:
            raise ValueError(f"Invalid end state: {end_state}")

        sql = """
        UPDATE `task_executions`
        SET exec_state = %s
        WHERE job_id = %s AND task_id = %s AND exec_state = %s;
        """
        # TODO option to override? maybe only for succeeded though - dont want them to flip succeeded to failed
        params = (end_state, job_id, task_id, int(TaskExecState.RUNNING))
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=params)
                if cursor.rowcount != 1:
                    raise Exception(f"rowcount was {cursor.rowcount}")
            conn.commit()

    # TODO probably want to add some kind of log table for events...like tasks getting force completed...


if __name__ == "__main__":
    from getpass import getpass   # pycharm: run -> edit configurations -> emulate terminal in output console
    import json
    import os

    if os.path.isfile("../local/mysqlcreds.json"):
        with open("../local/mysqlcreds.json") as f:
            d = json.loads(f.read())
        username, pwd = d["username"], d["password"]
    else:
        pwd = getpass("Password>")
        username = "root"
    dbname = "waterflow"
    host = "localhost"

    conn_pool = pooling.MySQLConnectionPool(
        pool_name="waterflow_dao",
        pool_size=5,  # max for mysql?  default is 5
        pool_reset_session=True,
        host=host,
        database=dbname,
        user=username,
        password=pwd,
    )
    #conn_pool.add_connection()

    conn_pool.get_connection()
    # conn = connect(host="localhost", user=username, password=pwd, database="waterflow")
    dao = DagDao(conn_pool, "waterflow")
    print(dao.query())
    print(dao.query())
    print(dao.db_check())



    # https://realpython.com/python-mysql/