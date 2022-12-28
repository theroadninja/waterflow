import datetime
from mysql.connector import connect, pooling
from typing import List, Dict
import uuid

from waterflow.task import Task, TaskState, TaskExecState, TaskView1, TaskAssignment
import waterflow.task
from waterflow.job import JobExecutionState, FetchDagTask, Dag, JobView1

WORKER_LENGTH = 255  # must match the varchar length in the db schema

# format string for datetime.strftime() for Mysql's DATETIME column
DATETIME_COL_FORMAT = "%Y-%m-%d %H:%M:%S"

class DagDao:
    ALL_TABLES = ['job_executions', 'jobs', 'task_deps', 'tasks']

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

        sql = f"""
        select table_name from information_schema.tables
        where table_schema = "{self.dbname}"
        and table_name in ({",".join(["%s" for _ in DagDao.ALL_TABLES])});
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(DagDao.ALL_TABLES))
                results = [row[0] for row in cursor.fetchall()]

        return sorted(results) == sorted(DagDao.ALL_TABLES)

    def count_jobs(self, job_state) -> int:
        if job_state == int(JobExecutionState.PENDING):
            sql = """select count(*) from jobs
            left join job_executions on jobs.job_id = job_executions.job_id where job_executions.job_id is NULL;
            """
        elif job_state == int(JobExecutionState.DAG_FETCH):
            sql = """select count(*) from jobs
            left join job_executions on jobs.job_id = job_executions.job_id
            WHERE job_executions.job_id is NOT NULL and job_executions.state = 1
            """  # DAG_FETCH is 1
        elif job_state == int(JobExecutionState.RUNNING):
            raise Exception("Not Implemented Yet")
        elif job_state == int(JobExecutionState.FAILED):
            raise Exception("Not Implemented Yet")
        elif job_state == int(JobExecutionState.SUCCEEDED):
            raise Exception("Not Implemented Yet")


        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:

                cursor.execute(sql)
                rows = cursor.fetchall()
                return rows[0][0]

    def add_job(self, job_input64: str, now_utc: datetime.datetime = None):
        """
        Called by scheduer/trigger to start a job (to enqueue it for starting).
        """
        if not isinstance(job_input64, str):
            raise ValueError("job_input must be a str; if you have bytes call .decode(UTF-8)")
        job_id = str(uuid.uuid4()).replace("-", "")

        now_utc = now_utc or datetime.datetime.utcnow()

        sql = """
        insert into `jobs` (job_id, created_utc,  job_input)
        VALUES (%s,   %s,     FROM_BASE64(%s));
        """
        # print(sql)  # TODO feature to auto log sql?
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=(job_id, now_utc.strftime(DATETIME_COL_FORMAT), job_input64))
                conn.commit()  # required

        return job_id

    def get_job_info(self, job_id):
        sql = """
        SELECT jobs.job_id, jobs.created_utc, TO_BASE64(jobs.job_input), job_executions.state, job_executions.worker, TO_BASE64(job_executions.dag)
        FROM jobs LEFT JOIN job_executions on jobs.job_id = job_executions.job_id
        WHERE jobs.job_id = %s
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=(job_id,))
                rows = cursor.fetchall()
                if len(rows) > 1:
                    Exception(f"too many rows fetched for job {job_id}")
                elif len(rows) == 1:
                    _, created_utc, job_input64, state, worker, dag64 = rows[0]
                    # NOTE:  the connector automatically reads the DATETIME column as a TZ-unaware python datetime object,
                    # but fortunately does not seem to fuck with it (mysql docs say that TIMESTAMP fields get fucked with but
                    # DATETIME fields do not)
                    if state is None:
                        # if the execution row is missing, that means it is PENDING
                        state = int(JobExecutionState.PENDING)

                    return JobView1(
                        job_id=job_id,
                        job_input64=job_input64,
                        created_utc=created_utc, # if it was str: datetime.datetime.strptime(row[1], DATETIME_COL_FORMAT)
                        state=state,
                        worker=worker,
                        dag64=dag64,
                    )
                else:
                    raise Exception(f"cannot find job {job_id}")

        # TODO finish


        # class JobView1:  # TODO not sure what the final form will be
        #     """
        #     Only used for pulling info out of the DB about a job
        #     """
        #     job_id: str
        #     job_input64: str
        #     created_utc: datetime
        #     state: Optional[int]
        #     worker: Optional[str]
        #     dag64: Optional[str]

    def get_tasks_by_job(self, job_id) -> List[Task]:
        sql = """
        select tasks.task_id, tasks.state, TO_BASE64(tasks.task_input), tasks.worker
        FROM tasks
        WHERE tasks.job_id = %s
        """
        tasks = []
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=(job_id,))
                rows = cursor.fetchall()
                for row in rows:
                    task_id, state, task_input, worker = row
                    tasks.append(TaskView1(job_id, task_id, state, task_input, worker))
                return tasks


    # a.k.a. get_work_1()
    def get_and_start_jobs(self, workers: List[str], now_utc=None) -> List[FetchDagTask]:
        """
        Called to get fetch-dag tasks for workers, which starts a job.

        Inserts the job_execution row and marks which worker is working on it

        TODO:  need another method for if the worker times out and we try again (or is that just a re-run????)
        """
        now_utc = now_utc or datetime.datetime.utcnow()

        # TODO add a work_queue column!

        # TODO add created_utc and last_updated_utc columns!

        for worker in workers:
            if len(worker) > WORKER_LENGTH:
                raise ValueError(f"worker cannot be longer than {WORKER_LENGTH}")

        jobs = []
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                fetch_sql = """
                SELECT jobs.job_id, jobs.job_input
                FROM jobs LEFT JOIN job_executions on jobs.job_id = job_executions.job_id
                WHERE job_executions.job_id is NULL
                LIMIT %s
                """
                cursor.execute(fetch_sql, params=(len(workers),))
                rows = cursor.fetchall()
                for i, row in enumerate(rows):
                    jobs.append(FetchDagTask(job_id=row[0], job_input64=row[1], worker=workers[i]))

                sql = """
                        insert into job_executions (job_id, created_utc, state, worker, dag)
                        values (%s, %s, %s, %s, NULL);
                        """
                for fetch_task in jobs:
                    params = (fetch_task.job_id, now_utc.strftime(DATETIME_COL_FORMAT), int(JobExecutionState.DAG_FETCH), fetch_task.worker)
                    cursor.execute(sql, params=params)
                conn.commit()

        return jobs

    #def set_dag(self, job_id: str, dag64: str, tasks: List[Task], task_deps: Dict[str, List[str]]):
    def set_dag(self, job_id: str, dag: Dag, now_utc: datetime.datetime = None):
        """
        Called when worker has finished fetching the dag.

        Q:  why are both dag64 and tasks specified?  why not only specify dag64?
        A:  the DAO shouldn't own the format of the DAG bytes
        """
        now_utc = now_utc or datetime.datetime.utcnow()

        # TODO if the dag is invalid, we need to set it anyway and fail the job

        if not isinstance(dag.raw_dag64, str):
            raise ValueError("dag64 must be a str; if you have bytes call .decode(UTF-8)")

        if not waterflow.task.is_valid(dag.tasks, dag.adj_list):
            raise ValueError("invalid task graph")

        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                update job_executions
                set dag=FROM_BASE64(%s), state=(%s)
                where job_id = %s AND state = %s AND dag IS NULL;
                """
                params = (dag.raw_dag64, int(JobExecutionState.RUNNING), job_id, int(JobExecutionState.DAG_FETCH))
                cursor.execute(sql, params=params)

                # sql = """
                # insert into `tasks` (job_id, task_id, eligibility_state, task_input)
                # values (%s, %s, %s, FROM_BASE64(%s));
                # """
                # for task in dag.tasks:
                #     params = (job_id, task.task_id, int(TaskEligibilityState.BLOCKED), task.input64)
                #     cursor.execute(sql, params=params)
                #
                # sql = """
                # INSERT INTO `task_deps` (job_id, task_id, neighboor_id)
                # VALUES (%s, %s, %s);
                # """
                # for task_id, deps in dag.adj_list.items():
                #     for neighboor_id in deps:
                #         params = (job_id, task_id, neighboor_id)
                #         cursor.execute(sql, params=params)

                sql = """
                insert into `tasks` (job_id, task_id, created_utc, state, task_input, worker)
                values (%s, %s, %s, 0, FROM_BASE64(%s), NULL);
                """
                for task in dag.tasks:
                    params = (job_id, task.task_id, now_utc.strftime(DATETIME_COL_FORMAT), task.input64)
                    cursor.execute(sql, params=params)

                sql = """
                INSERT INTO `task_deps` (job_id, task_id, neighboor_id)
                VALUES (%s, %s, %s);
                """
                for task_id, deps in dag.adj_list.items():
                    for neighboor_id in deps:
                        params = (job_id, task_id, neighboor_id)
                        cursor.execute(sql, params=params)


                conn.commit()
                # prints last call, not the whole transaction: print(f"rows updated: {cursor.rowcount}")

    def update_task_deps(self, job_id):
        """
        Find any tasks that have no unfinished dependencies and mark them safe for execution.

        See also:  update_job_state()
        """

        # NOTE this is very old
        SAVE_ME_SOMEWHERE = """
        select tasks.job_id, tasks.task_id, tasks.eligibility_state, 
        cast(from_base64(tasks.task_input) as char(255)) as input, 
        COUNT(task_deps.neighboor_id) as depcount,
        COUNT(IF(task_executions.exec_state = 2,1,NULL)) as donecount
        from tasks
        left join task_deps on tasks.task_id = task_deps.task_id
        left join task_executions on task_deps.neighboor_id = task_executions.task_id
        where tasks.job_id = "fe95688d1ada4eca9dc27731d390b1e5" and tasks.eligibility_state = 0
        
        group by tasks.job_id, tasks.task_id, input
        having depcount = donecount and depcount > 0 -- depcount > 0 is wrong!  tasks can have no deps...
        ;
        """

        ANOTHER_ONE = """
        select tasks.task_id,  tasks.state,
        COUNT(task_deps.neighboor_id) as depcount,
        COUNT(IF(tasks2.state = 4,1,NULL)) as donecount
        from tasks left join task_deps on tasks.task_id = task_deps.task_id
        left join `tasks` `tasks2` on task_deps.neighboor_id = tasks2.task_id
        where tasks.job_id = "eb4be85d0eb7449bbffa7bd2cb0c404e" and tasks.state = 0
        group by tasks.task_id, tasks.state
        """

        # find blocked tasks that can be switched to ready
        # this is useful for troubleshooting:  cast(from_base64(tasks.task_input) as char(255)) as input,
        # WARNING:  don't filter task_executions.exec_state in the where clause!
        ready_sql = """
        select tasks.task_id,  
        COUNT(task_deps.neighboor_id) as depcount,
        COUNT(IF(tasks2.state = 4,1,NULL)) as donecount
        from tasks left join task_deps on tasks.task_id = task_deps.task_id
        left join `tasks` `tasks2` on task_deps.neighboor_id = tasks2.task_id
        where tasks.job_id = %s and tasks.state = 0
        group by tasks.task_id
        having depcount = donecount
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:

                conn.start_transaction(readonly=False)

                params = (job_id,)
                cursor.execute(ready_sql, params)
                rows = cursor.fetchall()

                task_ids = [row[0] for row in rows]
                print(f"found task ids: {task_ids}")

                if len(task_ids) > 0:
                    markers = ",".join(["%s"] * len(task_ids))
                    update_sql = f"""
                    UPDATE `tasks` SET tasks.state = 1
                    WHERE tasks.job_id = %s and tasks.state = 0 and tasks.task_id in ({markers});
                    """
                    params = (job_id,) + tuple(task_ids)
                    cursor.execute(update_sql, params)
                    # print(f"update_task_deps() {cursor.rowcount} rows updated")

                # end the transaction either way
                conn.commit()  # TODO do we need a try catch to rollback?  probably not b/c last op is the write


    def update_job_state(self, job_id):  #a.k.a. "all tasks succeeded"
        """
        Checks if all tasks for a job have been completed.

        See also:  update_task_deps()

        :returns: True if the job state was changed to succeeded, False if it still has unfinished rows, or if it was already succeeded.
        """
        sql = """
        select tasks.job_id, count(*) as unfinished_count from tasks
        where tasks.job_id = %s and tasks.state != 4
        group by tasks.job_id;
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:

                # Need two queries because MYSQL doesnt like UPDATES that use GROUP BYs
                cursor.execute(sql, params=(job_id,))
                rows = cursor.fetchall()
                if len(rows) > 1:
                    raise Exception(f"expected 0 or 1 rows for job {job_id} but got {len(rows)}")

                elif len(rows) == 1:
                    if rows[0][0] != job_id:
                        raise Exception(f"{rows[0][0]} != {job_id}")

                    if rows[0][1] == 0:
                        raise Exception("this shouldn't happen?  unless there is no execution yet?")
                    else:
                        # some rows were unfinished
                        # print(cursor.statement)
                        return False

                elif len(rows) == 0:
                    # no unfinished rows
                    sql = """
                    update job_executions set job_executions.state = %s
                    where job_executions.job_id = %s and job_executions.state != %s;
                    """
                    cursor.execute(sql, params=(int(JobExecutionState.SUCCEEDED), job_id, int(JobExecutionState.SUCCEEDED)))
                    if cursor.rowcount > 1:
                        raise Exception()
                    conn.commit()
                    return cursor.rowcount == 1





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
        pass # actually this is implemented in get_and_start_jobs()

    def get_work_2(self):
        pass # TODO wont actually be implemented completely in the dao...will it?


    def get_and_start_tasks(self, workers: List[str]) -> List[TaskAssignment]:
        # a task with no task_execution row or a task_execution row marked as PENDING
        # TODO - might be a good reason to combine the two states?
        pass


    def start_task(self, job_id, task_id, worker):
        """
        Starts an individual task -- mostly for testing.
        Called when a task is assigned to a worker

        TODO this does not check if the task can be started -- probably dont want to expose this one (use it only for testing)
        """
        if len(worker) > WORKER_LENGTH:
            raise ValueError(f"worker cannot be longer than {WORKER_LENGTH}")

        if not isinstance(task_id, str):
            raise ValueError("task_id must be a str")

        # sql = """
        # INSERT INTO `task_executions` (job_id, task_id, exec_state, worker)
        # VALUES (%s, %s, %s, %s)
        # """
        # params = (job_id, task_id, int(TaskExecState.RUNNING), worker)
        # with self.conn_pool.get_connection() as conn:
        #     with conn.cursor() as cursor:
        #         cursor.execute(sql, params=params)
        #         if cursor.rowcount != 1:
        #             raise Exception(f"rowcount was {cursor.rowcount}")
        #     conn.commit()

        sql = """
        UPDATE tasks
        set tasks.state = 3, tasks.worker = %s
        where tasks.job_id = %s AND tasks.task_id = %s and tasks.state = 1;
        """
        params = (worker, job_id, task_id)
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=params)
                if cursor.rowcount != 1:
                    raise Exception(f"rowcount was {cursor.rowcount} for task_id={task_id}")
            conn.commit()

    def stop_task(self, job_id, task_id, end_state: int):
        if end_state not in [int(TaskState.FAILED), int(TaskState.SUCCEEDED)]:
            raise ValueError(f"Invalid end state: {end_state}")

        # sql = """
        # UPDATE `task_executions`
        # SET exec_state = %s
        # WHERE job_id = %s AND task_id = %s AND exec_state = %s;
        # """
        # # TODO option to override? maybe only for succeeded though - dont want them to flip succeeded to failed
        # params = (end_state, job_id, task_id, int(TaskExecState.RUNNING))
        # with self.conn_pool.get_connection() as conn:
        #     with conn.cursor() as cursor:
        #         cursor.execute(sql, params=params)
        #         if cursor.rowcount != 1:
        #             raise Exception(f"rowcount was {cursor.rowcount}")
        #     conn.commit()

        sql = """
        UPDATE `tasks`
        SET state = %s
        WHERE job_id = %s AND task_id = %s and state = %s;
        """
        params = (end_state, job_id, task_id, int(TaskState.RUNNING))
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