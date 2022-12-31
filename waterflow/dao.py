import datetime
from typing import List, Dict
import uuid

from waterflow.exceptions import InvalidJobError, InvalidTaskState, InvalidJobState, NotImplementedYet
from waterflow.task import Task, TaskState, TaskExecState, TaskView1
import waterflow.task
from waterflow.job import JobExecutionState, Dag, JobView1
from waterflow import event_codes
from waterflow.dao_models import PendingJob, FetchDagTask, TaskAssignment

WORKER_LENGTH = 255  # must match the varchar length in the db schema

# this must match the length of the `service_pointer` columns in the jobs and tasks tables.
MAX_SERVICE_PTR_LEN = 128

# must match the length of `tasks.task_name` column
MAX_TASK_NAME_LEN = 64

# the max # of workers the DAO itself will let you assign in a single call (just to have testable limits)
MAX_WORKER_ASSIGN = 1024  # this does not mean the API call is limited to 64.

# Max number of "tags" that a job can have.
# Don't know at what point too many tags becomes an issue, so just picking a low number that should work for now.
MAX_TAG_COUNT = 8

MAX_TAG_LENGTH = 255

# max length of `jobs`.`job_name` column
MAX_JOB_NAME_LEN = 64

# format string for datetime.strftime() for Mysql's DATETIME column
DATETIME_COL_FORMAT = "%Y-%m-%d %H:%M:%S"

class DagDao:
    ALL_TABLES = ['job_executions', 'jobs', 'job_tags', 'task_deps', 'tasks', 'error_events']

    def __init__(self, mysql_conn_pool, dbname):
        self.conn_pool = mysql_conn_pool
        self.dbname = dbname  # to check table existence

    def _markers(self, n):  # TODO unit test this
        if n < 1:
            raise ValueError("n must be >= 1")
        return ",".join(["%s"] * n)

    def _check_worker_args(self, workers):  # TODO unit test this
        if len(workers) > MAX_WORKER_ASSIGN:
            raise ValueError(f"cannot assign more than {MAX_WORKER_ASSIGN} workers in one transaction")
        for worker in workers:
            if len(worker) > WORKER_LENGTH:
                raise ValueError(f"worker cannot be longer than {WORKER_LENGTH}")

    def _check_base64_arg(self, s):  # TODO unit test this
        if not (s is None or isinstance(s, str)):
            raise ValueError("argument must be a str; if you have bytes call .decode(UTF-8)")

    def _check_tinyint_arg(self, i):
        if i > 255:
            raise ValueError("argument too large for TINYINT column")

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

    def add_job(self, job: PendingJob, now_utc: datetime.datetime = None):
        """
        Called by scheduer/trigger to start a job (to enqueue it for starting).
        """
        if job.job_name is None or len(job.job_name) > MAX_JOB_NAME_LEN:
            raise ValueError(f"Invalid job name: {job.job_name}")
        self._check_base64_arg(job.job_input64)
        self._check_tinyint_arg(job.job_input64_v)
        if job.service_pointer is not None and len(job.service_pointer) > MAX_SERVICE_PTR_LEN:
            raise ValueError(f"service pointer string cannot exceed {MAX_SERVICE_PTR_LEN}")

        if job.tags:
            if len(job.tags) > MAX_TAG_COUNT:
                raise ValueError(f"number of tags exceeds max of {MAX_TAG_COUNT}")
            for tag in job.tags:
                if len(tag) > MAX_TAG_LENGTH:
                    raise ValueError(f"tag {tag} is too long")

        job_id = str(uuid.uuid4()).replace("-", "")

        now_utc = now_utc or datetime.datetime.utcnow()
        now_s = now_utc.strftime(DATETIME_COL_FORMAT)

        sql = """
        insert into `jobs` (job_id, job_name, created_utc,  job_input, job_input_v, service_pointer, work_queue)
        VALUES (%s, %s, %s, FROM_BASE64(%s), %s, %s, %s);
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                params = (job_id, job.job_name, now_s, job.job_input64, job.job_input64_v, job.service_pointer, job.work_queue)
                cursor.execute(sql, params=params)
                conn.commit()  # required

                if job.tags:
                    sql = """
                    INSERT INTO `job_tags` (job_tags.job_id, job_tags.tag)
                    VALUES (%s, %s);
                    """
                    for tag in job.tags:
                        cursor.execute(sql, params=(job_id, tag))
                    conn.commit()

        return job_id

    def get_job_info(self, job_id) -> JobView1:
        sql = """
        SELECT jobs.job_id, jobs.job_name, jobs.created_utc, TO_BASE64(jobs.job_input), jobs.job_input_v,
        jobs.service_pointer, job_executions.state, job_executions.worker, TO_BASE64(job_executions.dag), jobs.work_queue
        FROM jobs LEFT JOIN job_executions on jobs.job_id = job_executions.job_id
        WHERE jobs.job_id = %s
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=(job_id,))
                rows = cursor.fetchall()
                if len(rows) == 1:
                    _, job_name, created_utc, job_input64, job_input64_v, service_pointer, state, worker, dag64, work_queue = rows[0]
                    # NOTE:  the connector automatically reads the DATETIME column as a TZ-unaware python datetime object,
                    # but fortunately does not seem to fuck with it (mysql docs say that TIMESTAMP fields get fucked with but
                    # DATETIME fields do not)
                    if state is None:
                        # if the execution row is missing, that means it is PENDING
                        state = int(JobExecutionState.PENDING)

                    sql = "SELECT job_tags.tag FROM job_tags WHERE job_tags.job_id = %s;"
                    cursor.execute(sql, params=(job_id,))
                    rows = cursor.fetchall()
                    tags = [row[0] for row in rows]

                    return JobView1(
                        job_id=job_id,
                        job_name=job_name,
                        job_input64=job_input64,
                        job_input64_v=job_input64_v,
                        service_pointer=service_pointer,
                        created_utc=created_utc, # if it was str: datetime.datetime.strptime(row[1], DATETIME_COL_FORMAT)
                        state=state,
                        worker=worker,
                        dag64=dag64,
                        work_queue=work_queue,
                        tags=tags,
                    )
                else:
                    raise Exception(f"cannot find job {job_id}")

    def get_tasks_by_job(self, job_id) -> List[Task]:
        sql = """
        select tasks.task_id, tasks.state, TO_BASE64(tasks.task_input), updated_utc
        FROM tasks
        WHERE tasks.job_id = %s
        """
        tasks = []
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=(job_id,))
                rows = cursor.fetchall()
                for row in rows:
                    task_id, state, task_input, updated_utc = row
                    tasks.append(TaskView1(job_id, task_id, state, task_input, updated_utc))
                return tasks

    # a.k.a. get_work_1()
    def get_and_start_jobs(self, workers: List[str], work_queue=0, now_utc=None) -> List[FetchDagTask]:
        """
        Called to get fetch-dag tasks for workers, which starts a job.
        """
        if not isinstance(work_queue, int):
            raise ValueError("work queue must be an int")
        now_utc = now_utc or datetime.datetime.utcnow()

        # TODO add a work_queue column!
        # TODO add created_utc and last_updated_utc columns!

        self._check_worker_args(workers)  # TODO unit test this being enforced

        jobs = []
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                fetch_sql = """
                SELECT jobs.job_id, jobs.job_input, jobs.service_pointer, jobs.work_queue
                FROM jobs LEFT JOIN job_executions on jobs.job_id = job_executions.job_id
                WHERE job_executions.job_id is NULL AND jobs.work_queue = %s
                LIMIT %s
                """
                cursor.execute(fetch_sql, params=(work_queue, len(workers),))
                rows = cursor.fetchall()
                for i, row in enumerate(rows):
                    job_id, job_input64, service_pointer, work_queue = row
                    jobs.append(FetchDagTask(job_id, job_input64, service_pointer, work_queue, worker=workers[i]))

                sql = """
                        insert into job_executions (job_id, created_utc, updated_utc, state, worker, dag, work_queue)
                        values (%s, %s, %s, %s, %s, NULL, %s);
                        """
                for fetch_task in jobs:
                    now_s = now_utc.strftime(DATETIME_COL_FORMAT)
                    params = (fetch_task.job_id, now_s, now_s, int(JobExecutionState.DAG_FETCH), fetch_task.worker, work_queue)
                    cursor.execute(sql, params=params)
                conn.commit()

        return jobs

    #def set_dag(self, job_id: str, dag64: str, tasks: List[Task], task_deps: Dict[str, List[str]]):
    def set_dag(self, job_id: str, dag: Dag, work_queue: int, now_utc: datetime.datetime = None):
        """
        Called when worker has finished fetching the dag.

        Q:  why are both dag64 and tasks specified?  why not only specify dag64?
        A:  the DAO shouldn't own the format of the DAG bytes
        """
        now_utc = now_utc or datetime.datetime.utcnow()
        now_s = now_utc.strftime(DATETIME_COL_FORMAT)

        for task in dag.tasks:
            self._check_tinyint_arg(task.input64_v)

        # TODO if the dag is invalid, we need to set it anyway and fail the job

        if not isinstance(dag.raw_dag64, str):
            raise ValueError("dag64 must be a str; if you have bytes call .decode(UTF-8)")

        if not waterflow.task.is_valid(dag.tasks, dag.adj_list):
            raise ValueError("invalid task graph")

        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                conn.start_transaction()

                sql = """
                update job_executions
                set dag=FROM_BASE64(%s), dag_v=%s, state=(%s), updated_utc=%s, work_queue = %s
                where job_id = %s AND state = %s AND dag IS NULL;
                """
                expected_state = int(JobExecutionState.DAG_FETCH)
                params = (dag.raw_dag64, dag.raw_dagv, int(JobExecutionState.RUNNING), now_s, work_queue, job_id, expected_state)
                cursor.execute(sql, params=params)
                if cursor.rowcount == 0:
                    conn.rollback()
                    raise InvalidJobState(f"job {job_id} does not exist or is in the wrong state")

                sql = """
                insert into `tasks` (job_id, task_id, created_utc, updated_utc, state, task_input, task_input_v, service_pointer, work_queue)
                values (%s, %s, %s, %s, 0, FROM_BASE64(%s), %s, %s, %s);
                """
                for task in dag.tasks:
                    params = (job_id, task.task_id, now_s, now_s, task.input64, task.input64_v, task.service_pointer, work_queue)
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

    def _set_job_state_failed(self, job_id: str, now_s: str):
        """
        Change a job's state to FAILED.  This logic is complicated because there is an implicit state of "PENDING"
        between the time a job is added and when the dag fetch starts, indicated only by the lack of a row in the
        `job_executions` table.
        """
        JOB_FETCHING = int(JobExecutionState.DAG_FETCH)
        JOB_RUNNING = int(JobExecutionState.RUNNING)
        JOB_FAILED = int(JobExecutionState.FAILED)  # 4

        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                conn.start_transaction()
                try:
                    sql = """
                    SELECT jobs.job_id, job_executions.state
                    FROM jobs LEFT JOIN job_executions on jobs.job_id = job_executions.job_id 
                    WHERE jobs.job_id = %s LIMIT 1;
                    """
                    cursor.execute(sql, params=(job_id,))
                    rows = cursor.fetchall()
                    if len(rows) != 1:
                        raise InvalidJobError(f"job {job_id} does not exist")

                    job_id, state = rows[0]

                    if state == JOB_RUNNING:
                        # TODO support this:
                        # 1. To do this we need to pre-emptively go out to all of the BLOCKED, PENDING, PAUSED tasks of
                        # the job and push them to the failed state.
                        # 2. we do NOT want to change RUNNING tasks (because the worker is going to come back and mark
                        # them complete anyway.  And obviously we don't undo the DONE state
                        # 3. we need to mark the tasks immediately, to prevent them from being fetched as work items
                        # (if we don't do this in the same transaction, we'll need to join the jobs table every time
                        # we pick up tasks to work on just to check the job state!)
                        # 4. test task fetch after failure
                        # 5. test task completion after failure (make sure it doesnt mark a failed job succeeded)
                        # 6. test job cancel happens after final task completes but before call to update_job_state()
                        # to mark it succeeded.
                        raise NotImplementedYet("canceling running jobs not implemented yet")
                    elif state in [JOB_FETCHING]:  # , JOB_RUNNING
                        sql = "UPDATE job_executions SET state = %s WHERE job_id = %s;"
                        cursor.execute(sql, params=(JOB_FAILED, job_id))
                        if cursor.rowcount != 1:
                            raise Exception("something went wrong")
                    elif state is None:
                        # no row b/c job is PENDING (dag fetch hasnt started)
                        sql = """
                        INSERT INTO job_executions (job_id, created_utc, updated_utc, state) VALUES (%s, %s, %s, %s);
                        """
                        cursor.execute(sql, params=(job_id, now_s, now_s, JOB_FAILED))
                        if cursor.rowcount != 1:
                            raise Exception("something went wrong")
                    else:
                        # not allowed to change the state
                        raise InvalidJobState("cannot change job state to failed")

                    conn.commit()

                except Exception as ex:
                    conn.rollback()
                    raise ex


    def fail_job(self, job_id: str, event_code, failure_message, failure_obj64) -> bool:
        """
        Tries to halt a job.   This will not work if the job is already in the SUCCEEDED for FAILED states.

        :returns: True IFF it moved the job to the FAILED state
        """
        self._check_base64_arg(failure_obj64)

        now_utc = datetime.datetime.utcnow()
        now_s = now_utc.strftime(DATETIME_COL_FORMAT)

        # TODO dag fetch needs to fail if job is canceled - TODO add unit test for this


        self._set_job_state_failed(job_id, now_s)

        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                INSERT INTO error_events (job_id, task_id, event_code, failure_message, failure_obj)
                VALUES (%s, %s, %s, %s, FROM_BASE64(%s));
                """
                cursor.execute(sql, params=(job_id, None, event_code, failure_message, failure_obj64))
                conn.commit()
                return True


    def update_task_deps(self, job_id):
        """
        Find any tasks that have no unfinished dependencies and mark them safe for execution.

        See also:  update_job_state()
        """

        # find blocked tasks that can be switched to ready
        # this is useful for troubleshooting:  cast(from_base64(tasks.task_input) as char(255)) as input,
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

                cursor.execute(ready_sql, (job_id,))
                rows = cursor.fetchall()

                task_ids = [row[0] for row in rows]

                if len(task_ids) > 0:
                    markers = self._markers(len(task_ids)) # markers = ",".join(["%s"] * len(task_ids))
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
        now_utc = datetime.datetime.utcnow()

        # TODO this should not take a job out of the failed state (even though all tasks succeeded...)

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
                    update job_executions set job_executions.state = %s, updated_utc = %s
                    where job_executions.job_id = %s and job_executions.state != %s;
                    """
                    cursor.execute(sql, params=(int(JobExecutionState.SUCCEEDED), now_utc.strftime(DATETIME_COL_FORMAT), job_id, int(JobExecutionState.SUCCEEDED)))
                    if cursor.rowcount > 1:
                        raise Exception()
                    conn.commit()
                    return cursor.rowcount == 1

    def complete_task(self, job_id, task_id):  # TODO rename; this is only for successful completion now
        end_state = int(TaskState.SUCCEEDED)
        ALLOWED_STATES = [int(TaskState.RUNNING), int(TaskState.FAILED)]
        self._change_task_state(job_id, task_id, end_state, ALLOWED_STATES)

    def _change_task_state(self, job_id: str, task_id: str, new_state: int, allowed_states: List[int], now_utc=None):
        if not isinstance(job_id, str):  # TODO unit test for this
            raise ValueError("job_id must be a str")
        if not isinstance(task_id, str):
            raise ValueError("task_id must be a str")

        now_utc = now_utc or datetime.datetime.utcnow()
        now_s = now_utc.strftime(DATETIME_COL_FORMAT)

        markers = self._markers(len(allowed_states))
        sql = f"""
        UPDATE tasks SET tasks.state = %s, updated_utc = %s
        WHERE tasks.job_id = %s AND tasks.task_id = %s AND tasks.state in ({markers});
        """
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=(new_state, now_s, job_id, task_id) + tuple(allowed_states))
                if cursor.rowcount == 1:
                    conn.commit()
                else:
                    raise InvalidTaskState(f"Task {task_id} for job {job_id} does not exist or cannot be canceled")

    def cancel_task(self, job_id, task_id):
        """
        Operator canceled a task in the BLOCKED or PENDING states.  This is different from a task failing at runtime.
        """
        ALLOWED_STATES = [int(TaskState.BLOCKED), int(TaskState.PENDING)]
        self._change_task_state(job_id, task_id, int(TaskState.FAILED), ALLOWED_STATES)

    def fail_task(self, job_id, task_id):
        """
        Called by worker to incidate that a task has failed while executing.
        """
        ALLOWED_STATES = [int(TaskState.RUNNING)]
        self._change_task_state(job_id, task_id, int(TaskState.FAILED), ALLOWED_STATES)

    def retry_task(self, job_id, task_id):  # actually just makes task go from FAILED to....
        ALLOWED_STATES = [int(TaskState.FAILED)]
        self._change_task_state(job_id, task_id, int(TaskState.PENDING), ALLOWED_STATES)

    def keep_task_alive(self, job_id, task_id, now_utc=None):
        """
        Called by a worker to say it is still alive and working on the task.  Updates the `updated_utc` field to the
        current time.
        """
        now_utc = now_utc or datetime.datetime.utcnow()
        RUNNING = int(TaskState.RUNNING)
        # we can re-use this method by making it a state change from RUNNING -> RUNNING.
        self._change_task_state(job_id, task_id, RUNNING, [RUNNING], now_utc=now_utc)

    def get_and_start_tasks(self, workers: List[str], work_queue=0, now_utc=None) -> List[TaskAssignment]:
        """
        Called to assign tasks to workers and market them as running.

        TODO - the caller (the service code) will need to call update_deps() for every returned
        job_id.
        """
        if not isinstance(work_queue, int):
            raise ValueError("work_queue is wrong type")
        self._check_worker_args(workers)  # TODO unit test this being enforced

        now_utc = now_utc or datetime.datetime.utcnow()
        now_s = now_utc.strftime(DATETIME_COL_FORMAT)

        # TODO maybe add a table to keep track of what worker has a task? - inserting different workers is probably a massive perf hit
        # not a log... `task_ownership` -- primary key is task_id, and we do UPSERTs

        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:

                conn.start_transaction()

                sql = """
                select tasks.job_id, tasks.task_id, tasks.task_name, TO_BASE64(tasks.task_input), tasks.task_input_v, 
                tasks.service_pointer from tasks
                where tasks.state = 1 limit %s;
                """
                cursor.execute(sql, params=(len(workers),))
                rows = cursor.fetchall()
                tasks = [TaskAssignment(job_id, task_id, task_name, task_input, task_input_v, service_pointer)
                         for job_id, task_id, task_name, task_input, task_input_v, service_pointer in rows
                ]
                task_ids = [task.task_id for task in tasks]

                if len(task_ids) > 0:
                    markers = self._markers(len(tasks))
                    sql = f"""
                    UPDATE tasks
                    set tasks.state = 3, updated_utc = %s
                    where tasks.task_id in ({markers}) and tasks.state = 1;
                    """
                    cursor.execute(sql, params=(now_s,) + tuple(task_ids))
                    if cursor.rowcount != len(rows):
                        raise Exception(f"rowcount: {cursor.rowcount}")  # TODO: proper transaction rollback

                    # TODO and then insert into some kind of `task_ownership` or `task_assignment` table

                conn.commit()

                return tasks

    def start_task(self, job_id, task_id, worker):
        """
        Starts a specific task -- mostly for testing.  This does not check if the task can be started -- probably dont
        want to expose this one (use it only for testing)

        TODO - maybe this would be useful for an API call where someone instructs a worker to start particular task
        """
        if len(worker) > WORKER_LENGTH:
            raise ValueError(f"worker cannot be longer than {WORKER_LENGTH}")

        if not isinstance(task_id, str):
            raise ValueError("task_id must be a str")

        sql = """
        UPDATE tasks set tasks.state = 3
        where tasks.job_id = %s AND tasks.task_id = %s and tasks.state = 1;
        """
        params = (job_id, task_id)
        with self.conn_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params=params)
                if cursor.rowcount != 1:
                    raise Exception(f"rowcount was {cursor.rowcount} for task_id={task_id}")
            conn.commit()

