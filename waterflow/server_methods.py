"""
This is the main code for the server.
"""

##
## Inspection Methods
##

# TODO


##
## Execution Methods
##

def submit_job(job):
    # TODO what if we did the idempotence check in memory?
    pass


def get_work_item():
    # works with both get_dag and tasks
    # TODO - do we prioritize dag fetching or tasks?  if we assume that dag fetching in millis to seconds
    # and tasks are minutes to hours, then it makes sense to prioritize dag fetching.
    pass


def set_dag_for_job(job_id, dag):
    pass


def stop_task():  # TODO different methods for succeed, failed, etc?
    pass


# TODO pause/unpause job, pause/unpause task, retry task


