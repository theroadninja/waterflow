

# Job moved to FAILED state because someone canceled it
JOB_CANCELED = 0

JOB_FAILED = 1

# Reserved for something going wrong with the scheduling system.
#UNKNOWN_ERROR = 0

# Got an error from the RPC call to the service to run the task
#RPC_ERROR = 1

# Someone called the API and manually canceled the job
#CANCELED_BY_USER = 2


# Task moved from PENDING, PAUSED, or RUNNING to FAILED because someone canceled it.
TASK_CANCELED = 1000  # NOTE:  canceled works from more states than failed

TASK_FAILED = 1001

TASK_RETRIED = 1002

TASK_FORCED_COMPLETE = 1003