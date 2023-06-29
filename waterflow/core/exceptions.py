class InvalidJobError(Exception):
    """
    Raised by DAO when an invalid job_id is given.
    """

    pass


class InvalidJobState(Exception):
    """
    A job was in the wrong state for whatever you tried to do to it
    """


class InvalidTaskState(Exception):
    """
    A task was in the wrong state for whatever you tried to do to it
    """


class NotImplementedYet(Exception):
    """
    The operation should have worked, but it has not been finished yet.
    """
