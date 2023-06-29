import inspect
import json
import logging
import os
import signal
import uuid

# used to make paths relative to this file instead of the CWD
MYPATH = os.path.dirname(os.path.abspath(inspect.stack()[0][1]))


def make_id():
    return str(uuid.uuid4()).replace("-", "")


class StopSignal:
    """
    Convenience class to store the fact that a signal was received, so that other parts of the code can poll.
    The meaning of any signal is assumed to be a request to gracefully halt.

    WARNING:  this does not play well with Pycharm on Windows
    """
    def __init__(self):
        self.stop_requested = False

    def handle_signal(self, sig, frame):
        self.stop_requested = True
        logger = logging.getLogger("core")
        logger.info(f"stop requested signal={sig}")

    def register(self, *signals):
        logger = logging.getLogger("core")
        for sig in signals:
            logger.info(f"registering signal={sig}")
            signal.signal(sig, self.handle_signal)


def resolve_local_path(filename):
    return f"{MYPATH}/../../local/{filename}"


def load_local_config(filename):
    """
    Simple hack to load config for testing in Pycharm.
    """
    with open(resolve_local_path(filename)) as f:
        return json.loads(f.read())