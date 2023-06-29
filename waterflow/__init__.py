import base64
import datetime
import logging
import uuid
from mysql.connector import pooling
from waterflow.core.mysql_config import MysqlConfig
import os
import time


def make_id():
    return str(uuid.uuid4()).replace("-", "")


def to_base64_str(s: str) -> str:
    """
    Base64 encodes a string and returns it as a string
    """
    if not isinstance(s, str):
        raise ValueError("s must be a string")
    return base64.b64encode(s.encode("UTF-8")).decode("UTF-8")


def from_base64_str(s: str) -> str:  # TODO unit test
    if not isinstance(s, str):
        raise ValueError("s must be a string")
    return base64.b64decode(s.encode("UTF-8")).decode("UTF-8")


def check_utc_or_unaware(d: datetime.datetime):
    """
    Throws an exception if the datetime has a tz set that is not utc
    """
    if d.tzinfo:
        if d.tzinfo.utcoffset(None) != datetime.timedelta(0):
            raise ValueError("datetime must be in UTC")


def get_connection_pool(dbconf, pool_name, pool_size=32):

    # NOTE:  somewhere on the internet said that the mysql connector supports a max of 32
    # but the server server can blow through more than 100 because I can't get it to share the conn pool object.
    logger = logging.getLogger("server")
    logger.info(f"getting connection pool host={dbconf.hostname}")
    return pooling.MySQLConnectionPool(
        pool_name=pool_name,
        pool_size=pool_size,  # mysql connector supports a max of 32?
        pool_reset_session=True,
        host=dbconf.hostname,
        database=dbconf.dbname,
        user=dbconf.username,
        password=dbconf.password,
        port=3306,  # TODO
    )


def get_connection_pool_from_file(filename, pool_name="waterflow_dao", pool_size=32):
    if not os.path.isfile(filename):
        raise Exception(f"Can't find {filename}")
    dbconf = MysqlConfig.from_file(filename)
    return get_connection_pool(dbconf, pool_name, pool_size)


class StopWatch:
    # wall clock time
    def __init__(self):
        self.started = time.time()
        self.stopped = None

    def stop(self):
        self.stopped = time.time()

    def elapsed_sec(self) -> float:
        stopped = self.stopped or time.time()
        return stopped - self.started

    def __str__(self):
        sec = self.elapsed_sec()
        return f"{sec} seconds"

    def __repr__(self):
        return self.__str__()
