import base64
import uuid
from mysql.connector import connect, pooling
from .mysql_config import MysqlConfig
import os


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


def get_connection_pool(dbconf, pool_name):
    return pooling.MySQLConnectionPool(
        pool_name=pool_name,
        pool_size=5,  # max for mysql?  default is 5
        pool_reset_session=True,
        host=dbconf.hostname,
        database=dbconf.dbname,
        user=dbconf.username,
        password=dbconf.password,
    )

def get_connection_pool_from_file(filename, pool_name="waterflow_dao"):
    if not os.path.isfile(filename):
        raise Exception(f"Can't find {filename}")
    dbconf = MysqlConfig.from_file(filename)
    return get_connection_pool(dbconf, pool_name)