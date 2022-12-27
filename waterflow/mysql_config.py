from dataclasses import dataclass
import json
import os


@dataclass
class MysqlConfig:
    username: str
    password: str
    hostname: str
    dbname: str
    port: int   # TODO not implemented yet

    @staticmethod
    def from_file(filename):
        if not os.path.isfile(filename):
            raise ValueError(f"not a valid file: {filename}")
        with open("../../local/mysqlconfig.json") as f:
            d = json.loads(f.read())

        return MysqlConfig(
            d.get("username"),
            d.get("password"),
            d.get("hostname"),
            d.get("dbname"),
            d.get("port"),
        )
