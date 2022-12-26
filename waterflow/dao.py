from mysql.connector import connect

class DagDao:
    def __init__(self, mysql_conn):
        self.conn = mysql_conn

    def query(self):
        with self.conn:
            query = "SELECT * FROM jobs";
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()


if __name__ == "__main__":
    from getpass import getpass   # pycharm: run -> edit configurations -> emulate terminal in output console

    pwd = getpass("Password>")
    conn = connect(host="localhost", user="root", password=pwd, database="waterflow")
    dao = DagDao(conn)
    print(dao.query())
    # https://realpython.com/python-mysql/