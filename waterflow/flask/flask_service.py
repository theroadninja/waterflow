"""
Provides a flask interface for the service.
"""
from flask import Flask, current_app, request
import json
from typing import Dict
from waterflow import get_connection_pool_from_file, service_methods
from waterflow.dao import DagDao
from .flask_adapter import read_pending_job

app = Flask("Waterflow Flask")


def before_first_request(app, mysql_config_file, pool_name, dbname):

    app.config["mysql_config_file"] = mysql_config_file
    app.config["mysql_pool_name"] = pool_name
    app.config["dbname"] = dbname

    # # apparently the @app.before_first_request notation is deprecated
    # with app.app_context():
    #     # NOTE:  can't use "g" because it is specific to a request.
    #     # see also https://stackoverflow.com/questions/24101056/how-to-use-mysql-connection-db-pool-with-python-flask
    #     g.mysql_conn_pool = conn_pool
    #     g.dbname = dbname


global_db_pool = None  # TODO explicitly test what happend when I attempt to create the same connection pool twice
#  or should I just give the pool a random name?


def get_connection_pool(app):
    # cant figure out a better way to do this -- flask.g doesnt work.
    global global_db_pool
    if not global_db_pool:
        global_db_pool = get_connection_pool_from_file(app.config["mysql_config_file"], app.config["mysql_pool_name"])
    return global_db_pool


def get_dao(app):
    return DagDao(get_connection_pool(app), app.config["dbname"])
    # Doesnt work:
    # with app.app_context():
    #     return DagDao(g.mysql_conn_pool, g.dbname)


@app.route("/")
def something():
    return "Waterflow Flask Server"

@app.route("/api/submit_job/<int:work_queue>", methods=["POST"])  # TODO /api/ to distinguish from /ui/ methods
def submit_job(work_queue):
    print("make it here")
    job = read_pending_job(work_queue, request.get_json())
    job_id = service_methods.submit_job(get_dao(current_app), job)
    return json.dumps({"job_id": job_id})

@app.route("/api/get_work/<int:work_queue>/<string:worker>", methods=["GET"])
def get_work_item(work_queue, worker):

    #TODO
    work_item = service_methods.get_work_item(get_dao(current_app), work_queue, worker)
    #TODO

@app.route("/api/set_dag/<string:job_id>")
def set_dag_for_job(job_id, methods=["POST"]):
    pass

@app.route("/api/complete_task/<string:job_id>/<string:task_id>", methods=["POST"])
def complete_task(job_id, task_id):
    pass


