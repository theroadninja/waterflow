"""
Provides a flask interface for the service.
"""
import dataclasses
from flask import Flask, current_app, request, Response
import json
import logging
from waterflow import get_connection_pool_from_file, service_methods
from waterflow.dao import DagDao
from .flask_adapter import read_pending_job, work_item_to_response, read_dag_from_request
import mysql

app = Flask("Waterflow Flask")


def before_first_request(app, mysql_config_file, pool_name, dbname, pool_size=32):

    app.config["mysql_config_file"] = mysql_config_file
    app.config["mysql_pool_name"] = pool_name
    app.config["dbname"] = dbname
    app.config["mysql_pool_size"] = pool_size

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
        print("*\n*\n*\nCREATING NEW CONNECTION POOL*\n*\n*\n")
        global_db_pool = get_connection_pool_from_file(
            app.config["mysql_config_file"],
            app.config["mysql_pool_name"],
            pool_size=app.config["mysql_pool_size"],
        )
    return global_db_pool


def get_dao(app):
    return DagDao(get_connection_pool(app), app.config["dbname"])
    # Doesnt work:
    # with app.app_context():
    #     return DagDao(g.mysql_conn_pool, g.dbname)


@app.route("/")
def something():
    return "Waterflow Flask Server"


@app.route("/ui/stats/jobs", methods=["GET"], strict_slashes=False)  # TODO TODO TODO needs to properly return 429s
def get_job_stats():
    job_stats = get_dao(current_app).get_job_stats()
    # just being lazy; will need a real transform method if we change the internal class:
    return json.dumps(dataclasses.asdict(job_stats))

@app.route("/ui/stats/tasks", methods=["GET"], strict_slashes=False)
def get_task_stats():
    task_stats = get_dao(current_app).get_task_stats()
    # just being lazy; will need a real transform method if we change the internal class:
    return json.dumps(dataclasses.asdict(task_stats))


@app.route("/api/submit_job/<int:work_queue>", methods=["POST"])  # TODO /api/ to distinguish from /ui/ methods
def submit_job(work_queue):
    """
    Returns an empty work item if there is no work.
    """
    job = read_pending_job(work_queue, request.get_json())
    job_id = service_methods.submit_job(get_dao(current_app), job)
    return json.dumps({"job_id": job_id})


@app.route("/api/get_work/<int:work_queue>/<string:worker>", methods=["GET"])
def get_work_item(work_queue, worker):
    logger = logging.getLogger("server")
    logger.info("received get_work call")
    try:
        work_item = service_methods.get_work_item(get_dao(current_app), work_queue, worker)
        return json.dumps(work_item_to_response(work_item))
    except mysql.connector.errors.PoolError as ex:
        logger.exception(str(ex))
        return Response(str(ex), status=429, mimetype="application/text")


# NOTE:  a 400 error could be caused by invalid json
@app.route("/api/set_dag/<int:work_queue>/<string:job_id>", methods=["POST"])  # TODO need to decide soon if work queues are ints or strings on the API
def set_dag_for_job(work_queue, job_id):
    logger = logging.getLogger("server")
    logger.info(f"received set_dag request for {job_id}")
    dag = read_dag_from_request(request.get_json())
    logger.info("make it past get_json()")
    try:
        service_methods.set_dag_for_job_REST(get_dao(current_app), job_id, dag, work_queue)
        logger.info("returned from set_dag_for_job_REST()")
        return "{}"  # need to return someting?
    except mysql.connector.errors.PoolError as ex:
        logger.exception(str(ex))
        return Response(str(ex), status=429, mimetype="application/text")


@app.route("/api/complete_task/<string:job_id>/<string:task_id>", methods=["POST"])
def complete_task(job_id, task_id):
    # NOTE:  this is also for "force-complete"
    try:
        service_methods.complete_task(get_dao(current_app), job_id, task_id)
        return "{}"
    except mysql.connector.errors.PoolError as ex:
        logger = logging.getLogger("server")
        logger.exception(str(ex))
        return Response(str(ex), status=429, mimetype="application/text")


