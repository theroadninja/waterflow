"""
Flask Service Implementation of a Fake "Business Logic" Service
"""
from flask import Flask, request

from waterflow.mocks import sample_dags_http

app = Flask("Waterflow Flask")

@app.route("/v1/ingest/get_dag/<string:job_id>", methods=["GET"])
def get_dag(job_id):
    """
    Returns the "DAG" for a job.

    This is a fake service so we are simply returning a hardcoded dag.
    """
    dag = sample_dags_http.make_diamond10()
    return dag.to_json()


@app.route("/v1/ingest/run_task/", methods=["POST"])
def run_task():
    pass