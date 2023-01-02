import logging
from waterflow import get_connection_pool_from_file
from waterflow.flask.flask_service import app, before_first_request


if __name__ == "__main__":
    import inspect
    import os
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("server")
    logger.info("Starting Flask Waterflow Server")

    MYPATH = os.path.dirname(os.path.abspath(inspect.stack()[0][1]))
    FILENAME = f"{MYPATH}/../../local/mysqlconfig.json"  # TODO get rid of hardcoded path
    DBNAME = "waterflow"  # TODO should come from config

    before_first_request(app, FILENAME, "flask_pool", DBNAME)

    # TODO in prod flask docs say we need a WSGI server...
    app.run(threaded=True, host="0.0.0.0", port=80)