import logging
from waterflow.flask.flask_service import app, before_first_request


if __name__ == "__main__":
    import inspect
    import os
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("server")
    logger.info("Starting Flask Waterflow Server")

    MYPATH = os.path.dirname(os.path.abspath(inspect.stack()[0][1]))
    FILENAME = f"{MYPATH}/../../local/mysqlconfig.json"
    DBNAME = "waterflow"

    before_first_request(app, FILENAME, "flask_pool", DBNAME, pool_size=32)  # warning:  cant make it higher than 32

    app.run(threaded=True, host="0.0.0.0", port=80)