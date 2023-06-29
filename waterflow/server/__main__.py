import logging

from waterflow.core import resolve_local_path
from waterflow.server.flask_service import app, before_first_request


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("server")
    logger.info("Starting Flask Waterflow Server")

    FILENAME = resolve_local_path("mysqlconfig.json")
    DBNAME = "waterflow"
    before_first_request(app, FILENAME, "flask_pool", DBNAME, pool_size=32)

    app.run(threaded=True, host="0.0.0.0", port=80, debug=True)
