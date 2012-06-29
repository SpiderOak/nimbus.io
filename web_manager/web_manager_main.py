# -*- coding: utf-8 -*-
"""
web_manager.py

web manager
"""
import logging
import os

from tools.standard_logging import initialize_logging

from flask import Flask
app = Flask(__name__)

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_web_manager_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_management_api_dest = os.environ["NIMBUSIO_MANAGEMENT_API_REQUEST_DEST"]

@app.route("/")
def hello():
    return "Hello World!"

if __name__ == "__main__":
    initialize_logging(_log_path)
    log = logging.getLogger("__main__")
    log.info("program starts")
    host, port_str = _management_api_dest.split(":")
    try:
        app.run(host=host, port=int(port_str))
    except Exception:
        log.exception("app.run")
    log.info("program terminates")

