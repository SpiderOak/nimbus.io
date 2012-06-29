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

@app.route("/")
def hello():
    return "Hello World!"

if __name__ == "__main__":
    initialize_logging(_log_path)
    log = logging.getLogger("__main__")
    log.info("program starts")
    try:
        app.run()
    except Exception:
        log.exception("app.run")
    log.info("program terminates")

