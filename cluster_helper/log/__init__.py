"""Utility functionality for logging.
"""
import os
import logging
from cluster_helper import utils

LOG_NAME = "cluster_helper"

logger = logging.getLogger(LOG_NAME)

def setup_logging(log_dir):
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        formatter = logging.Formatter('[%(asctime)s] %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        if log_dir:
            logfile = os.path.join(utils.safe_makedir(log_dir),
                                   "{0}.log".format(LOG_NAME))
            handler = logging.FileHandler(logfile)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
