import logging
import sys

from utils import he_constants

logger = logging.getLogger(he_constants.JINCHURIKI_LOGGER_NAME)

if he_constants.LOG_LEVEL == "DEBUG":
    logger.setLevel(logging.DEBUG)
    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - [%(filename)s:%(lineno)d - %(funcName)10s()] - %(levelname)s - %(message)s")
else:
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - [%(filename)s:%(lineno)d - %(funcName)10s()] - %(levelname)s - %(message)s")

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)


def get_logger():
    return logger
