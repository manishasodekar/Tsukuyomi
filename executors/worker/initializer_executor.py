import io
import logging
import os
import traceback
from datetime import datetime
import time
import requests
from utils import heconstants
from utils.s3_operation import S3SERVICE
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger

s3 = S3SERVICE()
producer = KafkaService(group_id="initializer")
logger = get_logger()
logger.setLevel(logging.INFO)


class initializer:
    def __init__(self):
        pass

    def execute_function(self):
        pass
