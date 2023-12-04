import json
import multiprocessing
import traceback
from datetime import datetime

import sentry_sdk
from sentry_sdk import capture_exception, capture_message

from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger
from elasticsearch import Elasticsearch
from utils import heconstants
import firebase_admin
from concurrent.futures import ThreadPoolExecutor
from executors.worker.file_downloader_executor import fileDownloader

kafka_service = KafkaService()
logger = get_logger()
num_of_workers = lambda: (multiprocessing.cpu_count() * 2) + 1
executor = ThreadPoolExecutor(max_workers=num_of_workers())
kafka_client = kafka_service.create_clients()


class Executor:
    def __init__(self):
        try:
            if heconstants.es_user is not None and heconstants.es_pass is not None and len(heconstants.es_user) > 0 and \
                    len(heconstants.es_pass) > 0:
                heconstants.es_client = Elasticsearch(heconstants.es_host, verify_certs=True, use_ssl=False,
                                                      send_get_body_as="POST", timeout=60,
                                                      max_retries=1,
                                                      http_auth=(heconstants.es_user, heconstants.es_pass))
            else:
                heconstants.es_client = Elasticsearch(heconstants.es_host, send_get_body_as="POST", timeout=60,
                                                      max_retries=1)

            logger.info("ES is up and running.")

        except Exception as exc:
            msg = "ES startup failed :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            return msg, 500

    def executor_task(self):
        try:
            while True:
                kafka_service.post_poll()
                for consumer in kafka_service.post_consumer:
                    if consumer.value.decode('utf-8') != '':
                        if consumer.topic == heconstants.EXECUTOR_TOPIC:
                            message_to_pass = consumer.value.decode('utf-8')
                            # kafka_client.commit()
                            start_time = datetime.utcnow()
                            message_dict = json.loads(message_to_pass)
                            if message_dict.get("state") == "Init":
                                stream_key = message_dict.get("care_req_id")
                                user_type = message_dict.get("user_type")
                                filedownloader = fileDownloader()
                                logger.info(f"Starting Downloading File :: {stream_key}")
                                executor.submit(filedownloader.save_rtmp_loop, stream_key, user_type, start_time)

        except Exception as exc:
            msg = "post message polling failed :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            return msg, 500


if __name__ == "__main__":
    ExecutorInstance = Executor()
    ExecutorInstance.executor_task()
