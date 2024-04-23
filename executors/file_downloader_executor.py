import json
import multiprocessing
import traceback
from datetime import datetime
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger
from utils import heconstants
from concurrent.futures import ThreadPoolExecutor
from executors.worker.file_downloader_executor import fileDownloader

logger = get_logger()
num_of_workers = lambda: (multiprocessing.cpu_count() * 2) + 1
executor = ThreadPoolExecutor(max_workers=num_of_workers())
kafka_service = KafkaService(group_id="filedownloader")
kafka_client = kafka_service.create_clients(group_id="filedownloader")


class Executor:
    def __init__(self):
        pass

    def executor_task(self):
        try:
            while True:
                kafka_service.post_poll()
                for consumer in kafka_service.post_consumer:
                    if consumer.value.decode('utf-8') != '':
                        if consumer.topic == heconstants.EXECUTOR_TOPIC:
                            message_to_pass = consumer.value.decode('utf-8')
                            kafka_client.commit()
                            start_time = datetime.utcnow()
                            message_dict = json.loads(message_to_pass)
                            if message_dict.get("state") == "Init":
                                if message_dict.get("req_type") == "encounter":
                                    stream_key = message_dict.get("care_req_id")
                                    filedownloader = fileDownloader()
                                    logger.info(f"Starting RTMP saver loop :: {stream_key}")
                                    executor.submit(filedownloader.save_rtmp_loop, message_dict, start_time)
                                else:
                                    request_id = message_dict.get("request_id")
                                    filedownloader = fileDownloader()
                                    logger.info(f"Downloading Audio File :: {request_id}")
                                    output_language = message_dict.get("output_language")
                                    logger.info(f"Output language :: {output_language}")
                                    executor.submit(filedownloader.download_file, message_dict, start_time)

        except Exception as exc:
            msg = "post message polling failed :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            return msg, 500


if __name__ == "__main__":
    ExecutorInstance = Executor()
    ExecutorInstance.executor_task()
