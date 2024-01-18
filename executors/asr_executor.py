import json
import logging
import multiprocessing
import traceback
from datetime import datetime
from services.kafka.kafka_service import KafkaService
from utils import heconstants
from concurrent.futures import ThreadPoolExecutor
from executors.worker.asr_executor import ASRExecutor
from config.logconfig import get_logger

logger = get_logger()
num_of_workers = lambda: (multiprocessing.cpu_count() * 2) + 1
executor = ThreadPoolExecutor(max_workers=num_of_workers())
kafka_service = KafkaService(group_id="asr")
kafka_client = kafka_service.create_clients(group_id="asr")


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
                            # kafka_client.commit()
                            start_time = datetime.utcnow()
                            message_dict = json.loads(message_to_pass)
                            if message_dict.get("state") == "SpeechToText" and not message_dict.get("completed"):
                                if message_dict.get("req_type") == "encounter":
                                    asrexecutor = ASRExecutor()
                                    stream_key = message_dict.get("care_req_id")
                                    file_path = message_dict.get("file_path")
                                    logger.info(f"Starting ASR  :: {stream_key} :: {file_path}")
                                    executor.submit(asrexecutor.execute_function, message_dict, start_time)
                                else:
                                    asrexecutor = ASRExecutor()
                                    request_id = message_dict.get("request_id")
                                    file_path = message_dict.get("file_path")
                                    logger.info(f"Starting ASR for platform  :: {request_id} :: {file_path}")
                                    executor.submit(asrexecutor.speechToText, message_dict, start_time)

        except Exception as exc:
            msg = "post message polling failed :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            return msg, 500


if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ExecutorInstance = Executor()
    ExecutorInstance.executor_task()
