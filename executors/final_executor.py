import json
import multiprocessing
import traceback
from datetime import datetime

from executors.worker.final_executor import finalExecutor
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger
from utils import heconstants
from concurrent.futures import ThreadPoolExecutor

logger = get_logger()
num_of_workers = lambda: (multiprocessing.cpu_count() * 2) + 1
executor = ThreadPoolExecutor(max_workers=num_of_workers())
kafka_service = KafkaService(group_id="final")
kafka_client = kafka_service.create_clients(group_id="final")


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
                            if message_dict.get("state") == "Final" and not message_dict.get("completed"):
                                file_path = message_dict.get("file_path")
                                request_id = message_dict.get("request_id")
                                logger.info(f"Merging Transcription and Ai_Preds :: {request_id} :: {file_path}")
                                final_executor = finalExecutor()
                                executor.submit(final_executor.get_merge_ai_preds, message_dict, start_time)

        except Exception as exc:
            msg = "post message polling failed :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            return msg, 500


if __name__ == "__main__":
    ExecutorInstance = Executor()
    ExecutorInstance.executor_task()
