import json
import multiprocessing
import traceback
from datetime import datetime
from executors.worker.soap_executor import soap
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger
from utils import heconstants
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

logger = get_logger()
num_of_workers = lambda: (multiprocessing.cpu_count() * 2) + 1
executor = ThreadPoolExecutor(max_workers=num_of_workers())
kafka_service = KafkaService(group_id="soap")
kafka_client = kafka_service.create_clients(group_id="soap")


class Executor:
    def __init__(self):
        pass

    def executor_task(self):
        try:
            futures = []  # To store the submitted futures
            while True:
                kafka_service.post_poll()
                for consumer in kafka_service.post_consumer:
                    if consumer.value.decode('utf-8') != '':
                        if consumer.topic == heconstants.EXECUTOR_TOPIC:
                            message_to_pass = consumer.value.decode('utf-8')
                            kafka_client.commit()
                            start_time = datetime.utcnow()
                            message_dict = json.loads(message_to_pass)
                            if message_dict.get("state") == "Analytics" and not message_dict.get("completed"):
                                stream_key = None
                                file_path = message_dict.get("file_path")

                                if message_dict.get("req_type") == "encounter":
                                    stream_key = message_dict.get("care_req_id")
                                    logger.info(f"Starting SOAP :: {stream_key} :: {file_path}")
                                else:
                                    stream_key = message_dict.get("request_id")
                                    logger.info(f"Starting SOAP for platform:: {stream_key} :: {file_path}")
                                    output_language = message_dict.get("output_language")
                                    logger.info(f"Output language :: {output_language}")

                                summary = soap()
                                executor.submit(summary.get_summary, message_dict, start_time)

                                # segments, last_ai_preds = summary.get_merge_ai_preds(conversation_id=stream_key,
                                #                                                      message=message_dict)
                                # futures = [
                                #     executor.submit(summary.get_subjective_summary, message_dict, start_time, segments,
                                #                     last_ai_preds
                                #                     ),
                                #     executor.submit(summary.get_objective_summary, message_dict, start_time, segments,
                                #                     last_ai_preds),
                                #     executor.submit(summary.get_clinical_assessment_summary, message_dict, start_time,
                                #                     segments,
                                #                     last_ai_preds),
                                #     executor.submit(summary.get_care_plan_summary, message_dict, start_time, segments,
                                #                     last_ai_preds)]
                                #
                                # # Wait for all futures to complete for this message
                                # for future in as_completed(futures):
                                #     result = future.result()
                                # summary.create_delivery_task(message=message_dict)

        except Exception as exc:
            msg = "post message polling failed :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            return msg, 500


if __name__ == "__main__":
    ExecutorInstance = Executor()
    ExecutorInstance.executor_task()
