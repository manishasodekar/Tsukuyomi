import json
import logging
import time
import traceback

from kafka import KafkaConsumer, KafkaProducer
from config.logconfig import get_logger
import multiprocessing
from utils import heconstants

logger = get_logger()
# logger = logging.getLogger("Kafka")
# logger.setLevel(logging.INFO)

max_poll_records = (multiprocessing.cpu_count() * 2) + 1


class KafkaService:
    def __init__(self, group_id: str):
        self.post_consumer = self.create_clients(group_id)
        self.producer = KafkaProducer(bootstrap_servers=heconstants.BOOTSTRAP_SERVERS,
                                      value_serializer=lambda x: x.encode('utf-8'))

    def create_clients(self, group_id: str):
        kafka_ping = False
        while kafka_ping == False:
            try:
                consumer_post_message = KafkaConsumer(heconstants.EXECUTOR_TOPIC,
                                                      bootstrap_servers=heconstants.BOOTSTRAP_SERVERS,
                                                      group_id=group_id,
                                                      enable_auto_commit=False,
                                                      reconnect_backoff_ms=int(heconstants.RECONNECT_BACKOFF_MS),
                                                      retry_backoff_ms=int(heconstants.RETRY_BACKOFF_MS),
                                                      heartbeat_interval_ms=int(heconstants.HEARTBEAT_INTERVAL_MS),
                                                      max_poll_records=int(max_poll_records),
                                                      session_timeout_ms=int(heconstants.SESSION_TIMEOUT_MS))

                topic = consumer_post_message.topics()
                if not topic:
                    logger.info("Rechecking the topics")
                    time.sleep(int(heconstants.KAFKA_SLEEP_TIME))
                    self.create_clients()
                else:
                    logger.info(f"{consumer_post_message} Health check completed. Connection successful")
                    kafka_ping = True
                return consumer_post_message
            except Exception as exc:
                msg = "create consumer failed :: {}".format(exc)
                logger.error(msg)
                trace = traceback.format_exc()
                # SentryUtilFunctions().send_event(exc, trace)
                time.sleep(int(heconstants.KAFKA_SLEEP_TIME))
                return msg, 500

    def post_poll(self):
        try:
            self.post_consumer.poll(timeout_ms=int(heconstants.CONSUMER_POLL_TIMEOUT))
            logger.info("Executor polling started")

        except Exception as exc:
            msg = "Executor polling failed :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            # SentryUtilFunctions().send_event(exc, trace)
            return msg, 500

    def publish_executor_message(self, data):
        try:
            self.producer.send(heconstants.EXECUTOR_TOPIC, value=json.dumps(data))
            logger.info("Message sent")
        except Exception as exc:
            msg = "producer failed to push message in {} :: {}".format(heconstants.EXECUTOR_TOPIC, exc)
            logger.error(msg)
            trace = traceback.format_exc()
            # SentryUtilFunctions().send_event(exc, trace)
            return msg, 500


# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
