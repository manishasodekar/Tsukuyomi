import json
from datetime import datetime
import sys
from services.kafka.kafka_service import KafkaService


def post_message(stream_key, start_time):
    data = {
        "es_id": f"{stream_key}_FILE_DOWNLOADER",
        "api_path": "asr",
        "file_path": None,
        "api_type": "asr",
        "req_type": "encounter",
        "user_type": "Provider",
        "executor_name": "FILE_DOWNLOADER",
        "state": "Init",
        "retry_count": None,
        "uid": None,
        "request_id": stream_key,
        "care_req_id": stream_key,
        "encounter_id": None,
        "provider_id": None,
        "review_provider_id": None,
        "completed": False,
        "exec_duration": 0.0,
        "start_time": start_time,
        "end_time": str(datetime.utcnow()),
    }

    # data = {
    #     "es_id": f"{stream_key}_ASR_EXECUTOR",
    #     "file_path": f"{stream_key}/{stream_key}_chunk1.wav",
    #     "api_path": "asr",
    #     "api_type": "asr",
    #     "req_type": "encounter",
    #     "executor_name": "ASR_EXECUTOR",
    #     "state": "SpeechToText",
    #     "retry_count": None,
    #     "uid": None,
    #     "request_id": stream_key,
    #     "care_req_id": stream_key,
    #     "encounter_id": None,
    #     "provider_id": None,
    #     "review_provider_id": None,
    #     "completed": False,
    #     "exec_duration": 0.0,
    #     "start_time": str(start_time),
    #     "end_time": str(datetime.utcnow()),
    # }

    # data = {
    #     "es_id": f"{stream_key}_AI_PRED",
    #     "chunk_no": 2,
    #     "file_path": f"{stream_key}/{stream_key}_chunk2.wav",
    #     "api_path": "asr",
    #     "api_type": "asr",
    #     "req_type": "encounter",
    #     "executor_name": "AI_PRED",
    #     "state": "AiPred",
    #     "retry_count": None,
    #     "uid": None,
    #     "request_id": stream_key,
    #     "care_req_id": stream_key,
    #     "encounter_id": None,
    #     "provider_id": None,
    #     "review_provider_id": None,
    #     "completed": False,
    #     "exec_duration": 0.0,
    #     "start_time": str(start_time),
    #     "end_time": str(datetime.utcnow()),
    # }

    # data = {
    #     "es_id": f"{stream_key}_SOAP",
    #     "chunk_no": 42,
    #     "file_path": f"{stream_key}/{stream_key}_chunk42.wav",
    #     "api_path": "asr",
    #     "api_type": "asr",
    #     "req_type": "encounter",
    #     "executor_name": "SOAP_EXECUTOR",
    #     "state": "Analytics",
    #     "retry_count": None,
    #     "uid": None,
    #     "request_id": stream_key,
    #     "care_req_id": stream_key,
    #     "encounter_id": None,
    #     "provider_id": None,
    #     "review_provider_id": None,
    #     "completed": False,
    #     "exec_duration": 0.0,
    #     "start_time": str(start_time),
    #     "end_time": str(datetime.utcnow()),
    # }

    KafkaService(group_id="asr").publish_executor_message(data)
    print("posted")


# Example usage
stream_key = sys.argv[1]
start_time = str(datetime.utcnow())
post_message(stream_key, start_time)
