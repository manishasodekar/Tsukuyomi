import logging
import os
import json
import boto3
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch

os.environ["ENVIRONMENT"] = "dev"
os.environ["AWS_ACCESS_KEY"] = "AKIA2WUJAEHPOWTHM6HX"
os.environ["AWS_SECRET_ACCESS_KEY"] = "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"
AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
env = os.environ["ENVIRONMENT"]

logger = logging.getLogger("heconstants")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

def get_secret():
    secret_name = f"{env}/healiom"
    region_name = "us-east-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as exc:
        raise exc

    secret = get_secret_value_response['SecretString']
    return secret


es_client = None

faster_clinical_info_extraction_functions = [
    {
        "name": "ClinicalInformation",
        "description": "print , separated clinical entities extracted from given text.",
        "parameters": {
            "type": "object",
            "properties": {
                "medications": {
                    "description": ", separated medication names",
                    "type": "string",
                },
                "symptoms": {
                    "description": ", separated symptom names",
                    "type": "string",
                },
                "diseases": {
                    "description": ", separated disease names",
                    "type": "string",
                },
                "diagnoses": {
                    "description": ", sepatated diagnosis names",
                    "type": "string",
                },
                "surgeries": {
                    "description": ", separated surgery names",
                    "type": "string",
                },
                "tests": {
                    "description": ", separated physical, clinical tests, imaging names",
                    "type": "string",
                },
                "details": {
                    "description": "age, gender, height, weight, bmi, ethnicity, substanceAbuse, insurance, physicalActivityExercise, bloodPressure, pulse, respiratoryRate, bodyTemperature",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "enum": [
                                    "age",
                                    "gender",
                                    "height",
                                    "weight",
                                    "bmi",
                                    "ethnicity",
                                    "substanceAbuse",
                                    "insurance",
                                    "physicalActivityExercise",
                                    "bloodPressure",
                                    "pulse",
                                    "respiratoryRate",
                                    "bodyTemperature",
                                ],
                                "type": "string",
                            },
                            "value": {"type": "string"},
                        },
                    },
                },
            },
        },
    }
]

clinical_summary_functions = [
    {
        "name": "ClinicalSummaries",
        "description": "write clinical summaries for given transcript",
        "parameters": {
            "type": "object",
            "properties": {
                "subjectiveSummary": {
                    "description": "Crisp summary of medication history, surgery history and symptoms.",
                    "type": "string",
                },
                "objectiveSummary": {
                    "description": "Crisp summary of results of imaging, physical exam, visual auditory physical touch signs, lab results",
                    "type": "string",
                },
                "clinicalAssessmentSummary": {
                    "description": "Crisp summary of diagnosis, reason for diagnosis, perspective on historical diagnosis management",
                    "type": "string",
                },
                "carePlanSummary": {
                    "description": "Crisp summary of medications prescribed, tests ordered, procedures ordered, surgeries ordered, treatement plan",
                    "type": "string",
                },
            },
        },
    }
]


secret_values = json.loads(get_secret())
log_name = secret_values.get("LOGGER_NAME")
log_level = secret_values.get("LOGGING_LEVEL")

es_request_timeout = 60
es_timeout = "3m"
es_type = "_doc"

AI_SERVER = secret_values.get("AI_SERVER")
HEALIOM_SERVER = secret_values.get("HEALIOM_SERVER")
transcript_index = secret_values.get("TRANSCRIPTION_META_INDEX")
websocket_logs_index = secret_values.get("WEBSOCKET_LOGS_INDEX")
RTMP_SERVER_URL = secret_values.get("RTMP_SERVER_URL")
OPENAI_APIKEY = secret_values.get("OPENAI_APIKEY")
API_KEY = "test_key"
GPT_MODELS = ["gpt-3.5-turbo-0613", "gpt-3.5-turbo-16k-0613", "gpt-4-0613"]
EXECUTOR_TOPIC = secret_values.get("EXECUTOR_TOPIC")
EXECUTOR_TOPIC = "exe-queue"
ASR_BUCKET = secret_values.get("ASR_BUCKET")
SYNC_SERVER = secret_values.get("SYNC_SERVER")
BOOTSTRAP_SERVERS = secret_values.get("BOOTSTRAP_SERVERS")
GROUP_ID = secret_values.get('GROUP_ID')
CONSUMER_POLL_TIMEOUT = secret_values.get('CONSUMER_POLL_TIMEOUT')
RECONNECT_BACKOFF_MS = secret_values.get('RECONNECT_BACKOFF_MS')
RETRY_BACKOFF_MS = secret_values.get('RETRY_BACKOFF_MS')
HEARTBEAT_INTERVAL_MS = secret_values.get('HEARTBEAT_INTERVAL_MS')
SESSION_TIMEOUT_MS = secret_values.get('SESSION_TIMEOUT_MS')
KAFKA_SLEEP_TIME = secret_values.get('KAFKA_SLEEP_TIME')
EXECUTOR_LOGGER_NAME = secret_values.get('EXECUTOR_LOGGER_NAME')
TIME_IN_SEC = secret_values.get('TIME_IN_SEC')
MAX_TIME = secret_values.get('MAX_TIME')
quick_loop_chunk_duration = int(secret_values.get('QUICK_LOOP_CHUNK_DURATION'))
chunk_duration = int(secret_values.get('CHUNK_DURATION'))
es_host = secret_values.get('ES_HOST')
es_user = secret_values.get('ES_USER')
es_pass = secret_values.get('ES_PASS')

