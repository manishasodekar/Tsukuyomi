from gevent import monkey

monkey.patch_all()
import os
import uuid
import falcon
import traceback
import logging
from utils.s3_operation import S3SERVICE
from typing import Optional
from utils import heconstants
from datetime import datetime
from config.logconfig import get_logger
from typing import Optional
from services.kafka.kafka_service import KafkaService

logger = get_logger()
logger.setLevel(logging.INFO)
s3 = S3SERVICE()
producer = KafkaService(group_id="sync")


def generate_request_id():
    # Generate a unique request ID
    return str(uuid.uuid4())


def get_merge_ai_preds(conversation_id, only_transcribe: Optional[bool] = False):
    try:
        if only_transcribe:
            logger.info("Only transcription being fetched")
        else:
            logger.info("Ai preds and transcription being fetched")

        merged_segments = []
        merged_ai_preds = {
            "age": {"text": None, "value": None, "unit": None},
            "gender": {"text": None, "value": None, "unit": None},
            "height": {"text": None, "value": None, "unit": None},
            "weight": {"text": None, "value": None, "unit": None},
            "bmi": {"text": None, "value": None, "unit": None},
            "ethnicity": {"text": None, "value": None, "unit": None},
            "insurance": {"text": None, "value": None, "unit": None},
            "physicalActivityExercise": {"text": None, "value": None, "unit": None},
            "bloodPressure": {"text": None, "value": None, "unit": None},
            "pulse": {"text": None, "value": None, "unit": None},
            "respiratoryRate": {"text": None, "value": None, "unit": None},
            "bodyTemperature": {"text": None, "value": None, "unit": None},
            "substanceAbuse": {"text": None, "value": None, "unit": None},
            "entities": {
                "medications": [],
                "symptoms": [],
                "diseases": [],
                "diagnoses": [],
                "surgeries": [],
                "tests": [],
            },
            "summaries": {
                "subjectiveClinicalSummary": [],
                "objectiveClinicalSummary": [],
                "clinicalAssessment": [],
                "carePlanSuggested": [],
            },
        }
        response_json = {}
        ai_preds_file_path = f"{conversation_id}/ai_preds.json"

        conversation_datas = s3.get_files_matching_pattern(
            pattern=f"{conversation_id}/{conversation_id}_*json")

        if conversation_datas:
            audio_metas = []
            for conversation_data in conversation_datas:
                merged_segments += conversation_data["segments"]
                audio_metas.append(
                    {
                        "audio_path": "../" + conversation_data["audio_path"].lstrip("."),
                        "duration": conversation_data["duration"],
                        "received_at": conversation_data["received_at"],
                    }
                )

                response_json["segments"] = merged_segments

            if not only_transcribe:
                if s3.check_file_exists(ai_preds_file_path):
                    merged_ai_preds = s3.get_json_file(ai_preds_file_path)
                    for summary_type in ["subjectiveClinicalSummary", "objectiveClinicalSummary", "clinicalAssessment",
                                         "carePlanSuggested"]:
                        summary_file = f"{conversation_id}/{summary_type}.json"
                        if s3.check_file_exists(summary_file):
                            summary_content = s3.get_json_file(s3_filename=summary_file)
                            if summary_content:
                                merged_ai_preds["summaries"][summary_type] = summary_content

                    response_json["ai_preds"] = merged_ai_preds

            response_json["meta"] = audio_metas
            response_json["success"] = True

            if merged_segments:
                response_json["transcript"] = " ".join([_["text"] for _ in merged_segments])

            return response_json

        return {"success": False,
                "text": "Transcription not found"}

    except Exception as exc:
        msg = "Failed to merge ai preds :: {}".format(exc)
        trace = traceback.format_exc()
        logger.error(msg, trace)


def create_task(request_id, webhook_url, audio_url, api_type):
    es_id = f"{request_id}_FILE_DOWNLOADER"
    executor_name = "FILE_DOWNLOADER"
    state = "Init"

    data = {
        "es_id": es_id,
        "webhook_url": webhook_url,
        "file_path": audio_url,
        "api_path": f"/{api_type}",
        "api_type": api_type,
        "req_type": "platform",
        "user_type": "Provider",
        "executor_name": executor_name,
        "state": state,
        "retry_count": 0,
        "uid": None,
        "request_id": request_id,
        "care_req_id": request_id,
        "encounter_id": None,
        "provider_id": None,
        "review_provider_id": None,
        "completed": False,
        "exec_duration": 0.0,
        "start_time": str(datetime.utcnow()),
        "end_time": str(datetime.utcnow()),
    }
    producer.publish_executor_message(data)

    return {
        "success": True,
        "request_id": request_id
    }


def create_aipred_task(request_id, webhook_url, text, api_type):
    if api_type == "ai_pred":
        es_id = f"{request_id}_AI_PRED"
        executor_name = "AI_PRED"
        state = "AiPred"
    elif api_type == "soap":
        es_id = f"{request_id}_SOAP"
        executor_name = "SOAP_EXECUTOR"
        state = "AiPred"
    file_key = f"{request_id}/{request_id}_input.json"
    transcript = {"transcript": text}
    s3.upload_to_s3(s3_filename=file_key, data=transcript, is_json=True)
    data = {
        "es_id": es_id,
        "webhook_url": webhook_url,
        "file_path": file_key,
        "api_path": f"/{api_type}",
        "api_type": api_type,
        "req_type": "platform",
        "user_type": "Provider",
        "executor_name": executor_name,
        "state": state,
        "retry_count": 0,
        "uid": None,
        "request_id": request_id,
        "care_req_id": request_id,
        "encounter_id": None,
        "provider_id": None,
        "review_provider_id": None,
        "completed": False,
        "exec_duration": 0.0,
        "start_time": str(datetime.utcnow()),
        "end_time": str(datetime.utcnow()),
    }
    producer.publish_executor_message(data)

    return {
        "success": True,
        "request_id": request_id
    }


class History(object):
    def on_get(self, req, resp):
        conversation_id = req.params.get("conversation_id")
        only_transcribe = req.params.get("only_transcribe")

        if not conversation_id:
            self.logger.error("Bad Request with missing conversation_id")
            raise falcon.HTTPError(status=400, description="Bad Request with missing conversation_id parameter")
        resp.media = get_merge_ai_preds(
            conversation_id, only_transcribe
        )


class clinicalNotes(object):
    def on_post(self, req, resp):
        webhook_url = req.params.get("webhook_url")
        data = req.media
        audio_url = data.get("audio_url")
        if audio_url is None:
            self.logger.error("Audio url is missing.")
            raise falcon.HTTPError(status=400, description="Audio url is missing.")
        request_id = generate_request_id()
        resp.set_header('Request_ID', request_id)
        resp.set_header('WEBHOOK_URL', webhook_url)
        resp.media = create_task(request_id, webhook_url, audio_url, api_type="clinical_notes")


class Transcription(object):
    def on_post(self, req, resp):
        webhook_url = req.params.get("webhook_url")
        data = req.media
        audio_url = data.get("audio_url")
        if audio_url is None:
            self.logger.error("Audio url is missing.")
            raise falcon.HTTPError(status=400, description="Audio url is missing.")
        request_id = generate_request_id()
        resp.set_header('Request-ID', request_id)
        resp.set_header('WEBHOOK-URL', webhook_url)
        resp.media = create_task(request_id, webhook_url, audio_url, api_type="transcription")


class AiPred(object):
    def on_post(self, req, resp):
        webhook_url = req.params.get("webhook_url")
        data = req.media
        text = data.get("text")
        if text is None:
            self.logger.error("text input is missing.")
            raise falcon.HTTPError(status=400, description="text input is missing.")
        request_id = generate_request_id()
        resp.set_header('Request-ID', request_id)
        resp.set_header('WEBHOOK-URL', webhook_url)
        resp.media = create_aipred_task(request_id, webhook_url, text, api_type="ai_pred")


class Summary(object):
    def on_post(self, req, resp):
        webhook_url = req.params.get("webhook_url")
        data = req.media
        text = data.get("text")
        if text is None:
            self.logger.error("text input is missing.")
            raise falcon.HTTPError(status=400, description="text input is missing.")
        request_id = generate_request_id()
        resp.set_header('Request-ID', request_id)
        resp.set_header('WEBHOOK-URL', webhook_url)
        resp.media = create_aipred_task(request_id, webhook_url, text, api_type="soap")


class Status(object):
    def on_get(self, req, resp):
        request_id = req.params.get("request_id")
        file_path = f"{request_id}/All_Preds.json"
        if s3.check_file_exists(file_path):
            merged_ai_preds = s3.get_json_file(file_path)
            resp.set_header('Request-ID', request_id)
            resp.media = merged_ai_preds
        else:
            resp.media = {"success": False,
                          "request_id": request_id,
                          "issue": [{
                              "error-code": "HE-101",
                              "message": "Failed to process the request."
                          }]
                          }


class Home(object):
    def on_get(self, req, resp):
        resp.text = "Welcome to API SERVER"


app = falcon.App(cors_enable=True)
app.req_options.auto_parse_form_urlencoded = True
app = falcon.App(
    middleware=falcon.CORSMiddleware(allow_origins="*", allow_credentials="*")
)

home = Home()
history_api = History()
clinical_notes_api = clinicalNotes()
transcription_api = Transcription()
ai_pred_api = AiPred()
soap_api = Summary()
status = Status()
app.add_route("/home", home)
app.add_route("/history", history_api)
app.add_route("/clinical_notes", clinical_notes_api)
app.add_route("/transcription", transcription_api)
app.add_route("/ai_pred", ai_pred_api)
app.add_route("/soap", soap_api)
app.add_route("/status", status)

if __name__ == "__main__":
    import gunicorn.app.base


    class StandaloneApplication(gunicorn.app.base.BaseApplication):
        def __init__(self, app, options=None, es_client: Optional = None):
            self.options = options or {}
            if es_client:
                self._es_client = es_client
                heconstants.es_client = self._es_client
            self.application = app
            super().__init__()

        def load_config(self):
            config = {
                key: value
                for key, value in self.options.items()
                if key in self.cfg.settings and value is not None
            }
            for key, value in config.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application


    port = int(os.getenv("PORT", "8080"))
    host = os.getenv("HOST", "0.0.0.0")

    options = {
        "preload": "",
        "bind": "%s:%s" % (host, port),
        "workers": 2,
        "worker_connections": 1000,
        "worker_class": "gevent",
        "timeout": 300,
        "allow_redirects": True,
        "limit_request_line": 0,
    }

    print(f"AI server active at http://{host}:{port}")

    StandaloneApplication(app, options).run()
