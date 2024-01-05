from typing import Optional

from gevent import monkey

monkey.patch_all()
import os
import falcon
import traceback
import logging
from utils.s3_operation import S3SERVICE
from utils import heconstants
from config.logconfig import get_logger
from typing import Optional

logger = get_logger()
logger.setLevel(logging.INFO)
s3 = S3SERVICE()


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


class Home(object):
    def on_get(self, req, resp):
        resp.text = "Welcome to SYNC API SERVER"


app = falcon.App(cors_enable=True)
app.req_options.auto_parse_form_urlencoded = True
app = falcon.App(
    middleware=falcon.CORSMiddleware(allow_origins="*", allow_credentials="*")
)

history_api = History()
home = Home()

app.add_route("/home", home)
app.add_route("/history", history_api)

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
