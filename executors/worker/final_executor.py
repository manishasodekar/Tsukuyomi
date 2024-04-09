import io
import json
import logging
import os
import re
import subprocess
import traceback
from datetime import datetime
import av
import time
import wave
from io import BytesIO

import requests
from pydub import AudioSegment
from utils import heconstants
from utils.s3_operation import S3SERVICE
from utils.send_logs import push_logs
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger
from fastpunct import FastPunct

fastpunct = FastPunct()
s3 = S3SERVICE()
producer = KafkaService(group_id="final")
logger = get_logger()
logger.setLevel(logging.INFO)


class finalExecutor:
    def __init__(self):
        pass

    def send_webhook(self, url, data):
        try:
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url, data=json.dumps(data), headers=headers)
            return response.status_code, response.text

        except Exception as exc:
            msg = "Failed to send webhook :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)

    def get_merge_ai_preds(self, message, starttime):
        try:
            request_id = message.get("request_id")
            req_type = message.get("req_type")
            webhook_url = message.get("webhook_url")
            api_type = message.get("api_type")
            file_path = message.get("file_path")
            failed_state = message.get("failed_state")
            language = message.get("language")

            logger.info("MERGING All AIPREDS")

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
            response_json = {"request_id": request_id}
            ai_preds_file_path = f"{request_id}/ai_preds.json"
            conversation_datas = None
            if req_type == "platform":
                if api_type in {"clinical_notes", "transcription"}:
                    conversation_datas = [s3.get_json_file(s3_filename=f"{request_id}/{request_id}.json")]
                elif api_type in {"ai_pred", "soap"}:
                    input_text = s3.get_json_file(s3_filename=file_path)
                    text = input_text.get("transcript")

                if conversation_datas or text:
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
                            response_json["meta"] = audio_metas

                    if s3.check_file_exists(ai_preds_file_path):
                        merged_ai_preds = s3.get_json_file(ai_preds_file_path)
                        summary_file = f"{request_id}/soap.json"
                        if s3.check_file_exists(summary_file):
                            summary_content = s3.get_json_file(s3_filename=summary_file)
                            if summary_content:
                                summary = {
                                    "summaries": {}
                                }
                                for summary_type in ["subjectiveClinicalSummary", "objectiveClinicalSummary",
                                                     "clinicalAssessment",
                                                     "carePlanSuggested"]:
                                    summary["summaries"][summary_type] = summary_content.get(summary_type)
                                merged_ai_preds.update(summary)

                        if api_type in {"clinical_notes", "ai_pred"}:
                            response_json["ai_preds"] = merged_ai_preds
                        elif api_type == "soap":
                            response_json["summaries"] = merged_ai_preds.get("summaries")

                    pattern = re.compile(
                        r'(?:\b(?:thanks|thank you|you|bye|yeah|beep|okay|peace)\b[.!?,-]*\s*){2,}',
                        re.IGNORECASE)
                    word_pattern = re.compile(r'\b(?:Thank you|Bye|You)\.')

                    if merged_segments:
                        long_transcript = " ".join([_["text"] for _ in merged_segments])
                        long_transcript = pattern.sub('', long_transcript)
                        long_transcript = word_pattern.sub('', long_transcript)
                        long_transcript = re.sub(' +', ' ', long_transcript).strip()
                        response_json["transcript"] = long_transcript
                    elif text:
                        text = pattern.sub('', text)
                        text = word_pattern.sub('', text)
                        text = re.sub(' +', ' ', text).strip()
                        response_json["transcript"] = text

                    if language == "en":
                        transcript = response_json.get("transcript")
                        payload = {
                            "data": [transcript]
                        }
                        if transcript:
                            punc_transcript = requests.post(
                                heconstants.AI_SERVER + f"/punctuation/infer",
                                json=payload)["prediction"][0]
                            if punc_transcript:
                                response_json["transcript"] = punc_transcript

                    if s3.check_file_exists(key=f"{request_id}/translated_transcript.json"):
                        translated_transcript_content = s3.get_json_file(
                            s3_filename=f"{request_id}/translated_transcript.json")
                        if translated_transcript_content:
                            response_json["translated_transcript"] = translated_transcript_content.get("transcript")

                    response_json["success"] = True

                    if not failed_state:
                        response_json["status"] = "Completed"
                    else:
                        response_json["status"] = "Failed"

                    merged_json_key = f"{request_id}/All_Preds.json"
                    s3.upload_to_s3(merged_json_key, response_json, is_json=True)
                    logger.info(f'MERGED AI PREDS')
                    if webhook_url:
                        logger.info(f'SENDING WEBHOOKS')
                        if len(response_json["transcript"]) > 0 and not failed_state:
                            status_code, response_text = self.send_webhook(webhook_url, response_json)
                            logger.info(f'Status Code: {status_code}\nResponse: {response_text}')
                        else:
                            error_resp = {"success": False,
                                          "request_id": request_id,
                                          "issue": [{
                                              "error-code": "HE-101",
                                              "message": "Failed to process the request."
                                          }]
                                          }

                            status_code, response_text = self.send_webhook(webhook_url, error_resp)
                            logger.info(f'Status Code: {status_code}\nResponse: {response_text}')

        except Exception as exc:
            msg = "Failed to merge and send ai_preds :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)
            response_json = {"request_id": request_id,
                             "status": "Failed"}
            merged_json_key = f"{request_id}/All_Preds.json"
            s3.upload_to_s3(merged_json_key, response_json, is_json=True)
