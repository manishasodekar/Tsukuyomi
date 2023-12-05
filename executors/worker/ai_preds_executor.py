import json
import logging
import os
import threading
import traceback
from datetime import datetime

import requests
import websocket
import av
import time
import wave
import boto3
import openai
from io import BytesIO
from elasticsearch import Elasticsearch
from utils import heconstants
from utils.es import Index
from utils.s3_operation import S3SERVICE
from utils.send_logs import push_logs
from botocore.exceptions import NoCredentialsError
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger

s3 = S3SERVICE()
producer = KafkaService(group_id="aipreds")
openai.api_key = heconstants.OPENAI_APIKEY


class aiPreds:
    def clean_pred(self,
                   clinical_information,
                   remove_strings=[
                       "not found",
                       "information not provided",
                       "unknown",
                       "n/a",
                       "null",
                       "undefined",
                       "no data",
                       "none",
                       "missing",
                       "not available",
                       "not applicable",
                       "unidentified",
                       "not specified",
                       "not mentioned",
                       "not detected",
                       "insufficient data",
                       "no mention of",
                       "absence of",
                       "not indicated",
                       "no information on",
                       "unable to determine",
                   ],
                   ):
        for key in list(clinical_information.keys()):
            if isinstance(clinical_information[key], str) and any(
                    rs.lower() in clinical_information[key].lower() for rs in remove_strings
            ):
                del clinical_information[key]

        for detail in list(clinical_information.get("details", [])):
            if isinstance(detail["value"], str) and any(
                    rs.lower() in detail["value"].lower() for rs in remove_strings
            ):
                clinical_information["details"].remove(detail)

        return clinical_information

    def execute_function(self, message, start_time):
        try:
            conversation_id = message.get("stream_key")
            file_path = message.get("file_path")
            prev_segment = s3.get_audio_file(file_path.replace("wav", "json"))
            text = "\n".join([seg["text"] for seg in prev_segment])
            extracted_info = self.get_preds_from_open_ai(
                text, heconstants.faster_clinical_info_extraction_functions, min_length=5
            )
            extracted_info = self.clean_pred(extracted_info)
            entities = {
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

            for _ in extracted_info.get("details", []):
                if _["name"] in entities:
                    if entities[_["name"]]["text"] is None:
                        entities[_["name"]]["text"] = _["value"]
                    else:
                        entities[_["name"]]["text"] += ", " + _["value"]

                    entities[_["name"]]["value"] = entities[_["name"]]["text"]

            all_texts_and_types = []
            for k in ["medications", "symptoms", "diseases", "diagnoses", "surgeries", "tests"]:
                v = extracted_info.get(k, "").replace(" and ", " , ").split(",")
                for _ in v:
                    if _.strip():
                        all_texts_and_types.append((_.strip(), k))

            text_to_codes = {}

            if all_texts_and_types:
                try:
                    codes = requests.post(heconstants.AI_SERVER + "/code_search/infer", json=all_texts_and_types).json()[
                        'prediction']
                except:
                    codes = [{"name": _, "code": None} for _ in all_texts_and_types]

                for (text, _type), code in zip(all_texts_and_types, codes):
                    text_to_codes[text] = {"name": code["name"], "code": code["code"], "score": code.get("score")}

            for k in ["medications", "symptoms", "diseases", "diagnoses", "surgeries", "tests"]:
                v = extracted_info.get(k, "").replace(" and ", " , ").split(",")
                v = [
                    {
                        "text": _.strip(),
                        "code": text_to_codes.get(_.strip(), {}).get("code", None),
                        "code_value": text_to_codes.get(_.strip(), {}).get("name", None),
                        "code_type": "",
                        "confidence": text_to_codes.get(_.strip(), {}).get("score", None),
                    }
                    for _ in v
                    if _.strip()
                ]
                entities["entities"][k] = v

            if entities:
                prev_segment["ai_preds"] = entities
            else:
                prev_segment["ai_preds"] = None

            s3.upload_to_s3(file_path.replace("wav", "json"), prev_segment)
            data = {
                "es_id": f"{conversation_id}_SOAP",
                "api_path": "asr",
                "file_path": None,
                "api_type": "asr",
                "req_type": "encounter",
                "executor_name": "SOAP",
                "state": "Analytics",
                "retry_count": None,
                "uid": None,
                "request_id": conversation_id,
                "care_req_id": conversation_id,
                "encounter_id": None,
                "provider_id": None,
                "review_provider_id": None,
                "completed": False,
                "exec_duration": 0.0,
                "start_time": str(start_time),
                "end_time": str(datetime.utcnow()),
            }
            producer.publish_executor_message(data)

        except Exception as exc:
            msg = "Failed to get AI PREDICTION :: {}".format(exc)
            trace = traceback.format_exc()
            self.logger.error(msg, trace)
            data = {
                "es_id": f"{conversation_id}_AI_PRED",
                "api_path": "asr",
                "file_path": None,
                "api_type": "asr",
                "req_type": "encounter",
                "executor_name": "AiPred",
                "state": "Failed",
                "completed": False,
                "retry_count": None,
                "uid": None,
                "request_id": conversation_id,
                "care_req_id": conversation_id,
                "encounter_id": None,
                "provider_id": None,
                "review_provider_id": None,
                "exec_duration": 0.0,
                "start_time": str(start_time),
                "end_time": str(datetime.utcnow()),
            }
            producer.publish_executor_message(data)

    def get_preds_from_open_ai(self,
                               transcript_text,
                               function_list=heconstants.faster_clinical_info_extraction_functions,
                               min_length=30,
                               ):
        transcript_text = transcript_text.strip()
        if not transcript_text or len(transcript_text) <= min_length:
            raise Exception("Transcript text is too short")

        messages = [
            {
                "role": "system",
                "content": """ Don't make assumptions about what values to plug into functions. return not found if you can't find the information.
                You are acting as an expert clinical entity extractor.
                Extract the described information from given clinical notes or consultation transcript.
                No extra information or hypothesis not present in the given text should be added. Separate items with , wherever needed.
                All the text returned should be present in the given TEXT. no new text should be returned.""",
            },
            {"role": "user", "content": f"TEXT: {transcript_text}"},
        ]

        for model_name in ["gpt-3.5-turbo-0613", "gpt-3.5-turbo-16k-0613", "gpt-4-0613"]:
            try:
                response = openai.ChatCompletion.create(
                    model=model_name,
                    messages=messages,
                    functions=function_list,
                    function_call={"name": "ClinicalInformation"},
                    temperature=1,
                )

                extracted_info = json.loads(
                    response.choices[0]["message"]["function_call"]["arguments"]
                )
                print("extracted_info", extracted_info)
                return extracted_info

            except Exception as ex:
                print(ex)
                pass

        raise Exception("openai call failed.")
