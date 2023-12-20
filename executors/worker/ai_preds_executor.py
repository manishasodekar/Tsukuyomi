import json
import logging
import traceback
from datetime import datetime
import requests
import openai
from utils import heconstants
from utils.s3_operation import S3SERVICE
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger

s3 = S3SERVICE()
producer = KafkaService(group_id="aipreds")
openai.api_key = heconstants.OPENAI_APIKEY
logger = get_logger()
logger.setLevel(logging.INFO)


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
        try:
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
        except Exception as e:
            logger.error(f"An error occurred clean_pred: {e}")

    def execute_function(self, message, start_time):
        try:
            conversation_id = message.get("care_req_id")
            file_path = message.get("file_path")
            chunk_no = message.get("chunk_no")
            retry_count = message.get("retry_count")
            merged_segments = []
            conversation_datas = s3.get_files_matching_pattern(
                pattern=f"{conversation_id}/{conversation_id}_*json")
            if conversation_datas:
                for conversation_data in conversation_datas:
                    merged_segments += conversation_data["segments"]

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

            ai_preds_file_path = f"{conversation_id}/ai_preds.json"
            if s3.check_file_exists(ai_preds_file_path):
                entities = s3.get_json_file(ai_preds_file_path)

            # current_segment = s3.get_json_file(file_path.replace("wav", "json"))
            # segments = current_segment.get("segments")
            # if segments:
            #     text = "\n".join([seg["text"] for seg in segments])

            if merged_segments:
                text = "\n".join([_["text"] for _ in merged_segments])

                extracted_info = self.get_preds_from_open_ai(
                    text, heconstants.faster_clinical_info_extraction_functions, min_length=5
                )
                extracted_info = self.clean_pred(extracted_info)

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
                        codes = \
                            requests.post(heconstants.AI_SERVER + "/code_search/infer",
                                          json=all_texts_and_types).json()[
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

                # if entities:
                #     current_segment["ai_preds"] = entities
                # else:
                #     current_segment["ai_preds"] = None

                s3.upload_to_s3(f"{conversation_id}/ai_preds.json", entities, is_json=True)
                data = {
                    "es_id": f"{conversation_id}_SOAP",
                    "chunk_no": chunk_no,
                    "file_path": file_path,
                    "api_path": "asr",
                    "api_type": "asr",
                    "req_type": "encounter",
                    "executor_name": "SOAP_EXECUTOR",
                    "state": "Analytics",
                    "retry_count": retry_count,
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
            logger.error(msg, trace)
            if retry_count <= 2:
                data = {
                    "es_id": f"{conversation_id}_SOAP",
                    "chunk_no": chunk_no,
                    "file_path": file_path,
                    "api_path": "asr",
                    "api_type": "asr",
                    "req_type": "encounter",
                    "executor_name": "SOAP_EXECUTOR",
                    "state": "Analytics",
                    "retry_count": retry_count,
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

    def get_preds_from_open_ai(self,
                               transcript_text,
                               function_list=heconstants.faster_clinical_info_extraction_functions,
                               min_length=30,
                               ):
        try:
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
                    logger.info(f"extracted_info :: {extracted_info}")
                    return extracted_info

                except Exception as ex:
                    logger.error(ex)
                    pass
        except Exception as exc:
            msg = "Failed to get OPEN AI PREDICTION :: {}".format(exc)
            logger.error(msg)
