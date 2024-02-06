import json
import logging
import traceback
from datetime import datetime
from typing import Optional
import nltk

import openai
from utils import heconstants
from utils.s3_operation import S3SERVICE
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger

nltk.download('punkt')
s3 = S3SERVICE()
producer = KafkaService(group_id="soap")
openai.api_key = heconstants.OPENAI_APIKEY
logger = get_logger()
logger.setLevel(logging.INFO)
remove_lines_with_words = [
    "none",
    "un known"
    "unknown",
    "not applicable",
    "not available",
    "not mentioned",
    "n/a",
    "undetermined",
    "not determined"
]


class soap:
    def __init__(self):
        self.logger = get_logger()
        self.logger.setLevel(logging.INFO)

    def filter_summary_properties(self, summary_type):
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

        # Filter properties based on summary_type
        properties = clinical_summary_functions[0]["parameters"]["properties"]
        filtered_properties = {key: value for key, value in properties.items() if key == summary_type}

        # Update the properties in the dictionary
        clinical_summary_functions[0]["parameters"]["properties"] = filtered_properties

        return clinical_summary_functions

    def string_to_dict(self, input_string):
        # Splitting the string into different sections
        sections = input_string.split("\n\n")
        result = {}

        # Function to process each sentence
        def process_sentence(sentence):
            sentence = sentence.strip().replace("- ", "")
            # Add a full stop if not present
            if not sentence.endswith('.'):
                sentence += '.'
            return sentence

        # Processing each section
        for section in sections:
            lines = section.split("\n")
            title = lines[0].strip(":").lower()  # Extracting the title (e.g., SUBJECTIVE)

            # Processing sentences
            sentences = []
            for line in lines[1:]:
                for sentence in line.split(" -"):
                    if sentence:
                        sentences.append(process_sentence(sentence))

            content = " ".join(sentences)

            # Mapping the title to the corresponding key in the result dictionary
            if title == "subjective":
                result["subjectiveSummary"] = content
            elif title == "objective":
                result["objectiveSummary"] = content
            elif title == "assessment":
                result["clinicalAssessmentSummary"] = content
            elif title == "plan":
                result["carePlanSummary"] = content

        return result

    def get_clinical_summaries_from_openai(self, text, summary_type: Optional[str] = None):
        try:
            messages = [
                {
                    "role": "system",
                    # "content": """Generate clinical summaries following their description for the following transcript""",
                    "content": """"Summarize the medical case in the following format: SUBJECTIVE,
                    OBJECTIVE, ASSESSMENT, PLAN. It is important to maintain accuracy and relevance to the medical
                    context and omit any non-medical chatter, assumptions, or speculations. Provide the asked
                    information in a clear and concise manner, structured, you are not suppose to assume anything and
                    dont use any hypothesis , rememeber to generate results in points.""",
                },
                {"role": "user", "content": f"TEXT: {text}"},
            ]

            # summary_function = self.filter_summary_properties(summary_type=summary_type)

            for model_name in heconstants.GPT_MODELS:
                try:
                    response = openai.ChatCompletion.create(
                        model=model_name,
                        messages=messages,
                        # functions=heconstants.clinical_summary_functions,
                        # function_call={"name": "ClinicalSummaries"},
                        temperature=0.6,
                    )

                    extracted_info = response.choices[0]["message"]["content"]
                    converted_info = self.string_to_dict(extracted_info)
                    return converted_info

                    # extracted_info = json.loads(
                    #     response.choices[0]["message"]["function_call"]["arguments"]
                    # )
                    # print(extracted_info)
                    # return extracted_info

                except Exception as ex:
                    self.logger.error(f"Failed to get clinical summary from openAI :: {ex}")
                    pass
        except Exception as exc:
            msg = "Failed to get OPEN AI SUMMARIES :: {}".format(exc)
            self.logger.error(msg)

    def get_merge_ai_preds(self, conversation_id, message):
        try:
            req_type = message.get("req_type")
            api_type = message.get("api_type")
            file_path = message.get("file_path")
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

            conversation_datas = None
            if req_type == "encounter":
                conversation_datas = s3.get_files_matching_pattern(
                    pattern=f"{conversation_id}/{conversation_id}_*json")
            else:
                # Check if call is from platform
                if api_type == "clinical_notes":
                    conversation_datas = [s3.get_json_file(s3_filename=f"{conversation_id}/{conversation_id}.json")]

            if conversation_datas:
                for conversation_data in conversation_datas:
                    merged_segments += conversation_data["segments"]

            ai_preds_file_path = f"{conversation_id}/ai_preds.json"
            if s3.check_file_exists(ai_preds_file_path):
                merged_ai_preds = s3.get_json_file(s3_filename=ai_preds_file_path)

            return merged_segments, merged_ai_preds

        except Exception as e:
            self.logger.error(f"An unexpected error occurred while merging ai preds  {e}")
            return merged_segments, merged_ai_preds

    def get_interested_text(self, last_ai_preds: dict = None, segments: Optional[list] = None,
                            transcript: Optional[str] = None):
        try:
            interest_texts = []
            if segments:
                for segment in segments:
                    text = segment["text"]
                    is_imp = False
                    for entity_type, values in last_ai_preds["entities"].items():
                        if is_imp:
                            break
                        for value in values:
                            words_in_value = [w for w in value["text"].split() if len(w) > 3]
                            if value["text"].lower() in text.lower() or any(
                                    [w.lower() in text.lower() for w in words_in_value]
                            ):
                                is_imp = True
                                break
                    if is_imp:
                        interest_texts.append(text)
            elif transcript:
                segments = transcript.split(",")
                for text in segments:
                    is_imp = False
                    for entity_type, values in last_ai_preds["entities"].items():
                        if is_imp:
                            break
                        for value in values:
                            words_in_value = [w for w in value["text"].split() if len(w) > 3]
                            if value["text"].lower() in text.lower() or any(
                                    [w.lower() in text.lower() for w in words_in_value]
                            ):
                                is_imp = True
                                break
                    if is_imp:
                        interest_texts.append(text)

            return interest_texts
        except Exception as e:
            self.logger.error(f"An unexpected error occurred  {e}")

    def get_subjective_summary(self, message, start_time, segments: list = [], last_ai_preds: dict = {}):
        try:
            conversation_id = message.get("care_req_id")
            api_type = message.get("api_type")
            file_path = message.get("file_path")
            subjective_summary = []
            for k in [
                "age",
                "gender",
                "height",
                "weight",
                "bmi",
                "ethnicity",
                "substanceAbuse",
                "physicalActivityExercise",
                "allergies",
            ]:
                if k in last_ai_preds:
                    subjective_summary.append(f"{k.capitalize()}: {last_ai_preds[k]['text']}")

            if api_type == "soap":
                input_text = s3.get_json_file(s3_filename=file_path)
                transcript = input_text.get("transcript")
                interest_texts = self.get_interested_text(last_ai_preds, transcript=transcript)
            else:
                interest_texts = self.get_interested_text(last_ai_preds, segments=segments)

            if interest_texts and len(" ".join(interest_texts).split()) >= 20:
                summaries = self.get_clinical_summaries_from_openai("\n".join(interest_texts),
                                                                    summary_type="subjectiveSummary")
                try:
                    subjective_summary += nltk.sent_tokenize(summaries["subjectiveSummary"])
                except Exception as e:
                    self.logger.error(f"NLTK error (subjectiveSummary) ::  {e}")
                    pass

                subjective_summary = [
                    line
                    for line in subjective_summary
                    if not any([word in line.lower() for word in remove_lines_with_words])
                ]

                data = {"subjectiveClinicalSummary": subjective_summary}
                s3.upload_to_s3(f"{conversation_id}/subjectiveClinicalSummary.json",
                                data.get("subjectiveClinicalSummary"), is_json=True)
            else:
                data = {"subjectiveClinicalSummary": subjective_summary}
                s3.upload_to_s3(f"{conversation_id}/subjectiveClinicalSummary.json",
                                data.get("subjectiveClinicalSummary"), is_json=True)

            print(data)
        except Exception as e:
            response_json = {"request_id": conversation_id,
                             "status": "Failed"}
            merged_json_key = f"{conversation_id}/All_Preds.json"
            s3.upload_to_s3(merged_json_key, response_json, is_json=True)
            self.logger.error(f"An unexpected error occurred while generating subjectiveClinicalSummary ::  {e}")

    def get_objective_summary(self, message, start_time, segments: list = [], last_ai_preds: dict = {}):
        try:
            conversation_id = message.get("care_req_id")
            api_type = message.get("api_type")
            file_path = message.get("file_path")
            objective_summary = []
            for k in ["bloodPressure", "pulse", "respiratoryRate", "bodyTemperature"]:
                if k in last_ai_preds:
                    objective_summary.append(f"{k.capitalize()}: {last_ai_preds[k]['text']}")

            if api_type == "soap":
                input_text = s3.get_json_file(s3_filename=file_path)
                transcript = input_text.get("transcript")
                interest_texts = self.get_interested_text(last_ai_preds, transcript=transcript)
            else:
                interest_texts = self.get_interested_text(last_ai_preds, segments=segments)

            if interest_texts and len(" ".join(interest_texts).split()) >= 20:
                summaries = self.get_clinical_summaries_from_openai("\n".join(interest_texts),
                                                                    summary_type="objectiveSummary")
                try:
                    objective_summary += nltk.sent_tokenize(summaries["objectiveSummary"])
                except Exception as e:
                    self.logger.error(f"NLTK error (objectiveSummary) ::  {e}")
                    pass

                objective_summary = [
                    line
                    for line in objective_summary
                    if not any([word in line.lower() for word in remove_lines_with_words])
                ]

                data = {
                    "objectiveClinicalSummary": objective_summary,
                }

                s3.upload_to_s3(f"{conversation_id}/objectiveClinicalSummary.json",
                                data.get("objectiveClinicalSummary"), is_json=True)

            else:
                data = {
                    "objectiveClinicalSummary": objective_summary,
                }

                s3.upload_to_s3(f"{conversation_id}/objectiveClinicalSummary.json",
                                data.get("objectiveClinicalSummary"), is_json=True)
            print(data)

        except Exception as e:
            response_json = {"request_id": conversation_id,
                             "status": "Failed"}
            merged_json_key = f"{conversation_id}/All_Preds.json"
            s3.upload_to_s3(merged_json_key, response_json, is_json=True)
            self.logger.error(f"An unexpected error occurred while generating  objectiveClinicalSummary ::  {e}")

    def get_clinical_assessment_summary(self, message, start_time, segments: list = [], last_ai_preds: dict = {}):
        try:
            conversation_id = message.get("care_req_id")
            api_type = message.get("api_type")
            file_path = message.get("file_path")

            clinical_assessment_summary = []

            if api_type == "soap":
                input_text = s3.get_json_file(s3_filename=file_path)
                transcript = input_text.get("transcript")
                interest_texts = self.get_interested_text(last_ai_preds, transcript=transcript)
            else:
                interest_texts = self.get_interested_text(last_ai_preds, segments=segments)

            if interest_texts and len(" ".join(interest_texts).split()) >= 20:
                summaries = self.get_clinical_summaries_from_openai("\n".join(interest_texts),
                                                                    summary_type="clinicalAssessmentSummary")
                try:
                    clinical_assessment_summary += nltk.sent_tokenize(
                        summaries["clinicalAssessmentSummary"]
                    )
                except Exception as e:
                    self.logger.error(f"NLTK error (clinicalAssessmentSummary) ::  {e}")
                    pass

                clinical_assessment_summary = [
                    line
                    for line in clinical_assessment_summary
                    if not any([word in line.lower() for word in remove_lines_with_words])
                ]

                data = {
                    "clinicalAssessment": clinical_assessment_summary
                }

                s3.upload_to_s3(f"{conversation_id}/clinicalAssessment.json", data.get("clinicalAssessment"),
                                is_json=True)
            else:
                data = {
                    "clinicalAssessment": clinical_assessment_summary
                }

                s3.upload_to_s3(f"{conversation_id}/clinicalAssessment.json", data.get("clinicalAssessment"),
                                is_json=True)
            print(data)

        except Exception as e:
            response_json = {"request_id": conversation_id,
                             "status": "Failed"}
            merged_json_key = f"{conversation_id}/All_Preds.json"
            s3.upload_to_s3(merged_json_key, response_json, is_json=True)
            self.logger.error(f"An unexpected error occurred while generating clinicalAssessment ::  {e}")

    def get_care_plan_summary(self, message, start_time, segments: list = [], last_ai_preds: dict = {}):
        try:
            conversation_id = message.get("care_req_id")
            api_type = message.get("api_type")
            file_path = message.get("file_path")

            care_plan_summary = []

            if api_type == "soap":
                input_text = s3.get_json_file(s3_filename=file_path)
                transcript = input_text.get("transcript")
                interest_texts = self.get_interested_text(last_ai_preds, transcript=transcript)
            else:
                interest_texts = self.get_interested_text(last_ai_preds, segments=segments)

            if interest_texts and len(" ".join(interest_texts).split()) >= 20:
                summaries = self.get_clinical_summaries_from_openai("\n".join(interest_texts),
                                                                    summary_type="carePlanSummary")
                try:
                    care_plan_summary += nltk.sent_tokenize(summaries["carePlanSummary"])
                except Exception as e:
                    self.logger.error(f"NLTK error (carePlanSummary) ::  {e}")
                    pass

                care_plan_summary = [
                    line
                    for line in care_plan_summary
                    if not any([word in line.lower() for word in remove_lines_with_words])
                ]

                data = {
                    "carePlanSuggested": care_plan_summary
                }
                s3.upload_to_s3(f"{conversation_id}/carePlanSuggested.json", data.get("carePlanSuggested"),
                                is_json=True)
            else:
                data = {
                    "carePlanSuggested": care_plan_summary
                }
                s3.upload_to_s3(f"{conversation_id}/carePlanSuggested.json", data.get("carePlanSuggested"),
                                is_json=True)
            print(data)
        except Exception as e:
            response_json = {"request_id": conversation_id,
                             "status": "Failed"}
            merged_json_key = f"{conversation_id}/All_Preds.json"
            s3.upload_to_s3(merged_json_key, response_json, is_json=True)
            self.logger.error(f"An unexpected error occurred while generating carePlanSuggested ::  {e}")

    def create_delivery_task(self, message):
        try:
            request_id = message.get("request_id")
            chunk_no = message.get("chunk_no")
            file_path = message.get("file_path")
            webhook_url = message.get("webhook_url")
            req_type = message.get("req_type")
            retry_count = message.get("retry_count")
            api_type = message.get("api_type")
            api_path = message.get("api_path")

            data = {
                "es_id": f"{request_id}_FINAL_EXECUTOR",
                "chunk_no": chunk_no,
                "file_path": file_path,
                "webhook_url": webhook_url,
                "api_path": api_path,
                "api_type": api_type,
                "req_type": req_type,
                "executor_name": "FINAL_EXECUTOR",
                "state": "Final",
                "retry_count": retry_count,
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

        except Exception as exc:
            msg = "Failed to create delivery task :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)

# if __name__ == "__main__":
#     soap_exe = soap()
#     stream_key = "test_new39"
#     start_time = datetime.utcnow()
#     message = {
#         "es_id": f"{stream_key}_SOAP",
#         "chunk_no": 42,
#         "file_path": f"{stream_key}/{stream_key}_chunk42.wav",
#         "api_path": "asr",
#         "api_type": "asr",
#         "req_type": "encounter",
#         "executor_name": "SOAP_EXECUTOR",
#         "state": "Analytics",
#         "retry_count": None,
#         "uid": None,
#         "request_id": stream_key,
#         "care_req_id": stream_key,
#         "encounter_id": None,
#         "provider_id": None,
#         "review_provider_id": None,
#         "completed": False,
#         "exec_duration": 0.0,
#         "start_time": str(start_time),
#         "end_time": str(datetime.utcnow()),
#     }
#     segments, last_ai_preds = soap_exe.get_merge_ai_preds(conversation_id=stream_key)
#     soap_exe.get_subjective_summary(message, start_time, segments, last_ai_preds)
#     soap_exe.get_objective_summary(message, start_time, segments, last_ai_preds)
#     # soap_exe.get_clinical_assessment_summary(message, start_time, segments, last_ai_preds)
#     # soap_exe.get_care_plan_summary(message, start_time, segments, last_ai_preds)
#     # soap_exe.execute_function(message=message, start_time=datetime.utcnow())
#     # soap_exe.get_clinical_summaries_from_openai(transcript_text)
