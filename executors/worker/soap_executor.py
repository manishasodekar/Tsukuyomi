import json
import logging
from typing import Optional

import nltk
import openai
from utils import heconstants
from utils.s3_operation import S3SERVICE
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger

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

    def filter_summary_properties(summary_type):
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

    def get_clinical_summaries_from_openai(self, text, summary_type: Optional[str] = None):
        try:
            messages = [
                {
                    "role": "system",
                    "content": """Generate clinical summaries following their description for the following transcript""",
                },
                {"role": "user", "content": f"TEXT: {text}"},
            ]

            summary_function = self.filter_summary_properties(summary_type)

            for model_name in heconstants.GPT_MODELS:
                try:
                    response = openai.ChatCompletion.create(
                        model=model_name,
                        messages=messages,
                        functions=summary_function,
                        function_call={"name": "ClinicalSummaries"},
                        temperature=1,
                    )

                    extracted_info = json.loads(
                        response.choices[0]["message"]["function_call"]["arguments"]
                    )
                    return extracted_info

                except Exception as ex:
                    self.logger.error(f"Failed to get clinical summary from openAI :: {ex}")
                    pass
        except Exception as exc:
            msg = "Failed to get OPEN AI SUMMARIES :: {}".format(exc)
            logger.error(msg)

    def get_merge_ai_preds(self, conversation_id):
        try:
            conversation_datas = s3.get_files_matching_pattern(
                pattern=f"{conversation_id}/{conversation_id}_*json")
            logger.info(f"conversation_datas :: {len(conversation_datas)}")

            if conversation_datas:
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
                for conversation_data in conversation_datas:
                    merged_segments += conversation_data["segments"]

                    ai_preds = conversation_data["ai_preds"]
                    if ai_preds:
                        for k in [
                            "age",
                            "gender",
                            "ethnicity",
                            "height",
                            "weight",
                            "bmi",
                            "ethnicity",
                            "insurance",
                            "physicalActivityExercise",
                            "bloodPressure",
                            "pulse",
                            "respiratoryRate",
                            "bodyTemperature",
                            "substanceAbuse",
                        ]:
                            if isinstance(ai_preds.get(k), dict) and ai_preds[k]["text"]:
                                if not merged_ai_preds[k]["text"]:
                                    merged_ai_preds[k]["text"] = ai_preds[k]["text"]
                                else:
                                    merged_ai_preds[k]["text"] += ", " + ai_preds[k]["text"]

                                merged_ai_preds[k]["value"] = merged_ai_preds[k]["text"]
                                merged_ai_preds[k]["unit"] = ai_preds[k]["unit"]

                        for k, v in ai_preds.get("entities", {}).items():
                            if k not in merged_ai_preds["entities"]:
                                merged_ai_preds["entities"][k] = []
                            merged_ai_preds["entities"][k] += v

                for k in list(merged_ai_preds.keys()):
                    if not merged_ai_preds[k]:
                        del merged_ai_preds[k]

                    elif (
                            isinstance(merged_ai_preds[k], dict)
                            and "value" in merged_ai_preds[k]
                            and not merged_ai_preds[k]["value"]
                    ):
                        del merged_ai_preds[k]

                for k in list(merged_ai_preds.get("entities", {}).keys()):
                    if not merged_ai_preds["entities"][k]:
                        del merged_ai_preds["entities"][k]
            return merged_segments, merged_ai_preds

        except Exception as e:
            self.logger.error(f"An unexpected error occurred while merging ai preds  {e}")

    def get_interested_text(self, last_ai_preds: dict = None, segments: list = None):
        try:
            interest_texts = []
            for segment in segments:
                text = segment["text"]
                print("text :: ", text)
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

    def get_subjective_summary(self, message, start_time):
        try:
            segments = []
            last_ai_preds = {}

            conversation_id = message.get("care_req_id")
            if conversation_id:
                segments, last_ai_preds = self.get_merge_ai_preds(conversation_id)

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

            interest_texts = self.get_interested_text(last_ai_preds, segments)

            if interest_texts and len(" ".join(interest_texts).split()) >= 20:
                summaries = self.get_clinical_summaries_from_openai("\n".join(interest_texts),
                                                                    summary_type="subjectiveSummary")
                try:
                    subjective_summary += nltk.sent_tokenize(summaries["subjectiveSummary"])
                except:
                    pass

                subjective_summary = [
                    line
                    for line in subjective_summary
                    if not any([word in line.lower() for word in remove_lines_with_words])
                ]

                data = {"subjectiveClinicalSummary": subjective_summary}
                logger.info("Interested_text:: ", data)
                s3.upload_to_s3(f"{conversation_id}/subjectiveClinicalSummary.json", data, is_json=True)
            else:
                data = {"subjectiveClinicalSummary": subjective_summary}
                logger.info("not Interested_text:: ", data)
                s3.upload_to_s3(f"{conversation_id}/subjectiveClinicalSummary.json", data, is_json=True)

        except Exception as e:
            self.logger.error(f"An unexpected error occurred while generating subjectiveClinicalSummary ::  {e}")

    def get_objective_summary(self, message, start_time):
        try:
            segments = []
            last_ai_preds = {}

            conversation_id = message.get("care_req_id")
            if conversation_id:
                segments, last_ai_preds = self.get_merge_ai_preds(conversation_id)

            objective_summary = []
            for k in ["bloodPressure", "pulse", "respiratoryRate", "bodyTemperature"]:
                if k in last_ai_preds:
                    objective_summary.append(f"{k.capitalize()}: {last_ai_preds[k]['text']}")

            interest_texts = self.get_interested_text(last_ai_preds, segments)

            if interest_texts and len(" ".join(interest_texts).split()) >= 20:
                summaries = self.get_clinical_summaries_from_openai("\n".join(interest_texts),
                                                                    summary_type="objectiveSummary")
                try:
                    objective_summary += nltk.sent_tokenize(summaries["objectiveSummary"])
                except:
                    pass

                objective_summary = [
                    line
                    for line in objective_summary
                    if not any([word in line.lower() for word in remove_lines_with_words])
                ]

                data = {
                    "objectiveClinicalSummary": objective_summary,
                }
                logger.info("Interested_text:: ", data)

                s3.upload_to_s3(f"{conversation_id}/objectiveClinicalSummary.json", data, is_json=True)

            else:
                data = {
                    "objectiveClinicalSummary": objective_summary,
                }
                logger.info("not Interested_text:: ", data)

                s3.upload_to_s3(f"{conversation_id}/objectiveClinicalSummary.json", data, is_json=True)


        except Exception as e:
            self.logger.error(f"An unexpected error occurred while generating  objectiveClinicalSummary ::  {e}")

    def get_clinical_assessment_summary(self, message, start_time):
        try:
            segments = []
            last_ai_preds = {}

            conversation_id = message.get("care_req_id")
            if conversation_id:
                segments, last_ai_preds = self.get_merge_ai_preds(conversation_id)

            clinical_assessment_summary = []
            interest_texts = self.get_interested_text(last_ai_preds, segments)

            if interest_texts and len(" ".join(interest_texts).split()) >= 20:
                summaries = self.get_clinical_summaries_from_openai("\n".join(interest_texts),
                                                                    summary_type="clinicalAssessmentSummary")
                try:
                    clinical_assessment_summary += nltk.sent_tokenize(
                        summaries["clinicalAssessmentSummary"]
                    )
                except:
                    pass

                clinical_assessment_summary = [
                    line
                    for line in clinical_assessment_summary
                    if not any([word in line.lower() for word in remove_lines_with_words])
                ]

                data = {
                    "clinicalAssessment": clinical_assessment_summary
                }
                logger.info("Interested_text:: ", data)

                s3.upload_to_s3(f"{conversation_id}/clinicalAssessment.json", data, is_json=True)
            else:
                data = {
                    "clinicalAssessment": clinical_assessment_summary
                }
                logger.info("not Interested_text:: ", data)

                s3.upload_to_s3(f"{conversation_id}/clinicalAssessment.json", data, is_json=True)



        except Exception as e:
            self.logger.error(f"An unexpected error occurred while generating clinicalAssessment ::  {e}")

    def get_care_plan_summary(self, message, start_time):
        try:
            segments = []
            last_ai_preds = {}

            conversation_id = message.get("care_req_id")
            if conversation_id:
                segments, last_ai_preds = self.get_merge_ai_preds(conversation_id)

            care_plan_summary = []
            interest_texts = self.get_interested_text(last_ai_preds, segments)

            if interest_texts and len(" ".join(interest_texts).split()) >= 20:
                summaries = self.get_clinical_summaries_from_openai("\n".join(interest_texts),
                                                                    summary_type="carePlanSummary")
                try:
                    care_plan_summary += nltk.sent_tokenize(summaries["carePlanSummary"])
                except:
                    pass

                care_plan_summary = [
                    line
                    for line in care_plan_summary
                    if not any([word in line.lower() for word in remove_lines_with_words])
                ]

                data = {
                    "carePlanSuggested": care_plan_summary
                }
                logger.info("Interested_text:: ", data)

                s3.upload_to_s3(f"{conversation_id}/carePlanSuggested.json", data, is_json=True)
            else:
                data = {
                    "carePlanSuggested": care_plan_summary
                }
                logger.info("not Interested_text:: ", data)

                s3.upload_to_s3(f"{conversation_id}/carePlanSuggested.json", data, is_json=True)

        except Exception as e:
            self.logger.error(f"An unexpected error occurred while generating carePlanSuggested ::  {e}")
