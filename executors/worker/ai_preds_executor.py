import gc
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
                       "no information found"
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
                       "no mention",
                       "not mentioned",
                       "none mentioned",
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

            details = clinical_information.get("details", {})
            if details:
                keys_to_remove = [k for k, v in details.items() if
                                  isinstance(v, str) and any(rs.lower() in v.lower() for rs in remove_strings)]

                for k in keys_to_remove:
                    del details[k]

                clinical_information["details"] = details

            return clinical_information
        except Exception as exc:
            msg = "Failed to get clean_pred :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)

    def clean_null_entries(self, entities):
        # List of keys to delete from the main dictionary
        keys_to_delete = []

        for key, value in entities.items():
            # Check if the value is a dictionary and has a 'text' key
            if isinstance(value, dict) and "text" in value:
                if value["text"] is None:
                    keys_to_delete.append(key)

            # Check if the value is an empty list in the 'entities' sub-dictionary
            elif key == "entities":
                # removes value with empty code
                for k, v in value.items():
                    for i, _ in enumerate(v):
                        if not _.get("code"):
                            v.pop(i)
                        else:
                            if k == "diagnoses":
                                if "ICD10CM" not in _.get("code"):
                                    v.pop(i)
                            elif k == "symptoms":
                                if "HE" not in _.get("code"):
                                    v.pop(i)
                            elif k == "medications":
                                if "NDC" not in _.get("code"):
                                    v.pop(i)
                            elif k == "surgeries":
                                if "CPT" not in _.get("code"):
                                    v.pop(i)
                            elif k == "procedures":
                                if "CPT" not in _.get("code"):
                                    v.pop(i)
                # removes null/[] entities
                empty_keys = [k for k, v in value.items() if v == []]
                for empty_key in empty_keys:
                    del value[empty_key]
            elif key == "summaries":
                empty_keys = [k for k, v in value.items() if v == []]
                for empty_key in empty_keys:
                    del value[empty_key]
                if not value:
                    keys_to_delete.append(key)

        # Delete the keys from the main dictionary
        for key in keys_to_delete:
            del entities[key]

        return entities

    # def clean_null_entries(self, entities):
    #     # Directly remove entries where `text` is `None`
    #     entities = {k: v for k, v in entities.items() if not (isinstance(v, dict) and v.get("text") is None)}
    #     print("here1", entities )
    #     # Function to filter valid items based on their code
    #     def valid_item(item, prefix):
    #         return item.get("code", "").startswith(prefix)
    #
    #     # Check and clean entities if exists
    #     if "entities" in entities:
    #         for key, items in entities["entities"].items():
    #             prefix = {"diagnoses": "ICD10CM", "symptoms": "HE", "medications": "NDC", "surgeries": "CPT",
    #                       "procedures": "CPT"}.get(key, "")
    #             if prefix:
    #                 # Keep items with valid codes
    #                 entities["entities"][key] = [item for item in items if valid_item(item, prefix)]
    #             else:
    #                 # Remove items without a code or with an empty code
    #                 entities["entities"][key] = [item for item in items if item.get("code")]
    #
    #             # Remove the key if the list is empty after filtering
    #             if not entities["entities"][key]:
    #                 del entities["entities"][key]
    #     print("here2", entities )
    #
    #     # Clean summaries if exists
    #     if "summaries" in entities:
    #         entities["summaries"] = {k: v for k, v in entities["summaries"].items() if v != []}
    #
    #     print("here3", entities )
    #
    #     return entities

    def merge_procedures_surgeries(self, ai_preds):
        try:

            # merging surgeries into procedures
            unique_procedures = {procedure["code"]: procedure for procedure in
                                 ai_preds["entities"].get("procedures", [])}
            surgeries = ai_preds["entities"].get("surgeries", [])
            for surgery in surgeries:
                if surgery["code"] not in unique_procedures:
                    # This is not a duplicate, so add it to 'procedures'
                    ai_preds["entities"]["procedures"].append(surgery)
                    unique_procedures[surgery["code"]] = surgery
            if "surgeries" in ai_preds["entities"]:
                del ai_preds["entities"]["surgeries"]
            del unique_procedures
            del surgeries
            return ai_preds

        except Exception as exc:
            msg = "Failed to get merge_procedures_surgeries :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)

    def remove_duplicates_ai_preds(self, ai_preds):
        try:
            # Remove duplicates based on "code" from all keys in ai_preds["entities"]
            for key in ai_preds["entities"]:
                # Create a new list with unique codes
                unique_entities = []
                codes_seen = set()
                for entity in ai_preds["entities"][key]:
                    if entity["code"] not in codes_seen:
                        unique_entities.append(entity)
                        codes_seen.add(entity["code"])
                # Update the list with the unique items
                ai_preds["entities"][key] = unique_entities

            return ai_preds

        except Exception as exc:
            msg = "Failed to get merge_ai_preds :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)

    def merge_suggestions(self, ai_preds, triage_ai_suggestion):
        try:
            # Ensure 'entities' key exists in ai_preds
            if "entities" not in ai_preds:
                ai_preds["entities"] = {}

            for key, suggestions in triage_ai_suggestion.items():
                # Initialize the key in ai_preds["entities"] if it doesn't exist
                if key not in ai_preds["entities"]:
                    ai_preds["entities"][key] = []

                for suggestion in suggestions:
                    found_duplicate = False
                    for ai_pred in ai_preds["entities"].get(key, []):
                        # If a duplicate code is found in ai_preds, update its source field
                        if ai_pred["code"] == suggestion["code"]:
                            if ai_pred["source"] == ["triage"]:
                                ai_pred["source"] = ["triage"]
                            else:
                                ai_pred["source"] = ["ai_suggestions", "triage"]
                            found_duplicate = True
                            break  # Stop searching once a duplicate is found

                    if not found_duplicate:
                        # If the code is unique, add the suggestion to ai_preds with its original source
                        ai_preds["entities"][key].append({**suggestion, "text": suggestion.get("code_value"),
                                                          "source": ["triage"]})
            return ai_preds


        except Exception as exc:
            msg = "Failed to get merge_suggestions :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)

    def execute_function(self, message, start_time):
        try:
            conversation_id = message.get("care_req_id")
            req_type = message.get("req_type")
            file_path = message.get("file_path")
            chunk_no = message.get("chunk_no")
            retry_count = message.get("retry_count")
            webhook_url = message.get("webhook_url")
            api_type = message.get("api_type")
            api_path = message.get("api_path")
            triage_ai_preds = None
            triage_ai_suggestion = None
            text = None
            merged_segments = []
            language_counts = {}
            conversation_datas = None
            dominant_language = None

            if req_type == "encounter":
                conversation_id = message.get("request_id")
                conversation_datas = s3.get_files_matching_pattern(
                    pattern=f"{conversation_id}/{conversation_id}_*json")
            else:
                # Check if call is from platform
                if api_type == "clinical_notes":
                    conversation_datas = [s3.get_json_file(s3_filename=f"{conversation_id}/{conversation_id}.json")]
                elif api_type in {"ai_pred", "soap"}:
                    input_text = s3.get_json_file(s3_filename=file_path)
                    text = input_text.get("transcript")
                    dominant_language = input_text.get("language")

            if conversation_datas:
                for conversation_data in conversation_datas:
                    merged_segments += conversation_data["segments"]
                    # Count the occurrence of each language
                    language = conversation_data["language"]
                    if language in language_counts:
                        language_counts[language] += 1
                    else:
                        language_counts[language] = 1

                # Calculate total number of conversations to find 80% threshold
                total_conversations = len(conversation_datas)
                threshold_80_percent = total_conversations * 0.8

                # Check if any language other than "en" meets the 80% threshold
                for language, count in language_counts.items():
                    if language != "en" and count >= threshold_80_percent:
                        dominant_language = language
                        break

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
                    "diagnoses": [],
                    "procedures": [],
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

            if merged_segments or text:
                if merged_segments:
                    text = " ".join([_["text"] for _ in merged_segments])

                if dominant_language and dominant_language != "en":
                    text = self.translate_transcript_open_ai(text, dominant_language)
                    transcript_data = {"transcript": text, "language": "en"}
                    s3.upload_to_s3(f"{conversation_id}/translated_transcript.json", transcript_data, is_json=True)

                triage_key = f"{conversation_id}/triage_ai_suggestion.json"
                if s3.check_file_exists(triage_key):
                    triage_ai_suggestion = s3.get_json_file(triage_key)

                if "clinical_ner" in api_path:
                    extracted_info = self.get_preds_from_open_ai(text)
                    extracted_info = self.clean_pred(extracted_info)
                    if extracted_info:
                        details = extracted_info.get("details", {})
                        if details:
                            for k, v in details.items():
                                if k in entities:
                                    if entities[k]["text"] is None:
                                        entities[k]["text"] = v
                                    # else:
                                    #     entities[k]["text"] += ", " + v

                                    entities[k]["value"] = entities[k]["text"]

                        all_texts_and_types = []
                        for k in ["medications", "symptoms", "diagnoses", "surgeries", "tests"]:
                            if isinstance(extracted_info.get(k), str):
                                v = extracted_info.get(k, "").replace(" and ", " , ").split(",")
                                for _ in v:
                                    if _.strip():
                                        all_texts_and_types.append((_.strip(), k))
                            elif isinstance(extracted_info.get(k), list):
                                v = extracted_info.get(k, "")
                                for _ in v:
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
                                text_to_codes[text] = {"name": code["name"], "code": code["code"],
                                                       "score": code.get("score")}

                        for k in ["medications", "symptoms", "diagnoses", "surgeries", "tests"]:
                            if isinstance(extracted_info.get(k), str):
                                v = extracted_info.get(k, "").replace(" and ", " , ").split(",")
                                v = [
                                    {
                                        "text": _.strip(),
                                        "code": text_to_codes.get(_.strip(), {}).get("code", None),
                                        "code_value": text_to_codes.get(_.strip(), {}).get("name", None),
                                        "code_type": "",
                                        "confidence": text_to_codes.get(_.strip(), {}).get("score", None),
                                        "source": ["ai_suggestions"]
                                    }
                                    for _ in v
                                    if _.strip()
                                ]
                                if k == "tests":
                                    entities["entities"]["procedures"] = v
                                else:
                                    entities["entities"][k] = v
                            elif isinstance(extracted_info.get(k), list):
                                v = extracted_info.get(k, "")
                                val = [
                                    {
                                        "text": _.strip(),
                                        "code": text_to_codes.get(_.strip(), {}).get("code", None),
                                        "code_value": text_to_codes.get(_.strip(), {}).get("name", None),
                                        "code_type": "",
                                        "confidence": text_to_codes.get(_.strip(), {}).get("score", None),
                                        "source": ["ai_suggestions"]
                                    }
                                    for _ in v
                                    if _.strip()
                                ]
                                if k == "tests":
                                    entities["entities"]["procedures"] = val
                                else:
                                    entities["entities"][k] = val
                else:
                    extracted_info = self.get_preds_from_clinicl_ner(text)
                    if extracted_info:
                        for k in ["medication", "symptom", "diagnoses", "surgeries", "procedures", "age", "gender"]:
                            if isinstance(extracted_info.get(k), str):
                                value = extracted_info.get(k, None)
                                entities[k] = {
                                    "text": value,
                                    "value": value,
                                    "unit": None
                                }
                            elif isinstance(extracted_info.get(k), list):
                                values_list = extracted_info.get(k, [])
                                if values_list:
                                    if k == "symptom":
                                        k = "symptoms"
                                    elif k == "medication":
                                        k = "medications"
                                    entities["entities"][k] = [
                                        {
                                            "text": val.get("text"),
                                            "code": val.get("code"),
                                            "code_value": val.get("code_value"),
                                            "code_type": val.get("type"),
                                            "confidence": val.get("confidence"),
                                            "source": ["ai_suggestions"]
                                        }
                                        for val in values_list
                                    ]

                # if entities:
                #     current_segment["ai_preds"] = entities
                # else:
                #     current_segment["ai_preds"] = None
                entities = self.clean_null_entries(entities)

                # merge procedures and surgeries
                try:
                    entities = self.merge_procedures_surgeries(entities)
                except:
                    pass

                if triage_ai_suggestion:
                    try:
                        # merge suggestions
                        entities = self.merge_suggestions(entities, triage_ai_suggestion)
                    except:
                        pass
                else:
                    try:
                        # rmeove duplicates ai_preds
                        entities = self.remove_duplicates_ai_preds(entities)
                    except:
                        pass

                s3.upload_to_s3(f"{conversation_id}/ai_preds.json", entities, is_json=True)

                if api_type in {"clinical_notes", "soap"}:
                    data = {
                        "es_id": f"{conversation_id}_SOAP",
                        "chunk_no": chunk_no,
                        "file_path": file_path,
                        "webhook_url": webhook_url,
                        "api_path": api_path,
                        "api_type": api_type,
                        "req_type": req_type,
                        "executor_name": "SOAP_EXECUTOR",
                        "state": "Analytics",
                        "retry_count": 0,
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
                elif api_type == "ai_pred":
                    self.create_delivery_task(message=message)

        except Exception as exc:
            msg = "Failed to get AI PREDICTION :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)
            data = {
                "es_id": f"{conversation_id}_AI_PRED",
                "file_path": file_path,
                "webhook_url": webhook_url,
                "api_path": api_path,
                "api_type": api_type,
                "req_type": req_type,
                "executor_name": "AI_PRED",
                "state": "AiPred",
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
            if retry_count <= 2:
                retry_count += 1
                data["retry_count"] = retry_count
                producer.publish_executor_message(data)
            else:
                response_json = {"request_id": conversation_id,
                                 "status": "Failed"}
                merged_json_key = f"{conversation_id}/All_Preds.json"
                s3.upload_to_s3(merged_json_key, response_json, is_json=True)
                # if api_type == "clinical_notes":
                data["failed_state"] = "AiPred"
                self.create_delivery_task(data)

    def string_to_dict(self, input_string):
        # Initialize an empty dictionary
        result = {}
        details = {}

        # Define the keys to be nested inside 'details'
        detail_keys = ["age_years", "gender", "height_cm", "weight_kg", "ethnicity",
                       "substanceAbuse", "bloodPressure", "pulseRate", "respiratoryRate",
                       "bodyTemperature_fahrenheit"]

        # Split the input string into lines
        lines = input_string.split(',\n')

        # Process each line
        for line in lines:
            if "{" in line:
                continue
            if "}" in line:
                continue
            # Split the line into key and value
            key, value = line.split(': ')

            # Clean up the key and value strings
            key = key.strip().replace('"', '')

            # Handle multiple values and 'not found' cases
            if ', ' in value:
                value = [item.strip().replace('"', '') for item in value.replace(" and ", " , ").split(', ')]
            else:
                value = value.strip().replace('"', '')

            # Check if the key should be nested inside 'details'
            if key in detail_keys:
                if key == "height_cm":
                    details["height"] = value
                elif key == "age_years":
                    details["age"] = value
                elif key == "weight_kg":
                    details["weight"] = value
                elif key == "bodyTemperature_fahrenheit":
                    details["bodyTemperature"] = value
                elif key == "pulseRate":
                    details["pulse"] = value
                else:
                    details[key] = value
            else:
                if key == "orders":
                    result["tests"] = value
                else:
                    result[key] = value

        # Add the 'details' dictionary to the result
        result['details'] = details

        return result

    def get_preds_from_clinicl_ner(self, transcript_text, min_length=30):
        try:
            transcript_text = transcript_text.strip()
            if not transcript_text or len(transcript_text) <= min_length:
                raise Exception("Transcript text is too short")

            headers = {
                'Content-Type': 'application/json'
            }
            payload = json.dumps({"text": transcript_text})
            response = requests.post(heconstants.AI_SERVER + "/v0/ai_codes", headers=headers, data=payload)
            if response.status_code == 200:
                response = json.loads(response.text)
                entities = response.get("results_grouped_by_type")
                age = response.get("realage")
                if age:
                    entities["age"] = age
                gender = response.get("gender")
                if gender:
                    entities["gender"] = gender
                if entities:
                    return entities
            else:
                return None
        except Exception as exc:
            msg = "Failed to get Clinical NER PREDICTION :: {}".format(exc)
            logger.error(msg)

    def get_preds_from_open_ai(self,
                               transcript_text,
                               function_list=heconstants.faster_clinical_info_extraction_functions,
                               min_length=30,
                               ):
        try:
            transcript_text = transcript_text.strip()
            if not transcript_text or len(transcript_text) <= min_length:
                raise Exception("Transcript text is too short")

            template = """
            "medications": <text>,
            "symptoms": <text>,
            "diagnoses": <text>,
            "surgeries": <text>,
            "orders": <text>,
            "age_years": <text>,
            "gender": <text>,
            "height_cm": <text>,
            "weight_kg": <text>,
            "ethnicity": <text>,
            "substanceAbuse": <text>,
            "bloodPressure": <text>,
            "pulseRate": <text>,
            "respiratoryRate": <text>,
            "bodyTemperature_fahrenheit": <text>
            """

            # if triage_ai_preds:
            #     messages = [
            #         {
            #             "role": "system",
            #             "content": """Don't make assumptions about what values to plug into functions. return not
            #             found if you can't find the information. You are acting as an expert clinical entity
            #             extractor. Extract the described information from given clinical notes, consultation
            #             transcript or PATIENT PROVIDER CONVERSATION, including AI TRIAGE CONVERSATION. No
            #             extra information or hypothesis not present in the given text should be added. Separate items
            #             with , wherever needed. All the text returned should be present in the given TEXT. no new
            #             text should be returned.""",
            #         },
            #         {
            #             "role": "system",
            #             "content": f"Use given template to return the response : {template}",
            #         },
            #         {"role": "user", "content": f"AI TRIAGE CONVERSATION:\n {triage_ai_preds} \n\nPATIENT PROVIDER "
            #                                     f"CONVERSATION:\n {transcript_text}"},
            #     ]
            # else:

            messages = [
                {
                    "role": "system",
                    "content": """Don't make assumptions about what values to plug into functions. return not 
                                    found if you can't find the information. You are acting as an expert clinical entity 
                                    extractor. Extract the described information from given clinical notes, consultation 
                                    transcript or PATIENT PROVIDER CONVERSATION. No extra information or hypothesis not present 
                                    in the given text should be added. Separate items with , wherever needed. All the text 
                                    returned should be present in the given TEXT. no new text should be returned.""",
                },
                {
                    "role": "system",
                    "content": """Return not found if you can't find the information for mentioned fields with expected 
                    type : age_years (years), gender (male or female), height_cm (cm), weight_kg (kg), 
                    and substanceAbuse (yes or no)""",
                },
                {
                    "role": "system",
                    "content": f"Use given template to return text response : {template}",
                },
                {"role": "user", "content": f"PATIENT PROVIDER CONVERSATION:\n {transcript_text}"},
            ]

            for model_name in ["gpt-3.5-turbo-0613", "gpt-3.5-turbo-16k-0613", "gpt-4-0613"]:
                try:
                    response = openai.ChatCompletion.create(
                        model=model_name,
                        messages=messages,
                        # functions=function_list,
                        # function_call={"name": "ClinicalInformation"},
                        temperature=0.6,
                    )
                    extracted_info = response.choices[0]["message"]["content"]
                    converted_info = self.string_to_dict(extracted_info)
                    logger.info(f"converted_info :: {converted_info}")
                    # extracted_info = json.loads(
                    #     response.choices[0]["message"]["function_call"]["arguments"]
                    # )
                    return converted_info

                except Exception as ex:
                    logger.error(ex)
                    pass
        except Exception as exc:
            msg = "Failed to get OPEN AI PREDICTION :: {}".format(exc)
            logger.error(msg)

    def translate_transcript_open_ai(self,
                                     transcript_text,
                                     language,
                                     min_length=30,
                                     ):
        try:
            transcript_text = transcript_text.strip()
            if not transcript_text or len(transcript_text) <= min_length:
                raise Exception("Transcript text is too short")

            messages = [
                {
                    "role": "system",
                    "content": f"""Please translate the following conversation from {language} to English.""",
                },
                {"role": "user", "content": f"{transcript_text}"},
            ]

            for model_name in ["gpt-3.5-turbo-0613", "gpt-3.5-turbo-16k-0613", "gpt-4-0613"]:
                try:
                    response = openai.ChatCompletion.create(
                        model=model_name,
                        messages=messages,
                    )
                    translated_text = response.choices[0]["message"]["content"]
                    logger.info(f"translated_text :: {translated_text}")
                    # extracted_info = json.loads(
                    #     response.choices[0]["message"]["function_call"]["arguments"]
                    # )
                    return translated_text

                except Exception as ex:
                    logger.error(ex)
                    pass
        except Exception as exc:
            msg = "Failed to get translate conversation from OPEN AI :: {}".format(exc)
            logger.error(msg)

    def create_delivery_task(self, message):
        try:
            request_id = message.get("request_id")
            chunk_no = message.get("chunk_no")
            file_path = message.get("file_path")
            webhook_url = message.get("webhook_url")
            req_type = message.get("req_type")
            api_type = message.get("api_type")
            api_path = message.get("api_path")
            retry_count = message.get("retry_count")
            failed_state = message.get("failed_state")

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
                "failed_state": failed_state,
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


if __name__ == "__main__":
    ai_pred = aiPreds()
    #     stream_key = "6618452a-fef3-42dd-8a0b-52a2224c5f0b"
    #     start_time = datetime.utcnow()
    #     message = {
    #         "es_id": f"{stream_key}_AI_PRED",
    #         "chunk_no": 2,
    #         "file_path": f"{stream_key}/{stream_key}_chunk2.wav",
    #         "api_path": "asr",
    #         "api_type": "asr",
    #         "req_type": "encounter",
    #         "executor_name": "AI_PRED",
    #         "state": "AiPred",
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
    #     message = {
    #         "es_id": f"{stream_key}_AI_PRED",
    #         "file_path": "6618452a-fef3-42dd-8a0b-52a2224c5f0b/6618452a-fef3-42dd-8a0b-52a2224c5f0b.wav",
    #         "webhook_url": "https://webhook.site/28942651-2973-4a2f-8219-a42689715833",
    #         "api_path": "asr",
    #         "api_type": "asr",
    #         "req_type": "paltform",
    #         "executor_name": "AI_PRED",
    #         "state": "AiPred",
    #         "retry_count": 0,
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
    #     ai_pred.execute_function(message=message, start_time=datetime.utcnow())
    # transcript_text = "Hello, my name's Vinod Patel. I'm one of the GP registrars here. So I'm just going to find out a little bit about the problem that you've come in with. Would that be alright? Oh yeah, that's fine. I'm just going to make some notes and basically this will just help me write it up onto the computer later on. So just in your own words, tell me what's brought you in today. Well, I've been getting some diarrhoea really. Yeah, for the last sort of, well, two or three weeks. Okay, so before two or three weeks no problems really? So before that no no I mean I know I've just been going normally which is once every couple of days or something yeah no no problems normally. Okay so just tell me a little bit more about the diarrhea what it's like and things like that. so like what my poo looks like so I think okay and so that's it's quite right it's runnier yeah looser than normal I don't think there's any change in that color or anything um and I probably um but but I'm just going a lot more often can I just check do you have any blood in it oh um gosh yes I'm surprised I haven't said that already it's worrying me um yeah that I've had um for a couple of a couple of days And is it difficult to flush away at all? No, no, no, it's not difficult to flush away. And do you ever see any food that's not digested properly in it? No, that wouldn't be something, no. So you said diarrhoea, but how many times a day does it actually happen? Well, I would say somewhere between Well at the moment probably somewhere like yesterday was probably about eight times. Eight times? Oh dear. I mean I don't think it's been like that every day for the last three weeks. But up to eight times a day? Up to eight times, yeah. Do you have to get up at night to go to the toilet? Yes, yeah. Oh dear. Yeah and I've never had to do that before. You're losing sleep over it? Oh yeah, yeah. And do you have any tummy pain at all? Yes, that's quite crampy, mainly just before I go to the toilet, but it can be other times. But then does that pain go away once you've been to the toilet? Yeah, a little bit, I would say so, a little bit. And does anything make the pain worse at all? um I was eating I do not that I can really think of. You point it to your tummy exactly where is it? It is it's just sort of around the middle really. Yeah and how do you describe that pain? Um I sort of it's sort of crampy. Crampy is what he said isn't it yeah and how bad is it? Um I don't know. If ten was excruciating and one was very little pain then where would you put it? I've had worse, so probably about four. So it's not agonizing, but it certainly is okay. And you've told me about when it comes on and what makes it a little bit better and worse as well, which is good. I'm just going to ask the rest of the questions just about the whole gut itself. Do you have any difficulty chewing your food at all? Oh, no, no. No mouth ulcers or anything like that? No. Any difficulty swallowing your food at all? No. Do you ever get indigestion? No. Not really? sometimes, maybe on a weekend sometimes. But not usually? No, not usually. So this was only about three weeks ago that you've had the problem? Yes. And prior to that, what was your bowel habit like? I don't go that often really, maybe once a day, once every two days. Once a day, once every two days, but certainly no diarrhoea, a normal form still? No, it's quite harder. But no blood, not black at all? Okay, that's great. Thank you very much for that. So just want to ask you some questions about the other systems of the body. So do you get any headaches, fits, faints, blackouts, anything like that? Yeah, I occasionally get headaches, but everyone gets headaches, don't they? I've been getting them for a few years. A few years, yeah. And they're not getting worse at all or anything like that? No. Okay, that's good, that's good. Any problems with breathing? Shortness of breath, cough, anything like that? No. Any chest pain? No. Do you have a feeling of your heart having extra beats? No. Any swelling of the ankles? No. Any difficulty breathing at night? No. Nothing like that at all. Okay, you've told me quite a bit about your tummy. So any muscle aches and pains at all? I sometimes get weakness in my arm with a headache. Alright, so you get weakness, but how long does that weakness last for? Oh, only about an hour. Okay, any skin problems at all? I've had a little bit of a rash that comes and goes a bit. Do you know what that's due to? No. But no joint problems? No, no joint problems. Any problems with the waterworks at all? No. Can you tell me about your .s, if you don't mind? Yeah, well, I'm on the pill actually. I don't know what it's called, but the one, I take it for three weeks and then I have a week off. So I get my . in that week. So the regular? Oh, yeah, yeah. And they haven't changed at all? No, no, I've been on that for years. So just a final check on your symptoms, so you've told me there's a little bit of blood in the diarrhoea. Yeah. Okay, and is that every time you have the diarrhoea? Oh, it's only been the last couple of days, that's what's worried me. Okay. And it's, I would, no, probably not every time. But you're not coughing up blood, nothing like that? Oh, no, no. Okay, no lumps and bumps anywhere? Not that I'm aware of, no. What about weight loss and changing appetite? I think I probably have lost weight actually in the last few weeks. My trousers definitely feel a bit looser. You don't know how much weight you've lost? I don't really weigh myself, so no I don't. Yeah, it's sort of overlapped a bit now. Okay, so a little bit of weight loss anyway. Okay, so that's fine. Now let's move on to your past medical history. Any operations in the past that you've had? Yeah, I did. When I was sort of in my teens, probably I think I was about 15, I had my appendix removed. In your teens, okay, but nothing since then? No new operations since then? No. Okay. You said that you take a little bit of paracetamol for your headaches? Yes. Okay. And are you doing any of the medicines at all? Just the pill, yeah. No, no, nothing else. Nothing else at all, okay. And nothing else over the counter? No, just paracetamol for my headaches. Any recreational drugs at all? No. Okay. And are you allergic to anything? Oh, yes. I'm allergic to amoxicillin. So is that penicillin? Yeah. So allergic to penicillin. Okay. What happens when you have penicillin? Oh, that's what I get. That's when, you know, talk about a rash, I get a rash. Oh, I see. So that's a rash. Well, no, but yeah. Okay. So a rash with penicillin. So do you avoid penicillin? Yeah, yeah, absolutely. I've only had it once. Well, as far as I'm aware, I've only had it once. I checked that you've had only the appendix operation but no other serious illnesses. No, oh, I am, well, no, not serious, no. Okay, that's fine, thank you. So I just want to move on to your social history, if you don't mind. Can I ask you, are you working at the moment? Yeah, yeah, I'm a teaching assistant. All right, do you enjoy it? Yeah, I do, actually, yeah. The kids can be a little bit of a nightmare sometimes, but, you know, on the whole, I really like it. So how's this condition impacting on your work? Well, I haven't really known where to go in, to be honest. Because if it's an infection, I don't want to pass it on to the children. So actually, I haven't been going in the last couple of weeks. So you're worried about the fact that you might pass it on to someone. Yeah, well, if it's an infection, yeah. Because you're supposed to be clear, aren't you, for like 48 hours? 48 hours, yeah. That's usual. So I don't really know what to do about that, because I've gone past, you know. And do you smoke at all? No, no. You've never smoked? Oh, I have, yeah. I smoked at college, yeah. How many did you use to smoke? About ten a day. And how many years was that for? I smoked for about three years. Three years, okay. That's your smoking. What about alcohol? Do you drink alcohol at all? Not that much, really. I drink on the weekends. So what would your typical intake be in a week, do you think? In the week probably nothing but then at the weekend one night I tend to go out with my friends or my partner and so I might get through a bottle of wine and maybe a couple of shots or something. A whole bottle of wine? Yeah, I think so, probably over the course of the evening. And then on the other night, we might just have a couple of glasses or share a bottle of wine. I mean, that sounds as if it's more than about 20 units, roughly. Right. Because a bottle of wine would be about eight, nine units. And then you've got the other half bottle and a few shots. So that's about 20 units, which is excessive, I have to say. And the fact that you have more than what we call six units, which is just over half a bottle in a single session, can cause disbenefits to your health, as it were, can harm your health in the future. We may need to talk about that. Would that be all right? Yeah, yeah. I mean, obviously, I don't want to, you know, that's why I'm here. Sure, sure. We'll concentrate on the main problem. So who's with you at home? Yeah, so I live with my partner, Sam. Okay, no particular problems there at all? No, no, no. Okay. About three years. Is there any family history of note? No, not that I'm aware of anyway. No gut conditions in the family? My aunt, my aunt maybe. My aunt had something to do with her tummy, but I don't know what it was really. Okay. It wasn't cancer. So just moving towards the end, I just wanted to explore your ideas, concerns and expectations really. So what do you think is going on with this problem? Well, I think it could be an infection. An infection, yeah. And you've told me about one of your concerns about passing it on to people at work and especially the children, I guess. Yeah, yeah. But any other concerns? Will they pass on to me? Yes. My friend's sister has got bowel cancer and she's only 30. Oh dear. And so it's just the bleeding really more than anything that's worried me. So I wondered about that really. I mean, I'm glad you told me that. We'll try and reassure you and let's see what turns out in your case. What do you think we need to do today to help you? Well, I wondered if I might need some tests. I mean, I'm really hoping, because I did have a little look on the internet and I'm really hoping it's not going to be one of the cameras, I think it's colonoscopy or something like that. Yeah, I'm hoping I don't have to have that. Sure, sure, sure. Okay, I think I've got everything there. Is there anything else that you want to tell me? Actually, I've just remembered, I forgot to ask you about travel. Have you had any travel in the last few months or weeks even? No, no, I haven't been away anywhere. Okay, and you can't distinctly say that this is due to a particular food that you've had? No. And nobody else has had this problem in your family? No, no, no. Just wanted to check that. Yeah. Okay, so is there anything else that you want to tell me about this problem? No, I did go travelling about three years ago to Africa. I worked in South Africa and up into Zimbabwe. Were you well there though? Yeah, I was well. Do you have any questions for me? No. How likely is it that I'm going to have a camera? Well, I think initially we'll probably run some blood tests and do the stool sample and make sure there's no infection. Then I'll probably need to get a gut specialist to have a look at you if it hasn't got better in a matter of weeks. And then we'll take it from there. But let me just summarize what's been happening there. So you've had this problem for about two or three weeks. It is distinct diarrhea around up to eight times a day, sometimes overnight as well. You have to get up to go. You've got cramping abdominal pain, tummy pain, and you've got some blood in the motions as well. There's no real past medical history apart from a little bit of migraine, you said, and the appendix. You don't smoke at all now. alcohol you know we may need to talk about it later on a little bit too much there I think but you're worried about this because one of your somebody you know had bowel cancer and you're a bit concerned about an endoscopy as well but would it be alright if we ran the blood test today and did the sample and then I can see you again. Oh yeah I want to get it. And then maybe I can see you again next week. Yeah absolutely. Okay right then well thank you very much. Thank you."
    # ai_pred.get_preds_from_open_ai(transcript_text, triage_ai_preds=None)
