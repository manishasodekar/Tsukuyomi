import io
import logging
import re
import traceback
import uuid
import wave
from concurrent import futures
from datetime import datetime
import time
from typing import Optional
import grpc
import requests
import transcription_service_pb2 as pb2
import transcription_service_pb2_grpc as pb2_grpc
from config.logconfig import get_logger
from services.kafka.kafka_service import KafkaService
from utils import heconstants
from utils.s3_operation import S3SERVICE

pattern = re.compile(
    r'(?:\b(?:thanks|thank you|you|bye|yeah|beep|okay|peace)\b[.!?,-]*\s*){2,}',
    re.IGNORECASE)
word_pattern = re.compile(r'\b(?:Thank you|Bye|You)\.')

s3 = S3SERVICE()
producer = KafkaService(group_id="grpc")

logger = get_logger()
logger.setLevel(logging.INFO)


class TranscriptionService(pb2_grpc.TranscriptionServiceServicer):
    def generate_request_id(self):
        return str(uuid.uuid4())

    def get_merge_ai_preds(self, conversation_id, only_transcribe: Optional[bool] = False):
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
            response_json = {}

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
                    ai_preds_file_path = f"{conversation_id}/ai_preds.json"
                    if s3.check_file_exists(ai_preds_file_path):
                        merged_ai_preds = s3.get_json_file(ai_preds_file_path)
                        summary_file = f"{conversation_id}/soap.json"
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
                        response_json["ai_preds"] = merged_ai_preds

                response_json["meta"] = audio_metas
                response_json["success"] = True

                if merged_segments:
                    response_json["transcript"] = " ".join([_["text"] for _ in merged_segments])

                if s3.check_file_exists(key=f"{conversation_id}/translated_transcript.json"):
                    translated_transcript_content = s3.get_json_file(
                        s3_filename=f"{conversation_id}/translated_transcript.json")
                    if translated_transcript_content:
                        response_json["translated_transcript"] = translated_transcript_content.get("transcript")

                return response_json

            return {"success": False,
                    "text": "Transcription not found"}

        except Exception as exc:
            msg = "Failed to merge ai preds :: {}".format(exc)
            trace = traceback.format_exc()
            print(msg, trace)

    def StreamTranscription(self, request_iterator, context):
        logger.info("Received request")
        stream_key = self.generate_request_id()
        logger.info(f"Request ID :: {stream_key}")
        transcript = ""
        if request_iterator is not None:
            chunk_count = 1
            frames_per_chunk = 16000 * 2  # N seconds of frames at 16000 Hz
            bytes_per_frame = 2  # Assuming 16-bit audio (2 bytes per frame

            iterations = 0
            wav_buffer_combined = io.BytesIO()
            WAV_F_combined = wave.open(wav_buffer_combined, "wb")
            WAV_F_combined.setnchannels(1)
            WAV_F_combined.setsampwidth(2)
            WAV_F_combined.setframerate(16000)
            combined_frames = 0
            chunk_start_time_10s = time.time()
            while True:
                chunk_start_time = time.time()
                wav_buffer = io.BytesIO()
                WAV_F = wave.open(wav_buffer, "wb")
                WAV_F.setnchannels(1)
                WAV_F.setsampwidth(2)
                WAV_F.setframerate(16000)
                frames_written = 0

                for request in request_iterator:
                    byte_data = request.audio_content
                    WAV_F.writeframes(byte_data)
                    frames_written += len(byte_data) // bytes_per_frame

                    WAV_F_combined.writeframes(byte_data)
                    combined_frames += len(byte_data) // bytes_per_frame
                    current_time = time.time()
                    if current_time - chunk_start_time >= heconstants.quick_loop_chunk_duration:
                        # if not self.is_speech_present(byte_data, model, get_speech_ts):
                        iterations += 1
                        break

                WAV_F.close()
                key = f"{stream_key}/{stream_key}_chunk{chunk_count}.wav"
                wav_buffer.name = key.split("/")[1]
                wav_buffer.seek(0)  # Reset buffer pointer to the beginning

                # logger.info(f"sending chunks for transcription :: {key}")
                transcription_result = requests.post(
                    heconstants.AI_SERVER + "/transcribe/infer",
                    files={"f1": wav_buffer},
                ).json()["prediction"][0]
                # print("transcription_result", transcription_result)
                segments = transcription_result.get("segments")
                if segments:
                    text = segments[0].get("text")
                    if text:
                        if transcript != "":
                            transcript += " " + text
                            transcript = pattern.sub('', transcript)
                            transcript = word_pattern.sub('', transcript)
                        else:
                            transcript = text
                            transcript = pattern.sub('', transcript)
                            transcript = word_pattern.sub('', transcript)
                if transcript:
                    transcript = re.sub(' +', ' ', transcript).strip()
                transcript_key = f"{stream_key}/transcript.json"
                transcript_data = {"transcript": transcript}
                s3.upload_to_s3(transcript_key, transcript_data, is_json=True)
                yield pb2.TranscriptionResult(cc=transcript, conversation_id=stream_key, success=True)

                if iterations >= 1:
                    WAV_F_combined.close()
                    wav_buffer_combined.seek(0)
                    chunk_audio_key = f"{stream_key}/{stream_key}_chunk{chunk_count}.wav"
                    s3.upload_to_s3(chunk_audio_key, wav_buffer_combined.read())
                    data = {
                        "es_id": f"{stream_key}_ASR_EXECUTOR",
                        "chunk_no": chunk_count,
                        "file_path": chunk_audio_key,
                        "api_path": "clinical_notes",
                        "api_type": "clinical_notes",
                        "req_type": "encounter",
                        "executor_name": "ASR_EXECUTOR",
                        "state": "SpeechToText",
                        "retry_count": 0,
                        "uid": None,
                        "request_id": stream_key,
                        "care_req_id": stream_key,
                        "encounter_id": None,
                        "provider_id": None,
                        "review_provider_id": None,
                        "completed": False,
                        "exec_duration": 0.0,
                        "start_time": str(datetime.utcnow()),
                        "end_time": str(datetime.utcnow()),
                    }
                    producer.publish_executor_message(data)
                    iterations = 0
                    wav_buffer_combined = io.BytesIO()
                    WAV_F_combined = wave.open(wav_buffer_combined, "wb")
                    WAV_F_combined.setnchannels(1)
                    WAV_F_combined.setsampwidth(2)
                    WAV_F_combined.setframerate(16000)
                    combined_frames = 0
                    chunk_count += 1

                if current_time - chunk_start_time < heconstants.quick_loop_chunk_duration:
                    # Break the while loop if the last chunk duration is less than 5 seconds
                    break

    def FetchAIPredictions(self, request, context):
        response_data = self.get_merge_ai_preds(conversation_id=request.conversation_id,
                                                only_transcribe=request.only_transcribe)
        # Conversion to AIPredictionsResponse (simplified for brevity)
        print(f"AI Predictions Response Received :: {response_data}")
        if response_data:
            response = pb2.AIPredictionsResponse(
                age=self._convert_to_predictions(response_data["age"]),
                gender=self._convert_to_predictions(response_data["gender"]),
                height=self._convert_to_predictions(response_data["height"]),
                weight=self._convert_to_predictions(response_data["weight"]),
                bmi=self._convert_to_predictions(response_data["bmi"]),
                ethnicity=self._convert_to_predictions(response_data["ethnicity"]),
                insurance=self._convert_to_predictions(response_data["insurance"]),
                physicalActivityExercise=self._convert_to_predictions(response_data["physicalActivityExercise"]),
                bloodPressure=self._convert_to_predictions(response_data["bloodPressure"]),
                pulse=self._convert_to_predictions(response_data["pulse"]),
                respiratoryRate=self._convert_to_predictions(response_data["respiratoryRate"]),
                bodyTemperature=self._convert_to_predictions(response_data["bodyTemperature"]),
                substanceAbuse=self._convert_to_predictions(response_data["substanceAbuse"]),
                entities=pb2.Entities(
                    medications=[self._convert_to_entity_detail(med) for med in response_data["medications"]],
                    symptoms=[self._convert_to_entity_detail(sym) for sym in response_data["symptoms"]],
                    diagnoses=[self._convert_to_entity_detail(dia) for dia in response_data["diagnoses"]],
                    procedures=[self._convert_to_entity_detail(proc) for proc in response_data["procedures"]],
                    orders=[self._convert_to_entity_detail(order) for order in response_data["orders"]],
                ),
                summaries=pb2.Summaries(
                    subjectiveClinicalSummary=response_data["summaries"]["subjectiveClinicalSummary"],
                    objectiveClinicalSummary=response_data["summaries"]["objectiveClinicalSummary"],
                    clinicalAssessment=response_data["summaries"]["clinicalAssessment"],
                    carePlanSuggested=response_data["summaries"]["carePlanSuggested"]
                )
            )

            yield response

    def _convert_to_predictions(self, data):
        return pb2.Predictions(text=data["text"], value=data.get("value", 0), unit=data.get("unit", ""))

    def _convert_to_entity_detail(self, entity):
        return pb2.EntityDetail(
            text=entity["text"],
            code=entity["code"],
            code_value=entity["code_value"],
            code_type=entity["code_type"],
            confidence=entity["confidence"],
            source=entity["source"]
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_TranscriptionServiceServicer_to_server(TranscriptionService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
