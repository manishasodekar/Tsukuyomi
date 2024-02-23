import logging
import os
import traceback
from datetime import datetime
from io import BytesIO

import av
import time
import requests
from utils import heconstants
from utils.s3_operation import S3SERVICE
from pydub.utils import mediainfo
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger
from pydub import AudioSegment

s3 = S3SERVICE()
producer = KafkaService(group_id="asr")
logger = get_logger()
logger.setLevel(logging.INFO)


class ASRExecutor:
    def __init__(self):
        self.AUDIO_DIR = "AUDIOS"

    def get_wav_duration(self, wav_file_path):
        return float(mediainfo(wav_file_path)["duration"])

    def get_audio_video_duration_and_extension(self, file_path):
        try:
            container = av.open(file_path)

            ext = container.format.name

            if container.streams.video and container.streams.audio:
                file_type = "video"
            elif container.streams.audio:
                file_type = "audio"
            else:
                file_type = "other"

            duration = container.duration / av.time_base

            return file_type, duration, "." + ext
        except Exception as e:
            print(f"An error occurred get_audio_video_duration_and_extension file: {e}")

    def execute_function(self, message, start_time):
        try:
            file_path = message.get("file_path")
            user_name = message.get("user_name")
            chunk_no = message.get("chunk_no")
            conversation_id = message.get("care_req_id")
            retry_count = message.get("retry_count", 0)
            force_summary = message.get("force_summary", False)
            api_key = message.get("api_key")
            received_at = time.time()
            # previous_conversation_ids_datas = []
            previous_conversation_ids_datas = s3.get_files_matching_pattern(
                pattern=f"{conversation_id}/{conversation_id}_*json")

            total_duration_until_now = 0
            if previous_conversation_ids_datas:
                total_duration_until_now = sum(
                    [v["duration"] for v in previous_conversation_ids_datas]
                )

            logger.info(f"total_duration_until_now :: {total_duration_until_now}")

            # if len(user_name.split()) > 1 or len(conversation_id.split()) > 1:
            #     raise Exception("Invalid user_name or conversation_id")

            # conversation_directory = f"{self.AUDIO_DIR}/{conversation_id}"

            # try:
            #     os.makedirs(conversation_directory, exist_ok=True)
            # except:
            #     pass
            #
            # audio_path = os.path.join(conversation_directory, file_path.split("/")[1])
            logger.info(f"audio_path :: {file_path}")

            audio_stream = BytesIO()
            if file_path:
                # Read the object from S3 in chunks
                # with open(audio_path, 'wb') as file:
                s3_object = s3.get_audio_file(file_path)
                stream = s3_object['Body']
                while True:
                    chunk = stream.read(2048)
                    if not chunk:
                        break
                    audio_stream.write(chunk)
                    # file.write(chunk)
            else:
                raise Exception("No audio file found")

            # try:
            #     file_type, duration, extension = self.get_audio_video_duration_and_extension(
            #         audio_path
            #     )
            #     logger.info(f"duration :: {duration}")
            #
            # except:
            #     os.system(
            #         f"ffmpeg -hide_banner -loglevel panic -y -i {audio_path} -acodec pcm_s16le -ac 1 -ar 16000 {audio_path}.wav"
            #     )
            #
            #     audio_path = audio_path + ".wav"
            #     try:
            #         file_type, duration, extension = self.get_audio_video_duration_and_extension(
            #             audio_path
            #         )
            #     except:
            #         # todo log exception
            #         "", 0, os.path.splitext(audio_path)[1]
            #
            # if not audio_path.endswith(extension):
            #     os.rename(audio_path, audio_path + extension)
            #     audio_path = audio_path + extension

        except Exception as ex:
            raise ex

        try:
            audio_file = audio_stream
            # Read audio data with pydub
            audio = AudioSegment.from_file(audio_file, format="wav")
            duration_in_milliseconds = len(audio)
            duration = duration_in_milliseconds / 1000.0
            audio_stream.name = file_path.split("/")[1]
            audio_stream.seek(0)
            transcription_result = requests.post(
                heconstants.AI_SERVER + "/transcribe/infer",
                files={"f1": audio_stream},
            ).json()["prediction"][0]
            # todo change fixed ip to DNS
            # transcription_result = requests.post(
            #     heconstants.AI_SERVER + "/transcribe/infer",
            #     files={"f1": open(audio_path, "rb")},
            # ).json()["prediction"][0]

        except Exception as ex:
            logger.error(f"Something wrong with whisper API :: {ex}")
            msg = "Something wrong with whisper API :: {ex}"
            trace = traceback.format_exc()
            logger.error(msg, trace)
            # esquery
            data = {
                "received_at": received_at,
                "chunk_no": chunk_no,
                "conversation_id": conversation_id,
                "user_name": user_name,
                "duration": duration,
                "success": False,
                "audio_path": file_path,
            }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)
            raise Exception("Transcription failed")

        current_segments = transcription_result["segments"]
        for i in range(len(current_segments)):
            current_segments[i]["start"] = (
                    total_duration_until_now + current_segments[i]["start"]
            )
            current_segments[i]["end"] = (
                    total_duration_until_now + current_segments[i]["end"]
            )

        language = transcription_result["language"]

        try:
            data = {"received_at": received_at,
                    "chunk_no": chunk_no,
                    "conversation_id": conversation_id,
                    "user_name": user_name,
                    "duration": duration,
                    "segments": current_segments,
                    "ai_preds": None,
                    "success": True,
                    "audio_path": file_path,
                    "language": language,
                    "retry_count": 0
                    }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)
            data = {
                "es_id": f"{conversation_id}_AI_PRED",
                "chunk_no": chunk_no,
                "file_path": file_path,
                "api_path": "clinical_notes",
                "api_type": "clinical_notes",
                "req_type": "encounter",
                "executor_name": "AI_PRED",
                "state": "AiPred",
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

        except:
            data = {"received_at": received_at,
                    "chunk_no": chunk_no,
                    "conversation_id": conversation_id,
                    "user_name": user_name,
                    "duration": duration,
                    "segments": current_segments,
                    "ai_preds": {},
                    "success": False,
                    "audio_path": file_path,
                    "language": language,
                    "retry_count": 0
                    }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)

            if retry_count <= 2:
                retry_count += 1
                data = {
                    "es_id": f"{conversation_id}_ASR_EXECUTOR",
                    "chunk_no": chunk_no,
                    "file_path": file_path,
                    "api_path": "clinical_notes",
                    "api_type": "clinical_notes",
                    "req_type": "encounter",
                    "executor_name": "ASR_EXECUTOR",
                    "state": "SpeechToText",
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

    def speechToText(self, message, start_time):
        try:
            file_path = message.get("file_path")
            user_name = message.get("user_name")
            request_id = message.get("request_id")
            retry_count = message.get("retry_count", 0)
            webhook_url = message.get("webhook_url")
            api_key = message.get("api_key")
            api_type = message.get("api_type")
            api_path = message.get("api_path")

            received_at = time.time()

            audio_stream = BytesIO()
            s3_object = s3.get_audio_file(file_path)
            stream = s3_object['Body']
            while True:
                chunk = stream.read(2048)
                if not chunk:
                    break
                audio_stream.write(chunk)

            try:
                audio_file = audio_stream
                audio = AudioSegment.from_file(audio_file, format="wav")
                duration_in_milliseconds = len(audio)
                duration = duration_in_milliseconds / 1000.0
                audio_stream.name = file_path.split("/")[1]
                audio_stream.seek(0)
                transcription_result = requests.post(
                    heconstants.AI_SERVER + "/transcribe/infer",
                    files={"f1": audio_stream},
                ).json()["prediction"][0]

            except Exception as ex:
                data = {
                    "received_at": received_at,
                    "conversation_id": request_id,
                    "user_name": user_name,
                    "duration": duration,
                    "success": False,
                    "audio_path": file_path,
                }
                s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)
                logger.error(f"An unexpected error occurred in transcribe {request_id} :: {ex}")

            current_segments = transcription_result.get("segments")
            language = transcription_result.get("language")

            data = {"received_at": received_at,
                    "conversation_id": request_id,
                    "user_name": user_name,
                    "duration": duration,
                    "segments": current_segments,
                    "ai_preds": None,
                    "success": True,
                    "audio_path": file_path,
                    "language": language,
                    "retry_count": 0
                    }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)

            if api_type == "clinical_notes":
                data = {
                    "es_id": f"{request_id}_AI_PRED",
                    "file_path": file_path,
                    "webhook_url": webhook_url,
                    "api_path": api_path,
                    "api_type": api_type,
                    "req_type": "platform",
                    "executor_name": "AI_PRED",
                    "state": "AiPred",
                    "retry_count": 0,
                    "uid": None,
                    "request_id": request_id,
                    "care_req_id": request_id,
                    "encounter_id": None,
                    "provider_id": None,
                    "review_provider_id": None,
                    "completed": False,
                    "exec_duration": 0.0,
                    "start_time": str(start_time),
                    "end_time": str(datetime.utcnow()),
                }
                producer.publish_executor_message(data)
            elif api_type == "transcription":
                self.create_delivery_task(message=message)

        except Exception as ex:
            data = {"received_at": received_at,
                    "conversation_id": request_id,
                    "user_name": user_name,
                    "duration": duration,
                    "segments": current_segments,
                    "ai_preds": {},
                    "success": False,
                    "audio_path": file_path,
                    "language": language,
                    "retry_count": 0
                    }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)
            data = {
                "es_id": f"{request_id}_ASR_EXECUTOR",
                "file_path": file_path,
                "webhook_url": webhook_url,
                "api_path": api_path,
                "api_type": api_type,
                "req_type": "platform",
                "executor_name": "ASR_EXECUTOR",
                "state": "SpeechToText",
                "retry_count": retry_count,
                "uid": None,
                "request_id": request_id,
                "care_req_id": request_id,
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
                response_json = {"request_id": request_id,
                                 "status": "Failed"}
                merged_json_key = f"{request_id}/All_Preds.json"
                s3.upload_to_s3(merged_json_key, response_json, is_json=True)
                data["failed_state"] = "SpeechToText"
                self.create_delivery_task(data)

            logger.error(f"An unexpected error occurred speechtotext {request_id} :: {ex}")

    def create_delivery_task(self, message):
        try:
            request_id = message.get("request_id")
            chunk_no = message.get("chunk_no")
            file_path = message.get("file_path")
            webhook_url = message.get("webhook_url")
            req_type = message.get("req_type")
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

        except Exception as exc:
            msg = "Failed to create delivery task :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)

# if __name__ == "__main__":
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
