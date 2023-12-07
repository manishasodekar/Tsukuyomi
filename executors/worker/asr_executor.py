import logging
import os
from datetime import datetime
import av
import time
import requests
from utils import heconstants
from utils.s3_operation import S3SERVICE
from config.logconfig import get_logger
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger

s3 = S3SERVICE()
producer = KafkaService(group_id="asr")
logger = get_logger()
logger.setLevel(logging.INFO)


class ASRExecutor:
    def __init__(self):
        self.AUDIO_DIR = "AUDIOS"

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
            logger.info(f"previous_conversation_ids_datas :: {previous_conversation_ids_datas}")

            total_duration_until_now = 0
            if previous_conversation_ids_datas:
                total_duration_until_now = sum(
                    [v["duration"] for v in previous_conversation_ids_datas]
                )

            logger.info(f"total_duration_until_now :: {total_duration_until_now}")

            # if len(user_name.split()) > 1 or len(conversation_id.split()) > 1:
            #     raise Exception("Invalid user_name or conversation_id")

            conversation_directory = f"{self.AUDIO_DIR}/{conversation_id}"

            try:
                os.makedirs(conversation_directory, exist_ok=True)
            except:
                pass

            audio_path = os.path.join(conversation_directory, file_path.split("/")[1])
            logger.info(f"audio_path :: {audio_path}")

            # audio_stream = BytesIO()
            if audio_path:
                # Read the object from S3 in chunks
                with open(audio_path, 'wb') as file:
                    s3_object = s3.get_audio_file(file_path)
                    stream = s3_object['Body']
                    while True:
                        chunk = stream.read(2048)
                        if not chunk:
                            break
                        # audio_stream.write(chunk)
                        file.write(chunk)
            else:
                raise Exception("No audio file found")

            try:
                file_type, duration, extension = self.get_audio_video_duration_and_extension(
                    audio_path
                )
            except:
                os.system(
                    f"ffmpeg -hide_banner -loglevel panic -y -i {audio_path} -acodec pcm_s16le -ac 1 -ar 16000 {audio_path}.wav"
                )

                audio_path = audio_path + ".wav"
                try:
                    file_type, duration, extension = self.get_audio_video_duration_and_extension(
                        audio_path
                    )
                    # logger.info("In exception")
                    # logger.info(f"file_type :: {file_type}")
                    # logger.info(f"duration :: {duration}")
                    # logger.info(f"extension :: {extension}")
                except:
                    # todo log exception
                    "", 0, os.path.splitext(audio_path)[1]

            if not audio_path.endswith(extension):
                os.rename(audio_path, audio_path + extension)
                audio_path = audio_path + extension

        except Exception as ex:
            raise ex

        try:
            # audio_stream.seek(0)
            # transcription_result = requests.post(
            #     heconstants.AI_SERVER + "/transcribe/infer",
            #     files={"f1": audio_stream},
            # ).json()["prediction"][0]
            # print("transcription_result ::", transcription_result)
            # todo change fixed ip to DNS
            transcription_result = requests.post(
                heconstants.AI_SERVER + "/transcribe/infer",
                files={"f1": open(audio_path, "rb")},
            ).json()["prediction"][0]
        except Exception as ex:
            print(ex)
            # esquery
            data = {
                "received_at": received_at,
                "conversation_id": conversation_id,
                "user_name": user_name,
                "duration": duration,
                "success": False,
                "audio_path": audio_path,
            }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)
            raise Exception("Transcription failed")

        current_segments = transcription_result["segments"]
        # logger.info(f"current_segments :: {current_segments}")
        for i in range(len(current_segments)):
            current_segments[i]["start"] = (
                    total_duration_until_now + current_segments[i]["start"]
            )
            current_segments[i]["end"] = (
                    total_duration_until_now + current_segments[i]["end"]
            )

        # logger.info(f"current_segments :: {current_segments}")
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
                    "audio_path": audio_path,
                    "language": language,
                    "retry_count": 0
                    }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)
            data = {
                "es_id": f"{conversation_id}_AI_PRED",
                "chunk_no": chunk_no,
                "file_path": file_path,
                "api_path": "asr",
                "api_type": "asr",
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
            # esquery
            data = {"received_at": received_at,
                    "chunk_no": chunk_no,
                    "conversation_id": conversation_id,
                    "user_name": user_name,
                    "duration": duration,
                    "segments": current_segments,
                    "ai_preds": {},
                    "success": False,
                    "audio_path": audio_path,
                    "language": language,
                    "retry_count": 0
                    }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)

            if retry_count <= 2:
                data = {
                    "es_id": f"{conversation_id}_ASR_EXECUTOR",
                    "chunk_no": chunk_no,
                    "file_path": file_path,
                    "api_path": "asr",
                    "api_type": "asr",
                    "req_type": "encounter",
                    "executor_name": "ASR_EXECUTOR",
                    "state": "SpeechToText",
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

# if __name__ == "__main__":
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
