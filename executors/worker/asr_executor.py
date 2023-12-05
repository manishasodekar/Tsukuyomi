import fnmatch
import json
import logging
import os
from io import BytesIO
from typing import Optional

import av
import time

import boto3
import requests
from botocore.exceptions import NoCredentialsError

from utils import heconstants
from utils.s3_operation import S3SERVICE
from config.logconfig import get_logger

s3 = S3SERVICE()

class ASRExecutor:
    def __init__(self):
        self.AUDIO_DIR = "AUDIOS"
        self.logger = get_logger()

    def get_files_matching_pattern(self, pattern, bucket_name: Optional[str] = None):
        json_data_list = []
        credentials = {
            'aws_access_key_id': heconstants.AWS_ACCESS_KEY,
            'aws_secret_access_key': heconstants.AWS_SECRET_ACCESS_KEY
        }
        s3_client = boto3.client('s3', 'us-east-2', **credentials)
        try:
            if bucket_name is None:
                bucket_name = "healiom-asr"
            # Extract the prefix from the pattern (up to the first wildcard)
            prefix = pattern.split('*')[0]

            # Paginate through results if there are more files than the max returned in one call
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    # Filter the objects whose keys match the pattern and read each JSON file
                    for obj in page['Contents']:
                        if fnmatch.fnmatch(obj['Key'], pattern):
                            try:
                                response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                                file_content = response['Body'].read().decode('utf-8')
                                json_data = json.loads(file_content)
                                json_data_list.append(json_data)
                            except NoCredentialsError:
                                print("Credentials not available for file:", obj['Key'])
                            except s3_client.exceptions.ClientError as e:
                                print(f"An error occurred with file {obj['Key']}: {e}")

            return json_data_list
        except Exception as exc:
            self.logger.error(str(exc))
            return []
        except NoCredentialsError:
            print("Credentials not available")
        except s3_client.exceptions.ClientError as e:
            print(f"An error occurred: {e}")
            return []


    def get_audio_video_duration_and_extension(self, file_path):
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

    def execute_function(self, message):
        try:
            file_path = message.get("file_path")
            user_name = message.get("user_name")
            conversation_id = message.get("care_req_id")
            force_summary = message.get("force_summary", False)
            api_key = message.get("api_key")
            received_at = time.time()
            # previous_conversation_ids_datas = []
            previous_conversation_ids_datas = self.get_files_matching_pattern(
                pattern=f"{conversation_id}/{conversation_id}_*json")
            self.logger.info(f"previous_conversation_ids_datas :: {previous_conversation_ids_datas}")

            total_duration_until_now = 0
            if previous_conversation_ids_datas:
                total_duration_until_now = sum(
                    [v["duration"] for v in previous_conversation_ids_datas]
                )

            self.logger.info(f"total_duration_until_now :: {total_duration_until_now}")

            # if len(user_name.split()) > 1 or len(conversation_id.split()) > 1:
            #     raise Exception("Invalid user_name or conversation_id")

            conversation_directory = f"{self.AUDIO_DIR}/{conversation_id}"

            try:
                os.makedirs(conversation_directory, exist_ok=True)
            except:
                pass

            audio_path = os.path.join(conversation_directory, file_path.split("/")[1])
            self.logger.info(f"audio_path :: {audio_path}")

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
                    "conversation_id": conversation_id,
                    "user_name": user_name,
                    "duration": duration,
                    "segments": current_segments,
                    "ai_preds": None,
                    "success": True,
                    "audio_path": audio_path,
                    "language": language
                    }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)
        except:
            # esquery
            data = {"received_at": received_at,
                    "conversation_id": conversation_id,
                    "user_name": user_name,
                    "duration": duration,
                    "segments": current_segments,
                    "ai_preds": {},
                    "success": False,
                    "audio_path": audio_path,
                    "language": language,
                    }
            s3.upload_to_s3(file_path.replace("wav", "json"), data, is_json=True)


# if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
