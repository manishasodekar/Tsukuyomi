import fnmatch
import io
import logging
import re
import traceback
from datetime import datetime
from typing import Optional
import av
import time
import json
# import torch
# import torchaudio
import wave
import boto3
import requests
from io import BytesIO
from config.logconfig import get_logger
from botocore.exceptions import NoCredentialsError
from gevent import Timeout
from utils import heconstants
import transcription_service_pb2 as pb2

logger = get_logger()
logger.setLevel(logging.INFO)

s16_resampler = av.AudioResampler(format="s16", rate="16000", layout="mono")
s3_client = boto3.client('s3', aws_access_key_id=heconstants.AWS_ACCESS_KEY,
                         aws_secret_access_key=heconstants.AWS_SECRET_ACCESS_KEY)

# Load Silero VAD
# model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad', model='silero_vad', force_reload=False)
# (get_speech_ts, _, read_audio, *_) = utils

pattern = re.compile(
    r'(?:\b(?:thanks|thank you|you|bye|yeah|beep|okay|peace)\b[.!?,-]*\s*){2,}',
    re.IGNORECASE)
word_pattern = re.compile(r'\b(?:Thank you|Bye|You)\.')


class S3SERVICE:
    def __init__(self):
        self.default_bucket = heconstants.ASR_BUCKET

    def upload_to_s3(self, s3_filename, data, bucket_name: Optional[str] = None, is_json: Optional[bool] = False):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            if is_json:
                data = json.dumps(data).encode('utf-8')
            s3_client.put_object(Bucket=bucket_name, Key=s3_filename, Body=data)
            print(f"Upload Successful: {s3_filename}")
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def get_json_file(self, s3_filename, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_filename)
            file_content = s3_object['Body'].read().decode('utf-8')
            json_data = json.loads(file_content)
            return json_data
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def get_audio_file(self, s3_filename, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_filename)
            return s3_object
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def check_file_exists(self, key, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            s3_client.head_object(Bucket=bucket_name, Key=key)
            return True
        except s3_client.exceptions.ClientError:
            return False

    def get_files_matching_pattern(self, pattern, bucket_name: Optional[str] = None):
        json_data_list = []
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
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
            json_data_list.sort(key=lambda x: x['chunk_no'])
            return json_data_list
        except Exception as exc:
            self.logger.error(str(exc))
            return []
        except NoCredentialsError:
            print("Credentials not available")
        except s3_client.exceptions.ClientError as e:
            print(f"An error occurred: {e}")
            return []

    def list_files_in_directory(self, directory, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory)
            return [item['Key'] for item in response.get('Contents', [])]
        except Exception as e:
            print(f"Error listing files: {e}")

    def download_from_s3(self, key, local_path, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            s3_client.download_file(bucket_name, key, local_path)
            print(f"Download Successful: {local_path}")
        except Exception as e:
            print(f"Error downloading file: {e}")


s3 = S3SERVICE()


def is_speech_present(byte_data, model, get_speech_ts):
    try:
        # Convert byte data to tensor
        # tensor = read_audio(io.BytesIO(audio_data))
        tensor = torch.frombuffer(byte_data, dtype=torch.int16).float() / 32768.0
        tensor = tensor.unsqueeze(0)  # Add channel dimension
        speech_timestamps = get_speech_ts(tensor, model)
        return len(speech_timestamps) > 0
    except Exception as e:
        logger.error(f"VAD error :: {e}")


def save_grpc_loop(
        stream_key,
        request_iterator,
        stream_url=heconstants.RTMP_SERVER_URL,
        DATA_DIR="healiom_websocket_asr",
):
    try:
        transcript = ""
        logger.info(f"WS quick loop received grpc stream")

        stream_url = heconstants.RTMP_SERVER_URL
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
                        # If there's no speech in the current byte_data, break
                        # if not is_speech_present(byte_data, model, get_speech_ts):
                            # iterations += 1
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
                chunk_count += 1
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
                yield pb2.TranscriptionResult(cc=transcript, success=True)

                # if iterations >= 1:
                # if time.time() - chunk_start_time_10s >= heconstants.chunk_duration:
                # WAV_F_combined.close()
                # key = f"{stream_key}/{stream_key}_chunk{chunk_count}.wav"
                # wav_buffer_combined.name = key.split("/")[1]
                # wav_buffer_combined.seek(0)
                # # logger.info(f"sending chunks for transcription :: {key}")
                # transcription_result = requests.post(
                #     heconstants.AI_SERVER + "/transcribe/infer",
                #     files={"f1": wav_buffer_combined},
                # ).json()["prediction"][0]
                # print("10sec_transcription_result", transcription_result)
                # iterations = 0
                # wav_buffer_combined = io.BytesIO()
                # WAV_F_combined = wave.open(wav_buffer_combined, "wb")
                # WAV_F_combined.setnchannels(1)
                # WAV_F_combined.setsampwidth(2)
                # WAV_F_combined.setframerate(16000)
                # combined_frames = 0
                # chunk_start_time_10s = time.time()

                # WAV_F_combined.close()
                # wav_buffer_combined.seek(0)
                # chunk_audio_key = f"{stream_key}/{stream_key}_chunk{chunk_count}.wav"
                # s3.upload_to_s3(chunk_audio_key, wav_buffer_combined.read())
                # data = {
                #     "es_id": f"{stream_key}_ASR_EXECUTOR",
                #     "chunk_no": chunk_count,
                #     "file_path": chunk_audio_key,
                #     "api_path": "clinical_notes",
                #     "api_type": "clinical_notes",
                #     "req_type": "encounter",
                #     "executor_name": "ASR_EXECUTOR",
                #     "state": "SpeechToText",
                #     "retry_count": 0,
                #     "uid": None,
                #     "request_id": stream_key,
                #     "care_req_id": stream_key,
                #     "encounter_id": None,
                #     "provider_id": None,
                #     "review_provider_id": None,
                #     "completed": False,
                #     "exec_duration": 0.0,
                #     "start_time": str(datetime.utcnow()),
                #     "end_time": str(datetime.utcnow()),
                # }
                # producer.publish_executor_message(data)

                if current_time - chunk_start_time < heconstants.quick_loop_chunk_duration:
                    # Break the while loop if the last chunk duration is less than 5 seconds
                    break
        else:
            logger.info("rtmp_iterator IS NONE")

        # esquery
        logger.info("Stopped writing chunks")
        key = f"{stream_key}/{stream_key}.json"
        data = s3.get_json_file(key)
        if data:
            data["stage"] = "rtmp_saving_done"
            s3.upload_to_s3(key, data, is_json=True)

    except Exception as exc:
        msg = "Failed rtmp loop saver :: {}".format(exc)
        trace = traceback.format_exc()
        logger.error(msg, trace)
