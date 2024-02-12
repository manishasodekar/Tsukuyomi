import io
import logging
import os
import subprocess
import traceback
from datetime import datetime
import av
import time
import wave
from io import BytesIO

import requests
import yt_dlp
from pydub import AudioSegment
from utils import heconstants
from utils.s3_operation import S3SERVICE
from utils.send_logs import push_logs
from services.kafka.kafka_service import KafkaService
from config.logconfig import get_logger

s3 = S3SERVICE()
producer = KafkaService(group_id="filedownloader")
logger = get_logger()
logger.setLevel(logging.INFO)


class fileDownloader:

    def __init__(self):
        self.s16_resampler = av.AudioResampler(format="s16", rate="16000", layout="mono")

    def retry_with_backoff(self, function, max_attempts=3):
        for attempt in range(max_attempts):
            try:
                return function()
            except Exception as e:
                if attempt == max_attempts - 1:
                    raise e
                backoff_time = 0.1
                time.sleep(backoff_time)

    def yield_chunks_from_rtmp_stream(
            self, stream_key, user_type, stream_url=heconstants.RTMP_SERVER_URL
    ):
        rtmp_stream = None
        current_position = None
        just_reconnected = False

        # Current position in the stream based on the latest packet PTS received

        def reconnect_to_stream():
            nonlocal just_reconnected, rtmp_stream
            rtmp_stream = self.retry_with_backoff(
                lambda: av.open(stream_url + stream_key, format="flv", timeout=10)
            )
            logger.info(f"Connection to stream :: {rtmp_stream}")
            just_reconnected = True  # Set the flag to indicate that we have just reconnected

        reconnect_to_stream()

        if rtmp_stream is None:
            logger.info("No RTMP stream found")
            return None

        try:
            bytes_buffer = BytesIO()
            try:
                aac_audio = next((s for s in rtmp_stream.streams if s.type == 'audio'), None)
            except Exception as e:
                logger.error(f"An unexpected error occurred reading rtmp_stream {e}")

            if aac_audio is None:
                logger.error(f"An unexpected error occurred aac_audio {e}")
                raise av.AVError("No audio stream found in RTMP stream.")

            s16_destination = av.open(bytes_buffer, mode="w", format="wav")
            s16_stream = s16_destination.add_stream("pcm_s16le", rate=16000, layout="mono")

            def demux_aac_audio():
                return rtmp_stream.demux(aac_audio)

            while True:
                try:
                    for packet in self.retry_with_backoff(demux_aac_audio):
                        if just_reconnected:
                            # If we just reconnected and the packet's PTS is not ahead of the current position, skip it
                            if current_position is not None and packet.pts <= current_position:
                                continue
                            just_reconnected = False
                        current_position = packet.pts  # Store the PTS to allow checking on reconnection
                        # Packet processing and yielding bytes from the encoded packet...
                        for decoded_packet in packet.decode():
                            for resampled_packet in self.s16_resampler.resample(decoded_packet):
                                for encoded_packet in s16_stream.encode(resampled_packet):
                                    yield bytes(encoded_packet)

                except av.AVError as e:  # Catch specific PyAV exceptions here
                    logger.error(f"PyAV exceptions: {e}")
                    if rtmp_stream:
                        rtmp_stream.close()
                        rtmp_stream = None
                    time.sleep(2)  # Wait before reconnecting
                    self.retry_with_backoff(reconnect_to_stream)
                    if rtmp_stream is None:
                        logger.error("Reconnection failed")
                        break  # Implement this function
                    else:
                        logger.info(f"PyAV rtmp_stream: {rtmp_stream}")
                        push_logs(care_request_id=stream_key,
                                  given_msg="Livestream started (RTMP)",
                                  he_type=user_type,
                                  req_type="rtmp_restart",
                                  source_type="backend")
                    continue  # Continue the loop after reconnection

                except av.error.OSError as e:  # Catch specific PyAV exceptions here
                    if "Input/output error" in str(e):
                        logger.error("Input/output error. Reconnecting...")
                        logger.error(f"PyAV exceptions: {e}")
                        if rtmp_stream:
                            rtmp_stream.close()
                            rtmp_stream = None
                        time.sleep(2)  # Wait before reconnecting
                        self.retry_with_backoff(reconnect_to_stream)
                        if rtmp_stream is None:
                            logger.error("Reconnection failed")
                            break
                        else:
                            logger.info(f"PyAV rtmp_stream: {rtmp_stream}")
                            push_logs(care_request_id=stream_key,
                                      given_msg="Livestream started (RTMP)",
                                      he_type=user_type,
                                      req_type="rtmp_restart",
                                      source_type="backend")
                        continue  # Continue the loop after reconnection

                except Exception as e:
                    logger.error(f"Connection lost: {e}")
                    if rtmp_stream:
                        rtmp_stream.close()
                        rtmp_stream = None
                    time.sleep(2)  # Wait before reconnecting
                    self.retry_with_backoff(reconnect_to_stream)
                    if rtmp_stream is None:
                        logger.error("Reconnection failed")
                        break  # Implement this function
                    else:
                        logger.info(f"rtmp_stream: {rtmp_stream}")
                        push_logs(care_request_id=stream_key,
                                  given_msg="Livestream started (RTMP)",
                                  he_type=user_type,
                                  req_type="rtmp_restart",
                                  source_type="backend")
                    continue  # Continue the loop after reconnection

            s16_destination.close()
        except Exception as e:
            logger.error(f"An unexpected error occurred  {e}")
            time.sleep(10)
            push_logs(care_request_id=stream_key,
                      given_msg="Livestream stopped (RTMP)",
                      he_type=user_type,
                      req_type="rtmp_stop",
                      source_type="backend")
            return None

    def save_rtmp_loop(self,
                       message,
                       start_time,
                       stream_url=heconstants.RTMP_SERVER_URL,
                       DATA_DIR="healiom_websocket_asr",
                       ):
        try:
            stream_key = message.get("care_req_id")
            user_type = message.get("user_type")
            file_path = message.get("file_path")
            retry_count = message.get("retry_count")
            logger.info("Received rtmp stream")
            push_logs(care_request_id=stream_key,
                      given_msg="Livestream started (RTMP)",
                      he_type=user_type,
                      req_type="rtmp_start",
                      source_type="backend")
            rtmp_iterator = self.yield_chunks_from_rtmp_stream(stream_key, user_type, stream_url)

            if rtmp_iterator is not None:
                started = False
                chunk_count = 1
                frames_per_chunk = 16000 * heconstants.chunk_duration  # 5 seconds of frames at 16000 Hz
                bytes_per_frame = 2  # Assuming 16-bit audio (2 bytes per frame)

                while True:
                    chunk_start_time = time.time()
                    chunk_start_datetime = datetime.utcnow()
                    wav_buffer = io.BytesIO()
                    WAV_F = wave.open(wav_buffer, "wb")
                    WAV_F.setnchannels(1)
                    WAV_F.setsampwidth(2)
                    WAV_F.setframerate(16000)

                    frames_written = 0

                    for byte_data in rtmp_iterator:
                        if not started:
                            data = {"stream_key": stream_key,
                                    "last_processed_end_time": 0,
                                    "stage": "rtmp_saving_started"}
                            s3_file = f"{stream_key}/{stream_key}.json"
                            if not s3.check_file_exists(s3_file):
                                s3.upload_to_s3(s3_file, data, is_json=True)
                            logger.info(f"Writing chunks started :: {stream_key}")
                            started = True

                        WAV_F.writeframes(byte_data)
                        frames_written += len(byte_data) // bytes_per_frame

                        current_time = time.time()

                        # reading chunks for 2 sec + frames written should be more than 2 sec
                        # if current_time - chunk_start_time >= heconstants.chunk_duration and \
                        #         frames_written >= frames_per_chunk:
                        #     break

                        # reading chunks for 2 sec
                        if current_time - chunk_start_time >= heconstants.chunk_duration:
                            break

                    WAV_F.close()
                    key = f"{stream_key}/{stream_key}_chunk{chunk_count}.wav"
                    wav_buffer.seek(0)  # Reset buffer pointer to the beginning
                    # Upload the finished chunk to S3
                    s3.upload_to_s3(key, wav_buffer.read())
                    data = {
                        "es_id": f"{stream_key}_ASR_EXECUTOR",
                        "chunk_no": chunk_count,
                        "file_path": key,
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
                        "start_time": str(chunk_start_datetime),
                        "end_time": str(datetime.utcnow()),
                    }
                    producer.publish_executor_message(data)

                    chunk_count += 1
                    if current_time - chunk_start_time < heconstants.chunk_duration:
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
            if retry_count <= 2:
                retry_count += 1
                data = {
                    "es_id": f"{stream_key}_FILE_DOWNLOADER",
                    "file_path": file_path,
                    "api_path": "clinical_notes",
                    "api_type": "clinical_notes",
                    "req_type": "encounter",
                    "executor_name": "FILE_DOWNLOADER",
                    "state": "Init",
                    "retry_count": retry_count,
                    "uid": None,
                    "request_id": stream_key,
                    "care_req_id": stream_key,
                    "encounter_id": None,
                    "provider_id": None,
                    "review_provider_id": None,
                    "completed": False,
                    "exec_duration": 0.0,
                    "start_time": str(start_time),
                    "end_time": str(datetime.utcnow()),
                }
                producer.publish_executor_message(data)

    def convert_to_wav(self, input_file):
        try:
            output_file = input_file.replace('.tmp', '.wav')

            # command = ['ffmpeg', '-i', input_file, '-ac', '1', '-ar', '16000', output_file]
            # subprocess.run(command, check=True)

            audio = AudioSegment.from_file(input_file)
            audio = audio.set_channels(1)
            audio = audio.set_frame_rate(16000)
            audio.export(output_file, format='wav')

            return output_file

        except Exception as e:
            logger.error(f"An unexpected error occurred convert_to_wav {e}")

    def download_file(self, message, start_time):
        try:
            request_id = message.get("request_id")
            file_path = message.get("file_path")
            retry_count = message.get("retry_count")
            user_type = message.get("user_type")
            webhook_url = message.get("webhook_url")
            api_type = message.get("api_type")
            api_path = message.get("api_path")

            if "youtube.com" in file_path or "youtu.be" in file_path:
                local_filename = f"tmp/{request_id}"  # YouTube videos are downloaded as mp4
                ydl_opts = {
                    'format': 'bestaudio/best',
                    'outtmpl': local_filename + '.%(ext)s',
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'wav',
                        'preferredquality': '192',
                    }],
                    'postprocessor_args': [
                        '-ac', '1',  # Set audio to mono (single channel)
                        '-ar', '16000'  # Set sample rate to 16k
                    ],
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([file_path])
            else:
                local_filename = f"tmp/{request_id}.tmp"
                with requests.get(file_path, stream=True) as r:
                    r.raise_for_status()
                    with open(local_filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

            if "youtube.com" in file_path or "youtu.be" in file_path:
                wav_filename = local_filename + ".wav"
            else:
                wav_filename = self.convert_to_wav(local_filename)
            s3_path = f"{request_id}/{request_id}.wav"
            s3.upload_to_s3(s3_filename=s3_path, data=open(wav_filename, "rb"))

            try:
                if os.path.exists(local_filename):
                    os.remove(local_filename)
            except PermissionError:
                logger.error("Permission denied: You don't have permission to delete this file.")
            except OSError as e:
                logger.error(f"Error:: {e.strerror}")

            try:
                if os.path.exists(wav_filename):
                    os.remove(wav_filename)
            except PermissionError:
                logger.error("Permission denied: You don't have permission to delete this file.")
            except OSError as e:
                logger.error(f"Error:: {e.strerror}")

            data = {
                "es_id": f"{request_id}_ASR_EXECUTOR",
                "file_path": s3_path,
                "webhook_url": webhook_url,
                "api_path": api_path,
                "api_type": api_type,
                "req_type": "platform",
                "user_type": "Provider",
                "executor_name": "ASR_EXECUTOR",
                "state": "SpeechToText",
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

        except Exception as e:
            logger.error(f"An unexpected error occurred  {e}")
            data = {
                "es_id": f"{request_id}_FILE_DOWNLOADER",
                "file_path": file_path,
                "webhook_url": webhook_url,
                "api_path": api_path,
                "api_type": api_type,
                "req_type": "platform",
                "user_type": "Provider",
                "executor_name": "FILE_DOWNLOADER",
                "state": "Init",
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
            if retry_count <= 2:
                retry_count += 1
                data["retry_count"] = retry_count
                producer.publish_executor_message(data)
            else:
                response_json = {"request_id": request_id,
                                 "status": "Failed"}
                merged_json_key = f"{request_id}/All_Preds.json"
                s3.upload_to_s3(merged_json_key, response_json, is_json=True)
                data["failed_state"] = "Init"
                self.create_delivery_task(data)
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     fileDownloader().save_rtmp_loop("123456", "patient")
