import io
import logging
import os
import traceback
from datetime import datetime
import av
import time
import wave
from io import BytesIO
from utils import heconstants
from utils.es import Index
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
                       stream_key,
                       user_type,
                       start_time,
                       stream_url=heconstants.RTMP_SERVER_URL,
                       DATA_DIR="healiom_websocket_asr",
                       ):
        try:
            logger.info("Received rtmp stream")
            push_logs(care_request_id=stream_key,
                      given_msg="Livestream started (RTMP)",
                      he_type=user_type,
                      req_type="rtmp_start",
                      source_type="backend")
            rtmp_iterator = self.yield_chunks_from_rtmp_stream(stream_key, user_type, stream_url)

            try:
                # esquery
                query = {"bool": {"must": [{"match": {"stream_key": stream_key}}]}}
                resp = Index().search(search_query=query, source_include="file_paths")
                source = resp[0].get("_source")
                current_file_paths = source.get("file_paths")

                if not current_file_paths:
                    current_file_paths = []
            except:
                current_file_paths = []

            # if rtmp_iterator is not None:
            #     # todo need to move it to bucket for sometime
            #     # Appending chunks to file (filename: streamkey)
            #     wav_path = os.path.join(DATA_DIR, stream_key)
            #     wav_path = os.path.join(wav_path, f"{time.time()}.wav")
            #     os.makedirs(os.path.dirname(wav_path), exist_ok=True)
            #     WAV_F = wave.open(wav_path, "wb")
            #     WAV_F.setnchannels(1)
            #     WAV_F.setsampwidth(2)
            #     WAV_F.setframerate(16000)
            #
            #     started = False
            #
            #     chunk_start_time = datetime.utcnow()
            #     chunk_count = 1
            #     frames_written = 0
            #     frames_per_chunk = 16000 * 10  # 10 seconds of frames at 16000 Hz
            #
            #     # Write bytes from audio stream to valid wav file
            #     for byte_data in rtmp_iterator:
            #         if not started:
            #             data = {"stream_key": stream_key,
            #                     "last_processed_end_time": 0,
            #                     "file_paths": current_file_paths + [wav_path],
            #                     "stage": "rtmp_saving_started"}
            #             s3_file = f"{stream_key}/{stream_key}.json"
            #             if not s3.check_file_exists(s3_file):
            #                 s3.upload_to_s3(s3_file, data, is_json=True)
            #             logger.info(f"Writing chunks started :: {stream_key}")
            #             started = True
            #
            #         key = f"{stream_key}/{stream_key}_chunk{chunk_count}.wav"
            #
            #         WAV_F.writeframes(byte_data)
            #         frames_written += len(byte_data) // 2  # 2 bytes per frame
            #         # Check if the current chunk reached 10 seconds
            #         if frames_written >= frames_per_chunk:
            #             # Close the current chunk file
            #             WAV_F.close()
            #
            #             # Upload the finished chunk to S3
            #             with open(wav_path, 'rb') as f:
            #                 s3.upload_to_s3(key, f.read())
            #                 data = {
            #                     "es_id": f"{stream_key}_ASR_EXECUTOR",
            #                     "chunk_no": chunk_count,
            #                     "file_path": key,
            #                     "api_path": "asr",
            #                     "api_type": "asr",
            #                     "req_type": "encounter",
            #                     "executor_name": "ASR_EXECUTOR",
            #                     "state": "SpeechToText",
            #                     "retry_count": None,
            #                     "uid": None,
            #                     "request_id": stream_key,
            #                     "care_req_id": stream_key,
            #                     "encounter_id": None,
            #                     "provider_id": None,
            #                     "review_provider_id": None,
            #                     "completed": False,
            #                     "exec_duration": 0.0,
            #                     "start_time": str(chunk_start_time),
            #                     "end_time": str(datetime.utcnow()),
            #                 }
            #                 producer.publish_executor_message(data)
            #
            #             # Prepare for the next chunk
            #             chunk_count += 1
            #             frames_written = 0
            #             wav_path = os.path.join(DATA_DIR, f"{stream_key}/{stream_key}_chunk{chunk_count}.wav")
            #             WAV_F = wave.open(wav_path, "wb")
            #             WAV_F.setnchannels(1)
            #             WAV_F.setsampwidth(2)
            #             WAV_F.setframerate(16000)
            #
            #     # Don't forget to handle the last chunk if it's not exactly 10 seconds
            #     if frames_written > 0:
            #         WAV_F.close()
            #         with open(wav_path, 'rb') as f:
            #             s3.upload_to_s3(key, f.read())
            #             data = {
            #                 "es_id": f"{stream_key}_ASR_EXECUTOR",
            #                 "chunk_no": chunk_count,
            #                 "file_path": key,
            #                 "api_path": "asr",
            #                 "api_type": "asr",
            #                 "req_type": "encounter",
            #                 "executor_name": "ASR_EXECUTOR",
            #                 "state": "SpeechToText",
            #                 "retry_count": None,
            #                 "uid": None,
            #                 "request_id": stream_key,
            #                 "care_req_id": stream_key,
            #                 "encounter_id": None,
            #                 "provider_id": None,
            #                 "review_provider_id": None,
            #                 "completed": False,
            #                 "exec_duration": 0.0,
            #                 "start_time": str(chunk_start_time),
            #                 "end_time": str(datetime.utcnow()),
            #             }
            #             producer.publish_executor_message(data)
            # else:
            #     logger.info("rtmp_iterator IS NONE")

            if rtmp_iterator is not None:
                wav_path_base = os.path.join(DATA_DIR, stream_key)
                wav_path_base = os.path.join(wav_path_base, f"{time.time()}.wav")
                os.makedirs(wav_path_base, exist_ok=True)

                started = False
                chunk_count = 1
                frames_per_chunk = 16000 * 10  # 10 seconds of frames at 16000 Hz
                bytes_per_frame = 2  # Assuming 16-bit audio (2 bytes per frame)

                while True:
                    chunk_start_time = time.time()
                    chunk_start_datetime = datetime.utcnow()
                    # wav_path = os.path.join(wav_path_base, f"{chunk_count}.wav")
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
                        if current_time - chunk_start_time >= 10 and frames_written >= frames_per_chunk:
                            break

                    WAV_F.close()
                    key = f"{stream_key}/{stream_key}_chunk{chunk_count}.wav"
                    wav_buffer.seek(0)  # Reset buffer pointer to the beginning
                    # Upload the finished chunk to S3
                    # with open(wav_path, 'rb') as f:
                    s3.upload_to_s3(key, wav_buffer.read())
                    data = {
                        "es_id": f"{stream_key}_ASR_EXECUTOR",
                        "chunk_no": chunk_count,
                        "file_path": key,
                        "api_path": "asr",
                        "api_type": "asr",
                        "req_type": "encounter",
                        "executor_name": "ASR_EXECUTOR",
                        "state": "SpeechToText",
                        "retry_count": None,
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
                    if current_time - chunk_start_time < 10:
                        # Break the while loop if the last chunk duration is less than 10 seconds
                        break
            else:
                logger.info("rtmp_iterator IS NONE")

            # esquery
            logger.info("Stopped writing chunks")
            script_body = {"doc": {"stage": "rtmp_saving_done"}}
            Index().update(script_body=script_body, doc_id=stream_key)

            data = {
                "es_id": f"{stream_key}_FILE_DOWNLOADER",
                "api_path": "asr",
                "file_path": None,
                "api_type": "asr",
                "req_type": "encounter",
                "executor_name": "FILE_DOWNLOADER",
                "state": "Completed",
                "retry_count": None,
                "uid": None,
                "request_id": stream_key,
                "care_req_id": stream_key,
                "encounter_id": None,
                "provider_id": None,
                "review_provider_id": None,
                "completed": True,
                "exec_duration": 0.0,
                "start_time": str(start_time),
                "end_time": str(datetime.utcnow()),
            }
            producer.publish_executor_message(data)

        except Exception as exc:
            msg = "Failed rtmp loop saver :: {}".format(exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)
            data = {
                "es_id": f"{stream_key}_FILE_DOWNLOADER",
                "api_path": "asr",
                "file_path": None,
                "api_type": "asr",
                "req_type": "encounter",
                "executor_name": "FILE_DOWNLOADER",
                "state": "Failed",
                "retry_count": None,
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


if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileDownloader().save_rtmp_loop("123456", "patient")
