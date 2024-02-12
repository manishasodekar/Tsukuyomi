from gevent import monkey

monkey.patch_all()

import os
import time
import gipc
import json
import requests
import traceback
from gevent.pywsgi import WSGIServer
from gevent import Timeout
from _ws import WebSocketHandler
import logging
import rtmp_saver
from utils import heconstants
from config.logconfig import get_logger
from utils.s3_operation import S3SERVICE
from services.kafka.kafka_service import KafkaService
from datetime import datetime

s3 = S3SERVICE()
producer = KafkaService(group_id="soap")
logger = get_logger()
logger.setLevel(logging.INFO)


def push_logs(care_request_id: str, given_msg: str, he_type: str, req_type: str, source_type: str):
    try:
        headers = {
            'Content-Type': 'application/json'
        }
        websocket_data = {
            "care_request_id": care_request_id,
            "he_type": he_type,
            "req_type": req_type,
            "message": given_msg,
            "source_type": source_type
        }
        response = requests.request("POST", heconstants.HEALIOM_SERVER + "/post_websocket_logs", headers=headers,
                                    data=json.dumps(websocket_data))

    except:
        pass


# check if PID is running python
def check_and_start_rtmp(connection_id):
    key = f"{connection_id}/{connection_id}.json"
    IS_FILE_EXIST = s3.check_file_exists(key)
    if IS_FILE_EXIST:
        current_stream_key_info = s3.get_json_file(f"{connection_id}/{connection_id}.json")
        if current_stream_key_info:
            state = current_stream_key_info.get("state")
            if state == "rtmp_saving_started":
                logger.info("Already running")
                return True
    else:
        data = {
            "es_id": f"{connection_id}_FILE_DOWNLOADER",
            "file_path": None,
            "api_path": "clinical_notes",
            "api_type": "clinical_notes",
            "req_type": "encounter",
            "user_type": "Provider",
            "executor_name": "FILE_DOWNLOADER",
            "state": "Init",
            "retry_count": 0,
            "uid": None,
            "request_id": connection_id,
            "care_req_id": connection_id,
            "encounter_id": None,
            "provider_id": None,
            "review_provider_id": None,
            "completed": False,
            "exec_duration": 0.0,
            "start_time": str(datetime.utcnow()),
            "end_time": str(datetime.utcnow()),
        }
        producer.publish_executor_message(data)
        return False


# check if PID is running python
def check_and_start_rtmp_for_connection_id(connection_id, user_type, ws):
    pid_for_connection_id = None
    data = None
    try:
        # esquery
        logger.info("searching pid")
        key = f"{connection_id}/{connection_id}.json"
        data = s3.get_json_file(key)
        pid_for_connection_id = data.get("pid")
    except:
        pass

    is_pid_running = False

    if pid_for_connection_id:
        try:
            # check if process with pid exists
            os.kill(pid_for_connection_id, 0)
            is_pid_running = False
        except OSError:
            is_pid_running = False
        else:
            is_pid_running = True

    if is_pid_running and pid_for_connection_id:
        logger.info("Already running rtmp saver loop")
        return True
    else:
        logger.info("Starting the rtmp saver loop")
        # start rtmp stream saver in background process using gipc
        process = gipc.start_process(
            target=rtmp_saver.save_rtmp_loop,
            args=(connection_id, user_type, ws),
        )
        if data:
            data["pid"] = process.pid
            s3.upload_to_s3(key, data, is_json=True)

        return False


def websocket_handler(env, start_response):
    if "wsgi.websocket" in env:
        ws = env["wsgi.websocket"]
        message = ws.receive()
        initial_timestamp = time.time()
        logger.info(f"message :: {message}")
        try:
            message = json.loads(message)
            connection_id = message["stream_key"]
            user_type = message["user_type"]
            uid = message.get("uid")
            push_logs(care_request_id=connection_id,
                      given_msg="Websocket has started",
                      he_type=user_type,
                      req_type="websocket_start",
                      source_type="backend")

            triage_ai_suggestion = message.get("triage_ai_suggestion", {})
            triage_key = f"{connection_id}/triage_ai_suggestion.json"
            if triage_ai_suggestion:
                s3.upload_to_s3(triage_key, triage_ai_suggestion, is_json=True)

            triage_ai_preds = message.get("ai_preds", None)
            triage_ai_preds_key = f"{connection_id}/triage_ai_preds.json"
            if triage_ai_preds or triage_ai_preds != "":
                triage_data = {"ai_preds": triage_ai_preds}
                s3.upload_to_s3(triage_ai_preds_key, triage_data, is_json=True)

            logger.info(f"ws :: {ws}")
            logger.info(f"Intializing trascription, coding, etc. :: {connection_id}")

        except Exception as ex:
            trace = traceback.format_exc()
            logger.error(f"Websocket received exception :: {ex} :: \n {trace}")
            push_logs(care_request_id=connection_id,
                      given_msg=f"Websocket received exception :: {ex} :: \n {trace}",
                      he_type=user_type,
                      req_type="websocket_stop",
                      source_type="backend")
            ws.send(json.dumps({"success": False, "message": str(ex)}))
            ws.close()
            return

        IS_RTMP_ALREADY_RUNNING = False
        if user_type in {"provider", "inclinic"}:
            IS_RTMP_ALREADY_RUNNING = check_and_start_rtmp(connection_id)
            IS_RTMP_ALREADY_RUNNING = check_and_start_rtmp_for_connection_id(
                connection_id, user_type, ws
            )

        logger.info(f"SENDING EMPTY AI PREDS TO WS :: {ws}")

        message = {
            "success": True,
            "uid": uid,
            "segments": [],
            "ai_preds": {}
        }

        # Modify 'ai_preds' only if 'triage_ai_suggestion' is available
        if triage_ai_suggestion:
            message["ai_preds"] = {"entities": triage_ai_suggestion}

        # Send the JSON message through the WebSocket connection
        ws.send(json.dumps(message))

        last_preds_sent_at = time.time()
        last_trans_sent_at = time.time()
        last_number_of_segments = 0
        last_ack_sent_at = time.time()

        while True:
            try:
                key = f"{connection_id}/{connection_id}.json"
                current_stream_key_info = s3.get_json_file(key)
                if user_type not in {"provider", "inclinic"}:
                    transcript_key = f"{connection_id}/transcript.json"
                    transcript = s3.get_json_file(transcript_key)
                    ws.send(json.dumps(
                        {
                            "cc": transcript.get("transcript", ""),
                            "success": True
                        }
                    ))
            except:
                time.sleep(2)
                continue

            if time.time() - last_ack_sent_at >= 1:
                ws.send(
                    json.dumps(
                        {
                            "success": True,
                            "uid": uid
                        }
                    )
                )
                last_ack_sent_at = time.time()

            if current_stream_key_info:
                current_stage = current_stream_key_info.get("stage")

                last_processed_end_time = current_stream_key_info.get(
                    "last_processed_end_time"
                )

                is_rtmp_done = current_stage == "rtmp_saving_done"
                if is_rtmp_done:
                    logger.info(f"current_stage: {current_stage}, is_rtmp_done: {is_rtmp_done}")
                    current_stream_key_info["stage"] = "finished"
                    s3.upload_to_s3(s3_filename=key, data=current_stream_key_info, is_json=True)
                    logger.info(f"finished AI rtmp: {connection_id}")
                    push_logs(care_request_id=connection_id,
                              given_msg=f"finished AI rtmp: {connection_id}",
                              he_type=user_type,
                              req_type="websocket_stop",
                              source_type="backend")
                    ws.close()
                try:
                    latest_ai_preds_resp = None
                    if time.time() - last_preds_sent_at >= 10:
                        ai_preds_resp = requests.get(
                            heconstants.SYNC_SERVER + f"/history?conversation_id={connection_id}"
                        )
                        if ai_preds_resp.status_code == 200:
                            latest_ai_preds_resp = json.loads(ai_preds_resp.text)
                        last_preds_sent_at = time.time()

                    elif time.time() - last_trans_sent_at >= 8:
                        ai_preds_resp = requests.get(
                            heconstants.SYNC_SERVER + f"/history?conversation_id={connection_id}&only_transcribe=True"
                        )
                        if ai_preds_resp.status_code == 200:
                            latest_ai_preds_resp = json.loads(ai_preds_resp.text)
                        last_trans_sent_at = time.time()

                    if latest_ai_preds_resp:
                        text = latest_ai_preds_resp.get("text")
                        is_transcript_not_ready = text == "Transcription not found"
                        if not is_transcript_not_ready:
                            try:
                                logger.info(f"SENDING AI PREDS TO WS :: {ws}")
                                # latest_ai_preds_resp["triage_ai_suggestion"] = triage_ai_suggestion
                                latest_ai_preds_resp["uid"] = uid
                                ws.send(json.dumps(latest_ai_preds_resp))
                                merged_json_key = f"{connection_id}/All_Preds.json"
                                s3.upload_to_s3(merged_json_key, latest_ai_preds_resp, is_json=True)
                                # with Timeout(2, False):  # Set the timeout to 2 seconds
                                #     message = ws.receive()
                                #     logger.info(f"ack :: {message}")

                            except Timeout:
                                logger.info("NO ACK RECEIVED CLOSED BY SERVER")
                                push_logs(care_request_id=connection_id,
                                          given_msg=f"Websocket has closed by server - NO ACK RECEIVED",
                                          he_type=user_type,
                                          req_type="websocket_stop",
                                          source_type="backend")
                                ws.close()
                                break

                            except Exception as ex:
                                trace = traceback.format_exc()
                                logger.error(f"CLOSED BY CLIENT :: {ex} :: \n {trace}")
                                push_logs(care_request_id=connection_id,
                                          given_msg=f"websocket has closed by client",
                                          he_type=user_type,
                                          req_type="websocket_stop",
                                          source_type="backend")
                                ws.close()
                                break

                except Exception as ex:
                    trace = traceback.format_exc()
                    logger.error(f"Error while sending latest AI PREDS: {ex} :: \n {trace}")
                    if is_rtmp_done:
                        ws.close()
                        break

            current_time = time.time()
            time_difference = current_time - initial_timestamp
            if time_difference > 1800:
                msg = "More than 30 minutes have passed since the recorded time."
                logger.info(msg)
                ws.send(json.dumps({"success": False, "issue": "time-exceeded", "message": msg}))
                ws.close()


if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    port = int(os.getenv("PORT", "1111"))
    host = os.getenv("HOST", "0.0.0.0")
    logger.info(f"host: {host}, port: {port}")
    server = WSGIServer((host, port), websocket_handler, handler_class=WebSocketHandler)
    server.serve_forever()
