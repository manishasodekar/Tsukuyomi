import json
import requests
from utils import heconstants
import logging

logger = logging.getLogger("push_logs")


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

    except Exception as e:
        logger.info(f"Couldn't push the log to ES :: {e}")
        pass


class pushLogs:
    pass
