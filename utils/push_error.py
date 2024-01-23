import json
import traceback

import requests
import logging
import heconstants
from config.logconfig import get_logger
import datetime
from sentryutils import SentryUtilFunctions

logger = get_logger()


class PushErrorToSlack:

    def send_message(self, error_message, input_used, slack_url=heconstants.SLACK_URL):
        try:
            dumps_input = json.dumps(input_used)
            error_time = str(datetime.datetime.utcnow()).split('.')
            data_to_send = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"Something Went Wrong @ {error_time[0]} V1",
                            "emoji": True
                        }
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f'*_TRACEBACK:_* {error_message}'
                            }
                        ]
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f'*_PAYLOAD:_* {dumps_input}'
                            }
                        ]
                    }
                ]
            }
            headers = {'Content-Type': "application/json"}
            response = requests.post(slack_url, data=json.dumps(data_to_send), headers=headers)
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)

        except Exception as exc:
            msg = "Failed to send error to slack channel :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            SentryUtilFunctions().send_event(exc, trace)
            return msg, 500

    def send_provider_notif(self, message_to_send):
        try:
            dumps_input = json.dumps(message_to_send)
            error_time = str(datetime.datetime.utcnow()).split('.')
            data_to_send = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f" hit @ {error_time[0]}",
                            "emoji": True
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": dumps_input
                            }
                        ]
                    }]}
            headers = {'Content-Type': "application/json"}
            response = requests.post(heconstants.SLACK_URL, data=json.dumps(data_to_send), headers=headers)
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)

        except Exception as exc:
            msg = "Failed to send error to slack channel :: {}".format(exc)
            logger.error(msg)
            trace = traceback.format_exc()
            SentryUtilFunctions().send_event(exc, trace)
            return msg, 500

    def push_notifications(self, message, user_id):
        try:
            dumps_input = json.dumps(message)
            error_time = str(datetime.datetime.utcnow()).split('.')
            data_to_send = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f" Notification for {user_id} has been sent !! @ {error_time[0]}",
                            "emoji": True
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": dumps_input
                            }
                        ]
                    }]}

            headers = {'Content-Type': "application/json"}
            response = requests.post(heconstants.NOTIFICATIONS_SLACK_URL, data=json.dumps(data_to_send),
                                     headers=headers)
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)

        except Exception as exc:
            msg = "Failed to push_notifications to slack channel :: {}".format(exc)
            self.logger.error(msg)
            trace = traceback.format_exc()
            SentryUtilFunctions().send_event(exc, trace)
            return msg, 500

    def push_commmon_messages(self, header, given_context, slack_url):
        try:
            dumps_input = json.dumps(given_context)
            data_to_send = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": header,
                            "emoji": True
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": dumps_input
                            }
                        ]
                    }]}

            headers = {'Content-Type': "application/json"}
            response = requests.post(slack_url, data=json.dumps(data_to_send),
                                     headers=headers)
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)

        except Exception as exc:
            msg = "Failed to push_notifications to slack channel :: {}".format(exc)
            self.logger.error(msg)
            trace = traceback.format_exc()
            SentryUtilFunctions().send_event(exc, trace)
            return msg, 500
