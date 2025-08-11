import datetime
import os

import requests


class CloudLog:
    """
    Posts logging messages to a given webhook URL (e.g., Microsoft Teams)
    """
    start_time: str
    log_url: str
    summary_attributes: dict = {}

    def __init__(self) -> None:
        self.log_url = os.getenv('WEBHOOK_LOG_URL', None)
        self.error_url = os.getenv('WEBHOOK_ERROR_URL', self.log_url)

    def set_summary_attribute(self, **kwargs):
        self.summary_attributes.update(**kwargs)

    def error(self, *args):
        all_args = [datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'), ] + list(args)
        message_json = {'text': '\n'.join(all_args)}
        self.ping(self.error_url, message_json)

    def log(self, *args):
        all_args = [datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'), ] + list(args)
        message_json = {'text': '\n'.join(all_args)}
        self.ping(self.log_url, message_json)

    def ping(self, url: str, message: dict) -> None:
        if not url:
            print(message["text"])
            return

        try:
            response = requests.post(
                self.log_url,
                headers={"Content-Type": "application/json"},
                json=message,
                timeout=10
            )

            if response.status_code != 200:
                print(f"Failed to post to webhook: {response.status_code}, {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Error sending message to webhook: {e}")
