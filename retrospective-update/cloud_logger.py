import datetime
import os
import warnings

import requests


class CloudLog:
    """
    Posts logging messages to a given webhook URL (e.g., Microsoft Teams)
    """
    start_time: str
    webhook_post_url: str
    summary_attributes: dict = {}

    def __init__(self, webhook_url: str = None) -> None:
        self.webhook_post_url = webhook_url if webhook_url is not None else os.getenv('WEBHOOK_URL')
        if self.webhook_post_url is None or self.webhook_post_url == '':
            warnings.warn("No webhook URL provided. No logging will be performed.")

    def set_summary_attribute(self, **kwargs):
        self.summary_attributes.update(**kwargs)

    def error(self, *args):
        self.ping('Error', *args)

    def log(self, *args):
        self.ping(*args)

    def ping(self, *args) -> None:
        all_args = [datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'), ] + list(args)
        message_json = {'text': '\n'.join(all_args)}

        if not self.webhook_post_url:
            print(message_json["text"])
            return

        try:
            response = requests.post(
                self.webhook_post_url,
                headers={"Content-Type": "application/json"},
                json=message_json,
                timeout=10
            )

            if response.status_code != 200:
                warnings.warn(f"Failed to post to webhook: {response.status_code}, {response.text}")

        except requests.exceptions.RequestException as e:
            warnings.warn(f"Error sending message to webhook: {e}")
