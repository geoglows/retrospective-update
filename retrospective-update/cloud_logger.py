import datetime
import os
import warnings

import requests


class CloudLog():
    """
    Posts logging messages to a give webhook URL
    """
    start_time: str
    webhook_post_url: str
    summary_attributes: dict = {}

    def __init__(self, webhook_url: str = None) -> None:
        """
        Initializes the CloudLog object.
        """
        self.webhook_post_url = os.getenv('WEBHOOK_URL', webhook_url)
        self.start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if self.webhook_post_url is None or self.webhook_post_url == '':
            warnings.warn("No webhook URL provided. No logging will be performed.")

    def set_summary_attribute(self, **kwargs):
        """
        Sets summary attributes for the log.
        """
        self.summary_attributes.update(**kwargs)

    def ping(self, status: str, message: str = None) -> None:
        if not self.webhook_post_url:
            return
        if 'error' in status.lower():
            message = f"Error:{message}"
        requests.post(f"{self.webhook_post_url}?task=weekly-retro-update&status={message}")
