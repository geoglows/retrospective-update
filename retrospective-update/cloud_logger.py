import datetime
import os
import json
import sys
import time
import logging

import requests
import boto3
import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('log.log')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

PING_URL = os.getenv('PING_URL')

class CloudLog():
    """
    Class made to log messages to AWS Cloudwatch

    Attributes:
    - save_path (str): The path to save the log file.
    - start (str): The start time of the logging process.
    - last_date (str): The most recent date in the Zarr.
    - qinit (str): The Qinit used.
    - time_period (str): The time period of the logging process.
    - message (str): The log message.

    Methods:
    - __init__(self, ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION, save_path): Initializes the CloudLog object.
    - add_last_date(self, date): Adds the most recent date in the Zarr.
    - add_qinit(self, qinit): Adds the Qinit used.
    - add_time_period(self, time_range): Adds the time period of the logging process.
    - add_message(self, msg): Adds the log message.
    - clear(self): Clears the log attributes.
    - log_message(self, status, error): Logs the message to AWS Cloudwatch.

    """

    def __init__(self,
                 LOG_GROUP_NAME: str,
                 LOG_STREAM_NAME: str,
                 ACCESS_KEY_ID: str,
                 SECRET_ACCESS_KEY: str,
                 REGION: str,
                 save_path: str = 'log.json',
                 ):
        self.LOG_GROUP_NAME = LOG_GROUP_NAME
        self.LOG_STREAM_NAME = LOG_STREAM_NAME
        self.warn = False

        self.save_path = save_path
        self.start = datetime.datetime.now().ctime()
        self.last_date = None
        self.qinit = None
        self.time_period = None
        self.message = ''
        # Create a CloudWatch Logs client
        if not REGION:
            REGION = 'us-west-2'

        self.client = boto3.client(
            'logs',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
            region_name=REGION
        )

    def add_last_date(self, date) -> None:
        """
        Adds the last date to the logger.

        Parameters:
            date: The date to be added. Can be either a numpy datetime64 object or a string.

        Returns:
            None
        """
        if isinstance(date, np.datetime64):
            self.last_date = np.datetime_as_string(date, unit='h')
        else:
            self.last_date = str(date)

    def add_qinit(self, qinit: datetime.datetime) -> None:
        """
        Adds the initial query date to the logger.

        Args:
            qinit (datetime.datetime): The initial query date.

        Returns:
            None
        """
        self.qinit = qinit.strftime('%m/%d/%Y')

    def add_time_period(self, time_range: list[datetime.datetime]) -> None:
        """
        Adds a time period to the logger.

        Args:
            time_range (list[datetime.datetime]): A list of datetime objects representing the start and end time of the period.

        Returns:
            None
        """
        if not time_range:
            self.time_period = "No time period"
        elif len(time_range) == 1:
            self.time_period = time_range[0].strftime('%m/%d/%Y')
        else:
            self.time_period = f"{time_range[0].strftime('%m/%d/%Y')} to {time_range[-1].strftime('%m/%d/%Y')}"

    def add_message(self, msg) -> None:
        """
        Adds a message to the logger.

        Args:
            msg (str): The message to be added.

        Returns:
            None
        """
        self.message = str(msg)

    def clear(self):
        """
        Clears the logger by resetting all attributes to their initial values.
        """
        self.last_date = None
        self.qinit = None
        self.time_period = None
        self.message = ''

    def ping(self, status: str, message: str = None) -> None:
        self.log_message(status, message)
        if not PING_URL:
            return
        if 'error' in status.lower():
            message = f"Error:{message}"

        requests.post(f"{PING_URL}?task=weekly-retro-update&status={message}")
        pass


    def log_message(self, status: str, message: str = None) -> dict:
        """
        Logs a message to CloudWatch.

        Args:
            status (str): The status of the log message.
            error (Exception, optional): The error associated with the log message. Defaults to None.

        Returns:
            dict: The response from the CloudWatch API.
        """
        if message is not None:
            self.message = message
        log_message = {
            'Start time': self.start,
            'Message time': datetime.datetime.now().ctime(),
            'Status': status,
            'Message': self.message,
            'Most recent date in Zarr': self.last_date,
            'Qinit used': self.qinit,
            'Time period': self.time_period
        }
        if status == 'Error':
            logging.error(self.message)
        else:
            logging.info(self.message)

        # Send the log message to CloudWatch
        try:
            response = self.client.put_log_events(
                logGroupName=self.LOG_GROUP_NAME,
                logStreamName=self.LOG_STREAM_NAME,
                logEvents=[
                    {
                        'timestamp': int(round(time.time() * 1000)),
                        'message': json.dumps(log_message)
                    }
                ]
            )
            return response
        except Exception as e:
            if not self.warn:
                logging.warning(f"Failed to log message to CloudWatch: {e}")
                self.warn = True
            else:
                pass
