import datetime
import json
import os
import time
import logging

import boto3
import numpy as np

LOG_GROUP_NAME = os.getenv('AWS_LOG_GROUP_NAME')
LOG_STREAM_NAME = os.getenv('AWS_LOG_STREAM_NAME')
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID') 
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') 
REGION = os.getenv('AWS_REGION')

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
                 ACCESS_KEY_ID = ACCESS_KEY_ID,
                 SECRET_ACCESS_KEY= SECRET_ACCESS_KEY,
                 REGION= REGION,
                 save_path: str = 'log.json',
                 ):
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


    def log_message(self, status: str, message: str = None) -> dict:
        """
        Logs a message to CloudWatch.

        Args:
            status (str): The status of the log message.
            error (Exception, optional): The error associated with the log message. Defaults to None.

        Returns:
            dict: The response from the CloudWatch API.
        """
        global LOG_GROUP_NAME
        global LOG_STREAM_NAME
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

        # Send the log message to CloudWatch
        try:
            response = self.client.put_log_events(
                logGroupName=LOG_GROUP_NAME,
                logStreamName=LOG_STREAM_NAME,
                logEvents=[
                    {
                        'timestamp': int(round(time.time() * 1000)),
                        'message': json.dumps(log_message)
                    }
                ]
            )
            return response
        except Exception as e:
            logging.error(e)
