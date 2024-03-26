import glob
import os
import subprocess
import traceback
from datetime import datetime
from queue import Queue
from threading import Thread

import boto3
import cdsapi
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from natsort import natsorted

from cloud_logger import CloudLog

# ERA5 has a lag time of ~6 days (a week for rounding)
MIN_LAG_TIME_DAYS = 5

GEOGLOWS_ODP_RETROSPECTIVE_BUCKET = 's3://geoglows-v2-retrospective'
GEOGLOWS_ODP_RETROSPECTIVE_ZARR = 'retrospective.zarr'
GEOGLOWS_ODP_REGION = 'us-west-2'

MNT_DIR = os.getenv('VOLUME_DIR')

region_name = os.getenv('REGION_NAME')
s3_era_bucket = os.getenv('S3_ERA_BUCKET')
compute_instance = os.getenv('COMPUTE_INSTANCE')
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('REGION_NAME')

CL = CloudLog()

# print all the stuff from the environment variables
print(f'region_name: {region_name}')
print(f'S3_ERA_BUCKET: {s3_era_bucket}')
print(f'compute_instance: {compute_instance}')
print(f'ACCESS_KEY_ID: {ACCESS_KEY_ID}')
print(f'SECRET_ACCESS_KEY: {SECRET_ACCESS_KEY}')
print(f'REGION: {REGION}')


class DownloadWorker(Thread):
    """
    A worker thread that downloads data using the provided parameters.

    Args:
        queue (Queue): The queue from which to retrieve the download parameters.

    Attributes:
        queue (Queue): The queue from which to retrieve the download parameters.
    """

    def __init__(self, queue: Queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        """
        The main method of the worker thread.
        Retrieves download parameters from the queue and calls the retrieve_data function.
        """
        while True:
            client, year, month, days = self.queue.get()
            try:
                retrieve_data(client, year, month, days)
                # ...
            finally:
                self.queue.task_done()


def download_era5() -> None:
    """
    Downloads era5 runoff data. Logs to the CloudLog class. 
    Converts hourly runoff to daily. 
    """
    era_dir = os.path.join(MNT_DIR, 'era5_data')
    os.makedirs(era_dir, exist_ok=True)

    print('connecting to CDS')
    c = cdsapi.Client()

    print('connecting to ODP')
    bucket_uri = os.path.join(GEOGLOWS_ODP_RETROSPECTIVE_BUCKET, GEOGLOWS_ODP_RETROSPECTIVE_ZARR)
    s3_odp = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=GEOGLOWS_ODP_REGION))
    retro_zarr = s3fs.S3Map(root=bucket_uri, s3=s3_odp, check=False)

    print('checking last date')
    try:
        last_date = xr.open_zarr(retro_zarr)['time'][-1].values
    except IndexError:
        last_date = xr.open_zarr(retro_zarr)['time'].values
    last_date = pd.to_datetime(last_date)
    today = pd.to_datetime(datetime.now().date())
    print(f'last_date: {last_date}')
    CL.add_last_date(last_date)

    if pd.to_datetime(last_date + np.timedelta64(MIN_LAG_TIME_DAYS, 'D')) > datetime.now():
        # If the last date in the zarr file is within MIN_LAG_TIME_DAYS of today then exit
        CL.log_message(f'{last_date} is within {MIN_LAG_TIME_DAYS} days of today. Not running')
        return

    date_range = pd.date_range(start=last_date + pd.DateOffset(days=1),
                               end=today - pd.DateOffset(days=MIN_LAG_TIME_DAYS),
                               freq='D', )
    print(date_range[0])
    print(date_range[-1])
    CL.add_time_period(date_range.tolist())
    CL.log_message('RUNNING', "Beginning download")

    # authenticate with AWS for the bucket that contains the ERA5 data using boto3
    print('checking for files already on s3')
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
    s3_files = s3.list_objects(Bucket=s3_era_bucket[5:])  # remove the s3:// prefix
    s3_files = [file['Key'] for file in s3_files['Contents']] if 'Contents' in s3_files else []
    print('files found on s3')
    print(s3_files)

    # make a list of unique year and month combinations in the list
    download_requests = []
    year_month_combos = {(d.year, d.month) for d in date_range}
    # sort by year and month
    year_month_combos = natsorted(year_month_combos)
    print(year_month_combos)
    for year, month in year_month_combos:
        download_dates = [d for d in date_range if d.year == year and d.month == month]
        days = [d.day for d in download_dates]
        expected_file_name = date_to_file_name(year, month, days)
        # if we already have the file locally, skip
        if os.path.exists(os.path.join(MNT_DIR, 'era5_data', expected_file_name)):
            print(f'{expected_file_name} already exists locally')
            continue
        # if we already have the file on the s3 bucket, skip
        if f'{expected_file_name}' in s3_files:
            print(f'{expected_file_name} already exists in the bucket')
            continue
        download_requests.append((c, year, month, days))

    if download_requests:
        print('beginning downloads')
        print(download_requests)
        CL.log_message('DOWNLOADING', "Beginning downloads")
        num_processes = min(len(download_requests), os.cpu_count())
        queue = Queue()
        for _ in range(num_processes):
            worker = DownloadWorker(queue)
            worker.daemon = True
            worker.start()
        for request in download_requests:
            queue.put(request)
        queue.join()

        print('downloads completed')

    print('processing files')
    CL.log_message('PROCESSING', "Processing downloaded era5 files")
    downloaded_files = natsorted(glob.glob(os.path.join(era_dir, '*.nc')))
    if not downloaded_files:
        print('No downloaded files to process')
        CL.log_message('FINISHED', "No files were downloaded")
        return

    print('converting to daily cumulative')
    for downloaded_file in downloaded_files:
        daily_cumulative_file_name = os.path.basename(downloaded_file).replace('.nc', '_daily_cumulative.nc')
        with xr.open_dataset(downloaded_file) as ds:
            print(f'processing {downloaded_file}')

            if ds['time'].shape[0] == 0:
                print(f'No time steps were downloaded- the shape of the time array is 0.')
                print(f'Removing {downloaded_file}')
                os.remove(downloaded_file)
                continue

            if 'expver' in ds.dims:
                print('expver in dims')
                # find the time steps where the runoff is not nan when expver=1
                a = ds.ro.sel(latitude=0, longitude=0, expver=1)
                expver1_timesteps = a.time[~np.isnan(a)]

                # find the time steps where the runoff is not nan when expver=5
                b = ds.ro.sel(latitude=0, longitude=0, expver=5)
                expver5_timesteps = b.time[~np.isnan(b)]

                # assert that the two timesteps combined are the same as the original
                assert len(ds.time) == len(expver1_timesteps) + len(expver5_timesteps)

                # combine the two
                ds = (
                    xr
                    .concat(
                        [
                            ds.sel(expver=1, time=expver1_timesteps.values).drop_vars('expver'),
                            ds.sel(expver=5, time=expver5_timesteps.values).drop_vars('expver')
                        ],
                        dim='time'
                    )
                )

            print('converting to daily cumulative')
            ds = (
                ds
                .groupby('time.date')
                .sum(dim='time')  # Convert to daily
                .rename({'date': 'time'})
            )
            ds['time'] = ds['time'].values.astype('datetime64[ns]')
            ds.to_netcdf(os.path.join(era_dir, daily_cumulative_file_name))
            print(f'uploading {daily_cumulative_file_name}')
            subprocess.call(['aws', 's3', 'cp', os.path.join(era_dir, daily_cumulative_file_name),
                             os.path.join(s3_era_bucket, os.path.basename(downloaded_file))])

            # remove the original file
            os.remove(downloaded_file)

            # remove the consolidated file
            os.remove(os.path.join(era_dir, daily_cumulative_file_name))


def date_to_file_name(year: int, month: int, days: list[int]) -> str:
    padded_month = str(month).zfill(2)
    padded_day_0 = str(days[0]).zfill(2)
    padded_day_1 = str(days[-1]).zfill(2)
    return f'era5_{year}{padded_month}{padded_day_0}-{year}{padded_month}{padded_day_1}.nc'


def retrieve_data(client: cdsapi.Client,
                  year: int,
                  month: int,
                  days: list[int], ) -> None:
    """
    Retrieves era5 data.

    Args:
        client (cdsapi.Client): The CDS API client.
        year (int): The year of the data.
        month (int): The month of the data.
        days (list[int]): The list of days for which data will be retrieved.

    Returns:
        None
    """
    era_dir = os.path.join(MNT_DIR, 'era5_data')
    file_name = date_to_file_name(year, month, days)
    client.retrieve(
        f'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': 'runoff',
            'year': year,
            'month': str(month).zfill(2),
            'day': [str(day).zfill(2) for day in days],
            'time': [f'{x:02d}:00' for x in range(0, 24)],
        },
        target=os.path.join(era_dir, file_name)
    )


if __name__ == "__main__":
    """
    We assume the the volume already has a file system and data (the treospective zarr)
    """
    try:
        print('Starting')
        CL.log_message('STARTING', 'preparing to download era5 data')
        download_era5()
        ec2 = boto3.client('ec2', region_name=region_name)
        ec2.start_instances(InstanceIds=[compute_instance])
        CL.log_message('FINISHED')
    except Exception as e:
        print(traceback.format_exc())
        CL.log_message('FAIL', traceback.format_exc())
