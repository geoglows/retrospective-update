import cdsapi
import dask
import boto3
import s3fs
import xarray as xr
import numpy as np
import pandas as pd

from threading import Thread
from queue import Queue
import traceback
import glob
import os
from datetime import datetime

from cloud_logger import CloudLog

# ERA5 has a lag time of ~6 days (a week for rounding)
MIN_LAG_TIME_DAYS = 7

GEOGLOWS_ODP_RETROSPECTIVE_BUCKET = 's3://geoglows-v2-retrospective'
GEOGLOWS_ODP_RETROSPECTIVE_ZARR = 'retrospective.zarr'
GEOGLOWS_ODP_REGION = 'us-west-2'

MNT_DIR = os.getenv('VOLUME_DIR')

region_name = os.getenv('REGION_NAME')
s3_era_dir = os.getenv('S3_ERA_DIR')
compute_instance = os.getenv('COMPUTE_INSTANCE')
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('AWS_REGION')

CL = CloudLog()

# print all the stuff from the environment variables
print(f'region_name: {region_name}')
print(f's3_era_dir: {s3_era_dir}')
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
            era_dir, client, year, month, days = self.queue.get()
            try:
                retrieve_data(era_dir, client, year, month, days)
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

    print('connecting to S3')
    s3_era5 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)

    print('checking last date')
    try:
        last_date = xr.open_zarr(retro_zarr)['time'][-1].values
    except IndexError:
        last_date = xr.open_zarr(retro_zarr)['time'].values
    print(last_date)
    CL.add_last_date(last_date)

    # run_again = False
    if pd.to_datetime(last_date + np.timedelta64(MIN_LAG_TIME_DAYS, 'D')) > datetime.now():
        # If the last date in the zarr file is within MIN_LAG_TIME_DAYS of today then exit
        CL.log_message(f'{last_date} is within {MIN_LAG_TIME_DAYS} days of today. Not running')
        return

    last_date = pd.to_datetime(last_date)
    today = pd.to_datetime(datetime.now().date())
    date_range = pd.date_range(start=last_date + pd.DateOffset(days=1),
                               end=today - pd.DateOffset(days=MIN_LAG_TIME_DAYS),
                               freq='D', )
    number_of_days = len(date_range)
    times_to_download = [date_range[i:i + 7].tolist() for i in range(0, number_of_days, 7)]
    # Remove the last list if it is less than 7 days
    if len(times_to_download[-1]) < MIN_LAG_TIME_DAYS:
        times_to_download.pop(-1)

    CL.add_time_period(date_range.tolist())
    CL.log_message('RUNNING', "Beginning download")
    max_weeks = 200  # 50 weeks buffer gives 20 GB extra space
    times_to_download_split = [times_to_download[i:i + max_weeks] for i in range(0, len(times_to_download), max_weeks)]
    print(f'time_to_download_split: {times_to_download_split}')
    for times_to_download in times_to_download_split:
        requests = []
        num_requests = 0
        ncs = glob.glob(os.path.join(era_dir, '*.nc'))
        for time_list in times_to_download:
            years = {d.year for d in time_list}
            months = {d.month for d in time_list}

            # Create a request for each month for each year, using only the days in that month. This will support any timespan
            for year in years:
                for month in months:
                    days = sorted({t.day for t in time_list if t.year == year and t.month == month})
                    if month in {t.month for t in time_list if t.year == year} and not is_downloaded(ncs, year, month,
                                                                                                     days):
                        requests.append((era_dir, c, year, month, days))
                        num_requests += 1

        # Use multithreading so that we can make more requests at once if need be
        num_processes = min(num_requests, os.cpu_count() * 8)

        queue = Queue()
        print(f'num_processes: {num_processes}')
        for _ in range(num_processes):
            worker = DownloadWorker(queue)
            worker.daemon = True
            worker.start()
        print(f'requests: {requests}')
        for request in requests:
            queue.put(request)
        queue.join()

        # Check that the number of files downloaded match the number of requests
        print(ncs)
        ncs = sorted(glob.glob(os.path.join(era_dir, '*.nc')), key=date_sort)

        netcdf_pairs = []
        skip = False
        for nc in ncs:
            if skip:
                skip = False
                continue
            first_day, last_day = nc.split('_')[4].split('.')[0].split('-')
            if int(last_day) - int(first_day) + 1 == 7:
                netcdf_pairs.append(nc)
            else:
                netcdf_pairs.append([nc, ncs[ncs.index(nc) + 1]])
                skip = True

        print(f'netcdf_pairs: {netcdf_pairs}')
        for ncs_to_use in netcdf_pairs:
            with dask.config.set(**{'array.slicing.split_large_chunks': False}):
                ds = (
                    xr
                    .open_mfdataset(
                        ncs_to_use,
                        concat_dim='time',
                        combine='nested',
                        parallel=True,
                        chunks={'time': 'auto', 'lat': 'auto', 'lon': 'auto'},
                        # Chunk to prevent weird Slicing behavior and missing data
                        preprocess=process_expver_variable
                    )
                    .sortby('time')
                    .groupby('time.date')
                    .sum(dim='time')  # Convert to daily
                    .rename({'date': 'time'})
                )
                ds['time'] = ds['time'].values.astype('datetime64[ns]')

            # Make sure all days were downloaded
            if ds['time'].shape[0] == 0:
                raise ValueError(f'No time steps were downloaded- the shape of the time array is 0.')

            ds.to_netcdf('temp.nc')
            if isinstance(ncs_to_use, list):
                outname = "era5_"
                nc_year = ncs_to_use[0].split('_')[2]
                nc_month = ncs_to_use[0].split('_')[3]
                nc_days = ncs_to_use[0].split('_')[4].split('.')[0].split('-')
                if nc_year != ncs_to_use[1].split('_')[2]:
                    outname += f"{nc_year}-{ncs_to_use[1].split('_')[2]}_"
                else:
                    outname += f"{nc_year}_"
                if nc_month != ncs_to_use[1].split('_')[3]:
                    outname += f"{nc_month}-{ncs_to_use[1].split('_')[3]}_"
                else:
                    outname += f"{nc_month}_"
                outname += f"{nc_days[0]}-{ncs_to_use[1].split('_')[4].split('.')[0].split('-')[-1]}.nc"
            else:
                outname = os.path.basename(ncs_to_use)
            print(f'outname: {outname}')
            print(f'{s3_era_dir}/{outname}')
            s3_era5.upload_file('temp.nc', s3_era_dir, f'{outname}')
            print('uploaded')

            # Remove uncombined netcdfs
            if isinstance(ncs_to_use, str):
                ncs_to_use = [ncs_to_use]
            for nc in ncs_to_use:
                os.remove(nc)


def retrieve_data(era_dir: str,
                  client: cdsapi.Client,
                  year: int,
                  month: int,
                  days: list[int], ) -> None:
    """
    Retrieves era5 data.

    Args:
        era_dir (str): The directory where the data will be saved.
        client (cdsapi.Client): The CDS API client.
        year (int): The year of the data.
        month (int): The month of the data.
        days (list[int]): The list of days for which data will be retrieved.

    Returns:
        None
    """
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
        target=os.path.join(era_dir, f'era5_{year}_{month}_{days[0]}-{days[-1]}.nc')
    )


def date_sort(s: str) -> datetime:
    """
    Sorts the string by the date.

    Args:
        s (str): The string to be sorted.

    Returns:
        bool: The sorted string.
    """
    x = os.path.basename(s).split('.')[0].split('_')[1:]
    return datetime(int(x[0]), int(x[1]), int(x[2].split('-')[1]))


def process_expver_variable(ds: xr.Dataset,
                            runoff: str = 'ro') -> xr.DataArray:
    """
    Function used in opening the downloaded files. If 'expver' is found, raise an error, since we should not use these files.

    Parameters:
    ds (xr.Dataset): The dataset containing the downloaded files.
    runoff (str): The variable name for the runoff data. Default is 'ro'.

    Returns:
    xr.DataArray: The selected variable data array.

    Raises:
    ValueError: If 'expver' dimension is found in the dataset.
    """
    if 'expver' in ds.dims:
        raise ValueError('"expver" found in downloaded ERA files')
    return ds[runoff]


def is_downloaded(ncs: list[str],
                  year: str,
                  month: str,
                  days: list[int]) -> bool:
    # Check any already downloaded era 5 files to see if we need to download this time period
    for nc in ncs:
        nc_year = int(nc.split('_')[2])
        nc_month = int(nc.split('_')[3])
        nc_days = [int(x) for x in nc.split('_')[4].split('.')[0].split('-')]
        if nc_year == year and nc_month == month and set(nc_days).issubset(set(days)):
            return True
    return False


if __name__ == "__main__":
    """
    We assume the the volume already has a file system and data (the treospective zarr)
    """
    try:
        print('Starting')
        download_era5()

        ec2 = boto3.client('ec2', region_name=region_name)
        ec2.start_instances(InstanceIds=[compute_instance])
        CL.log_message('FINISHED')
    except Exception as e:
        print(traceback.format_exc())
        CL.log_message('FAIL', traceback.format_exc())
