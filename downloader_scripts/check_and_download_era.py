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


def download_era5(working_dir: str,
                  retro_zarr: str,
                  s3_bucket: str,
                  mnt_dir: str,
                  cl: CloudLog) -> None:
    """
    Downloads era5 runoff data. Logs to the CloudLog class. 
    Converts hourly runoff to daily. 

    Parameters:
    - working_dir (str): The current working directory.
    - retro_zarr (str): The path to the retrospective zarr file.
    - s3_bucket (str): The S3 bucket where the downloaded data will be stored.
    - mnt_dir (str): The directory where the data will be saved.
    - cl (CloudLog): An instance of the CloudLog class for logging.

    Returns:
    - None
    """
    era_dir = os.path.join(mnt_dir,'era5_data')
    os.makedirs(era_dir, exist_ok=True)
    try:
        c = cdsapi.Client()
    except:
        cdsapirc_file = os.path.join(working_dir, '.cdsapirc')
        if not os.path.exists(cdsapirc_file):
            cdsapirc_file = os.path.join(os.getenv('HOME'), '.cdsapirc')
            if not os.path.exists(cdsapirc_file):
                raise FileNotFoundError(f"Cannot find a .cdsapirc in {working_dir} or {os.getenv['HOME']}")
        os.environ['CDSAPI_RC'] = cdsapirc_file
        c = cdsapi.Client()

    
    try:
        last_date = xr.open_zarr(retro_zarr)['time'][-1].values
    except IndexError:
        last_date = xr.open_zarr(retro_zarr)['time'].values
    cl.add_last_date(last_date)

    # run_again = False
    if pd.to_datetime(last_date + np.timedelta64(14,'D')) > datetime.now():
        # If the last date in the zarr file is within 2 weeks of today, then prohibit the script from continuing
        # since the ECMWF has a lag time of 6 days ish (a week for rounding)
        cl.log_message(f'{last_date} is within two weeks of today. Not running')
        return

    last_date = pd.to_datetime(last_date)
    today = pd.to_datetime(datetime.now().date())
    date_range = pd.date_range(start=last_date + pd.DateOffset(days=1), end=today - pd.DateOffset(days=14), freq='D')
    number_of_days = len(date_range)
    times_to_download = [date_range[i:i+7].tolist() for i in range(0, number_of_days, 7)]
    # Remove the last list if it is less than 7 days
    if len(times_to_download[-1]) < 7:
        times_to_download.pop(-1)

    cl.add_time_period(date_range.tolist())
    cl.log_message('RUNNING', "Beginning download")
    # This machine holds ~100 GB, which means we can download at most 250 weeks of ERA5 data (added to ensure we don't run out of space, but will probably never be used)
    max_weeks = 200 # 50 weeks buffer gives 20 GB extra space
    times_to_download_split = [times_to_download[i:i+max_weeks] for i in range(0, len(times_to_download), max_weeks)]
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
                    if month in {t.month for t in time_list if t.year == year} and not is_downloaded(ncs, year, month, days):
                        requests.append((era_dir, c, year, month, days))
                        num_requests += 1

        # Use multithreading so that we can make more requests at once if need be
        num_processes = min(num_requests, os.cpu_count() * 8)

        queue = Queue()
        for _ in range(num_processes):
            worker = DownloadWorker(queue)
            worker.daemon = True
            worker.start()
        for request in requests:
            queue.put(request)
        queue.join()

        # # Check that the number of files downloaded match the number of requests
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


        for ncs_to_use in netcdf_pairs:
            with dask.config.set(**{'array.slicing.split_large_chunks': False}):
                ds = (xr.open_mfdataset(ncs_to_use, 
                                    concat_dim='time', 
                                    combine='nested', 
                                    parallel=True, 
                                    chunks = {'time':'auto', 'lat':'auto','lon':'auto'}, # Chunk to prevent weird Slicing behavior and missing data
                                    preprocess=process_expver_variable)
                                    .sortby('time')
                                    .groupby('time.date')
                                    .sum(dim='time') # Convert to daily
                                    .rename({'date':'time'})
                )
                ds['time'] = ds['time'].values.astype('datetime64[ns]')
                
            # Make sure all days were downloaded
            if ds['time'].shape[0] != 7: 
                raise ValueError(f'The entire time series was not downloaded correctly ({ds["time"].shape[0]} of {7} for {ncs_to_use}) likely ECMWF has not updated their datasets')

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
            s3.put('temp.nc', s3_bucket + '/era_5/' + outname)

            # Remove uncombined netcdfs
            if isinstance(ncs_to_use, str):
                ncs_to_use = [ncs_to_use]
            for nc in ncs_to_use:
                os.remove(nc)

def retrieve_data(era_dir: str,
                  client: cdsapi.Client, 
                  year: int, 
                  month: int, 
                  days: list[int],) -> None:
    """
    Retrieves era5 data.

    Args:
        mnt_dir (str): The directory where the data will be saved.
        client (cdsapi.Client): The CDS API client.
        year (int): The year of the data.
        month (int): The month of the data.
        days (list[int]): The list of days for which data will be retrieved.
        index (int): The index of the data.
        index_2 (int): Another index of the data.

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
    cl = CloudLog()
    try:
        working_directory = os.getcwd()
        mnt_dir = os.getenv('MNT_DIR')
        other_instance = os.getenv('OTHER_INSTANCE')
        s3_bucket = os.getenv('S3_BUCKET')
        s3_zarr_name = os.getenv('S3_ZARR_NAME')
        region_name = os.getenv('REGION_NAME')
        
        bucket_uri = os.path.join(s3_bucket, s3_zarr_name)
        s3 = s3fs.S3FileSystem(anon=False, client_kwargs=dict(region_name=region_name))
        retro_zarr = s3fs.S3Map(root=bucket_uri, s3=s3, check=False)
        download_era5(working_directory, retro_zarr, s3_bucket, mnt_dir,cl)

        ec2 = boto3.client('ec2', region_name = "us-west-2")
        ec2.start_instances(InstanceIds=[other_instance])
        cl.log_message('FINISHED')
    except Exception as e:
        cl.log_message('FAIL', traceback.format_exc())