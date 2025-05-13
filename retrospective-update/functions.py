import os
import glob
import logging
import subprocess
from queue import Queue
from threading import Thread
from datetime import datetime
from multiprocessing.pool import Pool

import tqdm
import s3fs
import cdsapi
import psutil
import natsort
import numpy as np
import pandas as pd
import xarray as xr
import river_route as rr

from cloud_logger import CloudLog

storage_options={'profile':'odp'}

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
            client, era_dir, year, month, days = self.queue.get()
            try:
                retrieve_data(client, era_dir, year, month, days)
            finally:
                self.queue.task_done()

def handle_valid_time(ds: xr.Dataset) -> xr.Dataset:
    if 'valid_time' in ds.dims:
        ds = ds.rename({'valid_time':'time'})
    return ds

def download_era5(era_dir: str,
                  runoff_dir: str,
                  s3_daily_zarr: str,
                  min_lag_time_days: int,
                  CL: CloudLog,
                  ) -> None:
    """
    Downloads era5 runoff data. Logs to the CloudLog class. 
    Converts hourly runoff to daily. 
    """
    os.makedirs(era_dir, exist_ok=True)
    c = cdsapi.Client()

    try:
        last_date = xr.open_zarr(s3_daily_zarr, storage_options=storage_options)['time'][-1].values
    except IndexError:
        last_date = xr.open_zarr(s3_daily_zarr, storage_options=storage_options)['time'].values

    last_date = pd.to_datetime(last_date)
    today = pd.to_datetime(datetime.now().date())
    logging.info(f'last_date: {last_date}')
    CL.add_last_date(last_date)

    if pd.to_datetime(last_date + np.timedelta64(min_lag_time_days, 'D')) > datetime.now():
        # If the last date in the zarr file is within min_lag_time_days of today then exit
        CL.ping('STOPPING', f'{last_date}-is-within-{min_lag_time_days}-days-of-today.-Stopping')
        exit()

    date_range = pd.date_range(start=last_date + pd.DateOffset(days=1),
                               end=today - pd.DateOffset(days=min_lag_time_days),
                               freq='D', )
    logging.info(date_range[0])
    logging.info(date_range[-1])
    CL.add_time_period(date_range.tolist())
    CL.ping('RUNNING', "Downloading-era5-data")

    # make a list of unique year and month combinations in the list
    download_requests = []
    year_month_combos = {(d.year, d.month) for d in date_range}
    # sort by year and month
    year_month_combos = natsort.natsorted(year_month_combos)
    
    logging.info(year_month_combos)
    for year, month in year_month_combos:
        download_dates = [d for d in date_range if d.year == year and d.month == month]
        days = [d.day for d in download_dates]
        expected_file_name = date_to_file_name(year, month, days)
        # if we already have the file locally, skip
        if os.path.exists(os.path.join(era_dir, expected_file_name)) or os.path.exists(os.path.join(runoff_dir, expected_file_name)):
            logging.info(f'{expected_file_name} already exists locally')
            continue

        download_requests.append((c, era_dir, year, month, days))

    if download_requests:
        logging.info(download_requests)
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


    CL.log_message('PROCESSING', "Processing downloaded era5 files")
    downloaded_files = natsort.natsorted(glob.glob(os.path.join(era_dir, '*.nc')))
    if not downloaded_files:
        CL.log_message('FINISHED', "No files were downloaded")
        return
    
    for f in downloaded_files:
        output_file = os.path.join(runoff_dir, os.path.basename(f))
        if os.path.exists(output_file):
            logging.info(f'{output_file} already exists')
            continue

        with xr.open_mfdataset(f,
                            concat_dim='time', 
                            combine='nested', 
                            parallel=True, 
                            chunks = {'time':'auto', 'lat':'auto','lon':'auto'}, # Included to prevent weird slicing behavior and missing data
                            preprocess=handle_valid_time
                            ) as ds:
            logging.info(f'processing {f}')


            if ds['time'].shape[0] == 0:
                logging.info(f'No time steps were downloaded- the shape of the time array is 0.')
                logging.info(f'Removing {", ".join(downloaded_files)}')
                {os.remove(downloaded_file) for downloaded_file in downloaded_files}
                continue
            
            if 'expver' in ds.dims:
                logging.info('expver in dims')
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
            elif 'expver' in ds:
                # Sometimes this is just here not doing anything
                # Remove it to avoid warning when saving
                ds = ds.drop_vars('expver')
            
            # Make sure that the last timestep is T23:00 (i.e., a full day)
            if ds.time[-1].values != np.datetime64(f'{ds.time[-1].values.astype("datetime64[D]")}T23:00'):
                # Remove timesteps until the last full day
                ds = ds.sel(time=slice(None, np.datetime64(f'{ds.time[-1].values.astype("datetime64[D]")}') - np.timedelta64(1, 'h')))

                # If there is no more time, skip this file
                if len(ds.time) == 0:
                    continue

            ds.to_netcdf(output_file)

    # Now remove the downloaded files
    {os.remove(downloaded_file) for downloaded_file in downloaded_files}

def date_to_file_name(year: int, month: int, days: list[int]) -> str:
    padded_month = str(month).zfill(2)
    padded_day_0 = str(days[0]).zfill(2)
    padded_day_1 = str(days[-1]).zfill(2)
    return f'era5_{year}{padded_month}{padded_day_0}-{year}{padded_month}{padded_day_1}.nc'

def retrieve_data(client: cdsapi.Client,
                  era_dir: str,
                  year: int,
                  month: int,
                  days: list[int], ) -> None:
    """
    Retrieves era5 data.

    Args:
        client (cdsapi.Client): The CDS API client.
        era_dir (str): The directory where the data will be saved.
        year (int): The year of the data.
        month (int): The month of the data.
        days (list[int]): The list of days for which data will be retrieved.

    Returns:
        None
    """
    file_name = date_to_file_name(year, month, days)
    client.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': ['reanalysis'],
            'download_format': 'unarchived',
            'data_format': 'netcdf',
            'variable': ['runoff'],
            'year': year,
            'month': str(month).zfill(2),
            'day': [str(day).zfill(2) for day in days],
            'time': [f'{x:02d}:00' for x in range(0, 24)],
        },
        target=os.path.join(era_dir, file_name)
    )

def get_local_copy(s3_path: str, local_path: str, credentials: str) -> None:
    """
    Get a local copy of a file from S3.

    Args:
        s3_path (str): The S3 path of the file.
        local_path (str): The local path where the file will be saved.
        credentials (str): The path to the credentials file.

    Returns:
        None
    """
    result = subprocess.run(
        f's5cmd '
        f'--credentials-file {credentials} --profile odp '
        f'sync '
        '--exclude "*Q/0*" '
        f'{s3_path}/* '
        f'{local_path}',
        shell=True, capture_output=True, text=True,
    )
    if not result.returncode == 0:
        raise Exception(f"Download failed. Error: {result.stderr}")

    return

def check_installations(daily_zarr: str,
                        s3_daily_zarr: str,
                        hourly_zarr: str,
                        s3_hourly_zarr: str,
                        credentials: str) -> None:
    """
    Check that the following are installed:
    - awscli
    - s5cmd
    - docker
    """
    try:
        subprocess.run(['aws', '--version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError('Please install the AWS cli: conda install -c conda-forge awscli')

    try:
        subprocess.run(['s5cmd', 'version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError('Please install s5cmd: `conda install -c conda-forge s5cmd`')

    if not os.path.exists(daily_zarr):
        logging.info(f'Downloading {s3_daily_zarr}')
        get_local_copy(s3_daily_zarr, daily_zarr, credentials)

    if not os.path.exists(hourly_zarr):
        logging.info(f'Downloading {s3_hourly_zarr}')
        get_local_copy(s3_hourly_zarr, hourly_zarr, credentials)

def check_zarrs_match(local_zarr_path: str, s3_zarr_path: str, CL: CloudLog) -> None:
    """
    Check that the local zarr matches the s3 zarr.
    """
    local_zarr = xr.open_zarr(local_zarr_path)
    s3_zarr = xr.open_zarr(s3_zarr_path, storage_options=storage_options)

    if not (local_zarr['time'] == s3_zarr['time']).all():
        CL.ping('FAIL', f"Time-arrays-do-not-match-in-{local_zarr_path}")
        exit()

    if local_zarr['Q'].shape != s3_zarr['Q'].shape:
        CL.ping('FAIL', f"Shapes-do-not-match-{local_zarr_path}")
        exit()


def setup_configs(configs_dir: str,
                  s3_configs_dir: str,
                  CL: CloudLog,) -> None:
    """
    Setup all the directories we need, populate files
    """
    
    os.makedirs(configs_dir, exist_ok=True)
    if len(glob.glob(os.path.join(configs_dir, '*', '*.csv'))) == 0:
        result = subprocess.run(f"s5cmd sync {s3_configs_dir}/* {configs_dir}", shell=True, capture_output=True,
                                text=True)
        if result.returncode == 0:
            CL.log_message('RUNNING', "Obtained configs from S3")
        else:
            raise RuntimeError(f"Failed to obtain configs from S3: {result.stderr}")

def get_qinits_from_s3(s3: s3fs.S3FileSystem,
                       configs_dir: str,
                       s3_qfinal_dir: str,
                       outputs_dir: str) -> None:
    """
    Get q initialization files from S3.

    Parameters:
    - s3: An instance of s3fs.S3FileSystem for accessing S3.
    - vpu_dirs: A list of VPU directories.
    - s3_qfinal_dir: The directory in S3 where the Qfinal files are stored.
    - last_retro_time: The last retro time as a numpy datetime64 object.

    Raises:
    - FileNotFoundError: If the Qfinal files cannot be found or if the number of Qfinal files is not as expected.

    Returns:
    - None
    """
    # download the qfinal files
    for vpu in tqdm.tqdm(glob.glob(os.path.join(configs_dir, '*'))):
        vpu = os.path.basename(vpu)
        most_recent_qfinal = natsort.natsorted(s3.ls(f'{s3_qfinal_dir}/{vpu}/'))[-1]
        local_file_name = os.path.join(outputs_dir, vpu, os.path.basename(most_recent_qfinal))
        if not os.path.exists(local_file_name):
            s3.get(most_recent_qfinal, local_file_name)
    return

def verify_era5_data(runoff_dir: str, hourly_zarr: str, CL: CloudLog) -> None:
    """
    Verifies that the ERA5 data is compatible with the retrospective zarr
    """
    runoff_files = glob.glob(os.path.join(runoff_dir, '*.nc'))
    if not runoff_files:
        CL.ping('STOPPING', f"No-runoff-files-found")
        exit()
    
    with xr.open_mfdataset(runoff_files) as ds , xr.open_zarr(hourly_zarr) as hourly_ds:
        # Check the the time dimension
        ro_time = ds['time'].values
        retro_time = hourly_ds['time'].values
        total_time = np.concatenate((retro_time, ro_time))
        difs = np.diff(total_time)
        if not np.all(difs == difs[0]):
            CL.ping('STOPPING', f"Time-dimension-of-ERA5-is-not-compatible-with-the-retrospective-zarr")

        # Check that there are no nans
        if np.isnan(ds['ro'].values).any():
            CL.ping('STOPPING', f"ERA5-data-contains-nans")
        
def processes(runoff_dir) -> int:
    # For inflows files and multiprocess, for each 1GB of daily runoff data, we need ~ 6GB for peak memory consumption.
    # Otherwise, some m3 files will never be written and no error is raised
    sample_runoff_file = glob.glob(os.path.join(runoff_dir, '*.nc'))[0]
    processes = min(
        os.cpu_count(),
        max(round(psutil.virtual_memory().total * 0.9 / (os.path.getsize(sample_runoff_file) * 6*24 * len(glob.glob(os.path.join(runoff_dir, '*.nc'))))), 1)
    )
    return processes

def _make_inflow_for_vpu(vpu: str,
                         configs_dir: str,
                         inflows_dir: str,
                         runoff_dir: str) -> None:
    vpu_dir = os.path.join(configs_dir, vpu)
    inflow_dir = os.path.join(inflows_dir, vpu)
    os.makedirs(inflow_dir, exist_ok=True)

    weight_table = glob.glob(os.path.join(vpu_dir, 'gridweights_ERA5*.nc'))[0]

    df = rr.runoff.calc_catchment_volumes(glob.glob(os.path.join(runoff_dir, '*.nc')),
                                            weight_table,
                                            x_var='longitude',
                                            y_var='latitude',
                                            time_var='time',
                                            )
    
    start_date = df.index[0].strftime('%Y%m%d%H%M')
    end_date = df.index[-1].strftime('%Y%m%d%H%M')
    file_name = f'volumes_{start_date}_{end_date}.nc'
    inflow_file_path = os.path.join(inflow_dir, file_name)

    river_ids = df.columns.astype(int).values
    time = df.index.to_numpy()

    ds = xr.Dataset(
        data_vars={
            'volume': (('time', 'river_id'), df.to_numpy(), {
                'long_name': 'Incremental catchment runoff volume',
                'units': 'm3'
            }),
        },
        coords={
            'time': ('time', time, {
                'long_name': 'time',
                'standard_name': 'time',
                'axis': 'T',
                'time_step': f'{(df.index[1] - df.index[0]).seconds}'
            }),
            'river_id': ('river_id', river_ids, {
                'long_name': 'unique ID number for each river'
            }),
        },
        attrs={
            'Conventions': 'CF-1.6'
        }
    )

    # Prepare encoding properly
    encoding = {
        'volume': {'zlib': True, 'complevel': 5},
        'time': {
            'units': f'seconds since {df.index[0].strftime("%Y-%m-%d %H:%M:%S")}',
            'dtype': 'i4',
            'zlib': True,
            'complevel': 5
        },
        'river_id': {'dtype': 'i4', 'zlib': True, 'complevel': 5}
    }

    ds.to_netcdf(inflow_file_path, format='NETCDF4', encoding=encoding)

def _make_inflow_for_vpu_star(args):
    return _make_inflow_for_vpu(*args)

def inflows(runoff_dir: str,
            configs_dir: str,
            inflows_dir: str,
            p: Pool,
            CL: CloudLog) -> None:
    vpu_numbers = [
        (os.path.basename(d), configs_dir, inflows_dir, runoff_dir)
        for d in glob.glob(os.path.join(configs_dir, '*'))
    ]
    list(
        tqdm.tqdm(
            p.imap_unordered(_make_inflow_for_vpu_star, vpu_numbers),
            total=len(vpu_numbers)),
    )

    # number of expected files = num_configs_dirs
    expected_file_count = len(glob.glob(os.path.join(configs_dir, '*')))

    # check that all inflow files were created correctly
    if not len(glob.glob(os.path.join(inflows_dir, '*', '*.nc'))) == expected_file_count:
        CL.ping('FAIL', 'Not-all-inflow-files-were-created-correctly')
        exit()
    return

def _run_river_route(vpu_dir: str, outputs_dir: str, inflows_dir: str) -> None:
    params_file = glob.glob(os.path.join(vpu_dir, 'routing_parameters.parquet'))[0]
    connectivity_file = glob.glob(os.path.join(vpu_dir, 'connectivity.parquet'))[0]
    output_dir = os.path.join(outputs_dir, os.path.basename(vpu_dir))

    for catchment_volumes_file in natsort.natsorted(glob.glob(os.path.join(inflows_dir, os.path.basename(vpu_dir), '*.nc'))):
        outflow_file = os.path.join(output_dir, os.path.basename(catchment_volumes_file).replace('volumes', 'Qout'))
        initial_state_file = natsort.natsorted(glob.glob(os.path.join(output_dir, 'finalstate*.parquet')))[-1]
        final_state_file = os.path.join(output_dir, f"finalstate_{outflow_file.split('_')[-1].replace('.nc', '.parquet')}")
        (
            rr
            .Muskingum(
                routing_params_file = params_file,
                connectivity_file = connectivity_file,
                catchment_volumes_file = catchment_volumes_file,
                outflow_file = outflow_file,
                initial_state_file = initial_state_file,
                final_state_file = final_state_file,
                progress_bar = False,
                log_level = 'ERROR'
            )
            .route()
        )

def _run_river_route_star(args):
    return _run_river_route(*args)

def run_river_route(configs_dir: str,
                    outputs_dir: str,
                    inflows_dir: str,
                    p) -> None:
    vpus = glob.glob(os.path.join(configs_dir, '*'))
    list(
        tqdm.tqdm(
            p.imap_unordered(_run_river_route_star, [(vpu, outputs_dir, inflows_dir) for vpu in vpus]),
            total=len(vpus)),
    )

def drop_coords(ds: xr.Dataset, qout: str = 'Q'):
    """
    Helps load faster, gets rid of variables/dimensions we do not need (lat, lon, etc.)

    Parameters:
        ds (xr.Dataset): The input dataset.
        qout (str): The variable name to keep in the dataset.

    Returns:
        xr.Dataset: The modified dataset with only the specified variable.
    """
    return ds[[qout]].reset_coords(drop=True)


def concatenate_outputs(outputs_dir: str,
                        hourly_zarr_path: str,
                        daily_zarr_path: str,
                        CL: CloudLog) -> None:
    # Build the week dataset
    qouts = natsort.natsorted(glob.glob(os.path.join(outputs_dir, '*', 'Qout*.nc')))
    if not qouts:
        CL.ping('FAIL', f"No-Qout-files-found")
        exit()

    with xr.open_zarr(daily_zarr_path) as daily_zarr:
        with xr.open_mfdataset(
                qouts,
                combine='nested',
                concat_dim='river_id',
                parallel=True,
                # preprocess=drop_coords
        ).reindex(river_id=daily_zarr['river_id']) as new_ds:
            earliest_date = np.datetime_as_string(new_ds.time[0].values, unit="h")
            latest_date = np.datetime_as_string(new_ds.time[-1].values, unit="h")
            new_ds = new_ds.round(decimals=3)
            new_ds = new_ds.where(new_ds['Q'] >= 0.0, 0.0)

            with xr.open_zarr(hourly_zarr_path) as hourly_zarr:
                chunks = hourly_zarr.chunks

            # Append hourly data first
            CL.ping('RUNNING', f'Appending-to-hourly-zarr-{earliest_date}-to-{latest_date}')
            (
                new_ds
                .chunk({"time": chunks["time"][0], "river_id": chunks["river_id"][0]})
                .to_zarr(hourly_zarr_path, mode='a', append_dim='time', consolidated=True)
            )

            # Append daily data
            CL.ping('RUNNING', f'Appending-to-daily-zarr-{earliest_date}-to-{latest_date}')
            new_ds = new_ds.resample(time='1D').mean('time')
            chunks = daily_zarr.chunks
            (
                new_ds
                .chunk({"time": chunks["time"][0], "river_id": chunks["river_id"][0]})
                .to_zarr(daily_zarr_path, mode='a', append_dim='time', consolidated=True)
            )
    return

def verify_concatenated_outputs(zarr: str, CL: CloudLog) -> None:
    """
    Verifies that the concatenated outputs are correct
    """
    with xr.open_zarr(zarr) as ds:
        time_size = ds.chunks['time'][0]
        # Test a river to see if there are nans
        if np.isnan(ds.isel(river_id=1, time=slice(time_size, -1))['Q'].values).any():
            CL.ping('FAIL', f'{zarr}-contain-nans')
            exit()

        # Verify that the time dimension is correct
        times = ds['time'].values
        if not np.all(np.diff(times) == times[1] - times[0]):
            CL.ping('FAIL', f'Time-dimension-of-{zarr}-is-incorrect')
            exit()

def sync_local_to_s3(outputs_dir: str,
                     s3_qfinal_dir: str, 
                     local_hourly_zarr: str,
                     local_daily_zarr: str,
                     s3_hourly_zarr: str,
                     s3_daily_zarr: str,
                     credentials: str,
                     CL: CloudLog) -> None:
    """
    Put our local edits on the zarrs to S3.
    Also upload qfinal files to S3.

    Raises:
        Exception: If the sync command fails.
    """
    CL.ping('RUNNING', 'syncing-finalstates-to-S3')
    result = subprocess.run(
        f's5cmd '
        f'--credentials-file {credentials} --profile odp '
        f'sync --include "*finalstate*" --size-only '
        f'{outputs_dir}/ '
        f'{s3_qfinal_dir}/',
        shell=True, capture_output=True, text=True,
    )
    if not result.returncode == 0:
        CL.ping('FAIL', f"Syncing-finalstates-to-S3-failed")
        logging.error(result.stderr)
        exit()

    # sync the zarrs. We can use sync because 0.* files are not is not on local side
    for zarr, s3_zarr in zip([local_hourly_zarr, local_daily_zarr], [s3_hourly_zarr, s3_daily_zarr]):
    # for zarr, s3_zarr in zip([local_hourly_zarr], [s3_hourly_zarr]):
        # files = glob.glob(os.path.join(zarr), 'Q', '*')
        # chunks = sorted({os.path.basename(f).split('.')[0]  for f in files})
        CL.ping('RUNNING', f'syncing-{zarr}-to-S3')
        result = subprocess.run(
            f"s5cmd "
            f"--credentials-file {credentials} --profile odp "
            f"sync "
            f"{zarr}/ "
            f"{s3_zarr}/",
            shell=True, capture_output=True, text=True,
        )
        if not result.returncode == 0:
            CL.ping('FAIL', f"Syncing-{zarr}-to-S3-failed")
            logging.error(result.stderr)
            exit()
        
    return

def update_monthly_zarrs(daily_zarr: str,
                         monthly_timesteps: str,
                         monthly_timeseries: str,
                         CL: CloudLog) -> None:
    daily_ds = xr.open_zarr(daily_zarr)
    monthly_steps_ds = xr.open_zarr(monthly_timesteps, storage_options=storage_options)

    # Check if there is at least a whole month of data in the daily zarr not in the daily zarr
    last_daily_time = daily_ds['time'][-1].values
    last_monthly_time = monthly_steps_ds['time'][-1].values
    next_month = last_monthly_time.astype('datetime64[M]') + np.timedelta64(1, 'M')
    current_month = (last_daily_time + np.timedelta64(1, 'D')).astype('datetime64[M]')
    if current_month > next_month:
        CL.ping('RUNNING', f'Updating-monthly-zarrs-{next_month}-to-{current_month}')
        # Find number of months to add
        months_ds = daily_ds.sel(time=slice(next_month, current_month))
        months_ds = months_ds.resample({'time':'MS'}).mean()

        # Now chunk and append
        chunks = monthly_steps_ds.chunks
        (
            months_ds
            .chunk({"time": chunks["time"][0], "river_id": chunks["river_id"][0]})
            .to_zarr(monthly_timesteps, mode='a', append_dim='time', consolidated=True, storage_options=storage_options)
        )

        # Do the same for timeseries
        chunks = xr.open_zarr(monthly_timeseries, storage_options=storage_options).chunks
        (
            months_ds
            .chunk({"time": chunks["time"][0], "river_id": chunks["river_id"][0]})
            .to_zarr(monthly_timeseries, mode='a', append_dim='time', consolidated=True, storage_options=storage_options)
        )
    
def update_yearly_zarrs(hourly_zarr: str,
                        annual_timesteps: str,
                        annual_timeseries: str,
                        annual_maximums: str,
                        CL: CloudLog) -> None:
    hourly_ds = xr.open_zarr(hourly_zarr)
    annual_steps_ds = xr.open_zarr(annual_timesteps, storage_options=storage_options)

    # Check if there is at least a whole year of data in the hourly zarr not in the annual zarr
    last_hourly_time = hourly_ds['time'][-1].values
    last_annual_time = annual_steps_ds['time'][-1].values
    next_year = last_annual_time.astype('datetime64[Y]') + np.timedelta64(1, 'Y')
    current_year = (last_hourly_time + np.timedelta64(1, 'h')).astype('datetime64[Y]')

    if current_year > next_year:
        CL.ping('RUNNING', f'Updating-yearly-zarrs-{next_year}-to-{current_year}')
        # Find number of years to add
        years_ds = hourly_ds.sel(time=slice(next_year, current_year))
        years_ds = years_ds.resample({'time':'YS'}).mean()

        # Now chunk and append
        chunks = annual_steps_ds.chunks
        (
            years_ds
            .chunk({"time": chunks["time"][0], "river_id": chunks["river_id"][0]})
            .to_zarr(annual_timesteps, mode='a', append_dim='time', consolidated=True, storage_options=storage_options)
        )

        # Do the same for timeseries
        chunks = xr.open_zarr(annual_timeseries, storage_options=storage_options).chunks
        (
            years_ds
            .chunk({"time": chunks["time"][0], "river_id": chunks["river_id"][0]})
            .to_zarr(annual_timeseries, mode='a', append_dim='time', consolidated=True, storage_options=storage_options)
        )

        # Do the same for maximums
        years_ds = hourly_ds.sel(time=slice(next_year, current_year))
        years_ds = years_ds.resample({'time':'YS'}).max()
        chunks = xr.open_zarr(annual_maximums, storage_options=storage_options).chunks
        (
            years_ds
            .chunk({"time": chunks["time"][0], "river_id": chunks["river_id"][0]})
            .to_zarr(annual_maximums, mode='a', append_dim='time', consolidated=True, storage_options=storage_options)
        )

def cleanup(data_dir: str,
            era_dir: str,
            runoff_dir: str,
            inflows_dir: str,
            outputs_dir: str,) -> None:
    """
    Cleans up the working directory by deleting namelists, inflow files, and
    caching qfinals and qouts.
    """
    # change the owner of the data directory and all sub files and directories to the user
    os.system(f'sudo chown -R $USER:$USER {data_dir}')

    # delete runoff data
    logging.info('Deleting era data')
    if era_dir:
        for file in glob.glob(os.path.join(era_dir, '*')):
            os.remove(file)

    logging.info('Deleting runoff data')
    if runoff_dir:
        for file in glob.glob(os.path.join(runoff_dir, '*')):
            os.remove(file)

    # delete inflow files
    logging.info('Deleting inflow files')
    for file in glob.glob(os.path.join(inflows_dir, '*', '*.nc')):
        os.remove(file)

    # delete qouts
    logging.info('Deleting qouts')
    for file in glob.glob(os.path.join(outputs_dir, '*', 'Qout*.nc')):
        os.remove(file)

    # delete all but the most recent qfinal
    logging.info('Deleting qfinals')
    for vpu_dir in glob.glob(os.path.join(outputs_dir, '*')):
        qfinal_files = natsort.natsorted(glob.glob(os.path.join(vpu_dir, 'finalstate*.parquet')))
        if len(qfinal_files) > 1:
            for f in qfinal_files[:-1]:
                os.remove(f)