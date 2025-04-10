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
import netCDF4
import numpy as np
import pandas as pd
import xarray as xr

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
        CL.log_message(f'{last_date} is within {min_lag_time_days} days of today. Not running')
        return

    date_range = pd.date_range(start=last_date + pd.DateOffset(days=1),
                               end=today - pd.DateOffset(days=min_lag_time_days),
                               freq='D', )
    logging.info(date_range[0])
    logging.info(date_range[-1])
    CL.add_time_period(date_range.tolist())
    CL.log_message('RUNNING', "Beginning download")

    # make a list of unique year and month combinations in the list
    download_requests = []
    year_month_combos = {(d.year, d.month) for d in date_range}
    # sort by year and month
    year_month_combos = natsort.natsorted(year_month_combos)


    # year_1 = year_month_combos[0][0]
    # month_1 = year_month_combos[0][1]
    # if len(year_month_combos) > 1:
    #     year_2 = year_month_combos[-1][0]
    #     month_2 = year_month_combos[-1][1]
    # else:
    #     year_2 = year_1
    #     month_2 = month_1
    # day_1 = min({d.day for d in date_range if d.year == year_1 and d.month == month_1})
    # day_2 = max({d.day for d in date_range if d.year == year_2 and d.month == month_2})
    # hourly_cumulative_file_name = f'era5_{year_1}{str(month_1).zfill(2)}{str(day_1).zfill(2)}-{year_2}{str(month_2).zfill(2)}{str(day_2).zfill(2)}_daily_cumulative.nc'
    # hourly_cumulative_file_name = os.path.join(runoff_dir, hourly_cumulative_file_name)

    # if os.path.exists(hourly_cumulative_file_name):
    #     logging.info(f'{hourly_cumulative_file_name} already exists locally')
    #     return
    
    logging.info(year_month_combos)
    for year, month in year_month_combos:
        download_dates = [d for d in date_range if d.year == year and d.month == month]
        days = [d.day for d in download_dates]
        expected_file_name = date_to_file_name(year, month, days)
        # if we already have the file locally, skip
        if os.path.exists(os.path.join(era_dir, expected_file_name)):
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
            logging.info(f'processing {", ".join(downloaded_files)}')


            if ds['time'].shape[0] == 0:
                logging.info(f'No time steps were downloaded- the shape of the time array is 0.')
                logging.info(f'Removing {", ".join(downloaded_files)}')
                {os.remove(downloaded_file) for downloaded_file in downloaded_files}
            else:
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

    try:
        subprocess.run(['docker', '--version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError('Please install docker, and run "docker pull chdavid/rapid"')

    if not os.path.exists(daily_zarr):
        logging.info(f'Downloading {s3_daily_zarr}')
        get_local_copy(s3_daily_zarr, daily_zarr, credentials)

    if not os.path.exists(hourly_zarr):
        logging.info(f'Downloading {s3_hourly_zarr}')
        get_local_copy(s3_hourly_zarr, hourly_zarr, credentials)

def check_zarrs_match(local_zarr_path: str, s3_zarr_path: str):
    """
    Check that the local zarr matches the s3 zarr.
    """
    local_zarr = xr.open_zarr(local_zarr_path)
    s3_zarr = xr.open_zarr(s3_zarr_path, storage_options=storage_options)

    assert (local_zarr['time'] == s3_zarr['time']).all(), "Time arrays do not match"
    assert local_zarr['Q'].shape == s3_zarr['Q'].shape, "Shapes do not match"

def rapid_files_from_routing(
        in_params: str,
        in_connectivity: str,
        riv_bas_id: str,
        k: str,
        x: str,
        rapid_connect: str,
) -> None:
    """
    Generate RAPID input CSV files from routing parameter and connectivity parquet files

    Args:
        in_params: Path to input routing parameters parquet file
        in_connectivity: Path to input connectivity parquet file
        riv_bas_id: Output path for riv_bas_id CSV
        k: Output path for k CSV
        x: Output path for x CSV
        rapid_connect: Output path for rapid_connect CSV

    Returns:
        None
    """
    params_df = pd.read_parquet(in_params)
    conn_df = pd.read_parquet(in_connectivity)

    # Write individual RAPID files
    params_df['river_id'].to_csv(riv_bas_id, header=False, index=False)
    params_df['k'].to_csv(k, header=False, index=False)
    params_df['x'].to_csv(x, header=False, index=False)

    # Write connectivity file: two-column CSV
    conn_df[['river_id', 'ds_river_id']].to_csv(rapid_connect, header=False, index=False)
    return

def setup_configs(configs_dir: str,
                  s3_configs_dir: str,
                  credentials: str,
                  CL: CloudLog, 
                  s3: s3fs.S3FileSystem,
                  convert: bool = False) -> None:
    """
    Setup all the directories we need, populate files
    """
    
    os.makedirs(configs_dir, exist_ok=True)
    if len(glob.glob(os.path.join(configs_dir, '*', '*.csv'))) == 0:
        if convert:
            # We need to convert rr files to rapid files
            for connectivity, params in tqdm.tqdm(zip(natsort.natsorted(s3.glob(f'{s3_configs_dir}/*/connectivity.parquet')),
                                            natsort.natsorted(s3.glob(f'{s3_configs_dir}/*/routing_parameters.parquet'))), 
                                            total=len(glob.glob(os.path.join(configs_dir, '*')))):
                vpu = os.path.basename(os.path.dirname(connectivity))
                connectivity = f"s3://{connectivity}"
                params = f"s3://{params}"
                riv_bas_id = os.path.join(configs_dir, vpu, 'riv_bas_id.csv')
                k = os.path.join(configs_dir, vpu, 'k.csv')
                x = os.path.join(configs_dir, vpu, 'x.csv')
                rapid_connect = os.path.join(configs_dir, vpu, 'rapid_connect.csv')
                rapid_files_from_routing(params, connectivity, riv_bas_id, k, x, rapid_connect)
        else:
            result = subprocess.run(f"aws s3 sync {credentials} {configs_dir}", shell=True, capture_output=True,
                                    text=True)
            if result.returncode == 0:
                CL.log_message('RUNNING', "Obtained configs from S3")
            else:
                raise RuntimeError(f"Failed to obtain configs from S3: {result.stderr}")

def get_qinits_from_s3(s3: s3fs.S3FileSystem, configs_dir:str, s3_qfinal_dir: str, output_dir: str, convert: bool = False) -> None:
    """
    Get q initialization files from S3.

    Parameters:
    - s3: An instance of s3fs.S3FileSystem for accessing S3.
    - output_dir: The local directory where the Qinit files will be saved.
    - s3_qfinal_dir: The directory in S3 where the Qfinal files are stored.
    - last_retro_time: The last retro time as a numpy datetime64 object.

    Raises:
    - FileNotFoundError: If the Qfinal files cannot be found or if the number of Qfinal files is not as expected.

    Returns:
    - None
    """
    # download the qfinal files
    for vpu in glob.glob(os.path.join(configs_dir, '*')):
        vpu = os.path.basename(vpu)
        try:
            most_recent_qfinal = natsort.natsorted(s3.glob(f'{s3_qfinal_dir}/{vpu}/finalstate*.nc'))[-1]
        except IndexError and convert:
            most_recent_qfinal = natsort.natsorted(s3.glob(f'{s3_qfinal_dir}/{vpu}/finalstate*.parquet'))[-1]
            
        local_file_name = os.path.join(output_dir, vpu, os.path.basename(most_recent_qfinal))
        if not os.path.exists(local_file_name):
            if convert:
                (
                    pd.read_parquet(most_recent_qfinal)
                    .drop(columns='vpu')
                    .to_xarray()
                    .to_netcdf(local_file_name, format='NETCDF3_CLASSIC')
                )
            else:
                s3.get(most_recent_qfinal, local_file_name)
    return

def verify_era5_data(runoff_dir: str, hourly_zarr: str) -> None:
    """
    Verifies that the ERA5 data is compatible with the retrospective zarr
    """
    runoff_files = glob.glob(os.path.join(runoff_dir, '*.nc'))
    if not runoff_files:
        raise FileNotFoundError("No ERA5 runoff files found. ERA5 probably not downloaded correctly.")
    
    with xr.open_mfdataset(runoff_files) as ds , xr.open_zarr(hourly_zarr) as hourly_ds:
        # Check the the time dimension
        ro_time = ds['time'].values
        retro_time = hourly_ds['time'].values
        total_time = np.concatenate((retro_time, ro_time))
        difs = np.diff(total_time)
        if not np.all(difs == difs[0]):
            raise ValueError('Time dimension of ERA5 is not compatible with the retrospective zarr')

        # Check that there are no nans
        if np.isnan(ds['ro'].values).any():
            raise ValueError('ERA5 data contains nans')
        
def processes(runoff_dir) -> int:
    # For inflows files and multiprocess, for each 1GB of runoff data, we need ~ 6GB for peak memory consumption.
    # Otherwise, some m3 files will never be written and no error is raised
    sample_runoff_file = glob.glob(os.path.join(runoff_dir, '*.nc'))[0]
    processes = min(
        os.cpu_count(),
        round(psutil.virtual_memory().total * 0.8 / (os.path.getsize(sample_runoff_file) * 6))
    )
    return processes

def rapid_namelist(
        namelist_save_path: str,

        k_file: str,
        x_file: str,
        riv_bas_id_file: str,
        rapid_connect_file: str,
        vlat_file: str,
        qout_file: str,

        time_total: int,
        timestep_calc_routing: int,
        timestep_calc: int,
        timestep_inp_runoff: int,

        # Optional - Flags for RAPID Options
        run_type: int = 1,
        routing_type: int = 1,

        use_qinit_file: bool = False,
        qinit_file: str = '',  # qinit_VPU_DATE.csv

        write_qfinal_file: bool = True,
        qfinal_file: str = '',

        compute_volumes: bool = False,
        v_file: str = '',

        use_dam_model: bool = False,  # todo more options here
        use_influence_model: bool = False,
        use_forcing_file: bool = False,
        use_uncertainty_quantification: bool = False,

        opt_phi: int = 1,

        # Optional - Can be determined from rapid_connect
        reaches_in_rapid_connect: int = None,
        max_upstream_reaches: int = None,

        # Optional - Can be determined from riv_bas_id_file
        reaches_total: int = None,

        # Optional - Optimization Runs Only
        time_total_optimization: int = 0,
        timestep_observations: int = 0,
        timestep_forcing: int = 0,
) -> None:
    """
    Generate a namelist file for a RAPID routing run

    All units are strictly SI: meters, cubic meters, seconds, cubic meters per second, etc.

    Args:
        namelist_save_path (str): Path to save the namelist file
        k_file (str): Path to the k_file (input)
        x_file (str): Path to the x_file (input)
        rapid_connect_file (str): Path to the rapid_connect_file (input)
        qout_file (str): Path to save the Qout_file (routed discharge file)
        vlat_file (str): Path to the Vlat_file (inflow file)

    Returns:
        None
    """
    assert run_type in [1, 2], 'run_type must be 1 or 2'
    assert routing_type in [1, 2, 3, ], 'routing_type must be 1, 2, 3, or 4'
    assert opt_phi in [1, 2], 'opt_phi must be 1, or 2'

    if any([x is None for x in (reaches_in_rapid_connect, max_upstream_reaches)]):
        df = pd.read_csv(rapid_connect_file, header=None)
        reaches_in_rapid_connect = df.shape[0]
        rapid_connect_columns = ['rivid', 'next_down', 'count_upstream']  # plus 1 per possible upstream reach
        max_upstream_reaches = df.columns.shape[0] - len(rapid_connect_columns)

    if reaches_total is None:
        df = pd.read_csv(riv_bas_id_file, header=None)
        reaches_total = df.shape[0]

    namelist_options = {
        'BS_opt_Qfinal': f'.{str(write_qfinal_file).lower()}.',
        'BS_opt_Qinit': f'.{str(use_qinit_file).lower()}.',
        'BS_opt_dam': f'.{str(use_dam_model).lower()}.',
        'BS_opt_for': f'.{str(use_forcing_file).lower()}.',
        'BS_opt_influence': f'.{str(use_influence_model).lower()}.',
        'BS_opt_V': f'.{str(compute_volumes).lower()}.',
        'BS_opt_uq': f'.{str(use_uncertainty_quantification).lower()}.',

        'k_file': f"'{k_file}'",
        'x_file': f"'{x_file}'",
        'rapid_connect_file': f"'{rapid_connect_file}'",
        'riv_bas_id_file': f"'{riv_bas_id_file}'",
        'Qout_file': f"'{qout_file}'",
        'Vlat_file': f"'{vlat_file}'",
        'V_file': f"'{v_file}'",

        'IS_opt_run': run_type,
        'IS_opt_routing': routing_type,
        'IS_opt_phi': opt_phi,
        'IS_max_up': max_upstream_reaches,
        'IS_riv_bas': reaches_in_rapid_connect,
        'IS_riv_tot': reaches_total,

        'IS_dam_tot': 0,
        'IS_dam_use': 0,
        'IS_for_tot': 0,
        'IS_for_use': 0,

        'Qinit_file': f"'{qinit_file}'",
        'Qfinal_file': f"'{qfinal_file}'",

        'ZS_TauR': timestep_inp_runoff,
        'ZS_dtR': timestep_calc_routing,
        'ZS_TauM': time_total,
        'ZS_dtM': timestep_calc,
        'ZS_TauO': time_total_optimization,
        'ZS_dtO': timestep_observations,
        'ZS_dtF': timestep_forcing,
    }

    # generate the namelist file
    namelist_string = '\n'.join([
        '&NL_namelist',
        *[f'{key} = {value}' for key, value in namelist_options.items()],
        '/',
        ''
    ])

    with open(namelist_save_path, 'w') as f:
        f.write(namelist_string)


def rapid_namelist_from_directories(vpu_directory: str,
                                    inflow_file: str,
                                    namelists_directory: str,
                                    outputs_directory: str,
                                    datesubdir: bool = False,
                                    qinit_file: str = None,) -> str:
    vpu_code = os.path.basename(vpu_directory)
    k_file = os.path.join(vpu_directory, f'k.csv')
    x_file = os.path.join(vpu_directory, f'x.csv')
    riv_bas_id_file = os.path.join(vpu_directory, f'riv_bas_id.csv')
    rapid_connect_file = os.path.join(vpu_directory, f'rapid_connect.csv')

    for x in (k_file, x_file, riv_bas_id_file, rapid_connect_file):
        assert os.path.exists(x), f'{x} does not exist'

    os.makedirs(namelists_directory, exist_ok=True)

    inflow_file_name_params = os.path.basename(inflow_file).replace('.nc', '').split('_')
    start_date = inflow_file_name_params[2]
    end_date = inflow_file_name_params[3]
    file_label = inflow_file_name_params[4] if len(inflow_file_name_params) > 4 else ''

    namelist_file_name = f'namelist_{start_date}'
    qout_file_name = f'Qout_{vpu_code}_{start_date}_{end_date}.nc'
    vlat_file = inflow_file
    write_qfinal_file = True
    qfinal_file = os.path.join(outputs_directory, f'finalstate_{end_date}.nc')

    if file_label:
        namelist_file_name += f'_{file_label}'
        qout_file_name = qout_file_name.replace('.nc', f'_{file_label}.nc')
        qfinal_file = qfinal_file.replace('.nc', f'_{file_label}.nc')

    namelist_save_path = os.path.join(namelists_directory, namelist_file_name)
    qout_path = os.path.join(outputs_directory, qout_file_name)
    os.makedirs(outputs_directory, exist_ok=True)

    if datesubdir:
        os.makedirs(os.path.join(outputs_directory, f'{start_date}'), exist_ok=True)
        qout_path = os.path.join(outputs_directory, f'{start_date}', qout_file_name, )

    with netCDF4.Dataset(inflow_file) as ds:
        time_step_inflows = ds['time_bnds'][0, 1] - ds['time_bnds'][0, 0]
        time_total_inflow = ds['time_bnds'][-1, 1] - ds['time_bnds'][0, 0]
    time_total = time_total_inflow
    timestep_inp_runoff = time_step_inflows
    timestep_calc = time_step_inflows
    timestep_calc_routing = 900

    use_qinit_file = bool(qinit_file)

    rapid_namelist(namelist_save_path=namelist_save_path,
                   k_file=k_file,
                   x_file=x_file,
                   riv_bas_id_file=riv_bas_id_file,
                   rapid_connect_file=rapid_connect_file,
                   vlat_file=vlat_file,
                   qout_file=qout_path,
                   time_total=time_total,
                   timestep_calc_routing=timestep_calc_routing,
                   timestep_calc=timestep_calc,
                   timestep_inp_runoff=timestep_inp_runoff,
                   write_qfinal_file=write_qfinal_file,
                   qfinal_file=qfinal_file,
                   use_qinit_file=use_qinit_file,
                   qinit_file=qinit_file, )

    return qfinal_file

def _make_namelists_for_vpu(vpu: str,
                            configs_dir: str,
                            inflows_dir: str, 
                            namelists_dir: str, 
                            outputs_dir: str,
                            home: str) -> None:
    vpu_dir = os.path.join(configs_dir, vpu)
    inflow_dir = os.path.join(inflows_dir, vpu)
    namelist_dir = os.path.join(namelists_dir, vpu)
    output_dir = os.path.join(outputs_dir, vpu)
    qfinal_file = natsort.natsorted(glob.glob(os.path.join(output_dir, 'Qfinal*.nc')))[-1]

    for inflow_file in natsort.natsorted(glob.glob(os.path.join(inflow_dir, 'm3*.nc'))):
        qfinal_file = rapid_namelist_from_directories(
            vpu_dir,
            inflow_file,
            namelist_dir,
            output_dir,
            qinit_file=qfinal_file
        )

    for namelist in glob.glob(os.path.join(namelist_dir, f'namelist*')):
        # Correct the paths in the namelist file
        with open(namelist, 'r') as f:
            text = f.read().replace(os.path.join(home, 'data'), '/mnt')
        with open(namelist, 'w') as f:
            f.write(text)
    return

def create_inflow_file(
        runoff_data: str,
        weight_table: str,
        inflow_dir: str,
        vpu_name: str,
) -> None:
    with xr.open_dataset(weight_table) as ds:
        weight_df = ds[['river_id', 'x_index', 'y_index', 'proportion', 'area_sqm_total']].to_dataframe()
    unique_idxs = weight_df[['x_index', 'y_index']].drop_duplicates().reset_index(drop=True).reset_index().astype(int)
    unique_sorted_rivers = weight_df[['river_id', 'area_sqm_total']].drop_duplicates().sort_index()

    with xr.open_mfdataset(runoff_data) as ds:
        conversion_factor = 1

        df = pd.DataFrame(
            ds
            ['ro']
            .isel({
                'longitude': xr.DataArray(unique_idxs['x_index'].values, dims="points"),
                'latitude': xr.DataArray(unique_idxs['y_index'].values, dims="points")
            })
            .transpose("time", "points")
            .values,
            columns=unique_idxs[['x_index', 'y_index']].astype(str).apply('_'.join, axis=1),
            index=ds['time'].to_numpy()
        )
    df = df[weight_df[['x_index', 'y_index']].astype(str).apply('_'.join, axis=1)]
    df.columns = weight_df['river_id']
    df = df * weight_df['area_sqm_total'].values * conversion_factor
    df = df.T.groupby(by=df.columns).sum().T
    df = df[unique_sorted_rivers['river_id'].values]

    df = df.fillna(0)
    
    # logging.info("Writing inflows to file")
    os.makedirs(inflow_dir, exist_ok=True)
    datetime_array = df.index.to_numpy()
    start_date = datetime.datetime.fromtimestamp(datetime_array[0].astype(float) / 1e9, datetime.UTC).strftime('%Y%m%d')
    end_date = datetime.datetime.fromtimestamp(datetime_array[-1].astype(float) / 1e9, datetime.UTC).strftime('%Y%m%d')
    file_name = f'm3_{vpu_name}_{start_date}_{end_date}.nc'
    inflow_file_path = os.path.join(inflow_dir, file_name)
    logging.debug(f'Writing inflow file to {inflow_file_path}')

    with netCDF4.Dataset(inflow_file_path, "w", format="NETCDF3_CLASSIC") as inflow_nc:
        # create dimensions
        inflow_nc.createDimension('time', datetime_array.shape[0])
        inflow_nc.createDimension('rivid', unique_sorted_rivers['river_id'].values.shape[0])
        inflow_nc.createDimension('nv', 2)

        # m3_riv
        # note - nan's and fill values are not supported on netcdf3 files
        m3_riv_var = inflow_nc.createVariable('m3_riv', 'f4', ('time', 'rivid'))
        m3_riv_var[:] = df.to_numpy()
        m3_riv_var.long_name = 'accumulated inflow inflow volume in river reach boundaries'
        m3_riv_var.units = 'm3'
        # m3_riv_var.coordinates = 'lon lat'
        m3_riv_var.grid_mapping = 'crs'
        m3_riv_var.cell_methods = "time: sum"

        # rivid
        rivid_var = inflow_nc.createVariable('rivid', 'i4', ('rivid',))
        rivid_var[:] = unique_sorted_rivers['river_id'].values
        rivid_var.long_name = 'unique identifier for each river reach'
        rivid_var.units = '1'
        rivid_var.cf_role = 'timeseries_id'

        # time
        reference_time = datetime_array[0]
        time_step = (datetime_array[1] - reference_time).astype('timedelta64[s]')
        time_var = inflow_nc.createVariable('time', 'i4', ('time',))
        time_var[:] = (datetime_array - reference_time).astype('timedelta64[s]').astype(int)
        time_var.long_name = 'time'
        time_var.standard_name = 'time'
        time_var.units = f'seconds since {reference_time.astype("datetime64[s]")}'  # Must be seconds
        time_var.axis = 'T'
        time_var.calendar = 'gregorian'
        time_var.bounds = 'time_bnds'

        # time_bnds
        time_bnds = inflow_nc.createVariable('time_bnds', 'i4', ('time', 'nv',))
        time_bnds_array = np.stack([datetime_array, datetime_array + time_step], axis=1)
        time_bnds_array = (time_bnds_array - reference_time).astype('timedelta64[s]').astype(int)
        time_bnds[:] = time_bnds_array

        # # longitude
        # lon_var = inflow_nc.createVariable('lon', 'f8', ('rivid',))
        # lon_var[:] = comid_df['lon'].values
        # lon_var.long_name = 'longitude of a point related to each river reach'
        # lon_var.standard_name = 'longitude'
        # lon_var.units = 'degrees_east'
        # lon_var.axis = 'X'

        # # latitude
        # lat_var = inflow_nc.createVariable('lat', 'f8', ('rivid',))
        # lat_var[:] = comid_df['lat'].values
        # lat_var.long_name = 'latitude of a point related to each river reach'
        # lat_var.standard_name = 'latitude'
        # lat_var.units = 'degrees_north'
        # lat_var.axis = 'Y'

        # crs
        crs_var = inflow_nc.createVariable('crs', 'i4')
        crs_var.grid_mapping_name = 'latitude_longitude'
        crs_var.epsg_code = 'EPSG:4326'  # WGS 84
        crs_var.semi_major_axis = 6378137.0
        crs_var.inverse_flattening = 298.257223563

        # add global attributes
        inflow_nc.Conventions = 'CF-1.6'
        inflow_nc.history = 'date_created: {0}'.format(datetime.datetime.utcnow())
        inflow_nc.featureType = 'timeSeries'


def _make_inflow_for_vpu(vpu: str,
                         configs_dir: str,
                         inflows_dir: str,
                         runoff_dir: str) -> None:
    vpu_dir = os.path.join(configs_dir, vpu)
    inflow_dir = os.path.join(inflows_dir, vpu)

    for runoff_file in glob.glob(os.path.join(runoff_dir, '*.nc')):
        create_inflow_file(
            runoff_file,
            vpu_dir,
            inflow_dir,
            vpu_name=vpu,
        )
    return

def _make_inflow_for_vpu_star(args):
    return _make_inflow_for_vpu(*args)

def inflow_and_namelist(runoff_dir: str,
            configs_dir: str,
            inflows_dir: str,
            namelists_dir: str,
            outputs_dir: str,
            home: str,
            p: Pool) -> None:
    
    vpu_inputs = [
        (os.path.basename(d), configs_dir, inflows_dir, namelists_dir, outputs_dir, home)
        for d in glob.glob(os.path.join(configs_dir, '*'))
    ]
    list(tqdm.tqdm(p.imap_unordered(_make_namelists_for_vpu, vpu_inputs), total=len(vpu_inputs)))

    list(tqdm.tqdm(p.imap_unordered(_make_inflow_for_vpu_star, vpu_inputs), total=len(vpu_inputs)))

    # number of expected files = num_configs_dirs * num_runoff_files
    expected_file_count = len(glob.glob(os.path.join(configs_dir, '*'))) * len(glob.glob(os.path.join(runoff_dir, '*.nc')))

    # check that all inflow files were created correctly
    if not len(glob.glob(os.path.join(inflows_dir, '*', '*.nc'))) == expected_file_count:
        raise FileNotFoundError("Not all inflow files were created correctly")
    # check that all namelists were created correctly
    if not len(glob.glob(os.path.join(namelists_dir, '*', 'namelist*'))) == expected_file_count:
        raise FileNotFoundError("Not all namelists were created correctly")

    return

def run_rapid(home: str) -> None:
    rapid_result = subprocess.run(
        [
            f'sudo docker run --rm --name rapid --mount type=bind,source={os.path.join(home, "data")},target=/mnt chdavid/rapid python3 /mnt/runrapid.py'],
        shell=True,
        capture_output=True,
        text=True
    )

    if rapid_result.returncode != 0:
        raise RuntimeError(rapid_result.stderr)

    logging.info(rapid_result.stdout)
    return

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
        raise FileNotFoundError("No Qout files found. RAPID probably not run correctly.")

    with xr.open_zarr(daily_zarr_path) as daily_zarr:
        with xr.open_mfdataset(
                qouts,
                combine='nested',
                concat_dim='river_id',
                parallel=True,
                preprocess=drop_coords
        ).reindex(river_id=daily_zarr['river_id']) as new_ds:
            earliest_date = np.datetime_as_string(new_ds.time[0].values, unit="h")
            latest_date = np.datetime_as_string(new_ds.time[-1].values, unit="h")
            new_ds = new_ds.round(decimals=3)
            new_ds = new_ds.where(new_ds['Q'] >= 0.0, 0.0)

            with xr.open_zarr(hourly_zarr_path) as hourly_zarr:
                chunks = hourly_zarr.chunks

            # Append hourly data first
            CL.log_message('RUNNING', f'Appending to zarr: {earliest_date} to {latest_date}')
            (
                new_ds
                .chunk({"time": chunks["time"][0], "river_id": chunks["river_id"][0]})
                .to_zarr(hourly_zarr_path, mode='a', append_dim='time', consolidated=True)
            )

            new_ds = new_ds.resample(time='1D').mean('time')

            # Append daily data
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
            CL.log_message('FAIL', f'{zarr} contain nans')
            exit()

        # Verify that the time dimension is correct
        times = ds['time'].values
        if not np.all(np.diff(times) == times[1] - times[0]):
            CL.log_message('FAIL', f'Time dimension of {zarr} is incorrect')
            exit()

def sync_local_to_s3(outputs_dir: str,
                     s3_qfinal_dir: str, 
                     local_hourly_zarr: str,
                     local_daily_zarr: str,
                     s3_hourly_zarr: str,
                     s3_daily_zarr: str,
                     credentials: str) -> None:
    """
    Put our local edits on the zarrs to S3.
    Also upload qfinal files to S3.

    Raises:
        Exception: If the sync command fails.
    """
    # Deprecated: we no longer sync Qout files

    # qout_files = natsort.natsorted(glob.glob(os.path.join(outputs_dir, '*', 'Qout*.nc')))
    # if not qout_files:
    #     raise FileNotFoundError("No Qout files found. River-route probably not run correctly.")

    # logging.info('Syncing Qout files to S3')
    # for qout_file in qout_files:
    #     vpu = os.path.basename(os.path.dirname(qout_file))
    #     result = subprocess.run(
    #         f's5cmd '
    #         f'--credentials-file {ODP_CREDENTIALS_FILE} --profile odp '
    #         f'cp '
    #         f'{qout_file} '
    #         f'{GEOGLOWS_ODP_RETROSPECTIVE_BUCKET}/retrospective/{vpu}/{os.path.basename(qout_file)}',
    #         shell=True, capture_output=True, text=True,
    #     )
    #     if not result.returncode == 0:
    #         raise Exception(f"Sync failed. Error: {result.stderr}")

    # qfinal_files = natsort.natsorted(glob.glob(os.path.join(outputs_dir, '*', 'Qfinal*.nc')))
    # if not qfinal_files:
    #     raise FileNotFoundError("No Qfinal files found. River-route probably not run correctly.")

    logging.info('Syncing Qfinal files to S3')
    result = subprocess.run(
        f's5cmd '
        f'--credentials-file {credentials} --profile odp '
        f'sync --include "*finalstate*" --size-only '
        f'{outputs_dir}/ '
        f'{s3_qfinal_dir}/',
        shell=True, capture_output=True, text=True,
    )
    if not result.returncode == 0:
        raise Exception(f"Sync failed. Error: {result.stderr}")

    # sync the zarrs. We can use sync because 0.* files are not is not on local side
    for zarr, s3_zarr in zip([local_hourly_zarr, local_daily_zarr], [s3_hourly_zarr, s3_daily_zarr]):
        logging.info(f'Syncing zarr {zarr} to S3')
        result = subprocess.run(
            f"s5cmd "
            f"--credentials-file {credentials} --profile odp "
            f"sync "
            f"{zarr}/ "
            f"{s3_zarr}/",
            shell=True, capture_output=True, text=True,
        )
        if not result.returncode == 0:
            raise Exception(f"Sync failed. Error: {result.stderr}")
        
    return

def update_monthly_zarrs(daily_zarr: str,
                         monthly_timesteps: str,
                         monthly_timeseries: str) -> None:
    daily_ds = xr.open_zarr(daily_zarr)
    monthly_steps_ds = xr.open_zarr(monthly_timesteps, storage_options=storage_options)

    # Check if there is at least a whole month of data in the daily zarr not in the daily zarr
    last_daily_time = daily_ds['time'][-1].values
    last_monthly_time = monthly_steps_ds['time'][-1].values
    next_month = last_monthly_time.astype('datetime64[M]') + np.timedelta64(1, 'M')
    current_month = (last_daily_time + np.timedelta64(1, 'D')).astype('datetime64[M]')
    if current_month > next_month:
        logging.info(f'Updating monthly zarrs: [{next_month}, {current_month})')
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
                        annual_maximums: str) -> None:
    hourly_ds = xr.open_zarr(hourly_zarr)
    annual_steps_ds = xr.open_zarr(annual_timesteps, storage_options=storage_options)

    # Check if there is at least a whole year of data in the hourly zarr not in the annual zarr
    last_hourly_time = hourly_ds['time'][-1].values
    last_annual_time = annual_steps_ds['time'][-1].values
    next_year = last_annual_time.astype('datetime64[Y]') + np.timedelta64(1, 'Y')
    current_year = (last_hourly_time + np.timedelta64(1, 'h')).astype('datetime64[Y]')

    if current_year > next_year:
        logging.info(f'Updating yearly zarrs: [{next_year}, {current_year})')
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

def cleanup(home: str,
            runoff_dir: str,
            inflows_dir: str,
            outputs_dir: str,
            configs_dir: str,) -> None:
    """
    Cleans up the working directory by deleting namelists, inflow files, and
    caching qfinals and qouts.
    """
    # change the owner of the data directory and all sub files and directories to the user
    os.system(f'sudo chown -R $USER:$USER {home}/data')

    # delete runoff data
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
    for vpu_dir in glob.glob(os.path.join(configs_dir, '*')):
        qfinal_files = natsort.natsorted(glob.glob(os.path.join(vpu_dir, 'finalstate*.parquet')))
        if len(qfinal_files) > 1:
            for file in qfinal_files[:-1]:
                os.remove(file)