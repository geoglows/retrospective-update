import os
import subprocess
from glob import glob

import pandas as pd
import xarray as xr
from natsort import natsorted

from cloud_logger import CloudLog

# Parameters
MIN_LAG_TIME_DAYS = int(os.getenv('MIN_LAG_TIME_DAYS', 5))

DATA_DIR = os.getenv('WORK_DIR')
ERA5_DIR = os.getenv('ERA5_DIR')
CONFIGS_DIR = os.getenv('CONFIGS_DIR')
OUTPUTS_DIR = os.getenv('OUTPUTS_DIR')
HYDROSOS_DIR = os.getenv('HYDROSOS_DIR')

FINAL_STATES_DIR = os.getenv('FINAL_STATES_DIR')
S3_FINAL_STATES_DIR = os.getenv('S3_FINAL_STATES_DIR')
FORECAST_INITS_DIR = os.getenv('FORECAST_INITS_DIR')

HOURLY_STEP_ZARR = os.getenv('HOURLY_STEP_ZARR')
DAILY_STEP_ZARR = os.getenv('DAILY_STEP_ZARR')

DAILY_ZARR = os.getenv('DAILY_ZARR')
HOURLY_ZARR = os.getenv('HOURLY_ZARR')
S3_DAILY_ZARR = os.getenv('S3_DAILY_ZARR')
S3_HOURLY_ZARR = os.getenv('S3_HOURLY_ZARR')

S3_CONFIGS_DIR = os.getenv('S3_CONFIGS_DIR')
S3_QFINAL_DIR = os.getenv('S3_QFINAL_DIR')
S3_MONTHLY_TIMESTEPS = os.getenv('S3_MONTHLY_TIMESTEPS')
S3_MONTHLY_TIMESERIES = os.getenv('S3_MONTHLY_TIMESERIES')
S3_ANNUAL_TIMESTEPS = os.getenv('S3_ANNUAL_TIMESTEPS')
S3_ANNUAL_TIMESERIES = os.getenv('S3_ANNUAL_TIMESERIES')
S3_ANNUAL_MAXIMUMS = os.getenv('S3_ANNUAL_MAXIMUMS')


def check_local_zarrs_match_s3(local_zarr_path: str, s3_zarr_path: str) -> None:
    """
    Check that the local zarr matches the s3 zarr.
    """
    local_zarr = xr.open_zarr(local_zarr_path)
    s3_zarr = xr.open_zarr(s3_zarr_path, storage_options={'anon': True})

    if not (local_zarr['time'] == s3_zarr['time']).all():
        cl.ping('FAIL', f"Time-arrays-do-not-match-in-{local_zarr_path}")
        raise EnvironmentError('Time arrays do not match between local and s3 zarrs')

    if local_zarr['Q'].shape != s3_zarr['Q'].shape:
        cl.ping('FAIL', f"Q-array-shapes-do-not-match-{local_zarr_path}")
        raise EnvironmentError('Q array shapes do not match between local and s3 zarrs')


def check_for_init_files() -> None:
    # First find the last date in the local hourly zarr
    with xr.open_zarr(HOURLY_ZARR) as ds:
        last_hourly_retro_time = pd.to_datetime(ds['time'][-1].values).strftime('%Y%m%d%H%M')
    # todo also check the hourly steps zarr and for its last time step
    expected_final_state_file = f'finalstate_{last_hourly_retro_time}.parquet'

    # list all final state files and delete any that are not the state of interest
    states = natsorted(glob(os.path.join(FINAL_STATES_DIR, '*', 'finalstate*.parquet')))
    for state in states:
        if os.path.basename(state) != expected_final_state_file:
            os.remove(state)
    # now check if every vpu has the state of interest
    states = natsorted(glob(os.path.join(FINAL_STATES_DIR, '*', 'finalstate*.parquet')))
    if len(states) == len(list(glob(os.path.join(CONFIGS_DIR, '*')))):
        return

    # download the states we need
    cmd = f's5cmd --no-sign-request cp "{S3_FINAL_STATES_DIR}/*/{expected_final_state_file}" {FINAL_STATES_DIR}/'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, )
    if result.returncode != 0:
        cl.ping('FAIL', f"Error running s5cmd copy command")
        raise Exception(f"Syncing from S3 failed. Error: {result.stderr}")

    # now check if every vpu has the state of interest
    states = natsorted(glob(os.path.join(FINAL_STATES_DIR, '*', 'finalstate*.parquet')))
    if len(states) != len(list(glob(os.path.join(CONFIGS_DIR, '*')))):
        cl.ping('ERROR', 's5cmd to fetch inits succeeded but expected files are not present')
        raise RuntimeError("s5cmd to fetch inits succeeded but expected files are not present")
    return


if __name__ == '__main__':
    cl = CloudLog()

    try:
        cl.ping('RUNNING', 'Verifying local zarrs match s3')
        check_local_zarrs_match_s3(HOURLY_ZARR, S3_HOURLY_ZARR)
        check_local_zarrs_match_s3(DAILY_ZARR, S3_DAILY_ZARR)

        cl.ping('RUNNING', 'Looking for init files')
        check_for_init_files()
        exit(0)
    except Exception as e:
        cl.ping('FAIL', 'Error while preparing environment')
        exit(1)
