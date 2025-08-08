import os
import subprocess
import traceback
from glob import glob

import pandas as pd
import xarray as xr
from natsort import natsorted

from cloud_logger import CloudLog
from set_env_variables import (
    DAILY_ZARR, FINAL_STATES_DIR, HOURLY_ZARR, S3_HOURLY_ZARR, S3_DAILY_ZARR, S3_FINAL_STATES_DIR, CONFIGS_DIR,
    MONTHLY_TIMESTEPS_ZARR, MONTHLY_TIMESERIES_ZARR, S3_MONTHLY_TIMESTEPS_ZARR, S3_MONTHLY_TIMESERIES_ZARR,
)


def check_local_zarrs_match_s3(local_zarr_path: str, s3_zarr_path: str) -> None:
    """
    Check that the local zarr matches the s3 zarr.
    """
    local_zarr = xr.open_zarr(local_zarr_path)
    s3_zarr = xr.open_zarr(s3_zarr_path, storage_options={'anon': True})

    if not (local_zarr['time'] == s3_zarr['time']).all():
        cl.error(f"Time arrays do not match in {local_zarr_path}")
        raise EnvironmentError('Time arrays do not match between local and s3 zarrs')

    if not (local_zarr['river_id'] == s3_zarr['river_id']).all():
        cl.error(f'River id arrays do not match in {local_zarr_path}')
        raise EnvironmentError('River id arrays do not match between local and s3 zarrs')

    if local_zarr['Q'].shape != s3_zarr['Q'].shape:
        cl.error(f"Q array shapes do not match {local_zarr_path}")
        raise EnvironmentError('Q array shapes do not match between local and s3 zarrs')


def check_for_init_files() -> None:
    # First find the last date in the local hourly zarr
    with xr.open_zarr(HOURLY_ZARR) as ds:
        last_hourly_retro_time = pd.to_datetime(ds['time'][-1].values).strftime('%Y%m%d%H%M')
    expected_final_state_file = f'finalstate_{last_hourly_retro_time}.parquet'

    # list all final state files and delete any that are not the state of interest or
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
        cl.error(f"Error running s5cmd copy command")
        raise Exception(f"Syncing from S3 failed. Error: {result.stderr}")

    # now check if every vpu has the state of interest
    states = natsorted(glob(os.path.join(FINAL_STATES_DIR, '*', 'finalstate*.parquet')))
    if len(states) != len(list(glob(os.path.join(CONFIGS_DIR, '*')))):
        cl.error('s5cmd to fetch inits succeeded but expected files are not present')
        raise RuntimeError("s5cmd to fetch inits succeeded but expected files are not present")
    return


if __name__ == '__main__':
    cl = CloudLog()

    try:
        cl.log('Verifying local data and code are synced with s3 and prepared for success')
        check_local_zarrs_match_s3(HOURLY_ZARR, S3_HOURLY_ZARR)
        check_local_zarrs_match_s3(DAILY_ZARR, S3_DAILY_ZARR)
        check_local_zarrs_match_s3(MONTHLY_TIMESERIES_ZARR, S3_MONTHLY_TIMESERIES_ZARR)
        check_local_zarrs_match_s3(MONTHLY_TIMESTEPS_ZARR, S3_MONTHLY_TIMESTEPS_ZARR)
        check_for_init_files()
        exit(0)
    except Exception as e:
        cl.error('Error while preparing environment')
        print(e)
        print(traceback.format_exc())
        exit(1)
