import os
import subprocess
import warnings
from glob import glob

import numpy as np
import pandas as pd
import river_route as rr
import s3fs
import tqdm
import xarray as xr
from natsort import natsorted

from cloud_logger import CloudLog


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
        f's5cmd --credentials-file {credentials} sync --exclude "*Q/0*" {s3_path}/* {local_path}',
        shell=True, capture_output=True, text=True,
    )
    if not result.returncode == 0:
        raise Exception(f"Download failed. Error: {result.stderr}")

    return


def get_qinits_from_s3(s3: s3fs.S3FileSystem,
                       local_hourly_zarr: str,
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
    # First find the last date in the local hourly zarr
    with xr.open_zarr(local_hourly_zarr) as ds:
        last_retro_time = ds['time'][-1].values

    last_retro_time = pd.to_datetime(last_retro_time).strftime('%Y%m%d%H%M')
    # todo use s5cmd to find the init files directly since the date is known

    # download the qfinal files
    for vpu in tqdm.tqdm(glob(os.path.join(configs_dir, '*'))):
        vpu = os.path.basename(vpu)
        s3_qfinals = [f for f in s3.ls(f'{s3_qfinal_dir}/{vpu}/') if last_retro_time in f]
        local_qfinals = set(glob(os.path.join(outputs_dir, vpu, 'finalstate*.parquet')))
        if not s3_qfinals:
            raise FileNotFoundError(
                f"No finalstate files found for {vpu} in S3 at {s3_qfinal_dir} for {last_retro_time}")

        s3_qfinal = s3_qfinals[0]  # Take the first one, there should only be one

        exists = False
        for local_file_name in local_qfinals:
            if os.path.basename(local_file_name) == os.path.basename(s3_qfinal):
                # If the local file already exists, skip downloading
                exists = True
                continue

            # Otherwise, remove the local file that doesn't match
            os.remove(local_file_name)
        if not exists:
            # Download the file from S3
            s3.get(s3_qfinal, os.path.join(outputs_dir, vpu, os.path.basename(s3_qfinal)))

    return


def route_vpu(args):
    config_dir, era5_files, outputs_dir = args
    vpu = os.path.basename(config_dir)
    params_file = os.path.join(config_dir, 'routing_parameters.parquet')
    weight_table = os.path.join(config_dir, f'gridweights_ERA5_{vpu}.nc')
    connectivity_file = os.path.join(config_dir, 'connectivity.parquet')

    if not os.path.exists(params_file):
        raise FileNotFoundError(f"Routing parameters file not found: {params_file}")
    if not os.path.exists(connectivity_file):
        raise FileNotFoundError(f"Connectivity file not found: {connectivity_file}")
    if not os.path.exists(weight_table):
        raise FileNotFoundError(f"Weight table file not found: {weight_table}")

    output_dir = os.path.join(outputs_dir, vpu)
    os.makedirs(output_dir, exist_ok=True)

    # todo probably determine initial and final state timestampes earlier and pass them along to this
    # initial_state_file = natsorted(glob(os.path.join(output_dir, 'finalstate*.parquet')))[-1]
    initial_state_file = f'/data/final-states/{vpu}/finalstate_202507232300.parquet'
    final_state_file = os.path.join('/data/final-states', vpu, f"finalstate_{os.path.basename(initial_state_file)}")
    output_files = [os.path.join(output_dir, os.path.basename(era5_file).replace('era5_', 'Q_')) for era5_file in
                    era5_files]

    (
        rr
        .Muskingum(
            routing_params_file=params_file,
            connectivity_file=connectivity_file,
            runoff_depths_file=era5_files,
            weight_table_file=weight_table,
            var_t='valid_time',
            var_x='longitude',
            var_y='latitude',
            outflow_file=output_files,
            initial_state_file=initial_state_file,
            final_state_file=final_state_file,
            progress_bar=False,
            log=False,
        )
        .route()
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
    return ds[[qout, ]].reset_coords(drop=True)


def concatenate_outputs(outputs_dir: str,
                        hourly_zarr_path: str,
                        daily_zarr_path: str,
                        CL: CloudLog) -> None:
    # Build the week dataset
    # for each unique start date, sorted in order, open/merge the files from all vpus and append to the zarr
    vpu_outputs = natsorted(glob(os.path.join(outputs_dir, '*')))
    unique_outputs = natsorted(glob(os.path.join(vpu_outputs[0], '*')))
    if not unique_outputs:
        CL.ping('FAIL', f"No-Qout-files-found")
        raise FileNotFoundError(f"No Qout files found in {outputs_dir}")

    for unique_output in unique_outputs:
        discharges = natsorted(glob(os.path.join(outputs_dir, '*', unique_output)))
        if not len(discharges) == len(vpu_outputs):
            CL.ping('FAIL', 'Discharge-not-found-for-every-vpu')
            raise FileNotFoundError(f"Discharge-not-found-for-{unique_output}")

            with xr.open_mfdataset(qouts, combine='nested', concat_dim='river_id', parallel=True, ) as new_ds:
                earliest_date = np.datetime_as_string(new_ds.time[0].values, unit="h")
                latest_date = np.datetime_as_string(new_ds.time[-1].values, unit="h")
                new_ds = new_ds.round(decimals=3)
                new_ds = new_ds.where(new_ds['Q'] >= 0.0, 0.0)
                # load the dataset into memory from the individual files
                new_ds.load()

                # Append hourly data first
                CL.ping('RUNNING', f'Appending-to-hourly-zarr-{earliest_date}-to-{latest_date}')
                new_ds.to_zarr(hourly_zarr_path, mode='a', append_dim='time', consolidated=True)

                # Append daily data
                CL.ping('RUNNING', f'Appending-to-daily-zarr-{earliest_date}-to-{latest_date}')
                new_ds = new_ds.resample(time='1D').mean('time')
                new_ds.to_zarr(daily_zarr_path, mode='a', append_dim='time', consolidated=True)
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


def sync_qfinals_to_s3(outputs_dir: str,
                       s3_qfinal_dir: str,
                       credentials: str,
                       CL: CloudLog) -> None:
    CL.ping('RUNNING', 'syncing-finalstates-to-S3')
    result = subprocess.run(
        f's5cmd --credentials-file {credentials} sync --include "*finalstate*" --size-only {outputs_dir}/ {s3_qfinal_dir}/',
        shell=True, capture_output=True, text=True,
    )
    if result.returncode != 0:
        CL.ping('FAIL', f"Syncing-finalstates-to-S3-failed")
        exit()


def sync_local_to_s3(local_hourly_zarr: str,
                     local_daily_zarr: str,
                     s3_hourly_zarr: str,
                     s3_daily_zarr: str,
                     credentials: str,
                     CL: CloudLog) -> None:
    """
    Put our local edits on the zarrs to S3.

    Raises:
        Exception: If the sync command fails.
    """
    # sync the zarrs. We can use sync because 0.* files are not is not on local side
    for zarr, s3_zarr in zip([local_hourly_zarr, local_daily_zarr], [s3_hourly_zarr, s3_daily_zarr]):
        CL.ping('RUNNING', f'syncing-{zarr}-to-S3')
        result = subprocess.run(
            f"s5cmd --credentials-file {credentials} sync {zarr}/ {s3_zarr}/",
            shell=True, capture_output=True, text=True,
        )
        if result.returncode != 0:
            CL.ping('FAIL', f"Syncing-{zarr}-to-S3-failed")
            raise Exception(f"Syncing {zarr} to S3 failed. Error: {result.stderr}")

    return
