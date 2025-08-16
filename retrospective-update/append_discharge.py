import os
import traceback
from glob import glob

import numpy as np
import xarray as xr
from natsort import natsorted

from cloud_logger import CloudLog
from set_env_variables import (
    DAILY_ZARR, HOURLY_ZARR, DISCHARGE_DIR
)


def append(new_ds: xr.Dataset, zarr_path: str) -> None:
    # check that the time steps are not already
    earliest_date = np.datetime_as_string(new_ds.time[0].values, unit="h")
    latest_date = np.datetime_as_string(new_ds.time[-1].values, unit="h")
    existing_times = xr.open_zarr(zarr_path).time.values
    if new_ds.time.values[0] in existing_times:
        cl.error(f'Time steps for {earliest_date} to {latest_date} already in zarr. Needs human intervention.')
        raise RuntimeError
    if new_ds.river_id.shape != xr.open_zarr(zarr_path).river_id.shape:
        cl.error(f'River id shape mismatch. Probably corrupt or missing netcdfs.')
        raise RuntimeError
    cl.log(f'Appending time steps {earliest_date} to {latest_date} on {zarr_path}')
    new_ds.to_zarr(zarr_path, mode='a', append_dim='time', consolidated=True, zarr_format=2)
    cl.log('Finished appending to zarr: {zarr_path}')
    return


def concatenate_outputs() -> None:
    # for each unique start date, sorted in order, open/merge the files from all vpus and append to the zarr
    vpu_outputs = natsorted(glob(os.path.join(DISCHARGE_DIR, '*')))
    unique_outputs = [os.path.basename(f) for f in natsorted(glob(os.path.join(vpu_outputs[0], '*')))]
    if not unique_outputs:
        cl.error(f"No Qout files found in {DISCHARGE_DIR}")
        raise FileNotFoundError

    for unique_output in unique_outputs:
        discharges = list(natsorted(glob(os.path.join(DISCHARGE_DIR, '*', unique_output))))
        if not len(discharges) == len(vpu_outputs):
            cl.error(f"Discharge not found for {unique_output}")
            raise FileNotFoundError

        with xr.open_mfdataset(discharges, combine='nested', concat_dim='river_id') as new_ds:
            new_ds.load()
            append(new_ds=new_ds, zarr_path=HOURLY_ZARR)
            new_ds = new_ds.resample(time='1D').mean('time')
            append(new_ds=new_ds, zarr_path=DAILY_ZARR)
    return


def verify_concatenated_outputs(zarr) -> None:
    """
    Verifies that the concatenated outputs are correct
    """
    cl.log(f'Verifying {zarr} zarr after appending')
    with xr.open_zarr(zarr) as ds:
        time_size = ds.chunks['time'][0]
        # Test a river to see if there are nans
        if np.isnan(ds.isel(river_id=1, time=slice(time_size, -1))['Q'].values).any():
            cl.error(f'{zarr} contain nans')
            raise RuntimeError

        # Verify that the time dimension is correct
        times = ds['time'].values
        if not np.all(np.diff(times) == times[1] - times[0]):
            cl.error(f'Time dimension of {zarr} zarr is not correct')
            raise RuntimeError


if __name__ == '__main__':
    cl = CloudLog()
    try:
        cl.log('Appending new discharge to zarr files')
        concatenate_outputs()
        for z in [DAILY_ZARR, HOURLY_ZARR]:
            verify_concatenated_outputs(z)
        cl.log('Discharge zarrs updated successfully')
    except Exception as e:
        cl.error(str(e))
        cl.error(traceback.format_exc())
        exit(1)
