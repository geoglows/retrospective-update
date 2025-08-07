import traceback
from glob import glob

import numpy as np
import xarray as xr
from natsort import natsorted

from cloud_logger import CloudLog
from set_env_variables import *


def concatenate_outputs() -> None:
    # for each unique start date, sorted in order, open/merge the files from all vpus and append to the zarr
    vpu_outputs = natsorted(glob(os.path.join(OUTPUTS_DIR, '*')))
    unique_outputs = [os.path.basename(f) for f in natsorted(glob(os.path.join(vpu_outputs[0], '*')))]
    if not unique_outputs:
        cl.error(f"No-Qout-files-found")
        raise FileNotFoundError(f"No Qout files found in {OUTPUTS_DIR}")

    for unique_output in unique_outputs:
        discharges = list(natsorted(glob(os.path.join(OUTPUTS_DIR, '*', unique_output))))
        if not len(discharges) == len(vpu_outputs):
            cl.error('Discharge-not-found-for-every-vpu')
            raise FileNotFoundError(f"Discharge-not-found-for-{unique_output}")

        with xr.open_mfdataset(discharges, combine='nested', concat_dim='river_id', parallel=True, ) as new_ds:
            earliest_date = np.datetime_as_string(new_ds.time[0].values, unit="h")
            latest_date = np.datetime_as_string(new_ds.time[-1].values, unit="h")
            # load the dataset into memory from the individual files
            new_ds.load()

            # check that the time steps are not already
            hourly_times = xr.open_zarr(HOURLY_ZARR).time.values
            if new_ds.time.values[0] in hourly_times:
                cl.error(f'hourly steps already present for {earliest_date} to {latest_date}')
                raise RuntimeError(f'Hourly data found for {earliest_date} to {latest_date}. Needs human intervention.')
            cl.log(f'Appending to hourly time step zarr {earliest_date} to {latest_date}')
            new_ds.to_zarr(HOURLY_ZARR, mode='a', append_dim='time', consolidated=True, zarr_format=2)
            cl.log('Finished appending to hourly zarr')

            # Append daily data
            new_ds = new_ds.resample(time='1D').mean('time')
            daily_times = xr.open_zarr(DAILY_ZARR).time.values
            if new_ds.time.values[0] in daily_times:
                cl.error(f'daily steps already present for {earliest_date} to {latest_date}')
                raise RuntimeError(f'Daily data found for {earliest_date} to {latest_date}. Needs human intervention.')
            cl.log(f'Appending to daily time step zarr {earliest_date} to {latest_date}')
            new_ds.to_zarr(DAILY_ZARR, mode='a', append_dim='time', consolidated=True, zarr_format=2)
            cl.log('Finished appending to daily zarr')

    return


def verify_concatenated_outputs(zarr) -> None:
    """
    Verifies that the concatenated outputs are correct
    """
    with xr.open_zarr(zarr) as ds:
        time_size = ds.chunks['time'][0]
        # Test a river to see if there are nans
        if np.isnan(ds.isel(river_id=1, time=slice(time_size, -1))['Q'].values).any():
            cl.error(f'{zarr}-contain-nans')
            raise RuntimeError('Zarr contains nans')

        # Verify that the time dimension is correct
        times = ds['time'].values
        if not np.all(np.diff(times) == times[1] - times[0]):
            cl.error(f'Time-dimension-of-{zarr}-is-incorrect')
            raise RuntimeError(f'Time dimension of {zarr} zarr is not correct')


if __name__ == '__main__':
    cl = CloudLog()
    try:
        cl.log('concatenating-outputs')
        concatenate_outputs()

        cl.log('checking local zarr is good to go')
        for z in [DAILY_ZARR, HOURLY_ZARR]:
            verify_concatenated_outputs(z)
    except Exception as e:
        cl.error(f'Failed to append new discharge to existing zarrs. {str(e)}')
        print(traceback.format_exc())
        exit(1)
