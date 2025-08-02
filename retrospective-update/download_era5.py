import glob
import os
import traceback
from datetime import datetime

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
from natsort import natsorted

from cloud_logger import CloudLog


def download_era5(era5_dir: str, daily_zarr: str, hourly_zarr: str, min_lag_time_days: int) -> None:
    """
    1. determines the last simulated dateø in the daily zarr
    2. determines the list of days needed to download between the last simulation and today minus the lag time
    3. downloads the era5 data
    4. checks the era5 for covering the expected time range
    5. checks the era5 for compatible time steps with the existing discharge zarrs
    """
    last_date = xr.open_zarr(daily_zarr)['time'].values[-1]
    os.makedirs(era5_dir, exist_ok=True)

    today = pd.to_datetime(datetime.now().date())
    last_date = pd.to_datetime(last_date)
    first_date_to_simulate = last_date + pd.DateOffset(days=1)
    final_date_to_simulate = today - pd.DateOffset(days=min_lag_time_days)
    if pd.to_datetime(last_date + np.timedelta64(min_lag_time_days, 'D')) > datetime.now():
        # If the last date in the zarr file is within min_lag_time_days of today then exit
        cl.ping('STOPPING', f'{last_date}-is-within-{min_lag_time_days}-days-of-today.-Stopping')
        raise EnvironmentError('Last date in zarr is more recent than expected minimum lag time')
    date_range = pd.date_range(start=first_date_to_simulate, end=final_date_to_simulate, freq='D', )

    if not len(date_range):
        cl.ping('STOPPING', f'No dates to download between {first_date_to_simulate} and {final_date_to_simulate}')
        raise EnvironmentError('No dates to download')

    # make a list of unique year and month combinations in the list, in order. CDS API wants single month retrievals
    downloads = []
    year_month_combos = {(d.year, d.month) for d in date_range}
    year_month_combos = natsorted(year_month_combos)
    for year, month in year_month_combos:
        download_dates = [d for d in date_range if d.year == year and d.month == month]
        days = natsorted([d.day for d in download_dates])
        expected_file_name = os.path.join(era5_dir, date_to_file_name(year, month, days))
        if not os.path.exists(expected_file_name):
            downloads.append((year, month, days, expected_file_name))

    if not len(downloads):  # should already by caught by the previous dates check, this is redundancy
        cl.ping('WARNING', "No new ERA5 data need to be downloaded, proceeding to validation")

    for year, month, days, file in downloads:
        retrieve_data(year=year, month=month, days=days, file=file)

    cl.ping('PROCESSING', "Validating downloaded ERA5")
    downloaded_files = natsorted(glob.glob(os.path.join(era5_dir, '*.nc')))
    if not downloaded_files:
        cl.ping('FINISHED', "No ERA5 files were downloaded")
        return

    for downloaded_file in downloaded_files:
        with xr.load_dataset(
                downloaded_file,
                chunks={'time': 'auto', 'lat': 'auto', 'lon': 'auto'},
                # Included to prevent weird slicing behavior and missing data
        ) as ds:
            # cdsapi does not validate that the range of dates you ask for is all available. we need to validate that we got everything we expected
            if ds['valid_time'].shape[0] == 0:
                print(f'valid_time has no entries in file {downloaded_file}')
                os.remove(downloaded_file)
                continue

            # Make sure that the last timestep is T23:00 (i.e., a full day)
            if ds.valid_time[-1].values != np.datetime64(f'{ds.valid_time[-1].values.astype("datetime64[D]")}T23:00'):
                # Remove timesteps on partial days
                ds = ds.sel(valid_time=slice(None, np.datetime64(
                    f'{ds.valid_time[-1].values.astype("datetime64[D]")}') - np.timedelta64(1, 'h')))
                # If there is no more time, skip this file
                if len(ds.valid_time) == 0:
                    print(f'No valid time left in file {downloaded_file} after removing partial days')
                    os.remove(downloaded_file)
                    continue
                ds.to_netcdf(downloaded_file)

    downloaded_files = list(natsorted(glob.glob(os.path.join(era5_dir, '*.nc'))))
    if not downloaded_files:
        cl.ping('STOPPING', "ERA5-downloaded-but-no-usable-data-obtained")
        raise RuntimeError('No usable runoff data obtained from ERA5 download')

    with xr.open_mfdataset(downloaded_files) as ds, xr.open_zarr(hourly_zarr) as hourly_ds:
        # Check the time dimension
        ro_time = ds['valid_time'].values
        retro_time = hourly_ds['time'].values
        total_time = np.concatenate((retro_time, ro_time))
        difs = np.diff(total_time)
        if not np.all(difs == difs[0]):
            cl.ping('STOPPING', "Time-dimension-of-ERA5-is-not-compatible-with-the-retrospective-zarr")

        # Check that there are no nans
        if np.isnan(ds['ro'].values).any():
            cl.ping('STOPPING', "ERA5-data-contains-nans")


def date_to_file_name(year: int, month: int, days: list[int]) -> str:
    # Creates a zero padded YYYYMMDD date string from integer ymd for era5 files
    padded_month = str(month).zfill(2)
    padded_day_0 = str(days[0]).zfill(2)
    padded_day_1 = str(days[-1]).zfill(2)
    return f'era5_{year}{padded_month}{padded_day_0}-{year}{padded_month}{padded_day_1}.nc'


def retrieve_data(year: int, month: int, days: list[int], file: str, ) -> None:
    if os.path.exists(file):
        return
    c = cdsapi.Client()
    c.retrieve(
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
        target=file
    )


if __name__ == '__main__':
    # Parameters
    MIN_LAG_TIME_DAYS = int(os.getenv('MIN_LAG_TIME_DAYS', 5))
    ERA5_DIR = os.getenv('ERA5_DIR')
    DAILY_ZARR = os.getenv('DAILY_ZARR')
    HOURLY_ZARR = os.getenv('HOURLY_ZARR')
    S3_DAILY_ZARR = os.getenv('S3_DAILY_ZARR')
    S3_HOURLY_ZARR = os.getenv('S3_HOURLY_ZARR')

    cl = CloudLog()
    try:
        cl.ping('RUNNING', 'Downloading ERA5 data')
        download_era5(
            era5_dir=ERA5_DIR,
            daily_zarr=DAILY_ZARR,
            hourly_zarr=HOURLY_ZARR,
            min_lag_time_days=MIN_LAG_TIME_DAYS,
        )
        cl.ping('FINISHED', 'ERA5 data prepared successfully')
        exit(0)
    except Exception as e:
        cl.ping('ERROR', 'An error occurred while downloading ERA5 data')
        cl.ping('ERROR', f'{e}')
        cl.ping('ERROR', traceback.format_exc())
        exit(1)
