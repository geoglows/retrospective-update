import os
import traceback
from datetime import datetime
from glob import glob
from multiprocessing import Pool

import numcodecs
import numpy as np
import pandas as pd
import river_route as rr
import xarray as xr
from natsort import natsorted
from netCDF4 import Dataset, date2num
from tqdm import tqdm

from cloud_logger import CloudLog

# Parameters
MIN_LAG_TIME_DAYS = int(os.getenv('MIN_LAG_TIME_DAYS', 5))

DATA_DIR = os.getenv('WORK_DIR')
ERA5_DIR = os.getenv('ERA5_DIR')
CONFIGS_DIR = os.getenv('CONFIGS_DIR')
OUTPUTS_DIR = os.getenv('OUTPUTS_DIR')
HYDROSOS_DIR = os.getenv('HYDROSOS_DIR')

FINAL_STATES_DIR = os.getenv('FINAL_STATES_DIR')
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


def route_vpu(args):
    config_dir, era5_files, init_timestamp, final_timestamp = args
    vpu = os.path.basename(config_dir)
    params_file = os.path.join(config_dir, 'routing_parameters.parquet')
    weight_table = os.path.join(config_dir, f'gridweights_ERA5_{vpu}.nc')
    connectivity_file = os.path.join(config_dir, 'connectivity.parquet')
    initial_state_file = os.path.join(FINAL_STATES_DIR, vpu, f'finalstate_{init_timestamp}.parquet')
    final_state_file = os.path.join(FINAL_STATES_DIR, vpu, f'finalstate_{final_timestamp}.parquet')

    if not os.path.exists(params_file):
        raise FileNotFoundError(f"Routing parameters file not found: {params_file}")
    if not os.path.exists(connectivity_file):
        raise FileNotFoundError(f"Connectivity file not found: {connectivity_file}")
    if not os.path.exists(weight_table):
        raise FileNotFoundError(f"Weight table file not found: {weight_table}")
    if not os.path.exists(initial_state_file):
        raise FileNotFoundError(f"Initial state file not found: {initial_state_file}")

    outdir = os.path.join(OUTPUTS_DIR, vpu)
    os.makedirs(outdir, exist_ok=True)

    output_files = [
        os.path.join(outdir, os.path.basename(era5_file).replace('era5_', 'Q_')) for era5_file in era5_files
    ]

    if os.path.exists(final_state_file):  # final state is the last thing to be made so we only need to look for this
        return

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


def concatenate_outputs() -> None:
    # Build the week dataset
    # for each unique start date, sorted in order, open/merge the files from all vpus and append to the zarr
    vpu_outputs = natsorted(glob(os.path.join(OUTPUTS_DIR, '*')))
    unique_outputs = [os.path.basename(f) for f in natsorted(glob(os.path.join(vpu_outputs[0], '*')))]
    if not unique_outputs:
        cl.ping('FAIL', f"No-Qout-files-found")
        raise FileNotFoundError(f"No Qout files found in {OUTPUTS_DIR}")

    for unique_output in unique_outputs:
        discharges = list(natsorted(glob(os.path.join(OUTPUTS_DIR, '*', unique_output))))
        if not len(discharges) == len(vpu_outputs):
            cl.ping('FAIL', 'Discharge-not-found-for-every-vpu')
            raise FileNotFoundError(f"Discharge-not-found-for-{unique_output}")

        with xr.open_mfdataset(discharges, combine='nested', concat_dim='river_id', parallel=True, ) as new_ds:
            earliest_date = np.datetime_as_string(new_ds.time[0].values, unit="h")
            latest_date = np.datetime_as_string(new_ds.time[-1].values, unit="h")
            # QA/QC which should already have been done by river-route
            new_ds = new_ds.round(decimals=3)
            new_ds = new_ds.where(new_ds['Q'] >= 0.0, 0.0)
            # load the dataset into memory from the individual files
            new_ds.load()
            encoding = {
                'Q': {
                    'dtype': 'float32',
                    'compressor': numcodecs.Blosc(cname='zstd', clevel=3, shuffle=numcodecs.Blosc.BITSHUFFLE),
                }
            }

            # check that the time steps are not already
            hourly_step_times = xr.open_zarr(HOURLY_STEP_ZARR).time.values
            if new_ds.time.values[0] in hourly_step_times:
                cl.ping('Error', f'hourly steps already present for {earliest_date} to {latest_date}')
                raise RuntimeError(f'Hourly steps already present for {earliest_date} to {latest_date}. This requires human intervention to resolve.')
            else:
                cl.ping('RUNNING', f'Writing-hourly-time-stepped-zarr-{earliest_date}-to-{latest_date}')
                if os.path.isdir(HOURLY_STEP_ZARR):
                    new_ds.to_zarr(HOURLY_STEP_ZARR, mode='a', append_dim='time', consolidated=True, zarr_format=2)
                else:
                    (
                        new_ds
                        .chunk({'time': 24, 'river_id': 20_000})
                        .to_zarr(HOURLY_STEP_ZARR, mode='w', consolidated=True, zarr_format=2, encoding=encoding)
                    )

            # Append daily data
            new_ds = new_ds.resample(time='1D').mean('time')
            daily_step_time = xr.open_zarr(DAILY_STEP_ZARR).time.values
            if new_ds.time.values[0] in daily_step_time:
                cl.ping('Error', f'daily steps already present for {earliest_date} to {latest_date}')
                raise RuntimeError(f'Daily steps already present for {earliest_date} to {latest_date}. This requires human intervention to resolve.')
            else:
                cl.ping('RUNNING', f'Writing-daily-to-time-stepped-zarr-{earliest_date}-to-{latest_date}')
                if os.path.isdir(DAILY_STEP_ZARR):
                    new_ds.to_zarr(DAILY_STEP_ZARR, mode='a', append_dim='time', consolidated=True, zarr_format=2)
                else:
                    (
                        new_ds
                        .chunk({'time': 1, 'river_id': 20_000})
                        .to_zarr(DAILY_STEP_ZARR, mode='w', consolidated=True, zarr_format=2, encoding=encoding)
                    )

    return


def verify_concatenated_outputs(zarr: str, cl: CloudLog) -> None:
    """
    Verifies that the concatenated outputs are correct
    """
    with xr.open_zarr(zarr) as ds:
        time_size = ds.chunks['time'][0]
        # Test a river to see if there are nans
        if np.isnan(ds.isel(river_id=1, time=slice(time_size, -1))['Q'].values).any():
            cl.ping('FAIL', f'{zarr}-contain-nans')
            exit()

        # Verify that the time dimension is correct
        times = ds['time'].values
        if not np.all(np.diff(times) == times[1] - times[0]):
            cl.ping('FAIL', f'Time-dimension-of-{zarr}-is-incorrect')
            exit()


def make_rapid_style_inits(args) -> None:
    vpu, final_timestamp = args
    date_value = datetime.strptime(final_timestamp, '%Y%m%d%H%M')

    config_file = os.path.join(vpu, "routing_parameters.parquet")

    # Load data
    rivid = pd.read_parquet(config_file)["river_id"].values.astype(np.int32)
    lat = np.zeros_like(rivid, dtype=np.float64)
    lon = np.zeros_like(rivid, dtype=np.float64)
    print('about to read qinit')
    print(FINAL_STATES_DIR)
    print(os.path.basename(vpu))
    print(os.path.join(FINAL_STATES_DIR, os.path.basename(vpu), f'finalstate_{final_timestamp}.parquet'))
    qinit = pd.read_parquet(
        os.path.join(
            FINAL_STATES_DIR, os.path.basename(vpu), f'finalstate_{final_timestamp}.parquet'
        )
    )["Q"].values
    print('just read qinit')
    # Ensure qinit has no negative values
    qinit[qinit < 0] = 0

    # Time info
    time_units = f"seconds since {date_value.strftime('%Y-%m-%d %H:%M:%S')}"
    calendar = "gregorian"
    time_num = date2num(date_value, units=time_units, calendar=calendar)

    # Create output dataset
    output_basedir = os.path.join(
        FORECAST_INITS_DIR,
        f"Qinit_{date_value.strftime('%Y%m%d00')}",
        vpu.split("=")[-1]
    )
    os.makedirs(output_basedir, exist_ok=True)
    output_full_name = os.path.join(output_basedir, f"Qinit_{date_value.strftime('%Y%m%d00')}.nc")
    with Dataset(output_full_name, "w", format="NETCDF4") as nc:
        nc.Conventions = "CF-1.6"
        nc.featureType = "timeSeries"

        # Dimensions
        nc.createDimension("time", 1)
        nc.createDimension("rivid", len(rivid))

        # Variables
        Qout_var = nc.createVariable("Qout", "f8", ("time", "rivid"))
        Qout_var.long_name = "instantaneous river water discharge downstream of each river reach"
        Qout_var.units = "m3 s-1"
        Qout_var.coordinates = "lon lat"
        Qout_var.grid_mapping = "crs"
        Qout_var.cell_methods = "time: point"

        rivid_var = nc.createVariable("rivid", "i4", ("rivid",))
        rivid_var.long_name = "unique identifier for each river reach"
        rivid_var.units = "1"
        rivid_var.cf_role = "timeseries_id"

        time_var = nc.createVariable("time", "i4", ("time",))
        time_var.long_name = "time"
        time_var.standard_name = "time"
        time_var.units = time_units
        time_var.axis = "T"
        time_var.calendar = calendar

        lon_var = nc.createVariable("lon", "f8", ("rivid",))
        lon_var.long_name = "longitude of a point related to each river reach"
        lon_var.standard_name = "longitude"
        lon_var.units = "degrees_east"
        lon_var.axis = "X"

        lat_var = nc.createVariable("lat", "f8", ("rivid",))
        lat_var.long_name = "latitude of a point related to each river reach"
        lat_var.standard_name = "latitude"
        lat_var.units = "degrees_north"
        lat_var.axis = "Y"

        crs_var = nc.createVariable("crs", "i4")
        crs_var.grid_mapping_name = "latitude_longitude"
        crs_var.epsg_code = "EPSG:4326"
        crs_var.semi_major_axis = 6378137.0
        crs_var.inverse_flattening = 298.257223563

        # Assign values
        rivid_var[:] = rivid
        lat_var[:] = lat
        lon_var[:] = lon
        time_var[0] = time_num
        Qout_var[0, :] = qinit
        crs_var.assignValue(0)


if __name__ == '__main__':
    cl = CloudLog()
    try:
        # determine the first and last time step that will come out of routing by reading the era5 files
        era5_data = natsorted(glob(os.path.join(ERA5_DIR, 'era5_*.nc')))
        init_timestamp = pd.to_datetime(xr.open_zarr(HOURLY_ZARR).time[-1].values).strftime('%Y%m%d%H%M')
        final_timestamp = pd.to_datetime(xr.open_dataset(era5_data[-1]).valid_time[-1].values).strftime('%Y%m%d%H%M')

        init_timestamp_era5 = pd.to_datetime(xr.open_dataset(era5_data[0]).valid_time[0].values) - pd.Timedelta(hours=1)
        init_timestamp_era5 = init_timestamp_era5.strftime('%Y%m%d%H%M')
        if init_timestamp != init_timestamp_era5:
            cl.ping('ERROR', 'Last time step in the zarr timeseries is different than the first era5')
        vpus = natsorted(glob(os.path.join(CONFIGS_DIR, '*')))

        cl.ping('RUNNING', f'routing')
        with Pool(os.cpu_count()) as p:
            cl.ping('RUNNING', 'Running-river-route')
            list(
                tqdm(
                    p.imap_unordered(route_vpu, [(vpu, era5_data, init_timestamp, final_timestamp) for vpu in vpus]),
                    total=len(vpus), desc='Routing VPUs'
                ),
            )

            cl.ping('RUNNING', 'concatenating-outputs')
            concatenate_outputs()

            cl.ping('RUNNING', 'checking local zarr is good to go')
            for z in [DAILY_ZARR, HOURLY_ZARR]:
                verify_concatenated_outputs(z, cl)

            cl.ping('RUNNING', 'making-forecast-inits-from-retrospective-final-states')
            list(
                tqdm(
                    p.imap_unordered(make_rapid_style_inits, [(vpu, final_timestamp) for vpu in vpus]),
                    total=len(vpus), desc='Making forecast inits'
                )
            )

        cl.ping('COMPLETE', "Retrospective-update-complete")
        exit(0)
    except Exception as e:
        error = traceback.format_exc()
        cl.ping('FAIL', str(e))
        exit(1)
