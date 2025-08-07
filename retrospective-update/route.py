import traceback
from datetime import datetime
from glob import glob
from multiprocessing import Pool

import numpy as np
import pandas as pd
import river_route as rr
import xarray as xr
from natsort import natsorted
from netCDF4 import Dataset, date2num
from tqdm import tqdm

from cloud_logger import CloudLog
from set_env_variables import *


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

    if os.path.exists(final_state_file):
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


def make_rapid_style_inits(args) -> None:
    vpu, final_timestamp = args
    date_value = datetime.strptime(final_timestamp, '%Y%m%d%H%M')

    config_file = os.path.join(vpu, "routing_parameters.parquet")

    # Load data
    rivid = pd.read_parquet(config_file)["river_id"].values.astype(np.int32)
    lat = np.zeros_like(rivid, dtype=np.float64)
    lon = np.zeros_like(rivid, dtype=np.float64)

    expected_qinit_file = os.path.join(FINAL_STATES_DIR, os.path.basename(vpu), f'finalstate_{final_timestamp}.parquet')
    qinit = pd.read_parquet(expected_qinit_file)["Q"].values
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
    output_full_path = os.path.join(output_basedir, f"Qinit_{date_value.strftime('%Y%m%d00')}.nc")
    with Dataset(output_full_path, "w", format="NETCDF4") as nc:
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
            cl.error('Last time step in the zarr timeseries is different than the first era5')
            raise RuntimeError('Last time step in the zarr timeseries is different than the first era5')
        vpus = natsorted(glob(os.path.join(CONFIGS_DIR, '*')))

        with Pool(os.cpu_count()) as p:
            cl.log('Routing')
            list(
                tqdm(
                    p.imap_unordered(route_vpu, [(vpu, era5_data, init_timestamp, final_timestamp) for vpu in vpus]),
                    total=len(vpus), desc='Routing VPUs'
                ),
            )

            cl.log('Making Forecast Inits')
            list(
                tqdm(
                    p.imap_unordered(make_rapid_style_inits, [(vpu, final_timestamp) for vpu in vpus]),
                    total=len(vpus), desc='Making forecast inits'
                )
            )
        exit(0)
    except Exception as e:
        cl.error(e)
        print(traceback.format_exc())
        exit(1)
