import glob
import os
import datetime
import multiprocessing

import xarray as xr
import numpy as np
import netCDF4 as nc
import s3fs
import pandas as pd
"""
We create a qfinal file for the last date in the retrospective zarr file. This file is used to initialize the historical model.
"""

output_dir = '/home/ubuntu/data/outputs'
retro_zarr = '/mnt/retro_volume/geoglows_v2_retrospective.zarr/'

# Create an S3FileSystem instance to get comid files from geoglows bucket
s3 = s3fs.S3FileSystem(anon=True)
vpu_folders = [f for f in s3.ls('geoglows-v2/configs/') if not f.endswith('.parquet')]
comids: dict[str: str] = {}
for vpu in vpu_folders:
    comids[vpu.split('/')[-1]] = [f for f in s3.ls(vpu) if '/comid' in f][0]

def main() -> None:
    """
    This is the main function that creates the qfinal files for each VPU.

    Returns:
        None
    """
    processes = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=processes) as pool:
        pool.map(helper, split_into_sublists([vpu for vpu, _ in comids.items()], processes))
    print('Finished')

def helper(vpus: list[str]) -> None:
    """
    Helper function to create a netCDF file containing river water discharge data for each river reach.

    Parameters:
    vpus (list[str]): List of VPU identifiers.

    Returns:
    None
    """
    ds = xr.open_zarr(retro_zarr)
    last_retro_time = xr.open_zarr(retro_zarr).time[-1].values
    last_time: str = np.datetime_as_string(last_retro_time, unit='D').replace('-','')
    s3 = s3fs.S3FileSystem(anon=True)
    for vpu in vpus:
        with s3.open(comids[vpu], 'r') as f:
            df = pd.read_csv(f)

        out_file = os.path.join(output_dir, vpu, f"Qfinal_{vpu}_{last_time}.nc")

        with nc.Dataset(out_file, "w", format="NETCDF3_CLASSIC") as inflow_nc:
            # create dimensions
            inflow_nc.createDimension('time', 1)
            inflow_nc.createDimension('rivid', df['LINKNO'].shape[0])

            qout_var = inflow_nc.createVariable('Qout', 'f8', ('time', 'rivid'))
            qout_var[:] = ds.isel(time=-1).sel(rivid=df['LINKNO'].values)['Qout'].values
            qout_var.long_name = 'instantaneous river water discharge downstream of each river reach'
            qout_var.units = 'm3 s-1'
            qout_var.coordinates = 'lon lat'
            qout_var.grid_mapping = 'crs'
            qout_var.cell_methods = "time: point"

            # rivid
            rivid_var = inflow_nc.createVariable('rivid', 'i4', ('rivid',))
            rivid_var[:] = df['LINKNO'].values
            rivid_var.long_name = 'unique identifier for each river reach'
            rivid_var.units = '1'
            rivid_var.cf_role = 'timeseries_id'

            # time
            time_var = inflow_nc.createVariable('time', 'i4', ('time',))
            time_var[:] = [0]
            time_var.long_name = 'time'
            time_var.standard_name = 'time'
            time_var.units = f'seconds since {np.datetime_as_string(last_retro_time, unit="D")}'  # Must be seconds
            time_var.axis = 'T'
            time_var.calendar = 'gregorian'

            # longitude
            lon_var = inflow_nc.createVariable('lon', 'f8', ('rivid',))
            lon_var[:] = df['lon'].values
            lon_var.long_name = 'longitude of a point related to each river reach'
            lon_var.standard_name = 'longitude'
            lon_var.units = 'degrees_east'
            lon_var.axis = 'X'

            # latitude
            lat_var = inflow_nc.createVariable('lat', 'f8', ('rivid',))
            lat_var[:] = df['lat'].values
            lat_var.long_name = 'latitude of a point related to each river reach'
            lat_var.standard_name = 'latitude'
            lat_var.units = 'degrees_north'
            lat_var.axis = 'Y'

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

        ds.close()

def split_into_sublists(lst: list, n: int) -> list[list]:
    """
    Splits a list into sublists of approximately equal size.

    Args:
        lst (list): The list to be split.
        n (int): The number of sublists to create.

    Returns:
        list: A list of sublists, where each sublist contains approximately len(lst) // n elements.
    """
    # Calculate the size of each sublist
    sublist_size = len(lst) // n

    # Use list comprehension to create sublists
    sublists = [lst[i * sublist_size:(i + 1) * sublist_size] for i in range(n - 1)]
    
    # Add the remaining elements to the last sublist
    sublists.append(lst[(n - 1) * sublist_size:])
    
    return sublists

if __name__ == '__main__':
    multiprocessing.set_start_method('forkserver')
    main()