import datetime
import glob
import logging
import os
import re

import netCDF4 as nc
import numpy as np
import pandas as pd
import psutil
import xarray as xr


def _memory_check(size: int, dtype: type = np.float32, ram_buffer_percentage: float = 0.8):
    """
    Internal function to check if the arrays we create will be larger than the memory available.
    Also warns against very large arrays. Default datatype of arrays is np.float32.
    By default, the warning will be announced if memory consumption is projected to be greater than
    80% of avaiable memory
    """
    num_bytes = np.dtype(dtype).itemsize * size
    available_mem = psutil.virtual_memory().available

    if num_bytes >= available_mem:
        raise MemoryError(
            f"Trying to allocate {psutil._common.bytes2human(num_bytes)} of "
            f"{psutil._common.bytes2human(available_mem)} available")
    if num_bytes >= available_mem * ram_buffer_percentage:
        print(f"WARNING: arrays will use ~{round(num_bytes / available_mem, 1)}% of \
        {psutil._common.bytes2human(available_mem)} available memory...")


def _cumulative_to_incremental(df) -> pd.DataFrame:
    #logging.info('Converting to incremental values')
    return pd.DataFrame(
        np.vstack([df.values[0, :], np.diff(df.values, axis=0)]),
        index=df.index,
        columns=df.columns
    )


def _incremental_to_cumulative(df) -> pd.DataFrame:
    #logging.info('Converting to cumulative values')
    return df.cumsum()

def create_inflow_file(lsm_data: str,
                       input_dir: str,
                       inflow_dir: str,
                       vpu_name: str = None,
                       weight_table: str = None,
                       comid_lat_lon_z: str = None,
                       timestep: datetime.timedelta = None,
                       cumulative: bool = False,
                       file_label: str = None,
                       force_positive_runoff: bool = False, ) -> None:
    """
    Generate inflow files for use with RAPID.

    output file name pattern is: m3_{vpu_name}_{start_date}_{end_date}_{optional_file_label}.nc

    Parameters
    ----------
    lsm_data: str
        A list of LSM files or a glob pattern string to LSM files
    input_dir: str
        Path to directory of vpu files which contains weight tables
    inflow_dir: str
        Path to directory where inflows will be saved
    vpu_name: str
        Name of the vpu. If none, assumed to be the name of the input directory
    weight_table: str, list
        Path and name of the weight table
    comid_lat_lon_z: str
        Path to the comid_lat_lon_z.csv corresponding to the weight table
    timestep: int, optional
        Time step of the inflow data in hours. Default is 3 hours
    cumulative: bool, optional
        Convert the inflow data to incremental values. Default is False
    file_label: str, optional
        Label to include in the file name for organization purposes.
    force_positive_runoff: bool, optional
           Set all negative values to zero. Default is False
    """
    # Ensure that every input file exists
    if weight_table is not None and not os.path.exists(weight_table):
        raise FileNotFoundError(f'{weight_table} does not exist')
    if comid_lat_lon_z is None:
        comid_lat_lon_z = os.path.join(input_dir, 'comid_lat_lon_z.csv')
        if not os.path.exists(comid_lat_lon_z):
            raise FileNotFoundError(f'{comid_lat_lon_z} does not exist')

    if vpu_name is None:
        vpu_name = os.path.basename(input_dir)

    # open all the ncs and select only the area within the weight table
    if type(lsm_data) is list:
        ...  # this is correct, a list of files is allowed
    elif os.path.isdir(lsm_data):
        lsm_data = os.path.join(lsm_data, '*.nc*')
    elif os.path.isfile(lsm_data):
        ...  # this is correct, a single file is allowed
    elif '*' in lsm_data:
        ...  # this is correct, xarray will interpret the glob sting independently
    elif not os.path.exists(lsm_data) and '*' not in lsm_data:
        raise FileNotFoundError(f'{lsm_data} does not exist and is not a glob pattern')

    logging.info(f'Opening LSM files {lsm_data[0] if type(lsm_data) == list else lsm_data}')
    with xr.open_dataset(lsm_data) as ds:
        # Select the variable names
        runoff_variable = [x for x in ['ro', 'RO', 'runoff', 'RUNOFF'] if x in ds.variables][0]
        lon_variable = [x for x in ['lon', 'longitude', 'LONGITUDE', 'LON'] if x in ds.variables][0]
        lat_variable = [x for x in ['lat', 'latitude', 'LATITUDE', 'LAT'] if x in ds.variables][0]

        # Check that the input table dimensions match the dataset dimensions
        # This gets us the shape, while ignoring the time dimension
        variable_dims = ds[runoff_variable].dims
        dataset_shape = [ds[runoff_variable].shape[variable_dims.index(lat_variable)],
                         ds[runoff_variable].shape[variable_dims.index(lon_variable)]]

        if weight_table is None:
            # find a file in the input_dir which contains f"weight*{dataset_shape[0]}x{dataset_shape[1]}.csv"
            weight_table = glob.glob(os.path.join(input_dir, f'weight*{dataset_shape[0]}x{dataset_shape[1]}.csv'))
            if not len(weight_table):
                raise FileNotFoundError(f'Could not find a weight table in {input_dir} with shape {dataset_shape}')
            weight_table = weight_table[0]
            #logging.info(f'Using weight table: {weight_table}')
        # check that the grid dimensions are found in the weight table filename
        matches = re.findall(r'(\d+)x(\d+)', weight_table)[0]
        if len(matches) != 2:
            raise ValueError(f"Could not validate the grid shape and weight table. Grid shape not found in filename")
        if not (len(matches) == 2 and all(int(item) in dataset_shape for item in matches)):
            raise ValueError(f"{weight_table} dimensions don't match the input dataset shape: {dataset_shape}")

        # load in weight table and get some information
        logging.debug(f'Using weight table: {weight_table}')
        logging.debug(f'Using comid_lat_lon_z: {comid_lat_lon_z}')
        weight_df = pd.read_csv(weight_table)
        comid_df = pd.read_csv(comid_lat_lon_z)

        sorted_rivid_array = comid_df.iloc[:, 0].to_numpy()

        min_lon = weight_df['lon'].min()
        max_lon = weight_df['lon'].max()
        min_lat = weight_df['lat'].min()
        max_lat = weight_df['lat'].max()

        # for readability, select certain cols from the weight table
        n_wt_rows = weight_df.shape[0]
        stream_ids = weight_df.iloc[:, 0].to_numpy()
        lat_indices = weight_df['lat_index'].values  # - min_lat_idx
        lon_indices = weight_df['lon_index'].values  # - min_lon_idx

        ds = ds[runoff_variable]

        # Get approximate sizes of arrays and check if we have enough memory
        out_array_size = ds['time'].shape[0] * sorted_rivid_array.shape[0]
        in_array_size = ds['time'].shape[0] * n_wt_rows
        if ds.ndim == 4:
            in_array_size *= 2
        total_size = out_array_size + in_array_size
        _memory_check(total_size)

        # Get conversion factor
        conversion_factor = 1
        units = ds.attrs.get('units', False)
        if not units:
            logging.warning("No units attribute found. Assuming meters")
        elif ds.attrs['units'] == 'm':
            conversion_factor = 1
        elif ds.attrs['units'] == 'mm':
            conversion_factor = .001
        else:
            raise ValueError(f"Unknown units: {ds.attrs['units']}")

        # get the time array from the dataset
        #logging.info('Reading Time values')
        datetime_array = ds['time'].to_numpy()

        #logging.info('Reading Runoff values')
        if ds.ndim == 3:
            inflow_df = ds.values[:, lat_indices, lon_indices]
        elif ds.ndim == 4:
            inflow_df = ds.values[:, :, lat_indices, lon_indices]
            inflow_df = np.where(np.isnan(inflow_df[:, 0, :]), inflow_df[:, 1, :], inflow_df[:, 0, :]),
        else:
            raise ValueError(f"Unknown number of dimensions: {ds.ndim}")

    # This order of operations is important
    inflow_df = pd.DataFrame(inflow_df, columns=stream_ids, index=datetime_array)
    inflow_df = inflow_df.replace(np.nan, 0)
    if cumulative:
        inflow_df = _cumulative_to_incremental(inflow_df)
    if force_positive_runoff:
        inflow_df = inflow_df.clip(lower=0)
    inflow_df = inflow_df * weight_df['area_sqm'].values * conversion_factor
    inflow_df = inflow_df.T.groupby(by=stream_ids).sum().T
    inflow_df = inflow_df[sorted_rivid_array]

    # Check that all timesteps are the same
    time_diff = np.diff(datetime_array)
    if not np.all(time_diff == datetime_array[1] - datetime_array[0]):
        logging.warning('Timesteps are not all uniform')
        if timestep is None:
            logging.warning('Timesteps are not all uniform and a target timestep was not provided.')
            timestep = datetime_array[1] - datetime_array[0]
            logging.warning(f'Assuming the first timedelta is the target: {timestep.astype("timedelta64[s]")}')
        elif isinstance(timestep, datetime.timedelta):
            # Convert datetime timedelta to timedelta64[ns]
            timestep = np.timedelta64(timestep, 'ns')

        # everything is forced to be incremental before this step so we can use cumsum to get the cumulative values
        inflow_df = (
            _incremental_to_cumulative(inflow_df)
            .resample(rule=f'{timestep.astype("timedelta64[s]").astype(int)}S')
            .interpolate(method='linear')
        )
        inflow_df = _cumulative_to_incremental(inflow_df)

    # Create output inflow netcdf data
    #logging.info("Writing inflows to file")
    os.makedirs(inflow_dir, exist_ok=True)
    datetime_array = inflow_df.index.to_numpy()
    start_date = datetime.datetime.fromtimestamp(datetime_array[0].astype(float) / 1e9, datetime.UTC).strftime('%Y%m%d')
    end_date = datetime.datetime.fromtimestamp(datetime_array[-1].astype(float) / 1e9, datetime.UTC).strftime('%Y%m%d')
    file_name = f'm3_{vpu_name}_{start_date}_{end_date}.nc'
    if file_label is not None:
        file_name = f'm3_{vpu_name}_{start_date}_{end_date}_{file_label}.nc'
    inflow_file_path = os.path.join(inflow_dir, file_name)
    logging.debug(f'Writing inflow file to {inflow_file_path}')

    with nc.Dataset(inflow_file_path, "w", format="NETCDF3_CLASSIC") as inflow_nc:
        # create dimensions
        inflow_nc.createDimension('time', datetime_array.shape[0])
        inflow_nc.createDimension('rivid', sorted_rivid_array.shape[0])
        inflow_nc.createDimension('nv', 2)

        # m3_riv
        # note - nan's and fill values are not supported on netcdf3 files
        m3_riv_var = inflow_nc.createVariable('m3_riv', 'f4', ('time', 'rivid'))
        m3_riv_var[:] = inflow_df.to_numpy()
        m3_riv_var.long_name = 'accumulated inflow inflow volume in river reach boundaries'
        m3_riv_var.units = 'm3'
        m3_riv_var.coordinates = 'lon lat'
        m3_riv_var.grid_mapping = 'crs'
        m3_riv_var.cell_methods = "time: sum"

        # rivid
        rivid_var = inflow_nc.createVariable('rivid', 'i4', ('rivid',))
        rivid_var[:] = sorted_rivid_array
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

        # longitude
        lon_var = inflow_nc.createVariable('lon', 'f8', ('rivid',))
        lon_var[:] = comid_df['lon'].values
        lon_var.long_name = 'longitude of a point related to each river reach'
        lon_var.standard_name = 'longitude'
        lon_var.units = 'degrees_east'
        lon_var.axis = 'X'

        # latitude
        lat_var = inflow_nc.createVariable('lat', 'f8', ('rivid',))
        lat_var[:] = comid_df['lat'].values
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
        inflow_nc.geospatial_lat_min = min_lat
        inflow_nc.geospatial_lat_max = max_lat
        inflow_nc.geospatial_lon_min = min_lon
        inflow_nc.geospatial_lon_max = max_lon

    return