import os

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio.features
import xarray as xr
from natsort import natsorted
import traceback
from cloud_logger import CloudLog
from set_env_variables import (
    DAILY_ZARR, MONTHLY_TIMESERIES_ZARR, MONTHLY_TIMESTEPS_ZARR, HYDROSOS_DIR,
    HYDROSOS_ID_PAIRS, HYDROSOS_THRESHOLDS, HYDROSOS_BASINS
)


def update_monthly_products() -> None:
    # check which months are on the monthly datasets
    with xr.open_zarr(MONTHLY_TIMESERIES_ZARR, storage_options={'anon': True}) as ds:
        months_calculated = set(ds.time.dt.strftime('%Y-%m-01').values)

    # check which months are in the local daily zarr
    daily_zarr = xr.open_zarr(DAILY_ZARR, storage_options={'anon': True})
    dates_available = pd.to_datetime(daily_zarr.time.values)
    months_available = set(dates_available.strftime('%Y-%m-01'))

    # check if the last month in the daily zarr has the last day of the month available
    if dates_available[-1].day != pd.to_datetime(dates_available[-1]).days_in_month:
        months_available.remove(dates_available[-1].strftime('%Y-%m-01'))

    # determine which months are missing and should be computed
    months_to_compute = set(months_available) - set(months_calculated)
    months_to_compute = natsorted(list(months_to_compute))
    if not months_to_compute:
        cl.log("No months to compute, all monthly products are up to date.")
        exit(0)
    cl.log(f"Months to compute: {months_to_compute}")

    # filter the dataset to only the months that need to be computed, average them, and append to the monthly zarrs
    daily_zarr = (
        daily_zarr
        .sel(time=dates_available.strftime('%Y-%m-01').isin(months_to_compute))
        .resample(time='MS')  # MS -> Month Start
        .mean()
    )
    cl.log('Appending to S3 monthly timeseries')
    ds.to_zarr(MONTHLY_TIMESERIES_ZARR, mode='a', append_dim='time', consolidated=True, zarr_format=2)
    cl.log('Appending to S3 monthly timesteps')
    ds.to_zarr(MONTHLY_TIMESTEPS_ZARR, mode='a', append_dim='time', consolidated=True, zarr_format=2)
    cl.log('Finished appending monthly average products')
    months_to_compute = ['2025-06-01']

    # for each month in months_to_compute, also make a hydrosos geotiff
    id_pairs = pd.read_parquet(HYDROSOS_ID_PAIRS)
    thresholds_ds = xr.open_dataset(HYDROSOS_THRESHOLDS)
    basins = gpd.read_parquet(HYDROSOS_BASINS)
    hydrosos_color_map = {
        1: '#cd233f',  # red
        2: '#ffa885',  # peach
        3: '#e7e2bc',  # light yellow
        4: '#8eceee',  # light blue
        5: '#2c7dcd',  # medium blue
    }
    with xr.open_zarr(MONTHLY_TIMESTEPS_ZARR) as ds:
        # now we want to combine columns with the same HYBAS_ID in the id_pairs dataframe
        discharges = (
            ds
            .sel(river_id=id_pairs['LINKNO'].values, time=months_to_compute)
            ['Q']
            .to_dataframe()
            .reset_index()
            .pivot(columns='time', index='river_id', values='Q')
            .reset_index()
            .merge(id_pairs, how='inner', left_on='river_id', right_on='LINKNO')
            .drop(columns=['time', 'river_id', 'LINKNO'])
            .groupby('HYBAS_ID')
            .sum()
            .reset_index()
        )
        # convert the columns with dates to classes
        # add a column called classes to discharge with a random integer from 1 to 5
        discharges['classes'] = pd.Series(np.random.randint(1, 6, size=len(discharges))).map(hydrosos_color_map)
    for month in months_to_compute:
        # select only ['HYBAS_ID', 'geometry'] from basins to clear previous iteration classes, if they exist
        basins = basins[['HYBAS_ID', 'geometry']]
        basins = basins.merge(discharges, on='HYBAS_ID', how='inner')

        # create a raster for all the basin polygons
        output_path = os.path.join(HYDROSOS_DIR, f'{month[:-3]}.tif')
        resolution = 0.05
        shape = (int(180 / resolution), int(360 / resolution))
        transform = rasterio.transform.from_origin(-180, 90, resolution, resolution)
        fill = 0
        dtype = 'uint8'
        write_kwargs = {
            'driver': 'GTiff',
            'height': shape[0],
            'width': shape[1],
            'count': 1,
            'dtype': dtype,
            'crs': basins.crs,
            'transform': transform,
            'tiled': True,
            'blockxsize': 512,
            'blockysize': 512,
            'compress': 'deflate',
            'interleave': 'band'
        }
        with rasterio.open(output_path, 'w', **write_kwargs) as dst:
            dst.write(
                rasterio.features.rasterize(
                    [(geom, value) for geom, value in zip(basins.geometry, basins.classes)],
                    out_shape=shape,
                    transform=transform,
                    fill=fill,
                    nodata=fill,
                    dtype=dtype,
                ),
                1
            )


if __name__ == '__main__':
    cl = CloudLog()
    cl.log('Updating monthly products')

    # determine the months which need to be updated
    # compare the months in the monthly zarr and the completed months available in the daily zarr
    try:
        update_monthly_products()
        exit(0)
    except Exception as e:
        cl.error(f"Error updating monthly products: {e}")
        cl.error(traceback.format_exc())
        raise e
