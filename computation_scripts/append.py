import time
import logging
import os, shutil

import s3fs
import zarr
import xarray as xr
from rechunker import rechunk  # The rechunk library is the only one that can rechunk correctly, without breaking the zarr
from dask.diagnostics import ProgressBar

BUCKET_NAME = 'geoglows-scratch'
RETRO_NAME = 'retrospective.zarr'
RETRO_NEW_NAME = 'retro_new.zarr'
TEMP_NAME = 'temp.zarr'
WEEK_NAME = 'week_all.nc4'
MAX_SECOND_CHUNKSIZE = float('inf')  # Infinity means always append, never rechunk

s3 = s3fs.S3FileSystem()


def rechunk_to_one(retro_ds: xr.Dataset,
                   week_ds: xr.Dataset,
                   chunks,
                   time: str = 'time',
                   rivid: str = 'rivid',
                   qout: str = 'Qout') -> None:
    raise NotImplementedError('At this point we do not rechunk. If you got here something wonderfully weird happened')
    """
    Open the retro zarr, concatenate with the week netcdf, and rechunk to file with one time chunk
    """
    temp_zarr_path = 'temp.zarr'
    retro_new_zarr_path = f's3://{BUCKET_NAME}/{RETRO_NEW_NAME}'
    retro_zarr_path = f's3://{BUCKET_NAME}/{RETRO_NAME}'

    temp_zarr = s3fs.S3Map(root=temp_zarr_path, s3=s3, check=False)
    new_zarr = s3fs.S3Map(root=retro_new_zarr_path, s3=s3, check=False)

    temp_ds = (
        xr.concat([retro_ds, week_ds], dim=time)
        .chunk({time: sum(chunks[time]) + 7, rivid: chunks[rivid][0]})
    )

    # Delete these encodings to allow chunks to be correctly written
    for var in temp_ds.data_vars:
        del temp_ds[var].encoding['chunks']

    if os.path.exists(temp_zarr_path):
        shutil.rmtree(temp_zarr_path)

    if RETRO_NAME in s3.ls(f's3://{BUCKET_NAME}'):
        s3.rm(f's3://{BUCKET_NAME}/{RETRO_NAME}', recursive=True)

    rechunked = rechunk(temp_ds,
                        target_chunks={time: temp_ds[qout].shape[0], rivid: chunks[rivid][0]},
                        max_mem='4GB',
                        target_store=new_zarr,
                        temp_store=temp_zarr,
                        )

    logging.info('Rewriting zarr with one time chunk...')
    with ProgressBar():
        rechunked.execute()

    zarr.consolidate_metadata(new_zarr)

    # Remove temp zarr and old retro, rename the newly created zarr to the retro 
    shutil.rmtree(temp_zarr_path)
    s3.rm(retro_zarr_path, recursive=True)
    s3.mv(retro_new_zarr_path, retro_zarr_path, recursive=True)


def append(retro_zarr,
           week_ds: xr.Dataset,
           chunks,
           time: str = 'time',
           rivid: str = 'rivid', ) -> None:
    """
    Add a week to the retro zarr file. If there is only one chunk, this will add a second chunk.
    If there are two chunks, the second chunk is rewritten and expanded.

    Parameters:
    retro_zarr (str): The path to the retro zarr file.
    week_ds (xr.Dataset): The dataset containing the week data to be added.
    chunks (dict): A dictionary specifying the chunk sizes for the time and rivid dimensions.
    time (str, optional): The name of the time dimension. Defaults to 'time'.
    rivid (str, optional): The name of the rivid dimension. Defaults to 'rivid'.

    Returns:
    None
    """
    (
        week_ds
        .chunk({time: chunks[time][0], rivid: chunks[rivid][0]})
        .to_zarr(retro_zarr, mode='a', append_dim=time, consolidated=True)
    )
