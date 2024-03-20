import datetime
import glob
import logging
import multiprocessing
import os
import subprocess
import traceback

import numpy as np
import psutil
import s3fs
import xarray as xr

from append import append_week
from cloud_logger import CloudLog
from generate_namelist import rapid_namelist_from_directories
from inflow import create_inflow_file

logging.basicConfig(level=logging.INFO,
                    filename='log.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')

GEOGLOWS_ODP_RETROSPECTIVE_BUCKET = 's3://geoglows-v2-retrospective'
GEOGLOWS_ODP_RETROSPECTIVE_ZARR = 'retrospective.zarr'
GEOGLOWS_ODP_REGION = 'us-west-2'

GEOGLOWS_ODP_CONFIGS = os.getenv('S3_CONFIGS_DIR')

CL = CloudLog()
s3 = s3fs.S3FileSystem()

# HOME = os.getenv('HOME')
HOME = os.getcwd()

# The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab
volume_directory = os.getenv('VOLUME_DIR')
# Zarr located on S3
s3_zarr = os.getenv('S3_ZARR')
# Directory containing subdirectories, containing Qfinal files
qfinal_dir = os.getenv('S3_QFINAL_DIR')
# Directory containing the ERA5 data
ERA_DIR = os.getenv('S3_ERA_DIR')
# Local zarr to append to
local_zarr = os.path.join(volume_directory, os.getenv('LOCAL_ZARR_NAME'))  # Local zarr to append to


def inflow_and_namelist(
        namelist_dir: str,
        nc: str,
        vpu_dirs: list[str]) -> None:
    """
    Generate inflow files and namelist files for each VPU directory.

    Args:
        namelist_dir (str): The directory to store the namelist files.
        nc (str): The path to the nc file.
        vpu_dirs (list[str]): A list of VPU directories.

    Returns:
        None
    """

    for vpu_dir in vpu_dirs:
        if not os.path.isdir(vpu_dir):
            continue
        vpu = os.path.basename(vpu_dir)

        inflow_dir = os.path.join(HOME, 'data', 'inflows', vpu)
        output_dir = os.path.join(HOME, 'data', 'outputs', vpu)
        init = glob.glob(os.path.join(output_dir, 'Qfinal*.nc'))[0]

        create_inflow_file(
            nc,
            vpu_dir,
            inflow_dir,
            vpu,
        )

        rapid_namelist_from_directories(
            vpu_dir,
            inflow_dir,
            namelist_dir,
            output_dir,
            qinit_file=init
        )
        namelist = glob.glob(os.path.join(namelist_dir, f'namelist_{vpu}*'))[0]

        # Correct the paths in the namelist file
        with open(namelist, 'r') as f:
            text = f.read().replace(HOME, '/mnt')
        with open(namelist, 'w') as f:
            f.write(text)


def get_initial_qinits(s3: s3fs.S3FileSystem,
                       qfinal_dir: str,
                       last_retro_time: np.datetime64, ) -> None:
    """
    Get q initialization files from S3.

    Parameters:
    - s3: An instance of s3fs.S3FileSystem for accessing S3.
    - vpu_dirs: A list of VPU directories.
    - qfinal_dir: The directory in S3 where the Qfinal files are stored.
    - last_retro_time: The last retro time as a numpy datetime64 object.

    Raises:
    - FileNotFoundError: If the Qfinal files cannot be found or if the number of Qfinal files is not as expected.

    Returns:
    - None
    """
    last_retro_time: str = np.datetime_as_string(last_retro_time, unit='D').replace('-', '')
    local_qfinals = glob.glob(os.path.join(HOME, 'data', 'outputs', '*', '*Qfinal*.nc'))
    # If we don't have 125 qfinal files or the qfinal files we do have are not the latest, pull from s3.
    if len(local_qfinals) != 125 or (
            len(local_qfinals) > 0 and os.path.basename(local_qfinals[0]).split('_')[-1].split('.')[
        0] != last_retro_time):
        s3_qfinals = s3.glob(f"{qfinal_dir}/*/Qfinal*{last_retro_time}.nc")
        if not s3_qfinals:
            raise FileNotFoundError(f"Could not find any Qfinal files in {qfinal_dir} for {last_retro_time}")
        if len(s3_qfinals) != 125:
            raise FileNotFoundError(f"Expected 125 Qfinal files in {qfinal_dir}, got {len(s3_qfinals)}")

        for f in glob.glob(os.path.join(HOME, 'data', 'outputs', '*', '*Qfinal*.nc')):
            os.remove(f)
        logging.info('Pulling qfinal files from s3')
        for s3_file in s3_qfinals:
            vpu = os.path.basename(s3_file).split('_')[1]
            os.makedirs(os.path.join(HOME, 'data', 'outputs', vpu), exist_ok=True)
            with s3.open(s3_file, 'rb') as s3_f:
                with open(os.path.join(HOME, 'data', 'outputs', vpu, os.path.basename(s3_file)), 'wb') as local_f:
                    local_f.write(s3_f.read())


def cache_to_s3(s3: s3fs.S3FileSystem,
                s3_path: str,
                delete_all: bool = False) -> None:
    """
    Uploads files from the working directory to S3, while optionally deleting some files.

    Args:
        s3 (s3fs.S3FileSystem): An instance of the S3FileSystem class for S3 access.
        s3_path (str): The S3 path where the files will be uploaded.
        delete_all (bool, optional): If True, deletes all qfinal files. Defaults to False.
    """

    vpu_dirs = glob.glob(os.path.join(HOME, 'data', 'outputs', '*'))
    for vpu_dir in vpu_dirs:
        # Delete the earliest qfinal, upload the latest qfinal
        qfinals = [f for f in glob.glob(os.path.join(vpu_dir, 'Qfinal*.nc'))]
        qfinals = sorted(qfinals,
                         key=lambda x: datetime.datetime.strptime(os.path.basename(x).split('_')[-1].split('.')[0],
                                                                  '%Y%m%d'))

        if delete_all:
            for f in qfinals:
                os.remove(f)
        elif len(qfinals) == 2:
            os.remove(qfinals[0])
            upload_to_s3(s3, qfinals[1], f'{s3_path}/{os.path.basename(vpu_dir)}/{os.path.basename(qfinals[1])}')

        qouts = glob.glob(os.path.join(vpu_dir, 'Qout*.nc'))
        if qouts:
            qout = qouts[0]
            upload_to_s3(s3, qout, f'{s3_path}/{os.path.basename(vpu_dir)}/{os.path.basename(qout)}')
            os.remove(qout)


def drop_coords(ds: xr.Dataset, qout: str = 'Qout'):
    """
    Helps load faster, gets rid of variables/dimensions we do not need (lat, lon, etc.)
    
    Parameters:
        ds (xr.Dataset): The input dataset.
        qout (str): The variable name to keep in the dataset.
        
    Returns:
        xr.Dataset: The modified dataset with only the specified variable.
    """
    return ds[[qout]].reset_coords(drop=True)


def upload_to_s3(s3: s3fs.S3FileSystem,
                 file_path: str,
                 s3_path: str) -> None:
    """
    Uploads a file to Amazon S3.

    Args:
        s3 (s3fs.S3FileSystem): The S3FileSystem object used for the upload.
        file_path (str): The local file path of the file to be uploaded.
        s3_path (str): The S3 path where the file will be uploaded to.

    Returns:
        None
    """
    with open(file_path, 'rb') as f:
        with s3.open(s3_path, 'wb') as sf:
            sf.write(f.read())


def cleanup(qfinal_dir: str,
            delete_all: bool = False) -> None:
    """
    Cleans up the working directory by deleting namelists, inflow files, and
    caching qfinals and qouts.

    Args:
        qfinal_dir (str): The path to the qfinal directory.
        delete_all (bool, optional): If True, deletes all files in the qfinal
            directory. Defaults to False.
    """
    # Delete namelists
    for file in glob.glob(os.path.join(HOME, 'data', 'namelists', '*')):
        os.remove(file)

    # Delete inflow files
    for f in glob.glob(os.path.join(HOME, 'data', 'inflows', '*', '*.nc')):
        os.remove(f)

    # Cache qfinals and qouts, remove
    cache_to_s3(s3fs.S3FileSystem(), qfinal_dir, delete_all)


def sync_local_to_s3(local_zarr: str, s3_zarr: str) -> None:
    """
    Embarrassingly fast sync zarr to S3 (~3 minutes for 150k files). 
    Note we only sink necessary files: all Qout/1.*, time/*, and all . files in the zarr
    
    Args:
        local_zarr (str): The local path of the zarr directory.
        s3_zarr (str): The S3 path where the zarr directory will be synced.
    
    Raises:
        Exception: If the sync command fails.
    """
    # all . files in the top folder (.zgroup, .zmetadata), and all . files in the Qout var n(.zarray)
    files_to_upload = glob.glob(os.path.join(local_zarr, '.*')) + glob.glob(os.path.join(local_zarr, 'Qout', '.*'))
    command = ""
    for f in files_to_upload:
        if 'Qout' in f:
            destination = os.path.join(s3_zarr, 'Qout')
        else:
            # Otherwise, upload to the top-level folder
            destination = s3_zarr
        command += f"s5cmd cp {f} {destination}/\n"
    command += f"s5cmd sync --size-only --include=\"*1.*\" {local_zarr}/Qout/ {s3_zarr}/Qout/\n"
    command += f"s5cmd sync --size-only {local_zarr}/time/ {s3_zarr}/time/"

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    # Check if the command was successful
    if result.returncode == 0:
        logging.info("Sync completed successfully.")
    else:
        logging.error(f"Sync failed. Error: {result.stderr}")
        raise Exception(result.stderr)


def setup_configs() -> None:
    """
    Setup all the directories we need, populate files
    """
    c_dir = os.path.join(HOME, 'data', 'configs')
    os.makedirs(c_dir, exist_ok=True)
    if len(glob.glob(os.path.join(c_dir, '*', '*.csv'))) == 0:
        result = subprocess.run(f"aws s3 sync {GEOGLOWS_ODP_CONFIGS} {c_dir}", shell=True, capture_output=True,
                                text=True)
        if result.returncode == 0:
            logging.info("Obtained config files")
        else:
            logging.error(f"Config file sync error: {result.stderr}")


def date_sort(s: str) -> datetime.datetime:
    """
    Returns the date of the file as a datetime object.

    Args:
        s (str): The string .

    Returns:
        dateime: datetime representation of the string.
    """
    x = os.path.basename(s).split('.')[0].split('_')[1:]
    year = x[0] if '-' not in x[0] else x[0].split('-')[1]
    month = x[1] if '-' not in x[1] else x[1].split('-')[1]
    day = x[2].split('-')[1]
    return datetime.datetime(int(year), int(month), int(day))


def check_installations() -> None:
    """
    Check that the following are installed:
    - awscli
    - s5cmd
    - docker
    """
    try:
        subprocess.run(['aws', '--version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError('Please install the AWS cli: conda install -c conda-forge awscli')

    try:
        subprocess.run(['s5cmd', 'version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError('Please install s5cmd: `conda install -c conda-forge s5cmd`')

    try:
        subprocess.run(['docker', '--version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError('Please install docker, and run "docker pull chdavid/rapid"')

    if not os.path.exists(local_zarr):
        logging.error(f"{local_zarr} does not exist!")
        CL.log_message('FAIL', f"{local_zarr} does not exist!")
        exit()
    if not os.path.exists(os.path.join(HOME, 'data', 'runrapid.py')):
        msg = f"Please put 'runrapid.py' in {HOME}/data so that RAPID may use it"
        logging.error(msg)
        CL.log_message('FAIL', msg)
        exit()


def main(retro_zarr: str, nc: str, ) -> None:
    """
    Assumes docker is installed and this command was run: docker pull chdavid/rapid.
    Assumes AWS CLI and s5cmd are likewise installed. 

    Executes the main workflow for running the RAPID model.

    Args:
        retro_zarr (str): The path to the retro zarr dataset.
        nc (str): The path to the nc file.

    Raises:
        FileNotFoundError: If not all of the m3 files were generated.

    Returns:
        None
    """
    a_qfinal = glob.glob(os.path.join(HOME, 'data', 'outputs', '*', '*Qfinal*.nc'))[0]
    CL.add_qinit(datetime.datetime.strptime(os.path.basename(a_qfinal).split('_')[2].split('.')[0], '%Y%m%d'))

    # For inflows files and multiprocess, for each 1GB of runoff data, we need ~ 6GB for peak memory consumption. Otherwise, some m3 files will never be written and no error is raised
    processes = min(multiprocessing.cpu_count(), round(psutil.virtual_memory().total * 0.8 / (os.path.getsize(nc) * 6)))
    logging.info(f"Using {processes} processes for inflows")
    CL.log_message('RUNNING', f"Using {processes} processes for inflows")
    worker_lists = [config_vpu_dirs[i:i + processes] for i in range(0, len(config_vpu_dirs), processes)]
    with multiprocessing.Pool(processes) as pool:
        pool.starmap(inflow_and_namelist,
                     [(HOME, os.path.join(HOME, 'data', 'namelists'), nc, w) for w in worker_lists])

    if len(glob.glob(os.path.join(HOME, 'data', 'inflows', '*', 'm3*.nc'))) != 125:
        raise FileNotFoundError('Not all of the m3 files were generated!!!')

    # Run rapid
    logging.info('Running rapid')
    CL.log_message('RUNNING', 'Running RAPID')
    rapid_result = subprocess.run(
        [f'sudo docker run --rm --name rapid --mount type=bind,source={os.path.join(HOME, "data")},target=/mnt chdavid/rapid python3 /mnt/runrapid.py'],
        shell=True,
        capture_output=True,
        text=True
    )

    if rapid_result.returncode != 0:
        raise RuntimeError(rapid_result.stderr)

    logging.info(rapid_result.stdout)

    # Build the week dataset
    qouts = glob.glob(os.path.join(HOME, 'data', 'outputs', '*', 'Qout*.nc'))

    with xr.open_mfdataset(qouts,
                           combine='nested',
                           concat_dim='rivid',
                           preprocess=drop_coords, ).reindex(rivid=xr.open_zarr(retro_zarr)['rivid']) as ds:

        CL.log_message('RUNNING',
                       f'Appending to zarr: {np.datetime_as_string(ds.time[0].values, unit="h")} to {np.datetime_as_string(ds.time[-1].values, unit="h")}')
        append_week(ds, retro_zarr)


if __name__ == '__main__':
    try:
        check_installations()
        setup_configs()
        cleanup(qfinal_dir)
        config_vpu_dirs = glob.glob(os.path.join(HOME, 'data', 'configs', '*'))
        last_retro_time = xr.open_zarr(local_zarr)['time'][-1].values
        get_initial_qinits(s3, qfinal_dir, last_retro_time)

        ncs = sorted(s3.glob(f"{ERA_DIR}/*.nc"), key=date_sort)  # Sorted, so that we append correctly by date
        if not ncs:
            logging.error(f"Could not find any .nc files in {ERA_DIR}")
            raise FileNotFoundError(f"Could not find any .nc files in {ERA_DIR}")
        CL.log_message('START', f"Appending {len(ncs)} ERA5 file(s) to {local_zarr}")
        for i, era_nc in enumerate(ncs):
            with s3.open(era_nc, 'rb') as s3_file:
                local_era5_nc = os.path.basename(era_nc)
                with open(local_era5_nc, 'wb') as local_file:
                    local_file.write(s3_file.read())

            # Check that the time in the nc will accord with the retrospective zarr
            era_time = xr.open_dataset(local_era5_nc)['time'].values
            first_era_time = era_time[0].values
            last_retro_time = xr.open_zarr(local_zarr)['time'][-1].values
            CL.add_last_date(last_retro_time)
            try:
                CL.add_time_period(era_time)
            except:
                logging.warning(f"Could not add time period to cloud log for {era_time}")
            if last_retro_time + np.timedelta64(1, 'D') != first_era_time:
                raise ValueError(
                    f"Time mismatch between {local_era5_nc} and {local_zarr}: got {first_era_time} and {last_retro_time} respectively (the era file should be 1 day behind the zarr). Please check the time in the .nc file and the zarr.")

            main(local_zarr, local_era5_nc)
            if i + 1 < len(ncs):  # Log each time we will run again, except for the last time
                CL.log_message('RUNNING AGAIN')
            os.remove(local_era5_nc)
            s3.rm_file(era_nc)
            cleanup(qfinal_dir)

        # At last, sync to S3
        sync_local_to_s3(local_zarr, s3_zarr)
        cleanup(qfinal_dir)
        CL.log_message('COMPLETE')
    except Exception as e:
        error = traceback.format_exc()
        logging.error(error)
        CL.log_message('FAIL', error)
