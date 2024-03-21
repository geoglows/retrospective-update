import datetime
import glob
import logging
import os
import subprocess
import time
import traceback
from multiprocessing import Pool

import natsort
import numpy as np
import psutil
import s3fs
import xarray as xr

from cloud_logger import CloudLog
from generate_namelist import rapid_namelist_from_directories
from inflow import create_inflow_file

GEOGLOWS_ODP_RETROSPECTIVE_BUCKET = 's3://geoglows-v2-retrospective'
GEOGLOWS_ODP_RETROSPECTIVE_ZARR = 'retrospective.zarr'
GEOGLOWS_ODP_REGION = 'us-west-2'
GEOGLOWS_ODP_CONFIGS = os.getenv('S3_CONFIGS_DIR')
ODP_CREDENTIALS_FILE = os.getenv('ODP_CREDENTIALS_FILE')

CL = CloudLog()
s3 = s3fs.S3FileSystem()

# The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab
volume_directory = os.getenv('VOLUME_DIR')
s3_zarr = os.getenv('S3_ZARR')  # retrospective zarr on S3
s3_qfinal_dir = os.getenv('S3_QFINAL_DIR')  # Directory containing vpu subdirectories, containing Qfinal files
s3_era_bucket = os.getenv('S3_ERA_BUCKET')  # Directory containing the ERA5 data
local_zarr = os.path.join(volume_directory, os.getenv('LOCAL_ZARR_NAME'))  # Local zarr to append to

# set some file paths relative to HOME
HOME = os.getcwd()
configs_dir = os.path.join(HOME, 'data', 'configs')
inflows_dir = os.path.join(HOME, 'data', 'inflows')
runoff_dir = os.path.join(HOME, 'data', 'era5_runoff')
outputs_dir = os.path.join(HOME, 'data', 'outputs')
namelists_dir = os.path.join(HOME, 'data', 'namelists')

# create the required directory structure
os.makedirs(configs_dir, exist_ok=True)
os.makedirs(runoff_dir, exist_ok=True)
for d in glob.glob(os.path.join(configs_dir, '*')):
    os.makedirs(os.path.join(inflows_dir, os.path.basename(d)), exist_ok=True)
    os.makedirs(os.path.join(outputs_dir, os.path.basename(d)), exist_ok=True)
    os.makedirs(os.path.join(namelists_dir, os.path.basename(d)), exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    filename=f'{volume_directory}/log.log',
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w',  # Overwrite the log file each time
)


def _make_inflow_for_vpu(vpu: str) -> None:
    vpu_dir = os.path.join(configs_dir, vpu)
    inflow_dir = os.path.join(inflows_dir, vpu)

    for runoff_file in glob.glob(os.path.join(runoff_dir, '*.nc')):
        create_inflow_file(
            runoff_file,
            vpu_dir,
            inflow_dir,
            vpu_name=vpu,
        )
    return


def _make_namelists_for_vpu(vpu: str) -> None:
    vpu_dir = os.path.join(configs_dir, vpu)
    inflow_dir = os.path.join(inflows_dir, vpu)
    namelist_dir = os.path.join(namelists_dir, vpu)
    output_dir = os.path.join(outputs_dir, vpu)
    qfinal_file = natsort.natsorted(glob.glob(os.path.join(output_dir, 'Qfinal*.nc')))[-1]

    for inflow_file in natsort.natsorted(glob.glob(os.path.join(inflow_dir, 'm3*.nc'))):
        qfinal_file = rapid_namelist_from_directories(
            vpu_dir,
            inflow_file,
            namelist_dir,
            output_dir,
            qinit_file=qfinal_file
        )

    for namelist in glob.glob(os.path.join(namelist_dir, f'namelist*')):
        # Correct the paths in the namelist file
        with open(namelist, 'r') as f:
            text = f.read().replace(os.path.join(HOME, 'data'), '/mnt')
        with open(namelist, 'w') as f:
            f.write(text)
    return


def inflow_and_namelist() -> None:
    # For inflows files and multiprocess, for each 1GB of runoff data, we need ~ 6GB for peak memory consumption.
    # Otherwise, some m3 files will never be written and no error is raised
    sample_runoff_file = glob.glob(os.path.join(runoff_dir, '*.nc'))[0]
    processes = min(
        os.cpu_count(),
        round(psutil.virtual_memory().total * 0.8 / (os.path.getsize(sample_runoff_file) * 6))
    )
    logging.info(f"Using {processes} processes for inflows")
    vpu_numbers = [os.path.basename(d) for d in glob.glob(os.path.join(configs_dir, '*'))]
    logging.info(vpu_numbers)
    with Pool(processes) as p:
        p.map(_make_inflow_for_vpu, vpu_numbers)
        p.map(_make_namelists_for_vpu, vpu_numbers)

    # number of expected files = num_configs_dirs * num_runoff_files
    expected_file_count = len(glob.glob(os.path.join(configs_dir, '*'))) * len(
        glob.glob(os.path.join(runoff_dir, '*.nc')))

    # check that all inflow files were created correctly
    if not len(glob.glob(os.path.join(inflows_dir, '*', '*.nc'))) == expected_file_count:
        raise FileNotFoundError("Not all inflow files were created correctly")
    # check that all namelists were created correctly
    if not len(glob.glob(os.path.join(namelists_dir, '*', 'namelist*'))) == expected_file_count:
        raise FileNotFoundError("Not all namelists were created correctly")

    return


def get_qinits_from_s3() -> None:
    """
    Get q initialization files from S3.

    Parameters:
    - s3: An instance of s3fs.S3FileSystem for accessing S3.
    - vpu_dirs: A list of VPU directories.
    - s3_qfinal_dir: The directory in S3 where the Qfinal files are stored.
    - last_retro_time: The last retro time as a numpy datetime64 object.

    Raises:
    - FileNotFoundError: If the Qfinal files cannot be found or if the number of Qfinal files is not as expected.

    Returns:
    - None
    """
    # todo check the dates of the qfinal files compared with the zarr and local files
    # last_retro_time = xr.open_zarr(local_zarr)['time'][-1].values
    # last_retro_time = np.datetime_as_string(last_retro_time, unit='D').replace('-', '')
    #
    # latest_s3_qfinals_per_vpu = [
    #     natsort.natsorted(s3.glob(f'{s3_qfinal_dir}/{os.path.basename(vpu)}/Qfinal*.nc'))[-1] for vpu in
    #     glob.glob(os.path.join(HOME, 'data', 'configs', '*'))
    # ]
    #
    # # check that all qfinal dates are the same
    # # todo
    #
    # # check that the latest qfinal files match the date of the last simulation
    # if not all([last_retro_time in f for f in latest_s3_qfinals_per_vpu]):
    #     raise FileNotFoundError(f"Most recent Qfinal date doesn't match last date in zarr ({last_retro_time})")

    # download the qfinal files
    for vpu in glob.glob(os.path.join(HOME, 'data', 'configs', '*')):
        vpu = os.path.basename(vpu)
        most_recent_qfinal = natsort.natsorted(s3.glob(f'{s3_qfinal_dir}/{vpu}/Qfinal*.nc'))[-1]
        local_file_name = os.path.join(HOME, 'data', 'outputs', vpu, os.path.basename(most_recent_qfinal))
        if not os.path.exists(local_file_name):
            s3.get(most_recent_qfinal, local_file_name)
    return


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


def cleanup() -> None:
    """
    Cleans up the working directory by deleting namelists, inflow files, and
    caching qfinals and qouts.
    """
    # Delete namelists
    for file in glob.glob(os.path.join(namelists_dir, '*', '*')):
        os.remove(file)

    # delete runoff data
    for file in glob.glob(os.path.join(runoff_dir, '*')):
        os.remove(file)

    # delete inflow files
    for f in glob.glob(os.path.join(inflows_dir, '*', '*.nc')):
        os.remove(f)

    # delete qouts
    for f in glob.glob(os.path.join(outputs_dir, '*', 'Qout*.nc')):
        os.remove(f)

    # delete all but the most recent qfinal
    for vpu_dir in glob.glob(os.path.join(configs_dir, '*')):
        qfinal_files = natsort.natsorted(glob.glob(os.path.join(vpu_dir, 'Qfinal*.nc')))
        if len(qfinal_files) > 1:
            for f in qfinal_files[:-1]:
                os.remove(f)


def sync_local_to_s3() -> None:
    """
    Note we only sink necessary files: all Qout/1.*, time/*, and all . files in the zarr
    
    Args:
        local_zarr (str): The local path of the zarr directory.

    Raises:
        Exception: If the sync command fails.
    """
    # all . files in the top folder (.zgroup, .zmetadata), and all . files in the Qout var n(.zarray)
    files_to_upload = glob.glob(os.path.join(local_zarr, '.*')) + glob.glob(os.path.join(local_zarr, 'Qout', '.*'))
    commands = []
    for f in files_to_upload:
        if 'Qout' in f:
            destination = os.path.join(s3_zarr, 'Qout')
        else:
            # Otherwise, upload to the top-level folder
            destination = s3_zarr
        commands.append(f"s5cmd --credentials-file {ODP_CREDENTIALS_FILE} cp {f} {destination}/")
    commands.append(
        f"s5cmd sync --credentials-file {ODP_CREDENTIALS_FILE} --size-only --include=\"*1.*\" {local_zarr}/Qout/ {s3_zarr}/Qout/")
    commands.append(
        f"s5cmd sync --credentials-file {ODP_CREDENTIALS_FILE} --size-only {local_zarr}/time/ {s3_zarr}/time/")

    results = []
    for command in commands:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        results.append(result)
        # Check if the command was successful
        if result.returncode == 0:
            logging.info("Sync completed successfully.")
        else:
            logging.error(f"Sync failed. Error: {result.stderr}")

    if any(result.returncode != 0 for result in results):
        raise Exception("Sync failed. Consult the logs.")
    return


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


def _check_installations() -> None:
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


def concatenate_outputs() -> None:
    # Build the week dataset
    qouts = natsort.natsorted(glob.glob(os.path.join(HOME, 'data', 'outputs', '*', 'Qout*.nc')))

    if not qouts:
        raise FileNotFoundError("No Qout files found. RAPID probably not run correctly.")

    with xr.open_zarr(local_zarr) as retro_ds:
        chunks = retro_ds.chunks

    with xr.open_mfdataset(
            qouts,
            combine='nested',
            concat_dim='rivid',
            preprocess=drop_coords
    ) as new_ds:
        earliest_date = np.datetime_as_string(new_ds.time[0].values, unit="h")
        latest_date = np.datetime_as_string(new_ds.time[-1].values, unit="h")
        CL.log_message('RUNNING', f'Appending to zarr: {earliest_date} to {latest_date}')

        logging.info(f'Appending to zarr: {earliest_date} to {latest_date}')
        (
            new_ds
            .chunk({"time": chunks["time"][0], "rivid": chunks["rivid"][0]})
            .to_zarr(local_zarr, mode='a', append_dim=time, consolidated=True)
        )
        logging.info(f'Finished appending')
    return


def fetch_staged_era5():
    """
    Fetches staged ERA5 files from S3
    """
    # check that there are files to fetch
    if not s3.glob(f"{s3_era_bucket}/*.nc"):
        logging.error(f"No .nc files found in {s3_era_bucket}")
        raise FileNotFoundError(f"No .nc files found in {s3_era_bucket}")

    # fetch the files
    logging.info(f'Fetching ERA5 runoff files from {s3_era_bucket}')
    logging.info(f'aws s3 cp {s3_era_bucket} {runoff_dir} --recursive --include "*"')
    os.system(f'aws s3 cp {s3_era_bucket} {runoff_dir} --recursive --include "*"')
    return


def run_rapid():
    # Run rapid
    logging.info('Running rapid')
    CL.log_message('RUNNING', 'Running RAPID')
    rapid_result = subprocess.run(
        [
            f'sudo docker run --rm --name rapid --mount type=bind,source={os.path.join(HOME, "data")},target=/mnt chdavid/rapid python3 /mnt/runrapid.py'],
        shell=True,
        capture_output=True,
        text=True
    )

    if rapid_result.returncode != 0:
        raise RuntimeError(rapid_result.stderr)

    logging.info(rapid_result.stdout)
    return


if __name__ == '__main__':
    try:
        config_vpu_dirs = glob.glob(os.path.join(HOME, 'data', 'configs', '*'))

        CL.log_message('START')

        CL.log_message('RUNNING', 'checking installations and environment')
        _check_installations()

        CL.log_message('RUNNING', 'preparing config files')
        setup_configs()

        CL.log_message('RUNNING', 'running cleanup on previous runs')
        cleanup()

        CL.log_message('RUNNING', 'getting initial qinits')
        get_qinits_from_s3()

        CL.log_message('RUNNING', 'fetching staged daily cumulative era5 runoff netcdfs')
        fetch_staged_era5()

        CL.log_message('RUNNING', 'preparing inflows and namelists')
        inflow_and_namelist()

        CL.log_message('RUNNING', 'Running RAPID')
        run_rapid()

        CL.log_message('RUNNING', 'concatenating outputs')
        concatenate_outputs()

        CL.log_message('RUNNING', 'caching Qout, Qfinal, and Zarr to S3')
        sync_local_to_s3()

        CL.log_message('RUNNING', 'Verifying zarr on s3 matches local zarr')
        # todo

        CL.log_message('RUNNING', 'Deleting runoff from s3')
        # todo

        CL.log_message('RUNNING', 'cleaning up current run')
        cleanup()

        CL.log_message('COMPLETE')
    except Exception as e:
        error = traceback.format_exc()
        logging.error(error)
        CL.log_message('FAIL', error)
