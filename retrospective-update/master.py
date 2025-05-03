import os
import traceback
import aiobotocore
from multiprocessing import Pool, set_start_method

import s3fs

import functions as f
from cloud_logger import CloudLog

ODP_CREDENTIALS_FILE = os.path.expanduser(os.getenv('ODP_CREDENTIALS_FILE', '~/.aws/credentials'))

LOG_GROUP_NAME = os.getenv('AWS_LOG_GROUP_NAME')
LOG_STREAM_NAME = os.getenv('AWS_LOG_STREAM_NAME')
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('REGION_NAME')

# Update cdsapirc file location
os.environ['CDSAPI_RC'] = os.path.expanduser(os.getenv('CDSAPI_RC'))

# Parameters
MIN_LAG_TIME_DAYS = int(os.getenv('MIN_LAG_TIME_DAYS', 5))

# The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab
volume_directory = os.path.expanduser(os.getenv('VOLUME_DIR'))
LOCAL_DAILY_ZARR = os.path.join(volume_directory, os.getenv('LOCAL_DAILY_ZARR_NAME'))  # Local zarr to append to
LOCAL_HOURLY_ZARR = os.path.join(volume_directory, os.getenv('LOCAL_HOURLY_ZARR_NAME'))  # Local zarr to append to

S3_CONFIGS_DIR = os.getenv('S3_CONFIGS_DIR')
S3_QFINAL_DIR = os.getenv('S3_QFINAL_DIR')  # Directory containing vpu subdirectories, containing Qfinal files
S3_DAILY_ZARR = os.getenv('S3_DAILY_ZARR')  # retrospective zarr on S3
S3_HOURLY_ZARR = os.getenv('S3_HOURLY_ZARR')  # hourly zarr on S3
S3_MONTHLY_TIMESTEPS = os.getenv('S3_MONTHLY_TIMESTEPS')
S3_MONTHLY_TIMESERIES = os.getenv('S3_MONTHLY_TIMESERIES')
S3_ANNUAL_TIMESTEPS = os.getenv('S3_ANNUAL_TIMESTEPS')
S3_ANNUAL_TIMESERIES = os.getenv('S3_ANNUAL_TIMESERIES')
S3_ANNUAL_MAXIMUMS = os.getenv('S3_ANNUAL_MAXIMUMS')

# set some file paths
ERA_DIR = os.path.join(volume_directory, 'data', 'era5_data')
CONFIGS_DIR = os.path.join(volume_directory, 'data', 'configs')
INFLOWS_DIR = os.path.join(volume_directory, 'data', 'inflows')
RUNOFF_DIR = os.path.join(volume_directory, 'data', 'era5_runoff')
OUTPUTS_DIR = os.path.join(volume_directory, 'data', 'outputs')

if __name__ == '__main__':
    try:
        CL = CloudLog(LOG_GROUP_NAME, LOG_STREAM_NAME, ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION)
        session = aiobotocore.session.AioSession(profile='odp')
        s3 = s3fs.S3FileSystem(session=session)

        # create the required directory structure
        os.makedirs(CONFIGS_DIR, exist_ok=True)
        os.makedirs(RUNOFF_DIR, exist_ok=True)
        os.makedirs(OUTPUTS_DIR, exist_ok=True)
        os.makedirs(INFLOWS_DIR, exist_ok=True)

        CL.log_message('START')
        f.download_era5(ERA_DIR, RUNOFF_DIR, S3_DAILY_ZARR, MIN_LAG_TIME_DAYS, CL)

        CL.log_message('RUNNING', 'checking installations and environment')
        f.check_installations(LOCAL_DAILY_ZARR, S3_DAILY_ZARR, LOCAL_HOURLY_ZARR, S3_HOURLY_ZARR, ODP_CREDENTIALS_FILE)

        CL.log_message('RUNNING', 'Verifying zarr on s3 matches local zarr')
        for z, s in zip([LOCAL_DAILY_ZARR, LOCAL_HOURLY_ZARR], [S3_DAILY_ZARR, S3_HOURLY_ZARR]):
            f.check_zarrs_match(z, s)

        CL.log_message('RUNNING', 'verifying era5 data is compatible with the retrospective zarr')
        f.verify_era5_data(RUNOFF_DIR, LOCAL_HOURLY_ZARR)

        CL.log_message('RUNNING', 'preparing config files')
        f.setup_configs(CONFIGS_DIR, S3_CONFIGS_DIR, CL)

        CL.log_message('RUNNING', 'getting initial qinits')
        f.get_qinits_from_s3(s3, CONFIGS_DIR, S3_QFINAL_DIR, OUTPUTS_DIR)

        num_processes = f.processes(RUNOFF_DIR)
        CL.log_message('RUNNING', f'preparing inflows and namelists with {num_processes} processes')
        set_start_method('spawn', force=True)
        with Pool(num_processes) as p:
            CL.log_message('RUNNING', 'preparing inflows')
            f.inflows(RUNOFF_DIR, CONFIGS_DIR, INFLOWS_DIR, p)

            CL.log_message('RUNNING', 'Running river-route')
            f.run_river_route(CONFIGS_DIR, OUTPUTS_DIR, INFLOWS_DIR, p)
        
        CL.log_message('RUNNING', 'concatenating outputs')
        f.concatenate_outputs(OUTPUTS_DIR, LOCAL_HOURLY_ZARR, LOCAL_DAILY_ZARR, CL)

        CL.log_message('RUNNING', 'checking local zarr is good to go')
        for z in [LOCAL_DAILY_ZARR, LOCAL_HOURLY_ZARR]:
            f.verify_concatenated_outputs(z, CL)

        CL.log_message('RUNNING', 'syncing to s3')
        f.sync_local_to_s3(OUTPUTS_DIR, S3_QFINAL_DIR, LOCAL_HOURLY_ZARR, LOCAL_DAILY_ZARR, S3_HOURLY_ZARR, S3_DAILY_ZARR, ODP_CREDENTIALS_FILE, CL)

        CL.log_message('RUNNING', 'updating monthly zarrs')
        f.update_monthly_zarrs(LOCAL_DAILY_ZARR, S3_MONTHLY_TIMESTEPS, S3_MONTHLY_TIMESERIES)
        
        CL.log_message('RUNNING', 'updating annual zarrs')
        f.update_yearly_zarrs(LOCAL_HOURLY_ZARR, S3_ANNUAL_TIMESTEPS, S3_ANNUAL_TIMESERIES, S3_ANNUAL_MAXIMUMS)

        CL.log_message('RUNNING', 'cleaning up current run')
        f.cleanup(volume_directory, RUNOFF_DIR, INFLOWS_DIR, OUTPUTS_DIR)

        CL.log_message('COMPLETE')
    except Exception as e:
        error = traceback.format_exc()
        CL.log_message('FAIL', error)
