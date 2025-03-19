import glob
import os
import traceback
from multiprocessing import Pool, set_start_method

import s3fs
import aiobotocore.session

import functions as f
from cloud_logger import CloudLog

GEOGLOWS_ODP_RETROSPECTIVE_BUCKET = 's3://geoglows-v2-retrospective'
GEOGLOWS_ODP_RETROSPECTIVE_ZARR = 'retrospective.zarr'
GEOGLOWS_ODP_REGION = 'us-west-2'
GEOGLOWS_ODP_CONFIGS = os.getenv('S3_CONFIGS_DIR')
ODP_CREDENTIALS_FILE = os.getenv('ODP_CREDENTIALS_FILE')

LOG_GROUP_NAME = os.getenv('AWS_LOG_GROUP_NAME')
LOG_STREAM_NAME = os.getenv('AWS_LOG_STREAM_NAME')
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('REGION_NAME')

# Update cdsapirc file
os.environ['CDSAPI_RC'] = os.path.expanduser(os.getenv('CDSAPI_RC'))

# Parameters
MIN_LAG_TIME_DAYS = int(os.getenv('MIN_LAG_TIME_DAYS', 5))

# The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab
volume_directory = os.path.expanduser(os.getenv('VOLUME_DIR'))
S3_DAILY_ZARR = os.getenv('S3_DAILY_ZARR')  # retrospective zarr on S3
S3_HOURLY_ZARR = os.getenv('S3_HOURLY_ZARR')  # hourly zarr on S3
LOCAL_DAILY_ZARR = os.path.join(volume_directory, os.getenv('LOCAL_DAILY_ZARR_NAME'))  # Local zarr to append to
LOCAL_HOURLY_ZARR = os.path.join(volume_directory, os.getenv('LOCAL_HOURLY_ZARR_NAME'))  # Local zarr to append to

s3_qfinal_dir = os.getenv('S3_QFINAL_DIR')  # Directory containing vpu subdirectories, containing Qfinal files

S3_MONTHLY_TIMESTEPS = os.getenv('S3_MONTHLY_TIMESTEPS')
S3_MONTHLY_TIMESERIES = os.getenv('S3_MONTHLY_TIMESERIES')
S3_ANNUAL_TIMESTEPS = os.getenv('S3_ANNUAL_TIMESTEPS')
S3_ANNUAL_TIMESERIES = os.getenv('S3_ANNUAL_TIMESERIES')
S3_ANNUAL_MAXIMUMS = os.getenv('S3_ANNUAL_MAXIMUMS')

# set some file paths relative to HOME
HOME = os.path.expanduser(os.getenv('HOMR', '~'))
ERA_DIR = os.path.join(HOME, 'data', 'era5_data')
configs_dir = os.path.join(HOME, 'data', 'configs')
inflows_dir = os.path.join(HOME, 'data', 'inflows')
runoff_dir = os.path.join(HOME, 'data', 'era5_runoff')
outputs_dir = os.path.join(HOME, 'data', 'outputs')

if __name__ == '__main__':
    CL = CloudLog(LOG_GROUP_NAME, LOG_STREAM_NAME, ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION)
    session = aiobotocore.session.AioSession(profile='odp')
    s3 = s3fs.S3FileSystem(session=session)

    # create the required directory structure
    os.makedirs(configs_dir, exist_ok=True)
    os.makedirs(runoff_dir, exist_ok=True)
    for d in glob.glob(os.path.join(configs_dir, '*')):
        os.makedirs(os.path.join(inflows_dir, os.path.basename(d)), exist_ok=True)
        os.makedirs(os.path.join(outputs_dir, os.path.basename(d)), exist_ok=True)

    try:
        CL.log_message('START')

        CL.log_message('RUNNING', 'Downloading ERA5 runoff data')
        f.download_era5(ERA_DIR, runoff_dir, S3_DAILY_ZARR, MIN_LAG_TIME_DAYS, CL)

        CL.log_message('RUNNING', 'checking installations and environment')
        f.check_installations(LOCAL_DAILY_ZARR, S3_DAILY_ZARR, LOCAL_HOURLY_ZARR, S3_HOURLY_ZARR, ODP_CREDENTIALS_FILE)

        CL.log_message('RUNNING', 'Verifying zarr on s3 matches local zarr')
        for z, s in zip([LOCAL_DAILY_ZARR, LOCAL_HOURLY_ZARR], [S3_DAILY_ZARR, S3_HOURLY_ZARR]):
            f.check_zarrs_match(z, s)

        CL.log_message('RUNNING', 'preparing config files')
        f.setup_configs(configs_dir, GEOGLOWS_ODP_CONFIGS, CL)

        CL.log_message('RUNNING', 'getting initial qinits')
        f.get_qinits_from_s3(s3, configs_dir, s3_qfinal_dir, outputs_dir)

        CL.log_message('RUNNING', 'verifying era5 data is compatible with the retrospective zarr')
        f.verify_era5_data(runoff_dir, LOCAL_HOURLY_ZARR)

        set_start_method("spawn", force=True) # This avoids some issues I was seeing with multiprocessing (linux 'fork' was mystic...)
        with Pool(f.processes(runoff_dir)) as p:
            CL.log_message('RUNNING', 'preparing inflows')
            f.inflows(runoff_dir, configs_dir, inflows_dir, p)

            CL.log_message('RUNNING', 'Running river-route')
            f.run_river_route(configs_dir, outputs_dir, inflows_dir, p)

        CL.log_message('RUNNING', 'concatenating outputs')
        f.concatenate_outputs(outputs_dir, LOCAL_HOURLY_ZARR, LOCAL_DAILY_ZARR, CL)

        CL.log_message('RUNNING', 'checking local zarr is good to go')
        for z in [LOCAL_DAILY_ZARR, LOCAL_HOURLY_ZARR]:
            f.verify_concatenated_outputs(z, CL)

        CL.log_message('RUNNING', 'syncing to S3')
        f.sync_local_to_s3(outputs_dir, s3_qfinal_dir, LOCAL_HOURLY_ZARR, LOCAL_DAILY_ZARR, S3_HOURLY_ZARR, S3_DAILY_ZARR, ODP_CREDENTIALS_FILE)

        CL.log_message('RUNNING', 'updating other zarrs')
        f.update_monthly_zarrs(LOCAL_DAILY_ZARR, S3_MONTHLY_TIMESTEPS, S3_MONTHLY_TIMESERIES)
        f.update_yearly_zarrs(LOCAL_HOURLY_ZARR, S3_ANNUAL_TIMESTEPS, S3_ANNUAL_TIMESERIES, S3_ANNUAL_MAXIMUMS)


        CL.log_message('RUNNING', 'cleaning up current run')
        f.cleanup(HOME, runoff_dir, inflows_dir, outputs_dir, configs_dir)

        CL.log_message('COMPLETE')
    except Exception as e:
        error = traceback.format_exc()
        CL.log_message('FAIL', error)
