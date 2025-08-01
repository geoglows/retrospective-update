import os
import traceback
from glob import glob
from multiprocessing import Pool

from natsort import natsorted
from tqdm import tqdm
import functions as f
from cloud_logger import CloudLog

if __name__ == '__main__':
    # Parameters
    MIN_LAG_TIME_DAYS = int(os.getenv('MIN_LAG_TIME_DAYS', 5))

    DATA_DIR = os.getenv('WORK_DIR')
    ERA5_DIR = os.getenv('ERA5_DIR')
    CONFIGS_DIR = os.getenv('CONFIGS_DIR')
    OUTPUTS_DIR = os.getenv('OUTPUTS_DIR')
    HYDROSOS_DIR = os.getenv('HYDROSOS_DIR')
    FINAL_STATES_DIR = os.getenv('FINAL_STATES_DIR')

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

    CL = CloudLog()
    try:
        CL.ping('RUNNING', f'routing')
        with Pool(os.cpu_count()) as p:
            CL.ping('RUNNING', 'Running-river-route')
            era5_data = natsorted(glob(os.path.join(ERA5_DIR, 'era5_*.nc')))
            vpus = natsorted(glob(os.path.join(CONFIGS_DIR, '*')))
            list(
                tqdm(
                    p.imap_unordered(f.route_vpu, [(vpu, era5_data, OUTPUTS_DIR) for vpu in vpus]),
                    total=len(vpus),
                    desc='Routing VPUs',
                ),
            )

        CL.ping('RUNNING', 'concatenating-outputs')
        f.concatenate_outputs(OUTPUTS_DIR, HOURLY_ZARR, DAILY_ZARR, CL)

        # CL.ping('RUNNING', 'checking local zarr is good to go')
        # for z in [DAILY_ZARR, HOURLY_ZARR]:
        #     f.verify_concatenated_outputs(z, CL)
        #
        # # todo if today is sunday then transpose chunks and sync to s3
        # CL.ping('RUNNING', 'syncing qfinals to s3')
        # f.sync_qfinals_to_s3(OUTPUTS_DIR, S3_QFINAL_DIR, ODP_CREDENTIALS_FILE, CL)
        #
        # CL.ping('RUNNING', 'syncing to s3')
        # f.sync_local_to_s3(HOURLY_ZARR, DAILY_ZARR, S3_HOURLY_ZARR, S3_DAILY_ZARR, ODP_CREDENTIALS_FILE, CL)
        #
        # CL.ping('RUNNING', 'updating monthly zarrs')
        # f.update_monthly_zarrs(HOURLY_ZARR, S3_MONTHLY_TIMESTEPS, S3_MONTHLY_TIMESERIES, HYDROSOS_DIR, ODP_CREDENTIALS_FILE, CL)
        #
        # CL.ping('RUNNING', 'updating annual zarrs')
        # f.update_yearly_zarrs(HOURLY_ZARR, S3_ANNUAL_TIMESTEPS, S3_ANNUAL_TIMESERIES, S3_ANNUAL_MAXIMUMS, CL)
        #
        # # todo move to shell script
        # CL.ping('RUNNING', 'cleaning-up')
        # f.cleanup(DATA_DIR, ERA5_DIR, RUNOFF_DIR, INFLOWS_DIR, OUTPUTS_DIR, HYDROSOS_DIR)
        # # delete all but the most recent qfinal
        # for vpu_dir in glob.glob(os.path.join(outputs_dir, '*')):
        #     qfinal_files = natsort.natsorted(glob.glob(os.path.join(vpu_dir, 'finalstate*.parquet')))
        #     if clear_qinit:
        #         # remove all qfinal files
        #         for f in qfinal_files:
        #             os.remove(f)
        #     elif len(qfinal_files) > 1:
        #         for f in qfinal_files[:-1]:
        #             os.remove(f)
        #
        # CL.ping('COMPLETE', "Retrospective-update-complete")
        # exit(0)
    except Exception as e:
        error = traceback.format_exc()
        CL.ping('FAIL', str(e))
        # f.cleanup, DATA_DIR, ERA5_DIR, RUNOFF_DIR, INFLOWS_DIR, OUTPUTS_DIR, HYDROSOS_DIR, True
