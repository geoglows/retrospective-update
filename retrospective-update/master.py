import os
import traceback
from multiprocessing import Pool

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
        CL.ping('RUNNING', 'preparing config files')
        f.setup_configs(CONFIGS_DIR, S3_CONFIGS_DIR, CL)

        CL.ping('RUNNING', 'getting initial qinits')
        # todo
        f.get_qinits_from_s3(s3, HOURLY_ZARR, CONFIGS_DIR, S3_QFINAL_DIR, OUTPUTS_DIR)

        CL.ping('RUNNING', f'preparing inflows and namelists with {os.cpu_count()} processes')
        with Pool(os.cpu_count()) as p:
            CL.ping('RUNNING', 'Running-river-route')
            # f.run_river_route(CONFIGS_DIR, OUTPUTS_DIR, INFLOWS_DIR, p)
            # vpus = glob.glob(os.path.join(configs_dir, '*'))
            # list(
            #     tqdm.tqdm(
            #         p.imap_unordered(_run_river_route_star, [(vpu, outputs_dir, inflows_dir) for vpu in vpus]),
            #         total=len(vpus)),
            # )

        CL.ping('RUNNING', 'syncing qfinals to s3')
        f.sync_qfinals_to_s3(OUTPUTS_DIR, S3_QFINAL_DIR, ODP_CREDENTIALS_FILE, CL)

        CL.ping('RUNNING', 'concatenating-outputs')
        f.concatenate_outputs(OUTPUTS_DIR, HOURLY_ZARR, DAILY_ZARR, CL)

        CL.ping('RUNNING', 'checking local zarr is good to go')
        for z in [DAILY_ZARR, HOURLY_ZARR]:
            f.verify_concatenated_outputs(z, CL)

        # todo if today is sunday then transpose chunks and sync to s3

        CL.ping('RUNNING', 'syncing to s3')
        f.sync_local_to_s3(HOURLY_ZARR, DAILY_ZARR, S3_HOURLY_ZARR, S3_DAILY_ZARR, ODP_CREDENTIALS_FILE, CL)

        CL.ping('RUNNING', 'updating monthly zarrs')
        f.update_monthly_zarrs(HOURLY_ZARR, S3_MONTHLY_TIMESTEPS, S3_MONTHLY_TIMESERIES, HYDROSOS_DIR, ODP_CREDENTIALS_FILE, CL)

        CL.ping('RUNNING', 'updating annual zarrs')
        f.update_yearly_zarrs(HOURLY_ZARR, S3_ANNUAL_TIMESTEPS, S3_ANNUAL_TIMESERIES, S3_ANNUAL_MAXIMUMS, CL)

        # todo move to shell script
        CL.ping('RUNNING', 'cleaning-up')
        f.cleanup(DATA_DIR, ERA5_DIR, RUNOFF_DIR, INFLOWS_DIR, OUTPUTS_DIR, HYDROSOS_DIR)
        # delete all but the most recent qfinal
        for vpu_dir in glob.glob(os.path.join(outputs_dir, '*')):
            qfinal_files = natsort.natsorted(glob.glob(os.path.join(vpu_dir, 'finalstate*.parquet')))
            if clear_qinit:
                # remove all qfinal files
                for f in qfinal_files:
                    os.remove(f)
            elif len(qfinal_files) > 1:
                for f in qfinal_files[:-1]:
                    os.remove(f)

        CL.ping('COMPLETE', "Retrospective-update-complete")
        exit(0)
    except Exception as e:
        error = traceback.format_exc()
        CL.ping('FAIL', str(e))
        # f.cleanup, DATA_DIR, ERA5_DIR, RUNOFF_DIR, INFLOWS_DIR, OUTPUTS_DIR, HYDROSOS_DIR, True
