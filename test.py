# import computation_scripts.master as m
import aiobotocore
import natsort
import s3fs, os, glob, logging, subprocess

# session = aiobotocore.session.AioSession(profile='odp')
s3 = s3fs.S3FileSystem()
configs_dir = r's3://rfs-v2/routing-configs'
convert = True


# os.makedirs(configs_dir, exist_ok=True)
if len(glob.glob(os.path.join(configs_dir, '*', '*.csv'))) == 0:
    if convert:
        # We need to convert rr files to rapid files
        for connectivity, params in zip(natsort.natsorted(s3.glob(f'{configs_dir}/*/connectivity.parquet')),
                                        natsort.natsorted(s3.glob(f'{configs_dir}/*/routing_parameters.parquet'))):
            vpu = os.path.basename(os.path.dirname(connectivity))
            connectivity = f"s3://{connectivity}"
            params = f"s3://{params}"
        
        

    else:
        result = subprocess.run(f"aws s3 sync {credentials} {configs_dir}", shell=True, capture_output=True,
                                text=True)
        if result.returncode == 0:
            CL.log_message('RUNNING', "Obtained configs from S3")
        else:
            raise RuntimeError(f"Failed to obtain configs from S3: {result.stderr}")