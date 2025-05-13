export MIN_LAG_TIME_DAYS=5 # Minimum number of days to wait before attempting data download

export VOLUME_DIR="/mnt"  # The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab

export S3_DAILY_ZARR="s3://geoglows-v2/retrospective/daily.zarr" # Zarr located on S3
export S3_HOURLY_ZARR="s3://geoglows-v2/retrospective/hourly.zarr" # Zarr located on S3
export S3_QFINAL_DIR="s3://geoglows-v2/retrospective/final-states" # Directory containing vpu subdirectories, containing Qfinal files
export S3_CONFIGS_DIR="s3://geoglows-v2/routing-configs" # Directory containing subdirectories, containing the files to run river route.

export S3_MONTHLY_TIMESTEPS="s3://geoglows-v2/retrospective/monthly-timesteps.zarr" # Zarr located on S3
export S3_MONTHLY_TIMESERIES="s3://geoglows-v2/retrospective/monthly-timeseries.zarr" # Zarr located on S3
export S3_ANNUAL_TIMESTEPS="s3://geoglows-v2/retrospective/yearly-timesteps.zarr" # Zarr located on S3
export S3_ANNUAL_TIMESERIES="s3://geoglows-v2/retrospective/yearly-timeseries.zarr" # Zarr located on S3
export S3_ANNUAL_MAXIMUMS="s3://geoglows-v2/retrospective/annual-maximums.zarr" # Zarr located on S3

export LOCAL_DAILY_ZARR_NAME="daily.zarr" # Name of the Zarr file on the local machine
export LOCAL_HOURLY_ZARR_NAME="hourly.zarr" # Name of the Zarr file on the local machine

export ODP_CREDENTIALS_FILE="/home/ubuntu/.aws/credentials" # Path to the AWS credentials file
export AWS_LOG_GROUP_NAME="retrospective-updater-logs" # AWS CloudWatch log group name
export AWS_LOG_STREAM_NAME="updater-logs-new" # AWS CloudWatch log stream name
export REGION_NAME="us-east-1" # AWS region name
export CDSAPI_RC="/home/ubuntu/.cdsapirc" # Path to the CDS API key file
