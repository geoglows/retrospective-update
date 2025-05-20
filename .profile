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

export S3_HYDROSOS_DIR="s3://geoglows-v2/maptiles/hydrosos" # Directory containing hydrosos files

export LOCAL_DAILY_ZARR_NAME="daily.zarr" # Name of the Zarr file on the local machine
export LOCAL_HOURLY_ZARR_NAME="hourly.zarr" # Name of the Zarr file on the local machine

export ODP_CREDENTIALS_FILE="/home/ubuntu/.aws/credentials" # Path to the AWS credentials file
export AWS_LOG_GROUP_NAME="retrospective-updater-logs" # AWS CloudWatch log group name
export AWS_LOG_STREAM_NAME="updater-logs-new" # AWS CloudWatch log stream name
export REGION_NAME="us-east-1" # AWS region name
export CDSAPI_RC="/home/ubuntu/.cdsapirc" # Path to the CDS API key file

# Assests related to hydrobasin tile creation
export HYDROBASINS_COMBINED="/home/ubuntu/retrospective-update/retrospective-update/assets/HYBAS_LEVEL_4_combined.parquet" # Path to the combined hydrobasin file
export HYBASID_TO_LINKNO_CSV="/home/ubuntu/retrospective-update/retrospective-update/assets/final_matchings_may_12.csv"
export UNIQUE_HYBASIDS_CSV="/home/ubuntu/retrospective-update/retrospective-update/assets/unique_hybas_ids_new.csv"
export DUPLICATED_HYBASIDS_CSV="/home/ubuntu/retrospective-update/retrospective-update/assets/duplicated_hybas_ids_new.csv"
export FLOW_CUTOFF_NC="/home/ubuntu/retrospective-update/retrospective-update/assets/flow_cutoff_thresholds_may_12.nc"
export FLOW_CUTOFF_NC_BY_HYBASID="/home/ubuntu/retrospective-update/retrospective-update/assets/flow_cutoff_thresholds_by_hybas_may_12.nc"