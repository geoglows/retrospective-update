export VOLUME_DIR="/mnt"  # The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab
export S3_ZARR="s3://geoglows-v2-retrospective/retrospective.zarr" # Zarr located on S3
export S3_QFINAL_DIR="s3://geoglows-v2-retrospective/Qfinal" # Directory containing vpu subdirectories, containing Qfinal files
export S3_CONFIGS_DIR="s3://geoglows-v2/configs" # Directory containing subdirectories, containing the files to run RAPID. Only needed if running this for >
export S3_ERA_BUCKET="s3://geoglows-v2-retrospective-updater" # Directory containing the ERA5 data
export LOCAL_ZARR_NAME="geoglows_v2_retrospective.zarr" # Name of the Zarr file on the local machine
export AWS_LOG_GROUP_NAME="retrospective-updater-logs" # AWS CloudWatch log group name
export AWS_LOG_STREAM_NAME="retrospective-updater-log-stream" # AWS CloudWatch log stream name
export REGION_NAME="us-east-1" # AWS region name
export COMPUTE_INSTANCE="i-09176b22574ab46b9" # ID of the other EC2 instance
export CDSAPI_RC="/home/ubuntu/.cdsapirc" # Path to the CDS API key file
export ODP_CREDENTIALS_FILE="/home/ubuntu/odp_aws_credentials" # Path to the ODP credentials file