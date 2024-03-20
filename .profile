export VOLUME_DIR="/mnt"  # The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab
export S3_ZARR="s3://geoglows-scratch/retrospective.zarr" # Zarr located on S3
export S3_QFINAL_DIR="s3://geoglows-scratch/retro" # Directory containing vpu subdirectories, containing Qfinal files
export S3_CONFIGS_DIR="s3://geoglows-v2/configs" # Directory containing subdirectories, containing the files to run RAPID. Only needed if running this for the first time
export S3_ERA_DIR="s3://geoglows-scratch/era_5" # Directory containing the ERA5 data
export LOCAL_ZARR_NAME="geoglows_v2_retrospective.zarr" # Name of the Zarr file on the local machine
export AWS_LOG_GROUP_NAME="AppendWeekLog" # AWS CloudWatch log group name
export AWS_LOG_STREAM_NAME="EC2" # AWS CloudWatch log stream name
export REGION_NAME="us-west-2" # AWS region name
export S3_BUCKET="s3://geoglows-scratch" # S3 bucket to store the zarr files
export COMPUTE_INSTANCE="i-09b6572634aa5dcd6" # ID of the other EC2 instance