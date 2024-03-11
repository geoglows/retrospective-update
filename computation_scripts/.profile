export VOLUME_DIR="/mnt/retro_volume"  # The volume is mounted to this location upon each EC2 startup. To change, modify /etc/fstab
export S3_ZARR="s3://geoglows-scratch/retrospective.zarr" # Zarr located on S3
export QFINAL_DIR="s3://geoglows-scratch/retro" # Directory containing vpu subdirectories, containing Qfinal files
export CONFIGS_DIR="s3://geoglows-v2/configs" # Directory containing subdirectories, containing the files to run RAPID. Only needed if running this for the first time
export ERA_DIR="s3://geoglows-scratch/era_5" # Directory containing the ERA5 data
export LOCAL_ZARR="geoglows_v2_retrospective.zarr" # Name of the Zarr file on the local machine
export AWS_LOG_GROUP_NAME="AppendWeekLog" # AWS CloudWatch log group name
export AWS_LOG_STREAM_NAME="EC2" # AWS CloudWatch log stream name