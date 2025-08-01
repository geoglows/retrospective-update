#!/usr/bin/env bash

# first sleep for 4 minutes to allow for users to cancel the job
#sleep 240

# try to activate the conda environment
source /home/ubuntu/miniconda3/bin/activate
conda activate env
if [ $? -ne 0 ]; then
    echo "Error: Failed to activate conda environment."
    exit 1
fi

# todo check that the packages are installed
# we need curl, python, s5cmd, nco

# Environment variables
export WEBHOOK_URL=""

export WORK_DIR="/data"
export OUTPUTS_DIR="$WORK_DIR/discharge"
export ERA5_DIR="$WORK_DIR/era5"
export HYDROSOS_DIR="$WORK_DIR/hydrosos"

export DAILY_ZARR="$WORK_DIR/daily.zarr"
export HOURLY_ZARR="$WORK_DIR/hourly.zarr"

export CONFIGS_DIR="$WORK_DIR/routing-configs"
export S3_CONFIGS_DIR="s3://geoglows-v2/routing-configs"

export FINAL_STATES_DIR="$WORK_DIR/final-states"
export S3_FINAL_STATES_DIR="s3://geoglows-v2/retrospective/final-states"

export HOURLY_ZARR="$WORK_DIR/hourly.zarr"
export DAILY_ZARR="$WORK_DIR/daily.zarr"
export S3_HOURLY_ZARR="s3://geoglows-v2/retrospective/hourly.zarr"
export S3_DAILY_ZARR="s3://geoglows-v2/retrospective/daily.zarr"

export S3_MONTHLY_TIMESERIES="s3://geoglows-v2/retrospective/monthly-timeseries.zarr"
export S3_MONTHLY_TIMESTEPS="s3://geoglows-v2/retrospective/monthly-timesteps.zarr"
export S3_ANNUAL_TIMESERIES="s3://geoglows-v2/retrospective/annual-timeseries.zarr"
export S3_ANNUAL_TIMESTEPS="s3://geoglows-v2/retrospective/annual-timesteps.zarr"
export S3_ANNUAL_MAXIMUMS="s3://geoglows-v2/retrospective/annual-maximums.zarr"

# prepare directory structure
mkdir -p $WORK_DIR
mkdir -p $OUTPUTS_DIR
mkdir -p $ERA5_DIR
mkdir -p $FINAL_STATES_DIR

# todo check that the cdsapi file exists and that there are aws credentials and defaults set
# check that the configs directory and both local zarrs exist and are not empty
if [ ! -d "$CONFIGS_DIR" ] || [ -z "$(ls -A $CONFIGS_DIR)" ]; then
    echo "Error: Configs directory is empty or does not exist."
    exit 1
fi
if [ ! -d "$HOURLY_ZARR" ] || [ -z "$(ls -A $HOURLY_ZARR)" ]; then
    echo "Error: Hourly zarr directory is empty or does not exist."
    exit 1
fi
if [ ! -d "$DAILY_ZARR" ] || [ -z "$(ls -A $DAILY_ZARR)" ]; then
    echo "Error: Daily zarr directory is empty or does not exist."
    exit 1
fi
export CDSAPI_RC="/home/ubuntu/.cdsapirc" # Path to the CDS API key file, this exact env variable name is required by the cdsapi package

# run the era5 download script
python /home/ubuntu/retrospective-update/retrospective-update/download-era5.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to run the ERA5 download script."
    exit 1
fi

# run the main python script
#python /home/ubuntu/retrospective-update/retrospective-update/master.py
#if [ $? -ne 0 ]; then
#    echo "Error: Failed to run the routing script."
#    exit 1
#fi

# empty the outputs and era5 directories
rm -r $OUTPUTS_DIR/*
rm -r $ERA5_DIR/*
rm -r $HYDROSOS_DIR/*
