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

# check that the necessary packages are installed and in the PATH
commands=("curl" "python" "s5cmd")
for cmd in "${commands[@]}"; do
    if ! command -v $cmd &> /dev/null; then
        echo "Error: $cmd is not installed or not found in PATH."
        exit 1
    fi
done

# Environment variables
export WEBHOOK_URL=""
export WORK_DIR="/data"
export S3_BASE_URI="s3://geoglows-v2"
export CDSAPI_RC="/home/ubuntu/cdsapirc.txt"
export AWS_CREDENTIALS_FILE="/home/ubuntu/awscredentials"

export OUTPUTS_DIR="$WORK_DIR/discharge"
export ERA5_DIR="$WORK_DIR/era5"
export HYDROSOS_DIR="$WORK_DIR/hydrosos"

export CONFIGS_DIR="$WORK_DIR/routing-configs"
export S3_CONFIGS_DIR="$S3_BASE_URI/routing-configs"

export FINAL_STATES_DIR="$WORK_DIR/final-states"
export S3_FINAL_STATES_DIR="$S3_BASE_URI/retrospective/final-states"

export FORECAST_INITS_DIR="$WORK_DIR/forecast-inits"
export S3_FORECAST_INITS_DIR="$S3_BASE_URI/retrospective/forcast-inits"

export HOURLY_ZARR="$WORK_DIR/hourly.zarr"
export DAILY_ZARR="$WORK_DIR/daily.zarr"
export S3_HOURLY_ZARR="$S3_BASE_URI/retrospective/hourly.zarr"
export S3_DAILY_ZARR="$S3_BASE_URI/retrospective/daily.zarr"

export S3_MONTHLY_TIMESERIES="$S3_BASE_URI/retrospective/monthly-timeseries.zarr"
export S3_MONTHLY_TIMESTEPS="$S3_BASE_URI/retrospective/monthly-timesteps.zarr"
export S3_ANNUAL_TIMESERIES="$S3_BASE_URI/retrospective/annual-timeseries.zarr"
export S3_ANNUAL_TIMESTEPS="$S3_BASE_URI/retrospective/annual-timesteps.zarr"
export S3_ANNUAL_MAXIMUMS="$S3_BASE_URI/retrospective/annual-maximums.zarr"

# prepare directory structure
mkdir -p $WORK_DIR
mkdir -p $OUTPUTS_DIR
mkdir -p $ERA5_DIR
mkdir -p $FINAL_STATES_DIR
# make sure directory remains editable
chmod -R 777 $OUTPUTS_DIR
chmod -R 777 $ERA5_DIR
chmod -R 777 $FINAL_STATES_DIR

# check that the configs directory and both local zarrs exist and are not empty
if [ ! -d "$CONFIGS_DIR" ] || [ -z "$(ls -A $CONFIGS_DIR)" ]; then
    echo "Error: Configs directory is empty or does not exist."
    s5cmd --no-sign-request sync "$S3_CONFIGS_DIR/*" $CONFIGS_DIR
fi
if [ ! -d "$HOURLY_ZARR" ]; then
    echo "Hourly zarr directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_HOURLY_ZARR/*" $HOURLY_ZARR
fi
if [ ! -d "$DAILY_ZARR" ]; then
    echo "Daily zarr directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_DAILY_ZARR/*" $DAILY_ZARR
fi

# run a setup/preparation/validation check
python /home/ubuntu/retrospective-update/retrospective-update/prepare.py
if [ $? -ne 0 ]; then
    echo "Error identified when preparing for computations."
    exit 1
fi

# era5 download script
python /home/ubuntu/retrospective-update/retrospective-update/download_era5.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to run the ERA5 download script."
    exit 1
fi

# routing script
python /home/ubuntu/retrospective-update/retrospective-update/route.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to run the routing script."
    rm -r $OUTPUTS_DIR
    exit 1
fi

# synchronize inits to s3
s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$FINAL_STATES_DIR/*" $S3_FINAL_STATES_DIR/
s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$FORECAST_INITS_DIR/*" $S3_FORECAST_INITS_DIR/

# append the discharge to zarrs
python /home/ubuntu/retrospective-update/retrospective-update/append_discharge.py
if [ $? -ne 0 ]; then
  echo "Error: Failed to run the appending script."
  exit 1
fi

# clean up any existing data that shouldn't be there anymore
rm -r $OUTPUTS_DIR
rm -r $ERA5_DIR

# sync to s3
s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$HOURLY_ZARR/*" $S3_HOURLY_ZARR/
s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$DAILY_ZARR/*" $S3_DAILY_ZARR/
