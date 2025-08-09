#!/usr/bin/env bash

# first sleep for 5 minutes to allow for users to cancel the job
sleep 300

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
export SCRIPTS_ROOT="/home/ubuntu/retrospective-update/retrospective-update"
export WORK_DIR="/data"
export S3_BASE_URI="s3://geoglows-v2"
export CDSAPI_RC="/home/ubuntu/cdsapirc.txt"
export AWS_CREDENTIALS_FILE="/home/ubuntu/awscredentials"

export OUTPUTS_DIR="$WORK_DIR/discharge"
export ERA5_DIR="$WORK_DIR/era5"

export HYDROSOS_DIR="$WORK_DIR/hydrosos"
export S3_HYDROSOS_COGS="$S3_BASE_URI/hydrosos/cogs"
export S3_HYDROSOS_FILES="$S3_BASE_URI/hydrosos/*.parquet"
export HYDROSOS_ID_PAIRS="$HYDROSOS_DIR/hybas_linkno_pairs.parquet"
export HYDROSOS_BASINS="$HYDROSOS_DIR/hydrobasins_level_4.parquet"
export HYDROSOS_THRESHOLDS="$HYDROSOS_DIR/thresholds.parquet"

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

export MONTHLY_TIMESERIES="$WORK_DIR/monthly-timeseries.zarr"
export MONTHLY_TIMESTEPS="$WORK_DIR/monthly-timesteps.zarr"
export S3_MONTHLY_TIMESERIES="$S3_BASE_URI/retrospective/monthly-timeseries.zarr"
export S3_MONTHLY_TIMESTEPS="$S3_BASE_URI/retrospective/monthly-timesteps.zarr"

export S3_ANNUAL_TIMESERIES="$S3_BASE_URI/retrospective/annual-timeseries.zarr"
export S3_ANNUAL_TIMESTEPS="$S3_BASE_URI/retrospective/annual-timesteps.zarr"
export S3_ANNUAL_MAXIMUMS="$S3_BASE_URI/retrospective/annual-maximums.zarr"

# check that the configs directory and all local copies of zarrs exist and are not empty
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
if [ ! -d "$MONTHLY_TIMESERIES" ]; then
    echo "Monthly timeseries directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_MONTHLY_TIMESERIES/*" $WORK_DIR/monthly-timeseries.zarr
fi
if [ ! -d "$MONTHLY_TIMESTEPS" ]; then
    echo "Monthly timesteps directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync "$S3_MONTHLY_TIMESTEPS/*" $WORK_DIR/monthly-timesteps.zarr
fi

# prepare directory structure
mkdir -p $WORK_DIR
mkdir -p $OUTPUTS_DIR
mkdir -p $ERA5_DIR
mkdir -p $FINAL_STATES_DIR
mkdir -p $FORECAST_INITS_DIR
mkdir -p $HYDROSOS_DIR
# make sure directory remains editable
chmod -R 777 $OUTPUTS_DIR
chmod -R 777 $ERA5_DIR
chmod -R 777 $FINAL_STATES_DIR
chmod -R 777 $FORECAST_INITS_DIR
chmod -R 777 $HYDROSOS_DIR

# run a setup/preparation/validation check
python $SCRIPTS_ROOT/prepare.py
if [ $? -ne 0 ]; then
    echo "Error identified when preparing for computations."
    exit 1
fi

# era5 download script
python $SCRIPTS_ROOT/download_era5.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to run the ERA5 download script."
    exit 1
fi

# routing script
python $SCRIPTS_ROOT/route.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to run the routing script."
    rm -r $OUTPUTS_DIR
    exit 1
fi

# synchronize inits to s3
s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$FINAL_STATES_DIR/*" $S3_FINAL_STATES_DIR/
s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$FORECAST_INITS_DIR/*" $S3_FORECAST_INITS_DIR/

# append the discharge to zarrs
python $SCRIPTS_ROOT/append_discharge.py
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

# prepare monthly derived products
python $SCRIPTS_ROOT/monthly_products.py
if [ $? -eq 0 ]; then
  s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$WORK_DIR/monthly-timeseries.zarr/*" $S3_MONTHLY_TIMESERIES/
  s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$WORK_DIR/monthly-timesteps.zarr/*" $S3_MONTHLY_TIMESTEPS/
  s5cmd --credentials-file $AWS_CREDENTIALS_FILE cp "$HYDROSOS_DIR/*.tif" $S3_HYDROSOS_COGS/
  rm -r $HYDROSOS_DIR/*.tif
fi

# shutdown the machine
curl -X POST -H "Content-Type: application/json" -d '{"text": "All tasks completed successfully. Shutting down the machine."}' $WEBHOOK_URL
sudo shutdown -h now
