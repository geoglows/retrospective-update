#!/usr/bin/env bash
+set -Eeuo pipefail

# read the environment variables at ./variables.env relative to the location of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR"/variables.env

# first sleep for 5 minutes to allow for users to cancel the job
sleep "$SLEEP_LENGTH"

# try to activate conda then the environment
source "$CONDA_ACTIVATE_PATH"
if ! conda activate env; then
    curl -X POST -H "Content-Type: application/json" -d '{"text": "Failed to activate conda environment."}' "$WEBHOOK_ERROR_URL" || true
    sudo shutdown -h now
fi

# check that the necessary packages are installed and in the PATH
commands=("curl" "python" "s5cmd")
for cmd in "${commands[@]}"; do
    if ! command -v "$cmd" &> /dev/null; then
      curl -X POST -H "Content-Type: application/json" -d '{"text": "Environment variables not set"}' "$WEBHOOK_ERROR_URL" || true
      sudo shutdown -h now
    fi
done

# check that the configs directory and all local copies of zarrs exist and are not empty
if [ ! -d "$CONFIGS_DIR" ] || [ -z "$(ls -A "$CONFIGS_DIR")" ]; then
    echo "Error: Configs directory is empty or does not exist."
    s5cmd --no-sign-request sync "$S3_CONFIGS_DIR/*" "$CONFIGS_DIR"
fi
if [ ! -d "$HOURLY_ZARR" ]; then
    echo "Hourly zarr directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_HOURLY_ZARR/*" "$HOURLY_ZARR"
fi
if [ ! -d "$DAILY_ZARR" ]; then
    echo "Daily zarr directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_DAILY_ZARR/*" "$DAILY_ZARR"
fi
if [ ! -d "$MONTHLY_TIMESERIES_ZARR" ]; then
    echo "Monthly timeseries directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync --exclude "*Q/0.*" "$S3_MONTHLY_TIMESERIES_ZARR/*" "$WORK_DIR"/monthly-timeseries.zarr
fi
if [ ! -d "$MONTHLY_TIMESTEPS_ZARR" ]; then
    echo "Monthly timesteps directory is empty or does not exist. Downloading a copy."
    s5cmd --no-sign-request sync "$S3_MONTHLY_TIMESTEPS_ZARR/*" "$WORK_DIR"/monthly-timesteps.zarr
fi

# prepare directory structure
mkdir -p "$WORK_DIR" "$OUTPUTS_DIR" "$ERA5_DIR" "$FINAL_STATES_DIR" "$FORECAST_INITS_DIR" "$HYDROSOS_DIR"
# make sure directories remain editable
chmod -R 777 "$OUTPUTS_DIR" "$ERA5_DIR" "$FINAL_STATES_DIR" "$FORECAST_INITS_DIR" "$HYDROSOS_DIR"

# run a setup/preparation/validation check
if ! python "$SCRIPTS_ROOT"/prepare.py; then
    echo "Error identified when preparing for computations."
    exit 1
fi

# era5 download script
if ! python "$SCRIPTS_ROOT"/download_era5.py; then
    echo "Error: Failed to run the ERA5 download script."
    exit 1
fi

# routing script
if ! python "$SCRIPTS_ROOT"/route.py; then
    echo "Error: Failed to run the routing script."
    rm -r "$OUTPUTS_DIR"
    exit 1
fi

# synchronize inits to s3
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp "$FINAL_STATES_DIR/*" "$S3_FINAL_STATES_DIR"/
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp "$FORECAST_INITS_DIR/*" "$S3_FORECAST_INITS_DIR"/

# append the discharge to zarrs
if ! python "$SCRIPTS_ROOT"/append_discharge.py; then
  echo "Error: Failed to run the appending script."
  exit 1
fi

# clean up any existing data that shouldn't be there anymore
rm -r "$OUTPUTS_DIR" "$ERA5_DIR"

# sync to s3
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp "$HOURLY_ZARR/*" "$S3_HOURLY_ZARR"/
s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" cp "$DAILY_ZARR/*" "$S3_DAILY_ZARR"/

# prepare monthly derived products
if python "$SCRIPTS_ROOT"/monthly_products.py; then
  s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" sync "$WORK_DIR/monthly-timeseries.zarr/*" "$S3_MONTHLY_TIMESERIES_ZARR"/
  s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" sync "$WORK_DIR/monthly-timesteps.zarr/*" "$S3_MONTHLY_TIMESTEPS_ZARR"/
  s5cmd --credentials-file "$AWS_CREDENTIALS_FILE" sync "$HYDROSOS_DIR/*.tif" "$S3_HYDROSOS_COGS"/
  rm -r "$HYDROSOS_DIR"/*.tif
fi

# shutdown the machine. || true makes sure that the script does not exit on failed posts and will always hit the shutdown command
curl -X POST -H "Content-Type: application/json" -d '{"text": "All tasks completed successfully. Shutting down the machine."}' "$WEBHOOK_LOG_URL" || true
sudo shutdown -h now
