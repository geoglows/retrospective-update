#!/bin/bash

ENVIRONMENT_FILE="retrospective-update/environment.yaml"
ENVIRONMENT_NAME="update"

sudo apt-get update

# Download and install Miniconda
echo "Downloading and installing Miniconda..."
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
bash Miniforge3-$(uname)-$(uname -m).sh -b
~/miniforge3/bin/conda init --all
source ~/.bashrc

conda config --add channels conda-forge
conda config --set channel_priority strict

# Create conda environment from environment file
echo "Creating conda environment from $ENVIRONMENT_FILE..."
mamba env create -f $ENVIRONMENT_FILE -n $ENVIRONMENT_NAME
mamba init
source ~/.bashrc

