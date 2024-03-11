#!/bin/bash

MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
ENVIRONMENT_FILE="retrospective-update/computation_scripts/environment.yaml"
ENVIRONMENT_NAME="add_week"

sudo apt-get update

# Download and install Miniconda
echo "Downloading and installing Miniconda..."
mkdir -p ~/miniconda3
wget $MINICONDA_URL -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
source .bashrc

# Create conda environment from environment file
echo "Creating conda environment from $ENVIRONMENT_FILE..."
conda env create -f $ENVIRONMENT_FILE -n $ENVIRONMENT_NAME

# Activate the conda environment
echo "Activating conda environment..."
conda activate $ENVIRONMENT_NAME

# Install docker
echo "Downloading and installing Docker..."
sudo apt-get install -y docker.io
sudo systemctl start docker
sudo systemctl enable docker
sudo docker pull chdavid/rapid
