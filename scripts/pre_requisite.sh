#!/bin/bash

# Set the environment name to sprout_env
ENV_NAME="sprout_env"

# Create a new conda environment
echo "Creating a new conda environment: $ENV_NAME..."
conda create -n $ENV_NAME python=3.9 -y

# Activate the newly created environment
echo "Activating the environment..."
source $(conda info --base)/etc/profile.d/conda.sh
conda activate $ENV_NAME

# Check if requirements.txt exists
if [ ! -f requirements.txt ]; then
  echo "Error: requirements.txt not found in the current directory."
  exit 1
fi

# Install dependencies from requirements.txt
echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

# Verify installation
echo "Installed packages:"
pip list

# Completion message
echo "Environment $ENV_NAME created and dependencies installed successfully."
