#!/bin/bash

# Ensure the script is executable
chmod +x run_airflow.sh

# Step 1: Pull necessary Docker images
docker-compose pull

# Step 2: Initialize Airflow database
docker-compose up airflow-init

# Step 3: Start Airflow services
docker-compose up -d

# Step 4: Display logs (optional)
echo "Airflow services are starting..."
# docker-compose logs -f
