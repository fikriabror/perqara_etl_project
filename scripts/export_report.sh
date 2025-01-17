#!/bin/bash

# Variables
CONTAINER_NAME="sprout_etl_project-scheduler-1" 
CONTAINER_PATH="/opt/airflow/data/report.xlsx" 
LOCAL_PATH="/Users/abror/Documents/personal/test_assignment/sprout/sprout_etl_project/data"  # Path to save the file locally
LOCAL_FILE="$LOCAL_PATH/report.xlsx"  # Full local file path

# Check if the container is running
if docker ps | grep -q "$CONTAINER_NAME"; then
    echo "Exporting report.xlsx from $CONTAINER_NAME..."
    
    # Ensure the local directory exists
    if [ ! -d "$LOCAL_PATH" ]; then
        echo "Creating local directory: $LOCAL_PATH"
        mkdir -p "$LOCAL_PATH"
    fi
    
    # Copy the file from the container to the local machine
    docker cp "$CONTAINER_NAME:$CONTAINER_PATH" "$LOCAL_FILE"
    
    # Check if the copy was successful
    if [ $? -eq 0 ]; then
        echo "File successfully exported to $LOCAL_FILE"
    else
        echo "Failed to export the file. Please check the container and paths."
        exit 1
    fi
else
    echo "Error: Docker container $CONTAINER_NAME is not running."
    exit 1
fi
