#!/bin/bash

# Check if environment name is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <environment_name>"
    exit 1
fi

# Set environment name from command-line argument
ENV_NAME=$1

# Define directories
DEPLOY_DIR="/fis/nfs-share/devops/Deployment/$ENV_NAME"
FILES_DIR="/home/cloudbreak/SP7_files"
RELEASE_DIR="/fis/nfs-share/devops/Releasefiles"

# Create required directories
mkdir -p "$DEPLOY_DIR/24.4.1-7-BASE" "$DEPLOY_DIR/24.4.1-7-DP" "$DEPLOY_DIR/24.4.1-RL"

# Copy and unzip BASE package
cp "$FILES_DIR/ras-cds-package-cte2-24.4.1-1.zip" "$DEPLOY_DIR/24.4.1-7-BASE/"
cd "$DEPLOY_DIR/24.4.1-7-BASE" && unzip ras-cds-package-cte2-24.4.1-1.zip

# Copy and unzip DP package
cp "$FILES_DIR/ras-cds-package-cte2-24.4.1.zip" "$DEPLOY_DIR/24.4.1-7-DP/"
cd "$DEPLOY_DIR/24.4.1-7-DP" && unzip ras-cds-package-cte2-24.4.1.zip

# Copy and unzip RL package
cp "$FILES_DIR/ras-cds-package-cte2-24.4.1.zip" "$DEPLOY_DIR/24.4.1-RL/"
cd "$DEPLOY_DIR/24.4.1-RL" && unzip ras-cds-package-cte2-24.4.1.zip

# Copy other required files
cp "$FILES_DIR/mbp-ras-core-24.4.1-4.zip" "$RELEASE_DIR/RASCORE/$ENV_NAME/"
cp "$FILES_DIR/mbp-ras-dp-24.4.1-7.zip" "$RELEASE_DIR/RASCORE/$ENV_NAME/"
cp "$FILES_DIR/mbp-ras-ln-24.4.1.zip" "$RELEASE_DIR/RASCORE/$ENV_NAME/"
cp "$FILES_DIR/mbp-ras-core-db-24.4.1-2-BASE.zip" "$RELEASE_DIR/DB/$ENV_NAME/"
cp "$FILES_DIR/mbp-ras-core-db-24.4.1-DP.zip" "$RELEASE_DIR/DB/$ENV_NAME/"
cp "$FILES_DIR/ras-udf-2.3.0-v2.jar" "$RELEASE_DIR/RTJ/$ENV_NAME/"
cp "$FILES_DIR/ras-airflow-service-api-2.3.0-v4.tar.gz" "$RELEASE_DIR/AIRFLOW3/$ENV_NAME/"

echo "Deployment for $ENV_NAME completed successfully!"
