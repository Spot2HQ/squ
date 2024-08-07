#!/bin/bash

# Function to check the exit status of a command
check_exit_status() {
    if [ $1 -ne 0 ]; then
        echo "Error: $2 failed with status $1"
        exit $1
    fi
}

# Check if Docker is installed
if ! command -v docker &> /dev/null
then
    echo "Docker not found. Installing Docker..."
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo apt-get update
        sudo apt-get install -y docker.io
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew install --cask docker
    else
        echo "Unsupported OS. Please install Docker manually."
        exit -1
    fi
fi

echo "Docker is already installed."

# Ensure Docker daemon is running
if ! docker info &> /dev/null; then
    echo "Docker daemon is not running. Starting Docker..."
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo systemctl start docker
        check_exit_status $? "Starting Docker service"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        open --background -a Docker
        echo "Waiting for Docker to start..."
        while ! docker info &> /dev/null; do
            sleep 1
        done
    fi
fi

echo "Docker daemon is running."

DOCKER_COMPOSE_FILE="./docker/docker-compose.yml"

if [ ! -f $DOCKER_COMPOSE_FILE ]; then
    echo "Docker compose file not found at $DOCKER_COMPOSE_FILE"
    exit 1
fi

echo "Stopping and removing any existing Docker containers..."
docker-compose -f $DOCKER_COMPOSE_FILE down --remove-orphans
check_exit_status $? "Stopping and removing existing Docker containers"

echo "Building and starting the Docker container..."
docker-compose -f $DOCKER_COMPOSE_FILE up -d
check_exit_status $? "Starting Docker container"

echo "Docker container started successfully."

# Run tests with tox
echo "Running tests with tox..."
tox
check_exit_status $? "Running tests with tox"

echo "Tests completed successfully."

# Stop and remove Docker containers after tests
echo "Stopping and removing Docker containers..."
docker-compose -f $DOCKER_COMPOSE_FILE down --remove-orphans
check_exit_status $? "Stopping and removing Docker containers after tests"

echo "Docker container stopped and removed successfully."
