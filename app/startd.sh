#!/bin/bash

#####
#
# startd.sh - Start the docker environment
#
# Author: Eric Broda, eric.broda@brodagroupsoftware.com, September 24, 2023
#
# Parameters:
#   N/A
#
#####

if [ -z ${ROOT_DIR+x} ] ; then
    echo "Environment variables have not been set.  Run 'source bin/environment.sh'"
    exit 1
fi

# Show the environment
$PROJECT_DIR/bin/show.sh

NETWORK_NAME="localnet"
docker network create $NETWORK_NAME

compose() {
  docker-compose -f $PROJECT_DIR/docker/docker-compose.yml up
}

decompose() {
  docker-compose -f $PROJECT_DIR/docker/docker-compose.yml down
}

compose;
decompose;