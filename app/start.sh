#!/bin/bash

#####
#
# start.sh - Start the server
#
# Author: Eric Broda, eric.broda@brodagroupsoftware.com, August 13, 2023
#
#####

if [ -z ${ROOT_DIR+x} ] ; then
    echo "Environment variables have not been set.  Run 'source bin/environment.sh'"
    exit 1
fi

function showHelp {
    echo " "
    echo "ERROR: $1"
    echo " "
    echo "Usage:"
    echo " "
    echo "    start.sh "
    echo " "
}

python $PROJECT_DIR/src/server.py
