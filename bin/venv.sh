#!/bin/bash

#####
#
# venv.sh - Create virtual environment for the project
#
# Author: Eric Broda, eric.broda@brodagroupsoftware.com, December 1, 2023
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
    echo "    venv.sh "
    echo " "
}

# Select a python version.  Note: Streamlit does not seem
# to run on Mac using Python 3.11, so you can select
# your specific version of python to create your
# virtual environment:
WINDOWS_PYTHON="python.exe"
MAC_LINUX_PYTHON="python3"
MAC_LINUX_PYTHON="/usr/local/bin/python3.10"

get_python_command () {
  if [[ "$OSTYPE" == "msys" ]]; then
    echo $WINDOWS_PYTHON
  else
    # echo "python3"
    echo /usr/local/bin/python3.10
  fi

}

# Create the virtual environment in a directory called
cd $PROJECT_DIR
NAME="venv"

if [[ ! -d "$NAME" ]]; then
  $(get_python_command) -m venv $NAME
fi
