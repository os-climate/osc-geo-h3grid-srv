#!/bin/bash

#####
#
# dockerize.sh - Create Service Docker image,
# and optionally publish that image to dockerhub.
#
# Author: Eric Broda, eric.broda@brodagroupsoftware.com, January 15, 2024
#
#####

if [ -z ${ROOT_DIR+x} ] ; then
    echo "Environment variables have not been set.  Run 'source bin/environment.sh'"
    exit 1
fi

POSITIONAL_ARGS=()
PUBLISH="false" # default value

while [[ $# -gt 0 ]]; do
  POSITIONAL_ARGS+=("$1")
  case $1 in
    -p|--publish)
      PUBLISH="true"
      shift #move past arg
      ;;
    *) # ignore other args
      shift
      ;;
  esac
done

function showHelp {
    echo " "
    echo "ERROR: $1"
    echo " "
    echo "Usage:"
    echo " "
    echo "    docker.sh [-p|--publish]"
    echo " "
    echo "    --publish argument controls whether the docker image will be published to dockerhub."
    echo "        This argument defaults to false."
    echo " "
    echo "    The DOCKERHUB_USERNAME environment variable must exist to run this script, and "
    echo "        should be set to the user's dockerhub username. "
    echo " "
    echo "    The DOCKERHUB_TOKEN environment variable must exist if the publish argument is present"
    echo "        and should be set to a dockerhub access token that will allow docker hub access"
    echo " "
}

if [[ -z "$DOCKERHUB_USERNAME" ]]; then
    echo "environment variable DOCKERHUB_USERNAME is mandatory for this script, but was empty"
    exit 1
fi

if [[ -z "$DOCKERHUB_TOKEN" && "$PUBLISH" == "true" ]]; then
    echo "environment variable DOCKERHUB_TOKEN is mandatory if publishing image, but was empty"
    exit 1
fi

VERSION="0.0.1"
WORKING_DIR="$PROJECT_DIR/tmp"
DOCKER_IMAGE_NAME="$PROJECT"
IMAGE_NAME="$DOCKERHUB_USERNAME/$DOCKER_IMAGE_NAME"

# Show the environment
echo "--- Script Environment ---"
echo "PROJECT_DIR:                $PROJECT_DIR"
echo "WORKING_DIR:                $WORKING_DIR"
echo "DOCKERHUB_USERNAME:         $DOCKERHUB_USERNAME"
echo "VERSION (IMAGE):            $VERSION"
echo "IMAGE_NAME:                 $IMAGE_NAME"
echo " "

prepare() {
    echo "Preparing..."
    cp $PROJECT_DIR/requirements.txt $WORKING_DIR
    cp $PROJECT_DIR/docker/Dockerfile $WORKING_DIR
    cp -r $PROJECT_DIR/src $WORKING_DIR
}

cleanup() {
    echo "Cleaning up..."
    if [[ "$(docker images -q "$IMAGE_NAME" 2> /dev/null)" != "" ]]; then
        echo "Removing old $IMAGE_NAME images"
        docker images | grep "$IMAGE_NAME" | awk '{print $3}' | xargs docker rmi -f
        echo "Cleanup completed!"
    fi
}

build() {
    echo "Building Docker image $IMAGE_NAME:$VERSION"
    echo "Images built using version $VERSION and latest"

    docker build \
        -t "$IMAGE_NAME:$VERSION" \
        -t "$IMAGE_NAME:latest" \
        -f Dockerfile . \
        --no-cache=true
}

dockerhub_login() {
  echo "logging in to docker hub with username $DOCKERHUB_USERNAME"
  docker login -u "$DOCKERHUB_USERNAME" -p "$DOCKERHUB_TOKEN"
  LOGIN_RETURN=$?

  if [[ $LOGIN_RETURN -eq 0 ]]; then
    echo "Successfully logged into dockerhub as $DOCKERHUB_USERNAME"
    return 0
  else
    echo "Could not log into dockerhub as $DOCKERHUB_USERNAME"
    return 1
   fi
}

dockerhub_logout() {
  echo "logging out from dockerhub"
  docker logout
  LOGOUT_RETURN=$?
  if [[ $LOGOUT_RETURN -eq 0 ]]; then
    echo "Successfully logged out of dockerhub"
  else
    echo "problem occurred logging out of dockerhub!"
  fi
}

publish() {
  echo "Pushing image to DockerHub"
  docker push "$IMAGE_NAME"
  DOCKER_RETURN=$?

  if [[ $DOCKER_RETURN -eq 0 ]]; then
    echo "Image successfully published!"
  else
    echo "Attempt to publish docker image failed!"
  fi
}

echo "Changing directory to sandbox directory ($WORKING_DIR)"
mkdir -p "$WORKING_DIR"
cd "$WORKING_DIR"

prepare;
cleanup;
build;

if [[ "$PUBLISH" == "true" ]]; then
  dockerhub_login;
  PUBLISH_LOGIN_SUCCESS=$?
   if [[ $PUBLISH_LOGIN_SUCCESS -eq 0 ]]; then
       publish;
  else
    echo "Not publishing due to login failure"
    dockerhub_logout;
    exit 1
  fi

  dockerhub_logout;
  exit 0
fi