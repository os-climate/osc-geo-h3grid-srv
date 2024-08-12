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
    --publish)
      # acceptable values: false, custom, dockerhub
      shift; #move past arg
      PUBLISH="$1";
      case $PUBLISH in 
        false|dockerhub|custom);;
        *) echo "--publish must be followed by one of [false, dockerhub, custom]"
          exit 0;;
      esac
      shift
      ;;
    --latest)
      LATEST="true"
      shift
      ;;
    --version)
      shift; #move past arg
      VERSION="$1"
      shift
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
    echo "    docker.sh [--publish registry] [--latest] [--version ver]"
    echo " "
    echo "    --publish argument controls whether the docker image will be published to dockerhub."
    echo "        This argument defaults to false. Must be set to one of [false, dockerhub, custom]"
    echo "        The DOCKER_REGISTRY environment variable "
    echo " "
    echo "    The DOCKER_USERNAME environment variable must exist to run this script, and "
    echo "        should be set to the user's docker username. "
    echo " "
    echo "    The DOCKER_TOKEN environment variable must exist if the publish argument is present"
    echo "        and should be set to a docker access token that will allow docker access"
    echo " "
}

if [[ -z "$DOCKER_USERNAME" ]]; then
    echo "environment variable DOCKER_USERNAME is mandatory for this script, but was empty"
    exit 1
fi

if [[ -z "$DOCKER_TOKEN" && "$PUBLISH" != "false" ]]; then
    echo "environment variable DOCKER_TOKEN is mandatory if publishing image, but was empty"
    exit 1
fi

if [[ -z "$DOCKER_REGISTRY" && "$PUBLISH" == "custom" ]]; then
    echo "environment variable DOCKER_REGISTRY is mandatory if publishing an image to a custom registry, but was empty"
    exit 1
fi

VERSION="0.0.1"
WORKING_DIR="$PROJECT_DIR/tmp"
DOCKER_IMAGE_NAME="$PROJECT"
IMAGE_NAME="$DOCKER_USERNAME/$DOCKER_IMAGE_NAME"

if [[ "$PUBLISH" == custom ]]; then
  IMAGE_NAME="$DOCKER_REGISTRY/$IMAGE_NAME"
  echo "set image name to $IMAGE_NAME"
fi

# Show the environment
echo "--- Script Environment ---"
echo "PROJECT_DIR:                $PROJECT_DIR"
echo "WORKING_DIR:                $WORKING_DIR"
echo "DOCKER_USERNAME:            $DOCKER_USERNAME"
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

docker_login() {
  echo "logging in to docker registry with username $DOCKER_USERNAME"
  if [[ "$PUBLISH" == "custom" ]]; then
    echo "logging into custom registry: $DOCKER_REGISTRY with user $DOCKER_USERNAME"
    docker login "$DOCKER_REGISTRY" -u "$DOCKER_USERNAME" -p "$DOCKER_TOKEN"
  else
    docker login -u "$DOCKER_USERNAME" -p "$DOCKER_TOKEN"
  fi

  LOGIN_RETURN=$?

  if [[ $LOGIN_RETURN -eq 0 ]]; then
    echo "Successfully logged into docker registry as $DOCKER_USERNAME"
    return 0
  else
    echo "Could not log into docker registry as $DOCKER_USERNAME"
    return 1
   fi
}

docker_logout() {
  echo "logging out from docker"
  docker logout
  LOGOUT_RETURN=$?
  if [[ $LOGOUT_RETURN -eq 0 ]]; then
    echo "Successfully logged out of docker registry"
  else
    echo "problem occurred logging out of docker registry!"
  fi
}

publish() {
  echo "Pushing image to Docker"
  docker push "$IMAGE_NAME:$VERSION"
  DOCKER_RETURN=$?

  if [[ $DOCKER_RETURN -eq 0 ]]; then
    echo "Image successfully published!"
  else
    echo "Attempt to publish docker image failed!"
  fi

  if [[ "$LATEST" == "true" ]]
    echo "Pushing image to Docker as latest"
    docker push "$IMAGE_NAME:latest"
    DOCKER_RETURN=$?

    if [[ $DOCKER_RETURN -eq 0 ]]; then
      echo "Image successfully published as latest!"
    else
      echo "Attempt to publish docker image as latest failed!"
    fi
}

echo "Changing directory to sandbox directory ($WORKING_DIR)"
mkdir -p "$WORKING_DIR"
cd "$WORKING_DIR"

prepare;
cleanup;
build;

if [[ "$PUBLISH" != "false" ]]; then
  docker_login;
  PUBLISH_LOGIN_SUCCESS=$?
   if [[ $PUBLISH_LOGIN_SUCCESS -eq 0 ]]; then
       publish;
  else
    echo "Not publishing due to login failure"
    docker_logout;
    exit 1
  fi

  docker_logout;
  exit 0
fi