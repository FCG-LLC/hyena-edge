#!/bin/bash

set -ex

BUILD_DOCKER_IMAGE=true
DOCKERFILES_TO_BUILD="Dockerfile"
DOCKER_IMAGE_BUILD_DIR="dockerization"

destEnv="${destEnv:-dev}"

function retry() {
  count=$1
  slp=$2
  cmd=$3

  ( for i in $(seq 0 $count); do
    [ $i -gt 0 ] && echo "---- Retrying $i time ----"; $cmd && break || [ $i -lt $count ] && echo "---- FAILURE, waiting $slp secs ----" && sleep $slp || exit;
  done ) || return 1
}

DEB_PACKAGES=$(ls build/*.deb)

LOCAL_DOCKER_IMAGE="cs/hyena"
PORTUS_DOCKER_IMAGE="cs-hyena"

# copy DEB_PACKAGES
cp -vf $DEB_PACKAGES ${DOCKER_IMAGE_BUILD_DIR}/

cd ${DOCKER_IMAGE_BUILD_DIR}

for dockerfile in ${DOCKERFILES_TO_BUILD}; do
	echo "Building ${DOCKER_IMAGE_BUILD_DIR}/${dockerfile}..."

	sudo docker build --file ${dockerfile} --build-arg destEnv=${destEnv} --no-cache -t cs/${LOCAL_DOCKER_IMAGE} .
	sudo docker tag cs/${LOCAL_DOCKER_IMAGE} portus.cs.int:5000/${destEnv}/${PORTUS_DOCKER_IMAGE}
	retry 5 15 "sudo docker push portus.cs.int:5000/${destEnv}/${PORTUS_DOCKER_IMAGE}"

done
