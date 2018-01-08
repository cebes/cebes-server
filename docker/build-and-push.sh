#!/usr/bin/env bash

VERSION_TAG=${1:-"0.10.0-snapshot"}
HTTP_SERVER_TAG="phvu/cebes-private:server-${VERSION_TAG}"
REPOSITORY_TAG="phvu/cebes-private:repo-${VERSION_TAG}"

sbt compile assembly || exit

echo "Building a pushing ${HTTP_SERVER_TAG}"
docker build -t ${HTTP_SERVER_TAG} -f docker/http-server/Dockerfile . || exit
docker push ${HTTP_SERVER_TAG}

echo "Building a pushing ${REPOSITORY_TAG}"
docker build -t ${REPOSITORY_TAG} -f docker/repository/Dockerfile . || exit
docker push ${REPOSITORY_TAG}

