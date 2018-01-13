#!/usr/bin/env bash

VERSION_TAG=${1:-"0.10.0-snapshot"}
HTTP_SERVER_TAG="phvu/cebes-private:server-${VERSION_TAG}"
REPOSITORY_TAG="phvu/cebes-private:repo-${VERSION_TAG}"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd ${DIR}/..
sbt clean compile package assembly || exit

cd ${DIR}
mkdir -p binaries
cp ../cebes-http-server/target/scala-2.11/cebes-http-server-assembly-*.jar ./binaries/
cp ../cebes-pipeline-repository/target/scala-2.11/cebes-pipeline-repository-assembly-*.jar ./binaries/

echo "Building and pushing ${HTTP_SERVER_TAG}"
docker build -t ${HTTP_SERVER_TAG} -f http-server/Dockerfile . || exit
docker push ${HTTP_SERVER_TAG}

echo "Building and pushing ${REPOSITORY_TAG}"
docker build -t ${REPOSITORY_TAG} -f repository/Dockerfile . || exit
docker push ${REPOSITORY_TAG}

rm -r binaries