#!/usr/bin/env bash

set -e

VERSION_TAG=${1:-"0.10.0"}
HTTP_SERVER_TAG="cebesio/http-server:${VERSION_TAG}"
SERVING_TAG="cebesio/pipeline-serving:${VERSION_TAG}"
REPOSITORY_TAG="cebesio/pipeline-repo:${VERSION_TAG}"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd ${DIR}/..
sbt clean compile package assembly

cd ${DIR}
mkdir -p binaries
cp ../cebes-http-server/target/scala-2.11/cebes-http-server-assembly-*.jar ./binaries/
cp ../cebes-pipeline-serving/target/scala-2.11/cebes-pipeline-serving-assembly-*.jar ./binaries/
cp ../cebes-pipeline-repository/target/scala-2.11/cebes-pipeline-repository-assembly-*.jar ./binaries/

for maria_db in true false; do

    db_tag=""
    if [ "x$maria_db" = "xfalse" ]; then db_tag="-no-mariadb"; fi

    image_tag="${HTTP_SERVER_TAG}${db_tag}"
    echo "Building and pushing ${image_tag}"
    docker build -t ${image_tag} -f http-server/Dockerfile --build-arg WITH_MARIADB=${maria_db} .
    docker push ${image_tag}

    image_tag="${SERVING_TAG}${db_tag}"
    echo "Building and pushing ${image_tag}"
    docker build -t ${image_tag} -f serving/Dockerfile --build-arg WITH_MARIADB=${maria_db} .
    docker push ${image_tag}

    image_tag="${REPOSITORY_TAG}${db_tag}"
    echo "Building and pushing ${image_tag}"
    docker build -t ${image_tag} -f repository/Dockerfile --build-arg WITH_MARIADB=${maria_db} .
    docker push ${image_tag}
done

rm -r binaries