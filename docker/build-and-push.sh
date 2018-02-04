#!/usr/bin/env bash

set -e

VERSION_TAG=${1:-"0.11.0-snapshot"}
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

function build {
    local img_tag="$1"
    local docker_file="$2"

    for maria_db in true false; do
        local db_tag=""
        if [ "x$maria_db" = "xfalse" ]; then db_tag="-no-mariadb"; fi

        local full_img_tag="${img_tag}${db_tag}"
        echo "Building and pushing ${full_img_tag} from Dockerfile at ${docker_file}"
        docker build -t ${full_img_tag} -f ${docker_file} --build-arg WITH_MARIADB=${maria_db} .
        docker push ${full_img_tag}
    done
}

build ${HTTP_SERVER_TAG} http-server/Dockerfile
build ${SERVING_TAG} serving/Dockerfile
build ${REPOSITORY_TAG} repository/Dockerfile

rm -r binaries