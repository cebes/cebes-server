#!/usr/bin/env bash

VERSION_TAG=${1:-"0.10.0-snapshot"}
HTTP_SERVER_TAG="phvu/cebes-private:server-${VERSION_TAG}"
SERVING_TAG="phvu/cebes-private:serving-${VERSION_TAG}"
REPOSITORY_TAG="phvu/cebes-private:repo-${VERSION_TAG}"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd ${DIR}/..
sbt clean compile package assembly || exit

cd ${DIR}
mkdir -p binaries
cp ../cebes-http-server/target/scala-2.11/cebes-http-server-assembly-*.jar ./binaries/
cp ../cebes-pipeline-serving/target/scala-2.11/cebes-pipeline-serving-assembly-*.jar ./binaries/
cp ../cebes-pipeline-repository/target/scala-2.11/cebes-pipeline-repository-assembly-*.jar ./binaries/

for maria_db in true false; do

    server_tag="${HTTP_SERVER_TAG}"
    if [ "x$maria_db" = "xtrue" ]; then server_tag="${HTTP_SERVER_TAG}-mariadb"; fi
    echo "Building and pushing ${server_tag}"
    docker build -t ${server_tag} -f http-server/Dockerfile --build-arg WITH_MARIADB=${maria_db} . || exit
    docker push ${server_tag}

    serving_tag="${SERVING_TAG}"
    if [ "x$maria_db" = "xtrue" ]; then serving_tag="${SERVING_TAG}-mariadb"; fi
    echo "Building and pushing ${serving_tag}"
    docker build -t ${serving_tag} -f serving/Dockerfile --build-arg WITH_MARIADB=${maria_db} . || exit
    docker push ${serving_tag}

    repo_tag="${REPOSITORY_TAG}"
    if [ "x$maria_db" = "xtrue" ]; then repo_tag="${REPOSITORY_TAG}-mariadb"; fi
    echo "Building and pushing ${repo_tag}"
    docker build -t ${repo_tag} -f repository/Dockerfile --build-arg WITH_MARIADB=${maria_db} . || exit
    docker push ${repo_tag}
done

rm -r binaries