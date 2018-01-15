#!/usr/bin/env sh

WITH_MARIADB=${WITH_MARIADB:-"true"}
CEBES_REPO_DIR=${CEBES_REPO_DIR:-"/cebes/repository"}

mkdir -p "${CEBES_REPO_DIR}/mysql" "${CEBES_REPO_DIR}/pipelines" "${CEBES_REPO_DIR}/logs"

if [ "x${WITH_MARIADB}" = "xtrue" ]; then
    # start MariaDB
    /cebes/configure_mariadb.sh ${CEBES_REPO_DIR}/mysql

    CEBES_MYSQL_SERVER="127.0.0.1:3306"
    MYSQL_OPTIONS="?createDatabaseIfNotExist=true&nullNamePatternMatchesAll=true&useSSL=false"

    export CEBES_MYSQL_URL="jdbc:mysql://${CEBES_MYSQL_SERVER}/cebes_store${MYSQL_OPTIONS}"
    export CEBES_MYSQL_DRIVER="org.mariadb.jdbc.Driver"
    export CEBES_MYSQL_USERNAME="docker_cebes_server"
    export CEBES_MYSQL_PASSWORD="docker_cebes_server_pwd"
fi

export CEBES_REPOSITORY_PATH="${CEBES_REPO_DIR}/pipelines"
export CEBES_REPOSITORY_INTERFACE="0.0.0.0"
export CEBES_REPOSITORY_PORT="22000"

CEBES_REPOSITORY_JAR=`find /cebes -name cebes-pipeline-repository-assembly-*.jar | head -n 1`

java -Dcebes.logs.dir=${CEBES_REPO_DIR}/logs/ -jar ${CEBES_REPOSITORY_JAR}
