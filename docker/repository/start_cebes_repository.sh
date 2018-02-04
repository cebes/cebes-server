#!/usr/bin/env sh

WITH_MARIADB=${WITH_MARIADB:-"true"}
CEBES_EXPOSED_DIR=${CEBES_EXPOSED_DIR:-"/cebes/repository"}

mkdir -p "${CEBES_EXPOSED_DIR}/mysql" "${CEBES_EXPOSED_DIR}/pipelines" \
    "${CEBES_EXPOSED_DIR}/logs" "${CEBES_EXPOSED_DIR}/repos"

if [ "x${WITH_MARIADB}" = "xtrue" ]; then
    # start MariaDB
    /cebes/configure_mariadb.sh ${CEBES_EXPOSED_DIR}/mysql

    CEBES_MYSQL_SERVER="127.0.0.1:3306"
    MYSQL_OPTIONS="?createDatabaseIfNotExist=true&nullNamePatternMatchesAll=true&useSSL=false"

    export CEBES_MYSQL_URL="jdbc:mysql://${CEBES_MYSQL_SERVER}/cebes_store${MYSQL_OPTIONS}"
    export CEBES_MYSQL_DRIVER="org.mariadb.jdbc.Driver"
    export CEBES_MYSQL_USERNAME="docker_cebes_server"
    export CEBES_MYSQL_PASSWORD="docker_cebes_server_pwd"
fi

export CEBES_REPOSITORY_PATH="${CEBES_EXPOSED_DIR}/pipelines"
export CEBES_REPOSITORY_INTERFACE="0.0.0.0"
export CEBES_REPOSITORY_PORT="22000"

CEBES_REPOSITORY_JAR=`find /cebes -name cebes-pipeline-repository-assembly-*.jar | head -n 1`

java -Dcebes.logs.dir=${CEBES_EXPOSED_DIR}/logs/ -jar ${CEBES_REPOSITORY_JAR}
