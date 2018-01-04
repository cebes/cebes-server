#!/usr/bin/env sh


CEBES_DATA_DIR=${1:-"/cebes/data"}
CEBES_MYSQL_SERVER="127.0.0.1:3306"
MYSQL_OPTIONS="?createDatabaseIfNotExist=true&nullNamePatternMatchesAll=true&useSSL=false"

mkdir -p "${CEBES_DATA_DIR}/spark-warehouse" "${CEBES_DATA_DIR}/logs" "${CEBES_DATA_DIR}/mysql"

# start MariaDB
/cebes/configure_mariadb.sh ${CEBES_DATA_DIR}/mysql

export CEBES_HIVE_METASTORE_URL="jdbc:mysql://${CEBES_MYSQL_SERVER}/cebes_hive_metastore${MYSQL_OPTIONS}"
export CEBES_HIVE_METASTORE_DRIVER="org.mariadb.jdbc.Driver"
export CEBES_HIVE_METASTORE_USERNAME="docker_cebes_hive"
export CEBES_HIVE_METASTORE_PASSWORD="docker_cebes_hive_pwd"

export CEBES_SPARK_WAREHOUSE_DIR="${CEBES_DATA_DIR}/spark-warehouse"

export CEBES_MYSQL_URL="jdbc:mysql://${CEBES_MYSQL_SERVER}/cebes_store${MYSQL_OPTIONS}"
export CEBES_MYSQL_DRIVER="org.mariadb.jdbc.Driver"
export CEBES_MYSQL_USERNAME="docker_cebes_server"
export CEBES_MYSQL_PASSWORD="docker_cebes_server_pwd"

export CEBES_HTTP_PORT="21000"
export CEBES_HTTP_INTERFACE="0.0.0.0"

CEBES_JAR=`find /cebes -name cebes-http-server-assembly-*.jar | head -n 1`

/spark/bin/spark-submit --class "io.cebes.server.Main" \
    --master "local[*]" \
    --conf "spark.driver.extraJavaOptions=-Dcebes.logs.dir=${CEBES_DATA_DIR}/logs/" \
    ${CEBES_JAR}
