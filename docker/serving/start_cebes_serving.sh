#!/usr/bin/env sh

WITH_MARIADB=${WITH_MARIADB:-"true"}
CEBES_SERVING_DIR=${CEBES_SERVING_DIR:-"/cebes/serving"}
CEBES_MYSQL_SERVER="127.0.0.1:3306"
MYSQL_OPTIONS="?createDatabaseIfNotExist=true&nullNamePatternMatchesAll=true&useSSL=false"

mkdir -p "${CEBES_SERVING_DIR}/spark-warehouse" "${CEBES_SERVING_DIR}/logs" "${CEBES_SERVING_DIR}/mysql"

if [ "x${WITH_MARIADB}" = "xtrue" ]; then
    # start MariaDB
    /cebes/configure_mariadb.sh ${CEBES_SERVING_DIR}/mysql

    export CEBES_HIVE_METASTORE_URL="jdbc:mysql://${CEBES_MYSQL_SERVER}/cebes_hive_metastore${MYSQL_OPTIONS}"
    export CEBES_HIVE_METASTORE_DRIVER="org.mariadb.jdbc.Driver"
    export CEBES_HIVE_METASTORE_USERNAME="docker_cebes_hive"
    export CEBES_HIVE_METASTORE_PASSWORD="docker_cebes_hive_pwd"

    export CEBES_MYSQL_URL="jdbc:mysql://${CEBES_MYSQL_SERVER}/cebes_store${MYSQL_OPTIONS}"
    export CEBES_MYSQL_DRIVER="org.mariadb.jdbc.Driver"
    export CEBES_MYSQL_USERNAME="docker_cebes_server"
    export CEBES_MYSQL_PASSWORD="docker_cebes_server_pwd"
fi

export CEBES_SPARK_WAREHOUSE_DIR="${CEBES_SERVING_DIR}/spark-warehouse"
export CEBES_SERVING_CONFIG_FILE="/cebes/config.json"

CEBES_JAR=`find /cebes -name cebes-pipeline-serving-assembly-*.jar | head -n 1`

/spark/bin/spark-submit --class "io.cebes.serving.Main" \
    --master "local[*]" \
    --conf "spark.driver.extraJavaOptions=-Dcebes.logs.dir=${CEBES_SERVING_DIR}/logs/" \
    ${CEBES_JAR}
