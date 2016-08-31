#!/usr/bin/env bash

SPARK_HOME="${SPARK_HOME:-$HOME/bin/spark-2.0.0-bin-hadoop2.7}"
CEBES_PATH="`dirname $0`/../"

${SPARK_HOME}/bin/spark-submit --class "io.cebes.server.Main" \
    --master local[4] \
    --conf 'spark.driver.extraJavaOptions=-Dcebes.logs.dir=/tmp' \
    ${CEBES_PATH}/cebes-http-server/target/scala-2.11/cebes-http-server-assembly-0.1.0-SNAPSHOT.jar