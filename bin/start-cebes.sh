#!/usr/bin/env bash

SPARK_VERSION="${SPARK_VERSION:-2.1.0}"
CEBES_PATH="$(cd "$(dirname "$0")/../"; pwd)"
SPARK_HOME="${SPARK_HOME:-${CEBES_PATH}/spark/spark-${SPARK_VERSION}-bin-hadoop2.7}"

if [ ! -f "${CEBES_PATH}/bin/env.sh" ]
then
    echo "env.sh is not found in ${CEBES_PATH}/bin. Please copy env.sh.example to env.sh and update its content"
    exit 1
fi
source "${CEBES_PATH}/bin/env.sh"

# find Cebes jar files
CEBES_JARS=`find ${CEBES_PATH}/cebes-http-server/target/scala-2.11 -name cebes-http-server-assembly-*.jar`

if [[ ${#CEBES_JARS[@]} != 1 ]];
then
    echo "More than one cebes jar files found:"
    echo ${CEBES_JARS}
    exit 1
fi
echo "Using cebes http server jar file at ${CEBES_JARS[0]}"

${SPARK_HOME}/bin/spark-submit --class "io.cebes.server.Main" \
    --master "local[4]" \
    --conf 'spark.driver.extraJavaOptions=-Dcebes.logs.dir=/tmp/' \
    ${CEBES_JARS[0]}
