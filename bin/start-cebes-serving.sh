#!/usr/bin/env bash

SPARK_VERSION="${SPARK_VERSION:-2.2.0}"
CEBES_PATH="$(cd "$(dirname "$0")/../"; pwd)"
SPARK_HOME="${SPARK_HOME:-${CEBES_PATH}/spark/spark-${SPARK_VERSION}-bin-hadoop2.7}"

if [ ! -f "${CEBES_PATH}/bin/env.sh" ]
then
    echo "env.sh is not found in ${CEBES_PATH}/bin. Please copy env.sh.example to env.sh and update its content"
    exit 1
fi
source "${CEBES_PATH}/bin/env.sh"

# find Cebes jar files
CEBES_JAR=`find ${CEBES_PATH}/cebes-pipeline-serving/target/scala-2.11 -name cebes-pipeline-serving-assembly-*.jar | head -n 1`

export CEBES_SERVING_CONFIG_FILE="$1"

echo "Using cebes pipeline serving jar file at ${CEBES_JAR}"
${SPARK_HOME}/bin/spark-submit --class "io.cebes.serving.Main" \
    --master "local[4]" \
    --conf 'spark.driver.extraJavaOptions=-Dcebes.logs.dir=/tmp/' \
    ${CEBES_JAR}
