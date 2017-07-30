#!/usr/bin/env bash
set -e

SPARK_VERSION="${SPARK_VERSION:-2.2.0}"
CEBES_PATH="$(cd "$(dirname "$0")/../"; pwd)"
SPARK_FILE_NAME="spark-${SPARK_VERSION}-bin-hadoop2.7"

curl -o ${CEBES_PATH}/spark/${SPARK_FILE_NAME}.tgz \
 http://d3kbcqa49mib13.cloudfront.net/${SPARK_FILE_NAME}.tgz

if [ -d "${CEBES_PATH}/spark/${SPARK_FILE_NAME}" ]; then
    rm -rf "${CEBES_PATH}/spark/${SPARK_FILE_NAME}"
fi

tar -xzvf ${CEBES_PATH}/spark/${SPARK_FILE_NAME}.tgz -C ${CEBES_PATH}/spark
rm ${CEBES_PATH}/spark/${SPARK_FILE_NAME}.tgz

echo "Extracted spark into ${CEBES_PATH}/spark/${SPARK_FILE_NAME}"
echo "You should run: "
echo "  export SPARK_HOME=${CEBES_PATH}/spark/${SPARK_FILE_NAME}"
echo "before running ./bin/start-cebes.sh"