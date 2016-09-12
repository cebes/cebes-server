#!/usr/bin/env bash

CEBES_PATH="$(cd "$(dirname "$0")/../"; pwd)"
SPARK_FILE_NAME="spark-2.0.0-bin-hadoop2.7"

wget http://d3kbcqa49mib13.cloudfront.net/${SPARK_FILE_NAME}.tgz \
    -O ${CEBES_PATH}/spark/${SPARK_FILE_NAME}.tgz

if [ -d "${CEBES_PATH}/spark/${SPARK_FILE_NAME}" ]; then
    rm -rf "${CEBES_PATH}/spark/${SPARK_FILE_NAME}"
fi

tar -xzvf ${CEBES_PATH}/spark/${SPARK_FILE_NAME}.tgz -C ${CEBES_PATH}/spark
rm ${CEBES_PATH}/spark/${SPARK_FILE_NAME}.tgz

echo "Extracted spark into ${CEBES_PATH}/spark/${SPARK_FILE_NAME}"
echo "You should run: "
echo "  export SPARK_HOME=${CEBES_PATH}/spark/${SPARK_FILE_NAME}"
echo "before running ./bin/start-cebes.sh"