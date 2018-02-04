FROM openjdk:8-alpine

ARG SPARK_VERSION=2.2.0
ARG WITH_MARIADB

ENV CEBES_MYSQL_URL="jdbc:mysql://localhost/cebes_store" \
    CEBES_MYSQL_DRIVER="org.mariadb.jdbc.Driver" \
    CEBES_MYSQL_USERNAME="docker_cebes_server" \
    CEBES_MYSQL_PASSWORD="docker_cebes_server_pwd" \
    CEBES_HIVE_METASTORE_URL="jdbc:mysql://localhost/cebes_hive_metastore" \
    CEBES_HIVE_METASTORE_DRIVER="org.mariadb.jdbc.Driver" \
    CEBES_HIVE_METASTORE_USERNAME="docker_cebes_hive" \
    CEBES_HIVE_METASTORE_PASSWORD="docker_cebes_hive_pwd" \
    CEBES_EXPOSED_DIR="/cebes/serving" \
    WITH_MARIADB="${WITH_MARIADB:-true}"

# mariadb
# libc6-compat is needed for alpine: https://github.com/grpc/grpc/issues/6126
# also include openblas and lapack (for com.github.fommil.netlib in Spark)
RUN apk add --update curl bash libc6-compat && \
 if [ "x${WITH_MARIADB}" = "xtrue" ] ; then apk add --update mariadb mariadb-client ; fi && \
 rm -f /var/cache/apk/*

# Spark
ENV SPARK_FILE_NAME "spark-${SPARK_VERSION}-bin-hadoop2.7"

RUN \
    mkdir /spark && \
    curl -o /spark/${SPARK_FILE_NAME}.tgz http://d3kbcqa49mib13.cloudfront.net/${SPARK_FILE_NAME}.tgz && \
    tar -xzvf /spark/${SPARK_FILE_NAME}.tgz -C /spark && \
    rm /spark/${SPARK_FILE_NAME}.tgz && \
    cp -r /spark/${SPARK_FILE_NAME}/. /spark/ && \
    rm -rf /spark/${SPARK_FILE_NAME} && \
    mkdir /cebes

# cebes
ADD binaries/cebes-pipeline-serving-assembly-*.jar \
    serving/start_cebes_serving.sh \
    serving/configure_mariadb.sh \
    /cebes/
RUN chmod +x /cebes/configure_mariadb.sh

WORKDIR /cebes

EXPOSE 4040
EXPOSE 23000

VOLUME $CEBES_EXPOSED_DIR

ENTRYPOINT ["/cebes/start_cebes_serving.sh"]
