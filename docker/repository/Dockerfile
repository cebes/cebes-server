# Docker image containing cebes-pipeline-repository
FROM openjdk:8-alpine

ARG WITH_MARIADB

ENV CEBES_MYSQL_URL="jdbc:mysql://localhost/cebes_store" \
    CEBES_MYSQL_DRIVER="org.mariadb.jdbc.Driver" \
    CEBES_MYSQL_USERNAME="docker_cebes_server" \
    CEBES_MYSQL_PASSWORD="docker_cebes_server_pwd" \
    CEBES_REPO_DIR="${CEBES_REPO_DIR:-/cebes/repository}" \
    WITH_MARIADB="${WITH_MARIADB:-true}"

# mariadb
# libc6-compat is needed for alpine: https://github.com/grpc/grpc/issues/6126
RUN apk add --update curl bash libc6-compat && \
 if [ "x${WITH_MARIADB}" = "xtrue" ] ; then apk add --update mariadb mariadb-client ; fi && \
 rm -f /var/cache/apk/*

# cebes
ADD binaries/cebes-pipeline-repository-assembly-*.jar \
    repository/start_cebes_repository.sh \
    repository/configure_mariadb.sh \
    /cebes/
RUN chmod +x /cebes/configure_mariadb.sh

WORKDIR /cebes

EXPOSE 22000

VOLUME $CEBES_REPO_DIR

ENTRYPOINT ["/cebes/start_cebes_repository.sh"]
